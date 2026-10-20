"""Pruebas de la Dead-Letter Queue (US-09): estructura del mensaje, enrutamiento e inspección."""

import json
from unittest.mock import MagicMock

import pytest

from ingestion.connectors.csv_loader import FileLoaderConnector, MinioSink
from ingestion.connectors.kafka_producer import MarketEventProducer
from ingestion.connectors.market_rest import BinanceRestConnector
from ingestion.connectors.market_websocket import BinanceWebSocketConnector
from ingestion.contracts.dlq_message import build_dlq_message, dlq_key
from ingestion import dlq_inspect

REQUIRED = {"original_event", "error_reason", "failed_component", "timestamp_utc", "source"}


def sent_to_dlq(kafka):
    return [c.kwargs["value"] for c in kafka.send.call_args_list if c.args[0] == "market.dlq"]


class FakeWS:
    def __init__(self, messages):
        self.messages = list(messages)

    def recv(self):
        if not self.messages:
            raise ConnectionError("cerrado")
        return self.messages.pop(0)

    def close(self):
        pass


# --- Criterio 2: estructura del mensaje ---------------------------------------

def test_mensaje_tiene_payload_original_y_metadatos():
    event = {"event_id": "x", "source": "binance_ws", "exchange": "binance", "symbol": "BTCUSDT"}
    message = build_dlq_message(event, "precio negativo", "market_websocket")
    assert set(message) == REQUIRED
    assert message["original_event"] == event          # intacto
    assert message["source"] == "binance_ws"            # se toma del evento
    assert message["timestamp_utc"].endswith("Z")


def test_payload_crudo_se_conserva_como_texto():
    assert build_dlq_message(b"{roto", "json", "c")["original_event"] == "{roto"
    assert build_dlq_message("{roto", "json", "c", source="binance_ws")["source"] == "binance_ws"
    assert build_dlq_message("{roto", "json", "c")["source"] == "unknown"


def test_clave_de_la_dlq():
    assert dlq_key({"exchange": "binance", "symbol": "BTCUSDT"}) == "binance|BTCUSDT"
    assert dlq_key("texto crudo") == "unknown"


# --- Criterio 1: nada se descarta en silencio ---------------------------------

def test_websocket_json_corrupto_va_a_la_dlq_sin_reconectar():
    kafka = MagicMock()
    good = json.dumps({"data": {"e": "trade", "s": "BTCUSDT", "t": 1, "p": "10", "q": "1", "T": 1_700_000_000_000}})
    ws = FakeWS(["{esto no es json", good])
    conn = BinanceWebSocketConnector(MarketEventProducer(producer=kafka), ["BTCUSDT"],
                                     connect=lambda url: ws, max_reconnects=0, sleep=lambda s: None)
    with pytest.raises(ConnectionError):
        conn.run()
    (rejected,) = sent_to_dlq(kafka)
    assert rejected["original_event"] == "{esto no es json"
    assert rejected["failed_component"] == "market_websocket"
    assert rejected["source"] == "binance_ws"
    assert conn.reconnects == 0                       # el mensaje malo no tumba la conexión
    assert conn.stats["published"] == 1 and conn.stats["rejected"] == 1


def test_websocket_mensaje_que_no_es_objeto_va_a_la_dlq():
    kafka = MagicMock()
    conn = BinanceWebSocketConnector(MarketEventProducer(producer=kafka), ["BTCUSDT"],
                                     connect=lambda url: FakeWS(["[1, 2]"]), max_reconnects=0,
                                     sleep=lambda s: None)
    with pytest.raises(ConnectionError):
        conn.run()
    assert len(sent_to_dlq(kafka)) == 1


def test_rest_vela_malformada_va_a_la_dlq():
    kafka = MagicMock()
    conn = BinanceRestConnector(MarketEventProducer(producer=kafka), ["BTCUSDT"], 0, 1)
    conn.fetch = lambda: iter([{"symbol": "BTCUSDT", "kline": [1, "100"]}])   # vela truncada
    conn.run()
    (rejected,) = sent_to_dlq(kafka)
    assert rejected["failed_component"] == "market_rest"
    assert rejected["original_event"]["kline"] == [1, "100"]


def test_csv_fila_ilegible_y_fila_fuera_de_contrato_van_a_la_dlq(tmp_path):
    path = tmp_path / "d.csv"
    path.write_text("timestamp,symbol,price,quantity\n"
                    "2026-09-01T10:00:00Z,BTCUSDT,abc,1\n"
                    "2026-09-01T10:01:00Z,BTCUSDT,-5,1\n"
                    "2026-09-01T10:02:00Z,BTCUSDT,100,1\n")
    kafka = MagicMock()
    stats = FileLoaderConnector(MarketEventProducer(producer=kafka), path, "binance").run()
    ilegible, contrato = sent_to_dlq(kafka)
    assert "fila 2" in ilegible["error_reason"] and ilegible["original_event"]["price"] == "abc"
    assert contrato["original_event"]["price"] == -5.0
    assert {ilegible["failed_component"], contrato["failed_component"]} == {"csv_loader"}
    assert stats["published"] == 1 and stats["unparseable"] == 1 and stats["rejected"] == 1


def test_csv_a_minio_guarda_los_rechazos_en_cuarentena(tmp_path):
    path = tmp_path / "datos.csv"
    path.write_text("timestamp,symbol,price,quantity\n2026-09-01T10:00:00Z,BTCUSDT,abc,1\n")
    s3 = MagicMock()
    sink = MinioSink("binance", "datos", client=s3)
    FileLoaderConnector(sink, path, "binance").run()
    (key,) = sink.flush()
    call = s3.put_object.call_args.kwargs
    assert call["Bucket"] == "market-quarantine" and key.startswith("market-quarantine/source=binance/")
    assert set(json.loads(call["Body"].decode())) == REQUIRED


# --- Utilidad de inspección ---------------------------------------------------

def test_parse_message_tolera_basura():
    assert dlq_inspect.parse_message(b"\xff\xfe no json")["original_event"]
    assert dlq_inspect.parse_message(b"[1]") == {"original_event": [1]}


def test_summarize_cuenta_por_componente_fuente_y_motivo():
    messages = [
        build_dlq_message("x", "json corrupto", "market_websocket", "binance_ws"),
        build_dlq_message("y", "json corrupto", "market_websocket", "binance_ws"),
        build_dlq_message({"a": 1}, "precio", "csv_loader", "csv_backfill"),
        {"error": "CORRUPT_JSON_SYNTAX"},                 # formato antiguo de Spark
    ]
    summary = dlq_inspect.summarize(messages, last=2)
    assert summary["total"] == 4
    assert summary["by_component"]["market_websocket"] == 2
    assert summary["by_component"][dlq_inspect.LEGACY] == 1
    assert summary["by_reason"]["CORRUPT_JSON_SYNTAX"] == 1
    assert len(summary["recent"]) == 2
    assert "4 mensajes" in dlq_inspect.render(summary, 4, "market.dlq")


def test_summarize_vacio():
    assert dlq_inspect.summarize([])["total"] == 0
