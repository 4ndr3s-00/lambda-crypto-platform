"""Pruebas del cargador de archivos históricos (US-08): sin Kafka ni MinIO reales."""

import json
from unittest.mock import MagicMock

import pytest

from ingestion.connectors.csv_loader import (
    FileLoaderConnector, MinioSink, main, parse_timestamp_ms,
)
from ingestion.connectors.kafka_producer import MarketEventProducer
from ingestion.contracts.validator import validate_event

HEADER = "timestamp,symbol,price,quantity\n"


class ListSink:
    def __init__(self):
        self.events = []
        self.rejected = []

    def publish(self, event, component="test"):
        self.events.append(event)
        return True

    def reject(self, original, reason, component, source=None):
        self.rejected.append((original, reason, component))


def write(tmp_path, name, content):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


# --- Criterio 1: parseo, validación de tipos y contrato -----------------------

def test_csv_se_convierte_al_contrato(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "2026-09-01T10:00:00Z,btcusdt,65000.5,0.25\n")
    sink = ListSink()
    stats = FileLoaderConnector(sink, path, "binance").run()
    event = sink.events[0]
    validate_event(event)
    assert event["symbol"] == "BTCUSDT"
    assert event["source"] == "csv_backfill" and event["exchange"] == "binance"
    assert event["price"] == 65000.5 and event["quantity"] == 0.25
    assert event["event_time"] == "2026-09-01T10:00:00.000Z"
    assert stats["published"] == 1 and stats["unparseable"] == 0


def test_falta_columna_obligatoria_falla_antes_de_leer(tmp_path):
    path = write(tmp_path, "d.csv", "timestamp,symbol,price\n2026-09-01T10:00:00Z,BTCUSDT,1\n")
    with pytest.raises(ValueError, match="quantity"):
        FileLoaderConnector(ListSink(), path, "binance").run()


def test_filas_malas_se_descartan_sin_tumbar_la_carga(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "\n".join([
        "2026-09-01T10:00:00Z,BTCUSDT,100,1",
        "2026-09-01T10:01:00Z,BTCUSDT,abc,1",    # precio no numérico
        "no-es-fecha,BTCUSDT,100,1",             # fecha inválida
        "2026-09-01T10:03:00Z,,100,1",           # símbolo vacío
        "2026-09-01T10:04:00Z,BTCUSDT,100,2",
    ]) + "\n")
    sink = ListSink()
    stats = FileLoaderConnector(sink, path, "binance").run()
    assert stats["raw"] == 5 and stats["published"] == 2 and stats["unparseable"] == 3


def test_precio_negativo_lo_rechaza_el_contrato(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "2026-09-01T10:00:00Z,BTCUSDT,-5,1\n")
    kafka = MagicMock()
    stats = FileLoaderConnector(MarketEventProducer(producer=kafka), path, "binance").run()
    assert stats["rejected"] == 1
    assert kafka.send.call_args.args == ("market.dlq",)


@pytest.mark.parametrize("value,expected", [
    ("2026-09-01T10:00:00Z", 1788256800000),
    ("2026-09-01T10:00:00", 1788256800000),          # sin zona: se asume UTC
    ("2026-09-01T05:00:00-05:00", 1788256800000),
    ("1788256800", 1788256800000),                   # epoch en segundos
    ("1788256800000", 1788256800000),                # epoch en milisegundos
])
def test_formatos_de_timestamp(value, expected):
    assert parse_timestamp_ms(value) == expected


def test_event_id_es_deterministico(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "2026-09-01T10:00:00Z,BTCUSDT,100,1\n")
    ids = []
    for _ in range(2):
        sink = ListSink()
        FileLoaderConnector(sink, path, "binance").run()
        ids.append(sink.events[0]["event_id"])
    assert ids[0] == ids[1]


def test_json_y_jsonl(tmp_path):
    rows = [{"timestamp": "2026-09-01T10:00:00Z", "symbol": "ETHUSDT", "price": 2500, "quantity": 1}]
    as_array = write(tmp_path, "d.json", json.dumps(rows))
    as_lines = write(tmp_path, "d.jsonl", "\n".join(json.dumps(r) for r in rows) + "\n")
    for path in (as_array, as_lines):
        sink = ListSink()
        FileLoaderConnector(sink, path, "binance").run()
        assert sink.events[0]["symbol"] == "ETHUSDT"


def test_formato_no_soportado(tmp_path):
    path = write(tmp_path, "d.xml", "<x/>")
    with pytest.raises(ValueError, match="no soportado"):
        FileLoaderConnector(ListSink(), path, "binance").run()


# --- Criterio 2: destino kafka / minio ---------------------------------------

def test_destino_minio_agrupa_por_fecha_en_historical(tmp_path):
    path = write(tmp_path, "datos.csv", HEADER + "\n".join([
        "2026-09-01T10:00:00Z,BTCUSDT,100,1",
        "2026-09-01T23:59:00Z,BTCUSDT,101,1",
        "2026-09-02T00:01:00Z,BTCUSDT,102,1",
    ]) + "\n")
    s3 = MagicMock()
    sink = MinioSink("binance", "datos", client=s3)
    FileLoaderConnector(sink, path, "binance").run()
    keys = sink.flush()
    assert keys == [
        "historical/source=binance/date=2026-09-01/datos.json",
        "historical/source=binance/date=2026-09-02/datos.json",
    ]
    first = s3.put_object.call_args_list[0].kwargs
    assert first["Bucket"] == "market-lake"
    assert len(first["Body"].decode().splitlines()) == 2


def test_minio_envia_eventos_invalidos_a_cuarentena(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "2026-09-01T10:00:00Z,BTCUSDT,-1,1\n")
    s3 = MagicMock()
    sink = MinioSink("binance", "d", client=s3)
    FileLoaderConnector(sink, path, "binance").run()
    keys = sink.flush()
    # Nada en el lago histórico; el rechazo queda en cuarentena (US-09)
    assert all(k.startswith("market-quarantine/") for k in keys) and len(keys) == 1
    assert s3.put_object.call_args.kwargs["Bucket"] == "market-quarantine"


def test_destino_kafka_usa_market_events_con_clave(tmp_path):
    path = write(tmp_path, "d.csv", HEADER + "2026-09-01T10:00:00Z,BTCUSDT,100,1\n")
    kafka = MagicMock()
    FileLoaderConnector(MarketEventProducer(producer=kafka), path, "binance").run()
    assert kafka.send.call_args.args == ("market.events",)
    assert kafka.send.call_args.kwargs["key"] == "binance|BTCUSDT"


# --- CLI ------------------------------------------------------------------------

def test_cli_rechaza_destino_invalido(tmp_path, capsys):
    path = write(tmp_path, "d.csv", HEADER)
    with pytest.raises(SystemExit):
        main(["--file", str(path), "--source", "binance", "--target", "s3"])


def test_cli_rechaza_archivo_inexistente():
    with pytest.raises(SystemExit):
        main(["--file", "/no/existe.csv", "--source", "binance", "--target", "minio"])
