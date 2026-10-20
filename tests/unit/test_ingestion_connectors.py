"""Pruebas de los conectores de ingesta (US-07): sin red, sin Kafka."""

import json
from unittest.mock import MagicMock

import pytest
import requests

from ingestion.connectors.base_connector import TokenBucket, backoff_delay
from ingestion.connectors.kafka_producer import MarketEventProducer
from ingestion.connectors.market_rest import (
    BinanceRestConnector, CoinGeckoRestConnector, RateLimitedClient,
)
from ingestion.connectors.market_websocket import BinanceWebSocketConnector

MIN = 60_000


class ListPublisher:
    def __init__(self):
        self.events = []

    def publish(self, event):
        self.events.append(event)
        return True


def kline(open_time, close="100.5", volume="2"):
    return [open_time, "100", "101", "99", close, volume, open_time + MIN - 1]


def response(status=200, payload=None, headers=None):
    r = MagicMock()
    r.status_code = status
    r.headers = headers or {}
    r.json.return_value = payload
    r.raise_for_status.side_effect = (
        requests.HTTPError(f"HTTP {status}") if status >= 400 else None
    )
    return r


def client_for(session, sleeps=None):
    sleeps = sleeps if sleeps is not None else []
    return RateLimitedClient(
        rate_per_sec=1000, session=session, max_retries=3, base_delay=1.0,
        bucket=TokenBucket(1000), sleep=sleeps.append,
    )


# --- Criterio 1: rate limiting y backoff -------------------------------------

def test_token_bucket_espera_cuando_se_agota():
    now = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        now[0] += s

    bucket = TokenBucket(rate=2, capacity=2, clock=lambda: now[0], sleep=sleep)
    bucket.acquire()
    bucket.acquire()
    assert sleeps == []
    bucket.acquire()  # sin tokens: debe esperar 1/rate = 0.5 s
    assert sleeps == [pytest.approx(0.5)]


def test_backoff_crece_exponencialmente_y_se_acota():
    delays = [backoff_delay(a, base=1, cap=10, jitter=False) for a in range(6)]
    assert delays == [1, 2, 4, 8, 10, 10]


def test_429_reintenta_con_backoff_y_luego_tiene_exito():
    session = MagicMock()
    session.get.side_effect = [response(429), response(429), response(200, [])]
    sleeps = []
    assert client_for(session, sleeps).get_json("u", {}) == []
    assert session.get.call_count == 3
    assert len(sleeps) == 2 and sleeps[1] >= sleeps[0] * 0.5 / 1.0  # crece (con jitter)


def test_429_respeta_retry_after():
    session = MagicMock()
    session.get.side_effect = [response(429, headers={"Retry-After": "7"}), response(200, [1])]
    sleeps = []
    client_for(session, sleeps).get_json("u", {})
    assert sleeps == [7.0]


def test_429_persistente_agota_reintentos_y_falla():
    session = MagicMock()
    session.get.return_value = response(429)
    with pytest.raises(requests.HTTPError):
        client_for(session).get_json("u", {})
    assert session.get.call_count == 4  # 1 intento + 3 reintentos


def test_error_4xx_no_reintenta():
    session = MagicMock()
    session.get.return_value = response(400)
    with pytest.raises(requests.HTTPError):
        client_for(session).get_json("u", {})
    assert session.get.call_count == 1


def test_error_de_red_se_reintenta():
    session = MagicMock()
    session.get.side_effect = [requests.ConnectionError("caída"), response(200, [])]
    assert client_for(session).get_json("u", {}) == []


# --- Criterio 3: paginación sin duplicar ni omitir ---------------------------

def test_paginacion_con_cursor_no_duplica_ni_omite():
    start = 1_700_000_000_000 // MIN * MIN
    pages = [
        [kline(start + i * MIN) for i in range(3)],
        # El proveedor repite la última vela de la página anterior: debe descartarse
        [kline(start + 2 * MIN), kline(start + 3 * MIN), kline(start + 4 * MIN)],
        [kline(start + 5 * MIN)],
    ]
    session = MagicMock()
    session.get.side_effect = [response(200, p) for p in pages]
    pub = ListPublisher()
    conn = BinanceRestConnector(
        pub, ["BTCUSDT"], start, start + 10 * MIN, limit=3, client=client_for(session),
    )
    stats = conn.run()
    times = [int(e["trade_id"]) for e in pub.events]
    assert times == [start + i * MIN for i in range(6)]
    assert stats["published"] == 6
    # La 2.ª petición arranca justo después de la última vela de la 1.ª
    assert session.get.call_args_list[1].kwargs["params"]["startTime"] == start + 3 * MIN


def test_cursor_permite_reanudar():
    start = 1_700_000_000_000 // MIN * MIN
    session = MagicMock()
    session.get.side_effect = [response(200, [kline(start), kline(start + MIN)]),
                               response(200, [kline(start + 2 * MIN)])]
    conn = BinanceRestConnector(ListPublisher(), ["BTCUSDT"], start, start + 5 * MIN,
                                client=client_for(session))
    gen = conn.fetch_symbol("BTCUSDT")
    next(gen)
    next(gen)
    assert conn.cursors["BTCUSDT"] == start + MIN
    list(conn.fetch_symbol("BTCUSDT"))  # reanuda tras el cursor
    assert session.get.call_args_list[1].kwargs["params"]["startTime"] == start + 2 * MIN


def test_evento_rest_cumple_contrato_y_id_de_us14():
    start = 1_700_000_000_000 // MIN * MIN
    session = MagicMock()
    session.get.return_value = response(200, [kline(start)])
    pub = ListPublisher()
    BinanceRestConnector(pub, ["ETHUSDT"], start, start + MIN, client=client_for(session)).run()
    from ingestion.contracts.validator import validate_event
    validate_event(pub.events[0])
    assert pub.events[0]["event_id"] == f"binance_ETHUSDT_{start}"


def test_intervalo_invalido_se_rechaza():
    with pytest.raises(ValueError):
        BinanceRestConnector(ListPublisher(), ["BTCUSDT"], 0, 1, interval="3m")


def test_coingecko_normaliza_al_contrato():
    session = MagicMock()
    session.get.return_value = response(
        200, {"bitcoin": {"usd": 65000.5, "last_updated_at": 1_700_000_000}})
    pub = ListPublisher()
    CoinGeckoRestConnector(pub, {"bitcoin": "BTCUSD"}, client=client_for(session)).run()
    from ingestion.contracts.validator import validate_event
    validate_event(pub.events[0])
    assert pub.events[0]["source"] == "coingecko_rest"


# --- Criterio 2: WebSocket ----------------------------------------------------

def trade_msg(trade_id, symbol="BTCUSDT", price="65000.10"):
    return json.dumps({"stream": f"{symbol.lower()}@trade", "data": {
        "e": "trade", "E": 1_700_000_000_000, "s": symbol, "t": trade_id,
        "p": price, "q": "0.01", "T": 1_700_000_000_000}})


class FakeWS:
    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False

    def recv(self):
        if not self.messages:
            raise ConnectionError("cerrado")
        return self.messages.pop(0)

    def close(self):
        self.closed = True


def test_websocket_publica_trades_conformes_al_contrato():
    pub = ListPublisher()
    ws = FakeWS([trade_msg(1), trade_msg(2, "ETHUSDT")])
    conn = BinanceWebSocketConnector(pub, ["btcusdt", "ethusdt"], connect=lambda url: ws,
                                     max_reconnects=0, sleep=lambda s: None)
    with pytest.raises(ConnectionError):
        conn.run()
    from ingestion.contracts.validator import validate_event
    for e in pub.events:
        validate_event(e)
    assert [e["event_id"] for e in pub.events] == ["binance_ws_BTCUSDT_1", "binance_ws_ETHUSDT_2"]
    assert "btcusdt@trade/ethusdt@trade" in conn.url
    assert ws.closed


def test_websocket_reconecta_tras_desconexion_con_backoff():
    pub = ListPublisher()
    sockets = iter([FakeWS([trade_msg(1)]), FakeWS([trade_msg(2)]), FakeWS([trade_msg(3)])])
    sleeps = []
    conn = BinanceWebSocketConnector(pub, ["BTCUSDT"], connect=lambda url: next(sockets),
                                     max_reconnects=2, sleep=sleeps.append)
    with pytest.raises(ConnectionError):
        conn.run()
    assert [e["trade_id"] for e in pub.events] == ["1", "2", "3"]
    assert conn.reconnects == 2 and len(sleeps) == 2


def test_websocket_reintenta_si_la_conexion_inicial_falla():
    pub = ListPublisher()
    attempts = []

    def connect(url):
        attempts.append(url)
        if len(attempts) < 3:
            raise OSError("sin red")
        return FakeWS([trade_msg(9)])

    conn = BinanceWebSocketConnector(pub, ["BTCUSDT"], connect=connect,
                                     max_reconnects=3, sleep=lambda s: None)
    with pytest.raises(ConnectionError):
        conn.run()
    assert len(attempts) == 4 and pub.events[0]["trade_id"] == "9"


def test_websocket_ignora_mensajes_que_no_son_trades():
    conn = BinanceWebSocketConnector(ListPublisher(), ["BTCUSDT"], connect=lambda u: None)
    assert conn.normalize({"result": None, "id": 1}) is None


# --- Productor Kafka ----------------------------------------------------------

def valid_event():
    start = 1_700_000_000_000
    return BinanceRestConnector(ListPublisher(), ["BTCUSDT"], start, start + 1).normalize(
        {"symbol": "BTCUSDT", "kline": kline(start)})


def test_productor_usa_clave_exchange_symbol():
    kafka = MagicMock()
    producer = MarketEventProducer(producer=kafka)
    assert producer.publish(valid_event()) is True
    kafka.send.assert_called_once()
    assert kafka.send.call_args.args == ("market.events",)
    assert kafka.send.call_args.kwargs["key"] == "binance|BTCUSDT"


def test_productor_envia_eventos_invalidos_a_la_dlq():
    kafka = MagicMock()
    producer = MarketEventProducer(producer=kafka)
    event = valid_event()
    event["price"] = -1
    assert producer.publish(event) is False
    assert kafka.send.call_args.args == ("market.dlq",)
    assert "error" in kafka.send.call_args.kwargs["value"]
