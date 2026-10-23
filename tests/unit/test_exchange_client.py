from __future__ import annotations

import time

import pytest

from api import exchange_client
from api.query_service import QueryService


def test_fetch_klines_maps_binance_payload(monkeypatch) -> None:
    raw = [
        [1700000000000, "100.0", "105.0", "99.0", "104.0", "12.5"],
        [1700000900000, "104.0", "106.0", "103.0", "105.5", "8.0"],
    ]
    monkeypatch.setattr(exchange_client, "_request", lambda path, params, ttl=5: raw)
    payload = exchange_client.fetch_klines("BTCUSDT", "15m", 2)
    assert payload["is_live"] is True
    assert payload["source"] == "Binance Public REST"
    assert len(payload["candles"]) == 2
    assert payload["candles"][0]["close"] == 104.0
    assert payload["candles"][0]["timestamp"].endswith("Z")


def test_fetch_ticker_maps_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        exchange_client,
        "_request",
        lambda path, params, ttl=5: {
            "lastPrice": "84494.23",
            "priceChangePercent": "2.357",
            "quoteVolume": "1178445726.99",
            "highPrice": "85000",
            "lowPrice": "82000",
        },
    )
    ticker = exchange_client.fetch_ticker("BTCUSDT")
    assert ticker["price"] == 84494.23
    assert ticker["change_24h"] == 2.357
    assert ticker["is_live"] is True


def test_fetch_overview_combines_ticker_and_sparkline(monkeypatch) -> None:
    monkeypatch.setattr(
        exchange_client,
        "fetch_ticker",
        lambda symbol: {
            "symbol": symbol,
            "price": 100.0,
            "change_24h": 1.5,
            "volume_24h": 10_000.0,
            "source": "Binance Public REST",
            "is_live": True,
            "fetched_at": "2026-10-21T15:00:00Z",
        },
    )
    monkeypatch.setattr(
        exchange_client,
        "fetch_klines",
        lambda symbol, interval, limit: {
            "candles": [{"close": 99.0}, {"close": 100.0}],
            "source": "Binance Public REST",
        },
    )
    payload = exchange_client.fetch_overview(["BTCUSDT"])
    assert payload["is_live"] is True
    assert payload["assets"][0]["price"] == 100.0
    assert payload["assets"][0]["sparkline"] == [99.0, 100.0]


def test_fetch_depth_and_trades_mapping(monkeypatch) -> None:
    monkeypatch.setattr(
        exchange_client,
        "_request",
        lambda path, params, ttl=5: (
            {"bids": [["100", "2"]], "asks": [["101", "1"]]}
            if path.endswith("depth")
            else [
                {
                    "id": 1,
                    "price": "100",
                    "qty": "1500",
                    "time": 1700000000000,
                    "isBuyerMaker": False,
                }
            ]
        ),
    )
    book = exchange_client.fetch_depth("BTCUSDT")
    assert book["obi"] > 0
    assert book["is_live"] is True

    trades = exchange_client.fetch_trades("BTCUSDT")
    assert trades["is_live"] is True
    assert trades["trades"][0]["side"] == "buy"
    assert trades["trades"][0]["whale"] is True


def test_query_service_raises_when_no_real_source_available(monkeypatch) -> None:
    """Sin Postgres y sin Binance, el servicio nunca debe inventar velas."""
    service = QueryService()
    service._pg_available = False
    service._pg_checked_at = time.monotonic()

    def unavailable(*args, **kwargs):
        raise exchange_client.ExchangeUnavailable("test: exchange down")

    monkeypatch.setattr(exchange_client, "fetch_klines", unavailable)
    with pytest.raises(exchange_client.ExchangeUnavailable):
        service.get_market_ohlcv("BTCUSDT", "15m", 5)
