from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from api import exchange_client
from api.llm_service import OllamaUnavailable
from api.main import app


def test_market_endpoints_and_frontend(monkeypatch) -> None:
    def fake_klines(symbol: str, interval: str, limit: int):
        candles = [
            {
                "timestamp": f"2026-10-21T15:{index:02d}:00Z",
                "open": 84000.0 + index,
                "high": 84100.0 + index,
                "low": 83900.0 + index,
                "close": 84050.0 + index,
                "volume": 10.0 + index,
            }
            for index in range(min(limit, 24))
        ]
        return {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "candles": candles,
            "source": "Binance Public REST",
            "is_live": True,
            "fetched_at": "2026-10-21T15:30:00Z",
        }

    monkeypatch.setattr(exchange_client, "fetch_klines", fake_klines)

    with TestClient(app) as client:
        index = client.get("/")
        assert index.status_code in (200, 404)
        assert (Path(__file__).parents[2] / "frontend" / "src" / "app" / "page.tsx").exists()
        if index.status_code == 200:
            assert "IQ ANALYZER" in index.text

        assets = client.get("/api/market/assets").json()
        assert {asset["symbol"] for asset in assets["assets"]} >= {"BTCUSDT", "ETHUSDT"}

        ohlcv = client.get("/api/market/ohlcv", params={"symbol": "BTCUSDT", "interval": "15m", "limit": 12})
        assert ohlcv.status_code == 200
        payload = ohlcv.json()
        assert len(payload["candles"]) == 12
        assert payload["candles"][-1]["close"] > 0
        assert payload["is_live"] is True
        assert payload["source"] in {"Binance Public REST", "serving.ohlcv_reconciled (Postgres)"}
        assert payload["data_source"] in {"exchange_live", "postgres_reconciled"}

        metrics = client.get("/api/market/metrics", params={"symbol": "ETHUSDT", "window": "24h"})
        assert metrics.status_code == 200
        assert metrics.json()["vwap"] > 0
        assert metrics.json()["is_live"] is True

        pipeline = client.get("/api/market/pipeline")
        assert pipeline.status_code == 200
        assert pipeline.json()["kafka"]["consumer_lag"] == 0

        rag = client.get("/api/rag/status").json()
        assert rag["ready"] is True
        assert isinstance(rag["documents"], int)


def test_invalid_market_parameters() -> None:
    with TestClient(app) as client:
        assert client.get("/api/market/ohlcv", params={"symbol": "DOGEUSDT"}).status_code == 422
        assert client.get("/api/market/ohlcv", params={"interval": "3h"}).status_code == 422
        assert client.get("/api/market/ohlcv", params={"limit": 0}).status_code == 422


def test_chat_fallback_and_sse() -> None:
    with TestClient(app) as client:
        service = client.app.state.llm_service

        async def unavailable(*args, **kwargs):
            raise OllamaUnavailable("test: no ollama")

        service._run_ollama = unavailable  # type: ignore[method-assign]

        response = client.post(
            "/api/chat",
            json={
                "message": "Consultar owner y linaje del dataset OHLCV",
                "symbol": "BTCUSDT",
                "interval": "15m",
            },
        )
        assert response.status_code == 200
        payload = response.json()
        assert "referencial" in payload["disclaimer"].lower()
        assert payload["sources"]
        assert payload["cutoff_time"]
        assert payload["sql"]
        assert payload["provenance"] in {"BATCH_LAYER", "SPEED_LAYER", "SERVING_LAYER", "RAG"}

        stream = client.get(
            "/api/chat/stream",
            params={"message": "Precio mÃ¡ximo de BTC hoy", "symbol": "BTCUSDT", "interval": "15m"},
        )
        assert stream.status_code == 200
        assert "event: meta" in stream.text
        assert "event: token" in stream.text
        assert "event: done" in stream.text

def test_market_overview_uses_live_exchange(monkeypatch) -> None:
    monkeypatch.setattr(
        exchange_client,
        "fetch_overview",
        lambda symbols: {
            "assets": [
                {
                    "symbol": symbol,
                    "base": symbol.replace("USDT", ""),
                    "quote": "USDT",
                    "exchange": "Binance",
                    "price": 100.0,
                    "change_24h": 1.0,
                    "volume_24h": 5000.0,
                    "sparkline": [99.0, 100.0],
                    "source": "Binance Public REST",
                    "is_live": True,
                    "fetched_at": "2026-10-21T15:30:00Z",
                }
                for symbol in symbols
            ],
            "source": "Binance Public REST",
            "is_live": True,
            "fetched_at": "2026-10-21T15:30:00Z",
            "errors": [],
        },
    )
    with TestClient(app) as client:
        response = client.get("/api/market/overview")
        assert response.status_code == 200
        payload = response.json()
        assert payload["is_live"] is True
        assert len(payload["assets"]) == 4
        assert payload["assets"][0]["source"] == "Binance Public REST"
