from __future__ import annotations

from fastapi.testclient import TestClient

from api.llm_service import OllamaUnavailable
from api.main import app


def test_market_endpoints_and_frontend() -> None:
    with TestClient(app) as client:
        index = client.get("/")
        assert index.status_code == 200
        assert "IQ ANALYZER" in index.text
        assert "_next/static" in index.text

        assets = client.get("/api/market/assets").json()
        assert {asset["symbol"] for asset in assets["assets"]} >= {"BTCUSDT", "ETHUSDT"}

        ohlcv = client.get("/api/market/ohlcv", params={"symbol": "BTCUSDT", "interval": "15m", "limit": 12})
        assert ohlcv.status_code == 200
        payload = ohlcv.json()
        assert len(payload["candles"]) == 12
        assert payload["candles"][-1]["close"] > 0

        metrics = client.get("/api/market/metrics", params={"symbol": "ETHUSDT", "window": "24h"})
        assert metrics.status_code == 200
        assert metrics.json()["vwap"] > 0

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
