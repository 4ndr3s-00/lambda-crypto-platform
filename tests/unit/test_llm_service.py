from __future__ import annotations

import asyncio

from api.config import DISCLAIMER, get_settings
from api.llm_service import LLMService, OllamaUnavailable
from api.models import ChatRequest
from api.query_service import QueryService
from api.rag_indexer import RagIndex


def make_service() -> LLMService:
    settings = get_settings()
    query = QueryService()
    rag = RagIndex(settings.rag_index_path, settings.rag_docs_dir, settings.glossary_path)
    service = LLMService(query, rag, settings)

    async def unavailable(*args, **kwargs):
        raise OllamaUnavailable("test: no ollama")

    service._run_ollama = unavailable  # type: ignore[method-assign]
    return service


def ask(service: LLMService, message: str, symbol: str = "BTCUSDT", interval: str = "15m"):
    return asyncio.run(
        service.answer(ChatRequest(message=message, symbol=symbol, interval=interval))
    )


def test_pipeline_fallback_has_audit_metadata() -> None:
    response = ask(make_service(), "Auditar estado del pipeline, lag de Kafka y DLQ")
    assert response.answer
    assert "Kafka" in response.answer
    assert response.disclaimer == DISCLAIMER
    assert response.cutoff_time
    assert response.source == "serving.pipeline_status"
    assert response.provenance == "SERVING_LAYER"
    assert any(call.name == "query_pipeline_status" for call in response.tool_calls)


def test_metrics_fallback_uses_tool_and_sql() -> None:
    response = ask(make_service(), "Â¿CuÃ¡l es el retorno y volumen de ETH en 24h?", symbol="ETHUSDT")
    assert response.asset == "ETHUSDT"
    assert "Retorno" in response.answer
    assert response.sql and "serving.realtime_metrics" in response.sql
    assert response.provenance == "SPEED_LAYER"
    assert any(call.name == "query_metrics" for call in response.tool_calls)
    assert response.disclaimer in response.answer


def test_catalog_fallback_returns_owner_and_lineage() -> None:
    response = ask(make_service(), "Consultar owner y linaje del dataset OHLCV")
    assert "Data Engineering" in response.answer
    assert "Linaje" in response.answer
    assert response.provenance == "BATCH_LAYER"
    assert response.sql and "governance.dataset_catalog" in response.sql


def test_price_fallback_returns_timeseries_and_cutoff() -> None:
    response = ask(make_service(), "Â¿CuÃ¡l fue el precio mÃ¡ximo de BTC hoy?", symbol="BTCUSDT")
    assert "MÃ¡ximo" in response.answer
    assert response.cutoff_time
    assert response.sql and "serving.ohlcv_reconciled" in response.sql
    assert response.sources
    assert response.rag_used is True

def test_ollama_tool_call_loop_executes_read_only_tool() -> None:
    service = make_service()
    calls = {"count": 0}

    async def fake_post(messages, tools=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "query_metrics",
                                "arguments": {"symbol": "BTCUSDT", "window": "24h"},
                            }
                        }
                    ]
                }
            }
        return {
            "message": {
                "content": '{"answer":"BTCUSDT actualizado con mÃ©tricas de 24h.","sql":null,"provenance":"SPEED_LAYER"}'
            }
        }

    service._post_ollama = fake_post  # type: ignore[method-assign]
    service.__dict__.pop("_run_ollama", None)
    response = ask(service, "Â¿CÃ³mo viene BTC en 24h?")
    assert calls["count"] == 2
    assert response.answer.startswith("BTCUSDT actualizado")
    assert response.tool_calls and response.tool_calls[0].name == "query_metrics"
    assert response.sql and "serving.realtime_metrics" in response.sql
    assert response.disclaimer in response.answer
