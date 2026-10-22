from __future__ import annotations

import asyncio
from dataclasses import replace

from api.config import get_settings
from api.llm_service import LLMService
from api.models import ChatRequest
from api.query_service import QueryService
from api.rag_indexer import RagIndex
from api.semantic_router import SemanticRouter


def fake_embed(texts):
    vectors = []
    for text in texts:
        lower = text.lower()
        vector = [0.0] * 5
        if any(word in lower for word in ("hola", "hello", "hi", "buenos", "good morning")):
            vector[0] = 1.0
        elif any(word in lower for word in ("einstein", "capital", "pizza", "roma", "chiste", "joke", "python")):
            vector[1] = 1.0
        elif any(word in lower for word in ("precio", "price", "btc", "eth", "métrica", "metric", "volumen", "volume", "vwap", "pipeline", "dataset")):
            vector[2] = 1.0
        elif any(word in lower for word in ("quién eres", "who are you", "sobre ti", "about yourself")):
            vector[3] = 1.0
        elif any(word in lower for word in ("adiós", "bye", "goodbye", "hasta luego")):
            vector[4] = 1.0
        else:
            vector[2] = 0.1
        vectors.append(vector)
    return vectors


def router() -> SemanticRouter:
    return SemanticRouter(
        embed_fn=fake_embed,
        cache_path=None,
        threshold=0.7,
        enabled=True,
    )


def test_greeting_and_identity_are_local() -> None:
    decision = asyncio.run(router().route("Hola, ¿cómo estás?"))
    assert decision.kind == "greeting"
    assert decision.response and "IQ Analyzer" in decision.response
    assert decision.score >= 0.7


def test_english_greeting_gets_english_response() -> None:
    decision = asyncio.run(router().route("Hello, who are you?"))
    assert decision.kind in {"greeting", "identity"}
    assert decision.response and "Hello!" in decision.response


def test_out_of_scope_is_blocked_without_embeddings() -> None:
    decision = asyncio.run(router().route("¿Quién es Albert Einstein?"))
    assert decision.kind == "out_of_scope"
    assert decision.backend == "lexical"
    assert decision.response and "Disculpa" in decision.response


def test_analytics_hint_skips_greeting() -> None:
    decision = asyncio.run(router().route("Hola, ¿cuál es el precio de BTC?"))
    assert decision.kind == "analytics"
    assert decision.backend == "lexical"


def _service(groq_key: str = "test-key") -> LLMService:
    settings = replace(get_settings(), groq_api_key=groq_key, semantic_router_enabled=True)
    query = QueryService()
    rag = RagIndex(settings.rag_index_path, settings.rag_docs_dir, settings.glossary_path)
    return LLMService(query, rag, settings, router=router())


def test_canned_answer_does_not_call_groq() -> None:
    service = _service()
    calls = {"count": 0}

    async def fake_groq(*args, **kwargs):
        calls["count"] += 1
        return {"choices": [{"message": {"content": "no debería"}}]}

    service._run_groq = fake_groq  # type: ignore[method-assign]
    response = asyncio.run(service.answer(ChatRequest(message="Hola")))
    assert calls["count"] == 0
    assert response.source == "semantic_router"
    assert response.intent == "greeting"
    assert "IQ Analyzer" in response.answer


def test_out_of_scope_does_not_call_groq() -> None:
    service = _service()
    calls = {"count": 0}

    async def fake_groq(*args, **kwargs):
        calls["count"] += 1
        return {"choices": [{"message": {"content": "no debería"}}]}

    service._run_groq = fake_groq  # type: ignore[method-assign]
    response = asyncio.run(service.answer(ChatRequest(message="¿Quién es Albert Einstein?")))
    assert calls["count"] == 0
    assert response.source == "semantic_router"
    assert response.intent == "out_of_scope"
    assert "Disculpa" in response.answer


def test_analytics_calls_groq_and_executes_local_tool() -> None:
    service = _service()
    calls = {"count": 0}

    async def fake_post(messages, tools=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "query_metrics",
                                        "arguments": '{"symbol":"BTCUSDT","window":"24h"}',
                                    },
                                }
                            ]
                        }
                    }
                ]
            }
        return {
            "choices": [
                {
                    "message": {
                        "content": "BTCUSDT viene con datos reales: $84,000."
                    }
                }
            ]
        }

    service._post_groq = fake_post  # type: ignore[method-assign]
    response = asyncio.run(service.answer(ChatRequest(message="¿Cómo viene BTC en 24h?")))
    assert calls["count"] == 2
    assert response.tool_calls and response.tool_calls[0].name == "query_metrics"
    assert "BTCUSDT" in response.answer
    assert response.intent == "analytics"


def test_english_metrics_fallback_uses_real_tool_data() -> None:
    service = _service()
    result = service._fallback("Show me ETH metrics for 24h", "ETHUSDT", "15m")
    assert "Current price" in result["answer"]
    assert result["tool_results"] and result["tool_results"][0].name == "query_metrics"
    assert result["tool_results"][0].payload["symbol"] == "ETHUSDT"


def test_english_comparison_runs_two_metrics_tools() -> None:
    service = _service()
    result = service._fallback("Compare BTC and ETH returns", "BTCUSDT", "15m")
    assert "Comparison" in result["answer"]
    names = [item.name for item in result["tool_results"]]
    assert names == ["query_metrics", "query_metrics"]


def test_pipeline_query_uses_local_tool_without_groq() -> None:
    service = _service()
    calls = {"count": 0}

    async def fake_groq(*args, **kwargs):
        calls["count"] += 1
        return {"choices": [{"message": {"content": "no deber?a"}}]}

    service._run_groq = fake_groq  # type: ignore[method-assign]
    response = asyncio.run(service.answer(ChatRequest(message="How is the pipeline status?")))
    assert calls["count"] == 0
    assert response.tool_calls and response.tool_calls[0].name == "query_pipeline_status"
    assert response.source == "serving.pipeline_status"
