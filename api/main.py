"""FastAPI serving layer for the Lambda crypto platform.

Endpoints:
    GET  /api/health
    GET  /api/assets
    GET  /api/ohlcv
    GET  /api/metrics
    GET  /api/pipeline/status
    POST /api/chat
    GET  /api/chat/stream (SSE)
    GET  /api/rag/status

The frontend is mounted at `/` from frontend/ (dev) or /app/static (Docker).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .llm_service import LLMService
from .market_data import MarketDataError
from .models import (
    AssetsResponse,
    ChatRequest,
    ChatResponse,
    HealthResponse,
    HealthServices,
    MetricsResponse,
    OHLCVResponse,
    PipelineStatusResponse,
)
from .query_service import QueryService, QueryServiceError
from .rag_indexer import RagIndex, build_default_index

LOGGER = logging.getLogger("iq_analyzer.api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    query_service = QueryService(settings)
    rag_index = RagIndex(settings.rag_index_path, settings.rag_docs_dir, settings.glossary_path)
    try:
        if not rag_index.index_path.exists():
            build_default_index()
        rag_index.ensure_index()
    except Exception as exc:  # noqa: BLE001 - RAG is optional for API boot
        LOGGER.warning("No se pudo inicializar el índice RAG: %s", exc)

    app.state.settings = settings
    app.state.query_service = query_service
    app.state.rag_index = rag_index
    app.state.llm_service = LLMService(query_service, rag_index, settings)
    LOGGER.info("Serving API lista · model=%s · db=%s", settings.ollama_model, query_service.database_available())
    yield


app = FastAPI(
    title="IQ Analyzer Serving API",
    version="6.0.0",
    description="Serving layer Lambda + Qwen2.5 + guardrails SQL + dashboard en vivo",
    lifespan=lifespan,
)

_settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(_settings.cors_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def _query(request: Request) -> QueryService:
    return request.app.state.query_service


def _rag(request: Request) -> RagIndex:
    return request.app.state.rag_index


def _llm(request: Request) -> LLMService:
    return request.app.state.llm_service


def _market_error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=422, detail=str(exc))


@app.get("/api/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    started = time.perf_counter()
    query_service = _query(request)
    llm = _llm(request)
    rag = _rag(request)

    llm_health: dict[str, Any]
    try:
        llm_health = await llm.health()
    except Exception as exc:  # noqa: BLE001
        llm_health = {"status": "DOWN", "error": str(exc)}

    rag_status = rag.status()
    db_status = "UP" if query_service.database_available() else "FALLBACK"

    services = HealthServices(
        serving_api="UP",
        postgres=db_status,
        minio="FALLBACK",
        ollama=str(llm_health.get("status", "DOWN")),
        rag="READY" if rag_status.get("ready") else "MISSING",
    )
    status = "ok" if services.serving_api == "UP" and services.rag == "READY" else "degraded"
    return HealthResponse(
        status=status,
        application="IQ ANALYZER",
        version=request.app.state.settings.version,
        environment=request.app.state.settings.environment,
        services=services,
        serving_latency_ms=round((time.perf_counter() - started) * 1000, 2),
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


@app.get("/api/assets", response_model=AssetsResponse)
async def assets(request: Request) -> AssetsResponse:
    rows = _query(request).get_assets()
    return AssetsResponse(
        assets=rows,
        cutoff_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )


@app.get("/api/ohlcv", response_model=OHLCVResponse)
async def ohlcv(
    request: Request,
    symbol: str = Query(default="BTCUSDT", description="BTCUSDT, ETHUSDT o SOLUSDT"),
    interval: str = Query(default="15m", description="1m, 5m, 15m, 1h, 4h, 1d, 7d"),
    limit: int = Query(default=96, ge=1, le=500),
) -> OHLCVResponse:
    try:
        payload = _query(request).get_ohlcv(symbol, interval, limit)
        return OHLCVResponse.model_validate(payload)
    except MarketDataError as exc:
        raise _market_error(exc) from exc


@app.get("/api/metrics", response_model=MetricsResponse)
async def metrics(
    request: Request,
    symbol: str = Query(default="BTCUSDT"),
    window: str = Query(default="24h", pattern="^(1h|4h|24h|7d)$"),
) -> MetricsResponse:
    try:
        payload = _query(request).get_metrics(symbol, window=window)
        return MetricsResponse.model_validate(payload)
    except MarketDataError as exc:
        raise _market_error(exc) from exc


@app.get("/api/pipeline/status", response_model=PipelineStatusResponse)
async def pipeline_status(request: Request) -> PipelineStatusResponse:
    payload = _query(request).get_pipeline_status()
    return PipelineStatusResponse.model_validate(payload)


@app.get("/api/catalog/{dataset}")
async def catalog(request: Request, dataset: str = "market_ohlcv") -> dict[str, Any]:
    return _query(request).get_catalog_metadata(dataset)


@app.get("/api/rag/status")
async def rag_status(request: Request) -> dict[str, Any]:
    return _rag(request).status()


@app.post("/api/rag/reindex")
async def rag_reindex(request: Request, force: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return build_default_index(force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"no se pudo reindexar: {exc}") from exc


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: Request, body: ChatRequest) -> ChatResponse:
    try:
        return await _llm(request).answer(body)
    except Exception as exc:  # noqa: BLE001 - always return a useful error envelope
        LOGGER.exception("Chat failure")
        raise HTTPException(status_code=500, detail=f"fallo del asistente: {exc}") from exc


def _chunks(text: str, size: int = 28) -> list[str]:
    return [text[index : index + size] for index in range(0, len(text), size)]


def _sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


@app.get("/api/chat/stream")
async def chat_stream(
    request: Request,
    message: str = Query(min_length=1, max_length=4000),
    symbol: str = Query(default="BTCUSDT"),
    interval: str = Query(default="15m"),
) -> StreamingResponse:
    body = ChatRequest(message=message, symbol=symbol, interval=interval)
    response = await _llm(request).answer(body)

    async def event_stream() -> AsyncIterator[str]:
        yield _sse(
            "meta",
            {
                "asset": response.asset,
                "source": response.source,
                "provenance": response.provenance,
                "cutoff_time": response.cutoff_time,
                "disclaimer": response.disclaimer,
                "sources": [source.model_dump() for source in response.sources],
                "sql": response.sql,
                "tool_calls": [tool.model_dump() for tool in response.tool_calls],
                "latency_ms": response.latency_ms,
            },
        )
        for chunk in _chunks(response.answer):
            yield _sse("token", {"text": chunk})
            await asyncio.sleep(0.012)
        yield _sse("done", {"answer": response.answer})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------


def _frontend_dir() -> Path | None:
    settings = get_settings()
    for candidate in (settings.frontend_dir, settings.static_dir):
        if (candidate / "index.html").exists():
            return candidate
    return None


_frontend = _frontend_dir()
if _frontend is not None:
    app.mount("/", StaticFiles(directory=str(_frontend), html=True), name="frontend")
else:
    LOGGER.warning("No se encontró frontend/index.html; el dashboard no se servirá")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
