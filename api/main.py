"""
FastAPI Serving Layer API para la Plataforma Lambda.

Integra:
- US-22: Serving REST API (health, assets, OHLCV, metrics, pipeline, guardrails).
- US-28/29/30: Qwen2.5 local, RAG, tools de solo lectura y chat SSE.
- US-23/24: endpoints de mercado y telemetrÃ­a para el dashboard.

Los endpoints originales de US-22 conservan sus contratos; el dashboard y el
chat consumen los adaptadores `/api/market/*` y `/api/chat*`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

try:  # package imports (pytest -m api.main)
    from .config import get_settings
    from .llm_service import LLMService
    from .models import (
        AssetsResponse as MarketAssetsResponse,
        ChatRequest,
        ChatResponse,
        MetricsResponse as MarketMetricsResponse,
        MarketOverviewResponse,
        OHLCVResponse as MarketOHLCVResponse,
        OrderBookResponse as MarketOrderBookResponse,
        PipelineStatusResponse as MarketPipelineStatusResponse,
        TradesResponse as MarketTradesResponse,
    )
    from .query_service import QueryService
    from .rag_indexer import RagIndex, build_default_index
    from .schemas import (
        AssetsResponse,
        HealthResponse,
        IntervalEnum,
        MetricsResponse,
        OHLCVResponse,
        PipelineStatusResponse,
    )
    from .sql_guardrails import sanitize_symbol
except ImportError:  # Docker / uvicorn main:app
    from config import get_settings
    from llm_service import LLMService
    from models import (
        AssetsResponse as MarketAssetsResponse,
        ChatRequest,
        ChatResponse,
        MetricsResponse as MarketMetricsResponse,
        MarketOverviewResponse,
        OHLCVResponse as MarketOHLCVResponse,
        OrderBookResponse as MarketOrderBookResponse,
        PipelineStatusResponse as MarketPipelineStatusResponse,
        TradesResponse as MarketTradesResponse,
    )
    from query_service import QueryService
    from rag_indexer import RagIndex, build_default_index
    from schemas import (
        AssetsResponse,
        HealthResponse,
        IntervalEnum,
        MetricsResponse,
        OHLCVResponse,
        PipelineStatusResponse,
    )
    from sql_guardrails import sanitize_symbol

try:
    from .exchange_client import ExchangeUnavailable
except ImportError:  # Docker / uvicorn main:app
    from exchange_client import ExchangeUnavailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ServingAPI")

app = FastAPI(
    title="Plataforma Lambda - Serving REST API",
    version="6.0.0",
    description="""
API REST de la Serving Layer para la Plataforma de Datos Financieros y Criptomonedas.
Incluye mercado OHLCV, mÃ©tricas, telemetrÃ­a, dashboard, Qwen2.5 local, RAG y guardrails SQL.
    """,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Servicios
# ---------------------------------------------------------------------------

query_service = QueryService()
_settings = get_settings()
rag_index = RagIndex(_settings.rag_index_path, _settings.rag_docs_dir, _settings.glossary_path)
llm_service = LLMService(query_service, rag_index, _settings)
app.state.query_service = query_service
app.state.rag_index = rag_index
app.state.llm_service = llm_service


# ---------------------------------------------------------------------------
# Manejo de errores (US-22)
# ---------------------------------------------------------------------------


@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    logger.warning("ValidaciÃ³n fallida en %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "Bad Request",
            "detail": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    logger.warning("ParÃ¡metro invÃ¡lido en %s: %s", request.url.path, exc.errors())
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Unprocessable Entity",
            "detail": exc.errors(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Error no controlado en %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal Server Error",
            "detail": "OcurriÃ³ un error interno en el servicio de consulta.",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )


# ---------------------------------------------------------------------------
# US-22: endpoints originales (contratos intactos)
# ---------------------------------------------------------------------------


@app.get("/api/health", response_model=HealthResponse, tags=["Salud y DiagnÃ³stico"])
def get_health() -> dict[str, Any]:
    dependencies = query_service.check_dependencies()
    overall_status = "healthy"
    if any(state == "disconnected" for state in dependencies.values()):
        overall_status = "degraded"
    return {
        "status": overall_status,
        "service": "serving-api",
        "architecture": "Lambda (Batch + Speed + Serving)",
        "dependencies": dependencies,
        "timestamp_utc": datetime.now(timezone.utc),
    }


@app.get("/api/assets", response_model=AssetsResponse, tags=["CatÃ¡logo de Mercado"])
def get_assets(
    exchange: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    offset = (page - 1) * page_size
    return query_service.get_assets(exchange=exchange, status=status_filter, limit=page_size, offset=offset)


@app.get("/api/ohlcv", response_model=OHLCVResponse, tags=["Series Financieras"])
def get_ohlcv(
    symbol: str = Query(...),
    interval: IntervalEnum = Query(IntervalEnum.ONE_HOUR),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
) -> dict[str, Any]:
    clean_symbol = sanitize_symbol(symbol)
    return query_service.get_ohlcv(
        symbol=clean_symbol,
        interval=interval.value,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


@app.get("/api/metrics", response_model=MetricsResponse, tags=["MÃ©tricas en Tiempo Real"])
def get_metrics(symbol: Optional[str] = Query(None)) -> dict[str, Any]:
    clean_symbol = sanitize_symbol(symbol) if symbol else None
    return query_service.get_metrics(symbol=clean_symbol)


try:
    from .auth import verify_admin_auth
except ImportError:
    from auth import verify_admin_auth
from fastapi import Depends


@app.get(
    "/api/pipeline/status",
    response_model=PipelineStatusResponse,
    tags=["Observabilidad y Operaciones"],
    dependencies=[Depends(verify_admin_auth)],
)
def get_pipeline_status() -> dict[str, Any]:
    return query_service.get_pipeline_status()


# ---------------------------------------------------------------------------
# US-23 / US-24: adaptadores para el dashboard
# ---------------------------------------------------------------------------


@app.get("/api/market/assets", response_model=MarketAssetsResponse, tags=["Dashboard"])
def get_market_assets() -> MarketAssetsResponse:
    assets = query_service.get_market_assets()
    return MarketAssetsResponse(
        assets=assets,
        cutoff_time=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


@app.get("/api/market/ohlcv", response_model=MarketOHLCVResponse, tags=["Dashboard"])
def get_market_ohlcv(
    symbol: str = Query(default="BTCUSDT"),
    interval: str = Query(default="15m"),
    limit: int = Query(default=96, ge=1, le=500),
) -> MarketOHLCVResponse:
    try:
        payload = query_service.get_market_ohlcv(symbol, interval, limit)
        return MarketOHLCVResponse.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/market/metrics", response_model=MarketMetricsResponse, tags=["Dashboard"])
def get_market_metrics(
    symbol: str = Query(default="BTCUSDT"),
    window: str = Query(default="24h", pattern="^(1h|4h|24h|7d)$"),
) -> MarketMetricsResponse:
    try:
        payload = query_service.get_market_metrics(symbol, window=window)
        return MarketMetricsResponse.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/market/overview", response_model=MarketOverviewResponse, tags=["Dashboard"])
def get_market_overview() -> MarketOverviewResponse:
    payload = query_service.get_market_overview()
    return MarketOverviewResponse.model_validate(payload)


@app.get("/api/market/orderbook", response_model=MarketOrderBookResponse, tags=["Dashboard"])
def get_market_orderbook(
    symbol: str = Query(default="BTCUSDT"),
    limit: int = Query(default=20, ge=5, le=100),
) -> MarketOrderBookResponse:
    try:
        payload = query_service.get_market_orderbook(symbol, limit)
        return MarketOrderBookResponse.model_validate(payload)
    except ExchangeUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"exchange no disponible: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/market/trades", response_model=MarketTradesResponse, tags=["Dashboard"])
def get_market_trades(
    symbol: str = Query(default="BTCUSDT"),
    limit: int = Query(default=50, ge=1, le=500),
) -> MarketTradesResponse:
    try:
        payload = query_service.get_market_trades(symbol, limit)
        return MarketTradesResponse.model_validate(payload)
    except ExchangeUnavailable as exc:
        raise HTTPException(status_code=503, detail=f"exchange no disponible: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/market/pipeline", response_model=MarketPipelineStatusResponse, tags=["Dashboard"])
def get_market_pipeline() -> MarketPipelineStatusResponse:
    payload = query_service.get_market_pipeline_status()
    return MarketPipelineStatusResponse.model_validate(payload)


# ---------------------------------------------------------------------------
# US-28 / US-29 / US-30: catÃ¡logo, RAG y chat
# ---------------------------------------------------------------------------


@app.get("/api/catalog/{dataset}", tags=["Gobierno"])
def get_catalog(dataset: str = "market_ohlcv") -> dict[str, Any]:
    return query_service.get_catalog_metadata(dataset)


@app.get("/api/rag/status", tags=["IA"])
def get_rag_status() -> dict[str, Any]:
    return rag_index.status()


@app.get("/api/ai/status", tags=["IA"])
async def ai_status() -> dict[str, Any]:
    return await llm_service.health()


@app.post("/api/rag/reindex", tags=["IA"])
def reindex_rag(force: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return build_default_index(force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"no se pudo reindexar: {exc}") from exc


@app.post("/api/chat", response_model=ChatResponse, tags=["IA"])
async def chat(body: ChatRequest) -> ChatResponse:
    try:
        return await llm_service.answer(body)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Chat failure")
        raise HTTPException(status_code=500, detail=f"fallo del asistente: {exc}") from exc


def _chunks(text: str, size: int = 28) -> list[str]:
    return [text[index : index + size] for index in range(0, len(text), size)]


def _sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


@app.get("/api/chat/stream", tags=["IA"])
async def chat_stream(
    message: str = Query(min_length=1, max_length=4000),
    symbol: str = Query(default="BTCUSDT"),
    interval: str = Query(default="15m"),
) -> StreamingResponse:
    response = await llm_service.answer(
        ChatRequest(message=message, symbol=symbol, interval=interval)
    )

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
# Frontend estÃ¡tico
# ---------------------------------------------------------------------------

def _frontend_dir() -> Optional[str]:
    candidates = [
        os.getenv("FRONTEND_DIR"),
        os.getenv("STATIC_DIR"),
        os.path.join(os.path.dirname(__file__), "..", "frontend"),
        os.path.join(os.path.dirname(__file__), "..", "frontend", "out"),
        os.path.join(os.path.dirname(__file__), "..", "static"),
        "/app/static",
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(os.path.join(candidate, "index.html")):
            return candidate
    return None


_frontend = _frontend_dir()
if _frontend:
    app.mount("/static", StaticFiles(directory=_frontend), name="static")
    # Sirve index.html, styles.css y app.js en la raÃ­z.
    app.mount("/", StaticFiles(directory=_frontend, html=True), name="frontend")
else:
    logger.warning("No se encontrÃ³ frontend/index.html; el dashboard no se servirÃ¡")
