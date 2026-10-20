"""
FastAPI Serving Layer API para la Plataforma Lambda (US-22).
Responsable: Andrés Teherán (DevOps & Backend Engineer)

Provee una pasarela segura entre el almacenamiento de datos (PostgreSQL / MinIO)
y los consumidores (Dashboard Frontend y Asistente LLM), impidiendo accesos directos
no autorizados al clúster de datos interno.
"""

from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, Query, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
import os
import logging

from schemas import (
    IntervalEnum,
    HealthResponse,
    AssetsResponse,
    OHLCVResponse,
    MetricsResponse,
    PipelineStatusResponse
)
from query_service import QueryService
from sql_guardrails import sanitize_symbol

# Logging estructurado
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ServingAPI")

app = FastAPI(
    title="Plataforma Lambda - Serving REST API",
    version="2.0.0",
    description="""
API REST de la Serving Layer para la Plataforma de Datos Financieros y Criptomonedas (Arquitectura Lambda v2.0).
Provee endpoints de alta concurrencia y baja latencia para cotizaciones OHLCV reconciliadas,
métricas operativas en tiempo real, catálogo de activos y observabilidad de pipelines.
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# 1. Configuración de Middlewares (CORS y Cabeceras de Seguridad)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Inicializar servicio de consulta unificado
query_service = QueryService()

# 2. Manejo de Errores Global y Validación Estricta (Criterio 2 BDD)
@app.exception_handler(ValueError)
async def value_error_handler(request: Request, exc: ValueError):
    """Captura errores de sanitización o valores prohibidos y responde con HTTP 400 Bad Request."""
    logger.warning(f"Validación fallida en {request.url.path}: {exc}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "Bad Request",
            "detail": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    """Captura errores de esquema Pydantic y responde con HTTP 422 Unprocessable Entity."""
    logger.warning(f"Parámetro inválido en {request.url.path}: {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "Unprocessable Entity",
            "detail": exc.errors(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Manejo de excepciones no controladas sin exponer trazas de credenciales internas."""
    logger.error(f"Error no controlado en {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal Server Error",
            "detail": "Ocurrió un error interno en el servicio de consulta.",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

# ------------------------------------------------------------------------------
# 3. Endpoints del Sistema (Criterio 1 BDD)
# ------------------------------------------------------------------------------

@app.get(
    "/api/health",
    response_model=HealthResponse,
    summary="Estado de salud de servicios dependientes",
    tags=["Salud y Diagnóstico"]
)
def get_health():
    """
    Comprueba el estado de salud de los servicios dependientes (PostgreSQL, MinIO, Kafka)
    sin exponer credenciales ni cadenas de conexión internas.
    """
    dependencies = query_service.check_dependencies()
    overall_status = "healthy"
    if any(s == "disconnected" for s in dependencies.values()):
        overall_status = "degraded"

    return {
        "status": overall_status,
        "service": "serving-api",
        "architecture": "Lambda (Batch + Speed + Serving)",
        "dependencies": dependencies,
        "timestamp_utc": datetime.now(timezone.utc)
    }

@app.get(
    "/api/assets",
    response_model=AssetsResponse,
    summary="Lista de activos soportados con paginación y filtros",
    tags=["Catálogo de Mercado"]
)
def get_assets(
    exchange: Optional[str] = Query(None, description="Filtrar por exchange (ej. binance, coingecko)"),
    status: Optional[str] = Query(None, description="Filtrar por estado (ej. ACTIVE, INACTIVE)"),
    page: int = Query(1, ge=1, description="Número de página (inicio en 1)"),
    page_size: int = Query(20, ge=1, le=100, description="Tamaño de página (máximo 100)")
):
    """
    Retorna la lista paginada de activos y pares cripto/financieros disponibles en la plataforma.
    """
    offset = (page - 1) * page_size
    return query_service.get_assets(exchange=exchange, status=status, limit=page_size, offset=offset)

@app.get(
    "/api/ohlcv",
    response_model=OHLCVResponse,
    summary="Series temporales OHLCV reconciliadas (Batch + Speed)",
    tags=["Series Financieras"]
)
def get_ohlcv(
    symbol: str = Query(..., description="Símbolo del par, ej: BTCUSDT"),
    interval: IntervalEnum = Query(IntervalEnum.ONE_HOUR, description="Intervalo temporal válido (1m, 5m, 15m, 1h, 1d)"),
    start_time: Optional[datetime] = Query(None, description="Fecha/hora inicial UTC (ISO-8601)"),
    end_time: Optional[datetime] = Query(None, description="Fecha/hora final UTC (ISO-8601)"),
    limit: int = Query(100, ge=1, le=1000, description="Cantidad máxima de velas a retornar")
):
    """
    Consulta series de velas OHLCV reconciliadas.
    Aplica las reglas del patrón Lambda: ventanas cerradas consolidadas por la Batch Layer
    y ventanas recientes provisionales calculadas por la Speed Layer, sin doble conteo.
    """
    clean_sym = sanitize_symbol(symbol)
    return query_service.get_ohlcv(
        symbol=clean_sym,
        interval=interval.value,
        start_time=start_time,
        end_time=end_time,
        limit=limit
    )

@app.get(
    "/api/metrics",
    response_model=MetricsResponse,
    summary="Métricas operativas recientes y retornos",
    tags=["Métricas en Tiempo Real"]
)
def get_metrics(
    symbol: Optional[str] = Query(None, description="Filtrar por símbolo específico (ej. BTCUSDT)")
):
    """
    Retorna las métricas operativas recientes producidas por la Speed Layer (último precio,
    volumen acumulado en ventanas cortas y variación porcentual con fecha de corte de frescura).
    """
    clean_sym = sanitize_symbol(symbol) if symbol else None
    return query_service.get_metrics(symbol=clean_sym)

@app.get(
    "/api/pipeline/status",
    response_model=PipelineStatusResponse,
    summary="Estado de jobs, lag de Kafka y frescura de capas",
    tags=["Observabilidad y Operaciones"]
)
def get_pipeline_status():
    """
    Provee observabilidad integral del clúster Lambda:
    lag de consumidores Kafka, estado de jobs Batch/Speed y frescura de los datos ingeridos.
    """
    return query_service.get_pipeline_status()

# 4. Servidor de Archivos Estáticos para Frontend
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")
