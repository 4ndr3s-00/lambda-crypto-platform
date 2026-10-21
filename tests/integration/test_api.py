"""
Pruebas de Integración para la Serving REST API con FastAPI (US-22).
Responsable: Andrés Teherán (DevOps & Backend Engineer)

Criterios de Aceptación Validados:
- Criterio 1 (BDD): Endpoints requeridos y contratos de respuesta (/api/health, /api/assets, /api/ohlcv, /api/metrics, /api/pipeline/status).
- Criterio 2 (BDD): Validación estricta de parámetros de entrada (HTTP 422 o 400 ante intervalos desconocidos o inyecciones SQL).
- Criterio 3 (BDD): Pasarela segura exclusiva sin exponer credenciales ni acceso directo a Kafka/MinIO.
- Tarea Técnica 3: Documentación OpenAPI / Swagger automática (/docs).
- Tarea Técnica 4: Pruebas automatizadas con httpx / pytest.
"""

import sys
import os
import pytest
from httpx import AsyncClient, ASGITransport

# Agregar directorio api al path de importación
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "api"))

from main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ------------------------------------------------------------------------------
# 1. Pruebas de Criterio 1: Endpoints Requeridos y Contratos de Respuesta
# ------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_endpoint_health():
    """Valida que /api/health responda HTTP 200 con JSON estructurado sin exponer credenciales."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["healthy", "degraded", "unhealthy"]
    assert data["service"] == "serving-api"
    assert "architecture" in data
    assert "dependencies" in data
    assert "postgres" in data["dependencies"]
    assert "minio" in data["dependencies"]
    assert "kafka" in data["dependencies"]
    assert "timestamp_utc" in data

    # Verificación de que no se filtran secretos o contraseñas
    text_payload = response.text.lower()
    assert "password" not in text_payload
    assert "secret" not in text_payload
    assert "minioadmin" not in text_payload


@pytest.mark.asyncio
async def test_endpoint_assets_list_and_pagination():
    """Valida que /api/assets retorne lista de activos con paginación y estructura requerida."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/assets?page=1&page_size=10")

    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert data["page"] == 1
    assert data["page_size"] == 10
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0

    first_item = data["items"][0]
    assert "symbol" in first_item
    assert "name" in first_item
    assert "exchange" in first_item
    assert "base_asset" in first_item
    assert "quote_asset" in first_item
    assert "status" in first_item
    assert "is_active" in first_item


@pytest.mark.asyncio
async def test_endpoint_assets_filtering():
    """Valida filtros por exchange y status en /api/assets."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/assets?exchange=binance&status=ACTIVE")

    assert response.status_code == 200
    data = response.json()
    for item in data["items"]:
        assert item["exchange"].lower() == "binance"
        assert item["status"].upper() == "ACTIVE"


@pytest.mark.asyncio
async def test_endpoint_ohlcv_valid_query():
    """Valida que /api/ohlcv retorne serie temporal con temporalidad y campos requeridos."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/ohlcv?symbol=BTCUSDT&interval=1h&limit=50")

    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["interval"] == "1h"
    assert "count" in data
    assert "data" in data
    assert isinstance(data["data"], list)


@pytest.mark.asyncio
async def test_endpoint_ohlcv_with_time_range():
    """Valida consulta de velas OHLCV con rango temporal ISO-8601."""
    start = "2026-10-01T00:00:00Z"
    end = "2026-10-19T23:59:59Z"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/api/ohlcv?symbol=BTCUSDT&interval=1h&start_time={start}&end_time={end}")

    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_endpoint_metrics():
    """Valida que /api/metrics retorne métricas operativas recientes y retornos con frescura."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/metrics")

    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "last_updated_utc" in data
    assert "items" in data
    assert len(data["items"]) > 0

    metric = data["items"][0]
    assert "symbol" in metric
    assert "last_price" in metric
    assert "rolling_volume_5m" in metric
    assert "pct_change_5m" in metric
    assert "event_timestamp" in metric
    assert "last_updated_utc" in metric
    assert "source" in metric


@pytest.mark.asyncio
async def test_endpoint_metrics_filtered_by_symbol():
    """Valida filtrado por símbolo en /api/metrics."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/metrics?symbol=BTCUSDT")

    assert response.status_code == 200
    data = response.json()
    for m in data["items"]:
        assert m["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_endpoint_pipeline_status():
    """Valida que /api/pipeline/status provea observabilidad: jobs, lag de Kafka y frescura."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/api/pipeline/status",
            headers={"X-API-Key": os.getenv("ADMIN_API_KEY", "lambda_admin_secure_key_2026")},
        )

    assert response.status_code == 200
    data = response.json()
    assert "cluster_status" in data
    assert data["cluster_status"] in ["HEALTHY", "WARNING", "DEGRADED"]
    assert "timestamp_utc" in data
    assert "freshness_seconds" in data
    assert "components" in data
    assert isinstance(data["components"], list)
    assert len(data["components"]) > 0

    comp = data["components"][0]
    assert "component" in comp
    assert "status" in comp
    assert "consumer_lag" in comp
    assert comp["consumer_lag"] >= 0


# ------------------------------------------------------------------------------
# 2. Pruebas de Criterio 2: Validación Estricta de Parámetros de Entrada
# ------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_validation_unknown_interval():
    """Valida que un intervalo desconocido sea rechazado con HTTP 422."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/ohlcv?symbol=BTCUSDT&interval=27m")

    assert response.status_code == 422
    data = response.json()
    assert data["error"] == "Unprocessable Entity"


@pytest.mark.asyncio
async def test_validation_missing_required_symbol():
    """Valida que omitir el parámetro obligatorio symbol retorne HTTP 422."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/ohlcv?interval=1h")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_validation_sql_injection_rejection():
    """Valida que intentos de inyección SQL en el símbolo sean bloqueados con HTTP 400."""
    injections = [
        "BTCUSDT' OR '1'='1",
        "BTCUSDT; DROP TABLE serving.assets;--",
        "BTC' UNION SELECT * FROM users--"
    ]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for payload in injections:
            response = await client.get(f"/api/ohlcv?symbol={payload}&interval=1h")
            assert response.status_code in [400, 422], f"Fallo al bloquear payload: {payload}"


@pytest.mark.asyncio
async def test_validation_pagination_bounds():
    """Valida límites de paginación (page < 1 o page_size > 100)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res1 = await client.get("/api/assets?page=0")
        assert res1.status_code == 422

        res2 = await client.get("/api/assets?page_size=500")
        assert res2.status_code == 422


# ------------------------------------------------------------------------------
# 3. Pruebas de Documentación y Seguridad
# ------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_openapi_swagger_docs():
    """Valida que la documentación Swagger interactiva (/docs) y OpenAPI JSON estén disponibles."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res_docs = await client.get("/docs")
        assert res_docs.status_code == 200

        res_openapi = await client.get("/openapi.json")
        assert res_openapi.status_code == 200
        schema = res_openapi.json()
        assert "paths" in schema
        assert "/api/health" in schema["paths"]
        assert "/api/assets" in schema["paths"]
        assert "/api/ohlcv" in schema["paths"]
        assert "/api/metrics" in schema["paths"]
        assert "/api/pipeline/status" in schema["paths"]
