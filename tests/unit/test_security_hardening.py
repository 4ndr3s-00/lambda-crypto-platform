"""
Tests de Seguridad, Autenticación y Hardening (US-33).
Responsable: Andrés Teherán

Verifica:
1. Denegación de acceso no autorizado (HTTP 401) en endpoints sensibles (`/api/pipeline/status`).
2. Acceso concedido (HTTP 200) con Bearer Token o X-API-Key válidos.
3. Formato JSON estructurado en los registros de logging.
4. Ausencia de archivos de secretos rastreados por git y existencia de .env.example.
"""

import json
import logging
import os
from pathlib import Path
from fastapi.testclient import TestClient

from api.main import app
from api.auth import ADMIN_API_KEY
from api.logging_config import JSONFormatter

client = TestClient(app)


def test_pipeline_status_requires_authentication():
    """Criterio 2: Acceso sin credenciales a /api/pipeline/status debe retornar HTTP 401."""
    response = client.get("/api/pipeline/status")
    assert response.status_code == 401, f"Esperaba 401 pero obtuvo {response.status_code}"
    assert "no autorizado" in response.json()["detail"].lower()


def test_pipeline_status_authorized_with_bearer_token():
    """Verifica acceso exitoso suministrando Bearer Token correcto."""
    headers = {"Authorization": f"Bearer {ADMIN_API_KEY}"}
    response = client.get("/api/pipeline/status", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert "cluster_status" in data
    assert "components" in data
    assert "freshness_seconds" in data


def test_pipeline_status_authorized_with_x_api_key():
    """Verifica acceso exitoso suministrando cabecera X-API-Key."""
    headers = {"X-API-Key": ADMIN_API_KEY}
    response = client.get("/api/pipeline/status", headers=headers)
    assert response.status_code == 200


def test_json_logging_format():
    """Criterio 3: Los registros de logging deben formatearse como JSON estructurado con timestamp UTC."""
    formatter = JSONFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=42,
        msg="Operación completada con éxito",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test_logger"
    assert parsed["message"] == "Operación completada con éxito"
    assert "timestamp" in parsed


def test_git_security_and_env_example():
    """Criterio 1: Debe existir .env.example y .gitignore debe excluir archivos .env."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    env_example = repo_root / ".env.example"
    gitignore = repo_root / ".gitignore"

    assert env_example.exists(), "Falta el archivo .env.example en la raíz"
    assert gitignore.exists(), "Falta el archivo .gitignore en la raíz"

    gitignore_content = gitignore.read_text()
    assert ".env" in gitignore_content
