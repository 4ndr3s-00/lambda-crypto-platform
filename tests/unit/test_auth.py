"""
Tests unitarios de `api/auth.py` (US-33).

Complementa `test_security_hardening.py` (que ya cubre el camino feliz vía
HTTP) con los casos que ese archivo no probaba: credenciales inválidas,
seguridad deshabilitada, y el guardado contra arrancar sin `ADMIN_API_KEY`.
"""

import importlib
import os

import pytest
from fastapi import HTTPException

import api.auth as auth_module
from api.auth import verify_admin_auth


def test_rejects_wrong_x_api_key():
    with pytest.raises(HTTPException) as exc_info:
        verify_admin_auth(credentials=None, x_api_key="una-clave-incorrecta")
    assert exc_info.value.status_code == 401


def test_rejects_wrong_bearer_token():
    from fastapi.security import HTTPAuthorizationCredentials

    bad_credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token-invalido")
    with pytest.raises(HTTPException) as exc_info:
        verify_admin_auth(credentials=bad_credentials, x_api_key=None)
    assert exc_info.value.status_code == 401


def test_rejects_missing_credentials():
    with pytest.raises(HTTPException) as exc_info:
        verify_admin_auth(credentials=None, x_api_key=None)
    assert exc_info.value.status_code == 401


def test_accepts_correct_x_api_key():
    assert verify_admin_auth(credentials=None, x_api_key=auth_module.ADMIN_API_KEY) is True


def test_accepts_correct_bearer_token():
    from fastapi.security import HTTPAuthorizationCredentials

    good_credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=auth_module.ADMIN_API_KEY)
    assert verify_admin_auth(credentials=good_credentials, x_api_key=None) is True


def test_security_disabled_bypasses_auth(monkeypatch):
    monkeypatch.setattr(auth_module, "API_SECURITY_ENABLED", False)
    assert verify_admin_auth(credentials=None, x_api_key=None) is True


def test_raises_on_import_without_admin_key_when_security_enabled(monkeypatch):
    """Si API_SECURITY_ENABLED=true y no hay ADMIN_API_KEY, el módulo debe
    fallar al cargarse en vez de arrancar con una clave admin adivinable."""
    monkeypatch.setenv("API_SECURITY_ENABLED", "true")
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    with pytest.raises(RuntimeError):
        importlib.reload(auth_module)

    # Dejar el módulo en un estado válido para el resto de la suite.
    monkeypatch.setenv("ADMIN_API_KEY", os.environ.get("ADMIN_API_KEY", "test_admin_key_do_not_use_in_prod"))
    importlib.reload(auth_module)


def test_no_default_admin_key_fallback(monkeypatch):
    """El módulo nunca debe caer en una clave admin fija hardcodeada."""
    monkeypatch.setenv("API_SECURITY_ENABLED", "false")
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    importlib.reload(auth_module)
    assert auth_module.ADMIN_API_KEY is None

    # Restaurar el estado esperado por el resto de la suite.
    monkeypatch.setenv("API_SECURITY_ENABLED", "true")
    monkeypatch.setenv("ADMIN_API_KEY", os.environ.get("ADMIN_API_KEY", "test_admin_key_do_not_use_in_prod"))
    importlib.reload(auth_module)
