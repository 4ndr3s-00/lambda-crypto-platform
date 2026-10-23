"""
Middleware y Dependencias de Autenticación y Seguridad (US-33).
Responsable: Andrés Teherán (DevOps & Security Lead)

Implementa:
- Protección de endpoints sensibles y administrativos (`/api/pipeline/status`) mediante API Key / Bearer Token.
- Manejo de excepciones de seguridad retornando HTTP 401 Unauthorized cuando no se proveen credenciales válidas.
- Posibilidad de deshabilitar la autenticación en entornos locales de testing si `API_SECURITY_ENABLED=false`.
"""

import os
from typing import Optional
from fastapi import Header, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

API_SECURITY_ENABLED = os.environ.get("API_SECURITY_ENABLED", "true").lower() in ("true", "1", "yes")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "lambda_admin_secure_key_2026")

security_bearer = HTTPBearer(auto_error=False)


def verify_admin_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(security_bearer),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> bool:
    """
    Verifica que la petición contenga un Bearer Token o X-API-Key autorizado.
    Si `API_SECURITY_ENABLED=false`, permite el acceso para desarrollo sin auth.
    """
    if not API_SECURITY_ENABLED:
        return True

    # 1. Comprobar X-API-Key
    if x_api_key and x_api_key == ADMIN_API_KEY:
        return True

    # 2. Comprobar Bearer Token
    if credentials and credentials.scheme.lower() == "bearer" and credentials.credentials == ADMIN_API_KEY:
        return True

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Acceso no autorizado a endpoint administrativo de la plataforma.",
        headers={"WWW-Authenticate": "Bearer"},
    )
