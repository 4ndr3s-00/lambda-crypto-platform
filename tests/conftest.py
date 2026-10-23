"""
Variables de entorno mínimas para que la suite de tests pueda importar
`api.main` / `api.auth` sin depender del `.env` del desarrollador.

Se usa `setdefault` para no pisar un valor real si el entorno ya lo trae
(p. ej. corriendo dentro de un contenedor con `.env` cargado).
"""

import os

os.environ.setdefault("ADMIN_API_KEY", "test_admin_key_do_not_use_in_prod")
os.environ.setdefault("API_SECURITY_ENABLED", "true")
