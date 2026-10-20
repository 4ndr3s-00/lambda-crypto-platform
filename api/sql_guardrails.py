"""
Módulo de seguridad y guardrails para validación de consultas e inputs en la Serving API.
Protege contra ataques de inyección SQL, patrones maliciosos y parámetros anómalos.
"""

import re
from typing import Set

ALLOWED_TABLES: Set[str] = {
    "serving.dim_assets",
    "serving.assets",
    "serving.ohlcv_reconciled",
    "serving.realtime_metrics",
    "serving.pipeline_status",
    "serving.pipeline_execution_log"
}

FORBIDDEN_SQL_PATTERNS = [
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|EXECUTE|CREATE|REPLACE|MERGE)\b",
    r"\b(UNION\s+ALL|UNION\s+SELECT)\b",
    r"\b(PG_SLEEP|BENCHMARK|WAITFOR\s+DELAY)\b",
    r"(--|/\*|\*/|;)",
    r"\b(OR|AND)\s+['\"0-9]+=['\"0-9]+",
    r"'\s+OR\s+'1'='1"
]

def sanitize_symbol(symbol: str) -> str:
    """
    Valida y sanitiza el símbolo de activo.
    Solo admite caracteres alfanuméricos en mayúsculas, guiones y barras.
    Lanza ValueError si contiene caracteres no permitidos o patrones de inyección.
    """
    clean_symbol = symbol.strip().upper()
    if not re.match(r"^[A-Z0-9_-]{2,20}$", clean_symbol):
        raise ValueError(f"Símbolo inválido '{symbol}'. Debe ser alfanumérico entre 2 y 20 caracteres.")

    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, clean_symbol, re.IGNORECASE):
            raise ValueError(f"Detección de patrón no seguro en el símbolo '{symbol}'.")

    return clean_symbol

def validate_readonly_sql(query: str) -> str:
    """
    Valida que una consulta SQL sea estrictamente SELECT de solo lectura.
    """
    clean = query.strip()
    upper = clean.upper()

    if not upper.startswith("SELECT"):
        raise ValueError("Operación no autorizada: Solo se permiten consultas SELECT.")

    for pattern in FORBIDDEN_SQL_PATTERNS:
        if re.search(pattern, clean, re.IGNORECASE):
            raise ValueError("Violación de seguridad: Consulta contiene patrones no permitidos.")

    if "LIMIT" not in upper:
        clean = f"{clean} LIMIT 500"

    return clean
