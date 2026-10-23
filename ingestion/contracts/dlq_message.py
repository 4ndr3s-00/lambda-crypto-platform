"""
Estructura del mensaje de `market.dlq` (US-09).

Un mensaje de la DLQ es un sobre JSON que guarda el payload original intacto
y explica por qué se rechazó, para poder auditarlo y reprocesarlo después:

    {
      "original_event":   <payload tal como llegó: objeto JSON o texto crudo>,
      "error_reason":     "por qué se rechazó",
      "failed_component": "componente que lo detectó (market_websocket, csv_loader, spark_streaming...)",
      "timestamp_utc":    "2026-10-20T15:00:00.000Z",
      "source":           "fuente de origen del dato (binance_ws, csv_backfill...)"
    }

La clave `original_event` es la que ya entiende el DAG `replay_dlq` (US-17).
"""

import json
from typing import Any, Dict, Optional

try:
    from ingestion.connectors.base_connector import utc_iso
except ImportError:  # ejecución desde ingestion/
    from connectors.base_connector import utc_iso

UNKNOWN = "unknown"


def _original(payload: Any) -> Any:
    """Conserva el payload sin alterarlo; los bytes se decodifican a texto para poder serializarlos."""
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload).decode("utf-8", errors="replace")
    return payload


def build_dlq_message(
    original: Any,
    error_reason: str,
    failed_component: str,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    payload = _original(original)
    if source is None and isinstance(payload, dict):
        source = payload.get("source")
    return {
        "original_event": payload,
        "error_reason": error_reason,
        "failed_component": failed_component,
        "timestamp_utc": utc_iso(),
        "source": source or UNKNOWN,
    }


def dlq_key(original: Any) -> str:
    """Misma clave `exchange|symbol` que el topic principal, para que el rechazo caiga junto a su activo."""
    if isinstance(original, dict) and original.get("exchange") and original.get("symbol"):
        return f"{original['exchange']}|{original['symbol']}"
    return UNKNOWN


def serialize(message: Dict[str, Any]) -> str:
    return json.dumps(message, default=str)
