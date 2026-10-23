"""
Configuración de Logging Estructurado JSON para Observabilidad en Producción (US-33).
Responsable: Andrés Teherán

Emite eventos de logs estructurados en JSON con timestamp UTC, nivel de severidad,
módulo y mensaje para ingesta centralizada (Elasticsearch, Loki o CloudWatch).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """Formateador de logs que serializa cada registro como un objeto JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


def configure_structured_logging(level: int = logging.INFO) -> None:
    """Configura el handler raíz para utilizar el JSONFormatter."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remover handlers previos
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(console_handler)
