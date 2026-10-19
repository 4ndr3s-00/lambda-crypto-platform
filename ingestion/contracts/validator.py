"""
Validador de eventos de mercado contra el contrato formal
ingestion/contracts/market_event.schema.json (US-05).
"""

import json
from pathlib import Path

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

# Cargamos la "plantilla" (el schema) una sola vez, al importar este archivo,
# en vez de leerla del disco cada vez que llega un evento nuevo.
_SCHEMA_PATH = Path(__file__).parent / "market_event.schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text())
_VALIDATOR = Draft202012Validator(_SCHEMA)


def validate_event(event: dict) -> None:
    """Revisa si un evento cumple la plantilla.

    Si le falta algo o algo está mal (precio negativo, campo faltante, etc.),
    lanza un error explicando exactamente qué está mal. Si todo está bien,
    simplemente no hace nada y deja seguir el flujo normal.
    """
    # iter_errors junta TODOS los problemas del evento de una vez,
    # en vez de detenerse en el primero que encuentra.
    errores = sorted(_VALIDATOR.iter_errors(event), key=lambda e: e.path)
    if errores:
        mensajes = "; ".join(f"{list(e.path)}: {e.message}" for e in errores)
        raise ValidationError(f"Evento de mercado no conforme al contrato: {mensajes}")


def is_valid(event: dict) -> bool:
    """Versión 'silenciosa' del chequeo: True si está bien, False si no.

    Pensada para los conectores (US-07, US-08) y la DLQ (US-09), donde
    interesa decidir qué hacer con el evento, no detener el programa con un error.
    """
    return _VALIDATOR.is_valid(event)
