import json
from pathlib import Path

import pytest

from ingestion.contracts.validator import is_valid, validate_event
from jsonschema.exceptions import ValidationError

# Cargamos los eventos de ejemplo (los "casos de prueba" que pide la historia
# de usuario) desde un archivo aparte, en vez de escribirlos aquí mismo.
# Así, si luego alguien quiere agregar más ejemplos, solo toca ese archivo.
_FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "market_events.json").read_text()
)


# parametrize corre esta misma prueba una vez por cada evento "bueno" de la
# lista. Si agregamos un evento válido más al archivo, esta prueba lo toma
# automáticamente, sin tocar código.
@pytest.mark.parametrize("event", _FIXTURES["valid"])
def test_valid_events_pass(event):
    # Un evento bien formado no debería lanzar ningún error...
    validate_event(event)
    # ...y la versión "silenciosa" también debe confirmar que está bien.
    assert is_valid(event) is True


# Mismo truco, pero con los eventos "malos". El "ids" es solo para que,
# si una prueba falla, el reporte diga el nombre del caso (ej. "price negativo")
# en vez de un número sin sentido.
@pytest.mark.parametrize(
    "case",
    _FIXTURES["invalid"],
    ids=[c["case"] for c in _FIXTURES["invalid"]],
)
def test_invalid_events_are_rejected(case):
    event = case["event"]
    # Un evento mal formado debe ser detectado como inválido...
    assert is_valid(event) is False
    # ...y debe lanzar el error explicando qué está mal, no pasar de largo.
    with pytest.raises(ValidationError):
        validate_event(event)
