"""
Contrato unificado de los conectores de ingesta (US-07).

Todo conector (REST, WebSocket, otros proveedores) hereda de `BaseConnector`
y solo implementa dos cosas: cómo obtener registros crudos (`fetch`) y cómo
convertirlos al contrato canónico (`normalize`). La validación y la
publicación a Kafka viven aquí para que ningún conector las reimplemente
distinto.

También se incluyen el limitador de tasa y el cálculo de backoff, que son
comunes a REST y WebSocket.
"""

import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Protocol

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "2.0.0"


class EventPublisher(Protocol):
    """Destino de los eventos normalizados (productor Kafka o un doble de pruebas)."""

    def publish(self, event: Dict[str, Any], component: str = "ingestion") -> bool: ...

    def reject(self, original: Any, reason: str, component: str, source: Optional[str] = None) -> None: ...


class TokenBucket:
    """
    Limitador de tasa por token bucket.

    Permite ráfagas de hasta `capacity` peticiones y repone `rate` tokens por
    segundo. Se prefiere a un `sleep` fijo porque no penaliza la latencia de la
    propia petición y respeta la cuota real del proveedor (p. ej. peso por
    minuto de Binance). `clock` y `sleep` son inyectables para probarlo sin
    esperas reales.
    """

    def __init__(
        self,
        rate: float,
        capacity: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if rate <= 0:
            raise ValueError("rate debe ser > 0")
        self.rate = rate
        self.capacity = capacity if capacity is not None else max(1.0, rate)
        self._tokens = self.capacity
        self._clock = clock
        self._sleep = sleep
        self._last = clock()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        """Bloquea hasta disponer de `tokens`. Devuelve los segundos esperados."""
        waited = 0.0
        while True:
            with self._lock:
                now = self._clock()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                missing = (tokens - self._tokens) / self.rate
            self._sleep(missing)
            waited += missing


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0, jitter: bool = True) -> float:
    """
    Retroceso exponencial: base * 2^attempt acotado por `cap`.

    El jitter (entre 50% y 100% del valor) evita que varios conectores
    reintenten sincronizados tras una caída del proveedor.
    """
    delay = min(cap, base * (2 ** attempt))
    return delay * random.uniform(0.5, 1.0) if jitter else delay


def utc_iso(dt: Optional[datetime] = None) -> str:
    """Formato ISO-8601 UTC con milisegundos, el que espera el contrato."""
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def ms_to_iso(ms: int) -> str:
    return utc_iso(datetime.fromtimestamp(ms / 1000, tz=timezone.utc))


class BaseConnector(ABC):
    """Clase base abstracta: `fetch` -> `normalize` -> publicar."""

    #: valor del campo `source` del contrato (debe estar en el enum del schema)
    source: str
    #: valor del campo `exchange` del contrato
    exchange: str
    #: nombre con el que este conector aparece como `failed_component` en la DLQ
    component: str = "connector"

    def __init__(self, publisher: EventPublisher):
        self.publisher = publisher
        self.stats = {"raw": 0, "published": 0, "rejected": 0}

    @abstractmethod
    def fetch(self) -> Iterable[Any]:
        """Entrega registros crudos del proveedor (finito en REST, infinito en WebSocket)."""

    @abstractmethod
    def normalize(self, raw: Any) -> Optional[Dict[str, Any]]:
        """Convierte un registro crudo al contrato canónico; None si no es un dato de mercado."""

    def run(self) -> Dict[str, int]:
        for raw in self.fetch():
            self.stats["raw"] += 1
            try:
                event = self.normalize(raw)
            except (ValueError, KeyError, IndexError, TypeError) as err:
                # Payload corrupto o incompleto: no se descarta en silencio, va a la DLQ
                self.publisher.reject(raw, f"payload no normalizable: {err}", self.component, self.source)
                self.stats["rejected"] += 1
                continue
            if event is None:
                continue
            # El publisher valida contra el contrato y enruta lo inválido a la DLQ
            if self.publisher.publish(event, component=self.component):
                self.stats["published"] += 1
            else:
                self.stats["rejected"] += 1
        logger.info("%s finalizado: %s", self.source, self.stats)
        return dict(self.stats)
