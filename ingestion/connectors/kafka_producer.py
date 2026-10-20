"""
Productor Kafka para los conectores de ingesta (US-07).

Particiona por `exchange|symbol` para que todos los eventos de un mismo par
caigan en la misma partición y conserven su orden. Los eventos que no cumplen
el contrato no se pierden: van a `market.dlq` con el motivo del rechazo.
"""

import json
import logging
from typing import Any, Dict, Optional

from jsonschema.exceptions import ValidationError

try:
    from ingestion.contracts.dlq_message import build_dlq_message, dlq_key
    from ingestion.contracts.validator import validate_event
except ImportError:  # ejecución desde ingestion/
    from contracts.dlq_message import build_dlq_message, dlq_key
    from contracts.validator import validate_event

logger = logging.getLogger(__name__)

TOPIC_EVENTS = "market.events"
TOPIC_DLQ = "market.dlq"


def partition_key(event: Dict[str, Any]) -> str:
    return f"{event['exchange']}|{event['symbol']}"


class MarketEventProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:29092",
        topic: str = TOPIC_EVENTS,
        dlq_topic: str = TOPIC_DLQ,
        producer: Optional[Any] = None,
    ):
        self.topic = topic
        self.dlq_topic = dlq_topic
        if producer is None:
            # Import diferido: las pruebas y los módulos que no publican no necesitan kafka-python
            from kafka import KafkaProducer

            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",  # no perder eventos ante la caída de un broker
                retries=5,
                linger_ms=20,
            )
        self._producer = producer

    def publish(self, event: Dict[str, Any], component: str = "ingestion") -> bool:
        """True si se publicó en `market.events`; False si se desvió a la DLQ."""
        try:
            validate_event(event)
        except ValidationError as err:
            self.reject(event, err.message, component)
            return False
        self._producer.send(self.topic, key=partition_key(event), value=event)
        return True

    def reject(self, original: Any, reason: str, component: str, source: Optional[str] = None) -> None:
        """Envía a `market.dlq` el payload original intacto con los metadatos del rechazo."""
        logger.warning("Rechazado por %s: %s", component, reason)
        message = build_dlq_message(original, reason, component, source)
        self._producer.send(self.dlq_topic, key=dlq_key(original), value=message)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
