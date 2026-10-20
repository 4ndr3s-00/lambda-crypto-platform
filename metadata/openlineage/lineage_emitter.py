"""
Módulo Emisor y Validador de Eventos de Linaje OpenLineage (US-26).
Responsable: Samuel Roncancio (Data Architect & Storage Lead)

Proporciona la instrumentación para capturar y emitir eventos de linaje de extremo a extremo:
  Fuente API (Binance/CoinGecko)
    └── Topic Kafka (`market.events`)
         ├── Speed Layer: Spark Streaming -> `market.metrics` -> Serving Layer
         └── Batch Layer: MinIO (`historical/`) -> Spark Batch (`batch_historical.py`)
              └── MinIO (`batch-results/metric=ohlcv`) -> Serving API (`/api/ohlcv`)

Genera especificaciones estándar conforme al modelo OpenLineage (RunEvent, Job, Dataset, Facets).
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error

logger = logging.getLogger("openlineage.emitter")

OPENLINEAGE_NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "lambda-crypto-platform")
OPENLINEAGE_URL = os.environ.get("OPENLINEAGE_URL", "http://openmetadata:8585/api/v1/lineage")


class OpenLineageEmitter:
    """Emisor de eventos de linaje conforme a la especificación OpenLineage."""

    def __init__(self, namespace: str = OPENLINEAGE_NAMESPACE, endpoint_url: str = OPENLINEAGE_URL):
        self.namespace = namespace
        self.endpoint_url = endpoint_url

    def build_dataset(self, name: str, namespace: Optional[str] = None, facets: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Construye una entidad Dataset de OpenLineage."""
        return {
            "namespace": namespace or self.namespace,
            "name": name,
            "facets": facets or {},
        }

    def create_run_event(
        self,
        event_type: str,  # "START", "RUNNING", "COMPLETE", "FAIL", "ABORT"
        job_name: str,
        inputs: Optional[List[Dict[str, Any]]] = None,
        outputs: Optional[List[Dict[str, Any]]] = None,
        run_id: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Crea un RunEvent completo de OpenLineage."""
        event_time = datetime.now(timezone.utc).isoformat()
        current_run_id = run_id or str(uuid.uuid4())

        run_facets: Dict[str, Any] = {}
        if error_message:
            run_facets["errorMessage"] = {
                "_producer": "https://github.com/4ndr3s-00/lambda-crypto-platform",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                "message": error_message,
                "programmingLanguage": "PYTHON",
            }

        event = {
            "eventType": event_type.upper(),
            "eventTime": event_time,
            "run": {
                "runId": current_run_id,
                "facets": run_facets,
            },
            "job": {
                "namespace": self.namespace,
                "name": job_name,
                "facets": {},
            },
            "inputs": inputs or [],
            "outputs": outputs or [],
            "producer": "https://github.com/4ndr3s-00/lambda-crypto-platform/openlineage",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        }
        return event

    def emit(self, event: Dict[str, Any], dry_run: bool = False) -> bool:
        """
        Emite el evento al endpoint de recolección de linaje.
        En modo local o pruebas donde el endpoint HTTP no esté activo, realiza un fallback elegante.
        """
        payload = json.dumps(event).encode("utf-8")
        if dry_run or not self.endpoint_url.startswith("http"):
            logger.info(f"OpenLineage dry-run event: {event['eventType']} for job {event['job']['name']}")
            return True

        req = urllib.request.Request(
            self.endpoint_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                return resp.status in (200, 201, 202)
        except (urllib.error.URLError, OSError) as e:
            logger.warning(f"No se pudo enviar evento OpenLineage a {self.endpoint_url}: {e} (tolerancia a desconexión activa)")
            return False


def get_platform_lineage_graph() -> Dict[str, Any]:
    """
    Retorna la definición canónica del grafo de linaje de la plataforma Lambda (Criterio 2).
    Permite auditar y renderizar la trazabilidad completa aguas arriba y aguas abajo.
    """
    return {
        "nodes": [
            {"id": "binance_rest", "name": "Binance Public REST API", "type": "SOURCE"},
            {"id": "binance_ws", "name": "Binance WebSocket Trades", "type": "SOURCE"},
            {"id": "kafka_market_events", "name": "kafka://market.events", "type": "TOPIC"},
            {"id": "minio_historical", "name": "s3://market-lake/historical/", "type": "DATASET"},
            {"id": "spark_streaming", "name": "Spark Streaming (Speed Layer)", "type": "JOB"},
            {"id": "kafka_market_metrics", "name": "kafka://market.metrics", "type": "TOPIC"},
            {"id": "spark_batch", "name": "Spark Batch OHLCV (Batch Layer)", "type": "JOB"},
            {"id": "minio_batch_results", "name": "s3://market-lake/batch-results/metric=ohlcv/", "type": "DATASET"},
            {"id": "serving_api", "name": "FastAPI Serving Layer (/api/ohlcv)", "type": "SERVICE"},
            {"id": "frontend_dashboard", "name": "Frontend Market Dashboard", "type": "CONSUMER"},
            {"id": "qwen_rag", "name": "Qwen2.5 LLM Analytical Tools", "type": "CONSUMER"},
        ],
        "edges": [
            {"from": "binance_rest", "to": "kafka_market_events"},
            {"from": "binance_ws", "to": "kafka_market_events"},
            {"from": "kafka_market_events", "to": "minio_historical"},
            {"from": "kafka_market_events", "to": "spark_streaming"},
            {"from": "spark_streaming", "to": "kafka_market_metrics"},
            {"from": "minio_historical", "to": "spark_batch"},
            {"from": "spark_batch", "to": "minio_batch_results"},
            {"from": "minio_batch_results", "to": "serving_api"},
            {"from": "kafka_market_metrics", "to": "serving_api"},
            {"from": "serving_api", "to": "frontend_dashboard"},
            {"from": "serving_api", "to": "qwen_rag"},
        ],
    }
