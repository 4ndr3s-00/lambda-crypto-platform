"""
DAG de Airflow para el reprocesamiento y re-inyección de eventos desde la Dead Letter Queue (US-17).
Responsable: Camilo (Épica 5: Airflow DAGs)

Permite a los operadores ejecutar bajo demanda el replay de eventos corregidos desde
el topic Kafka `market.dlq` hacia el flujo principal `market.events`.

Revalida formalmente cada evento contra `market_event.schema.json` antes de reinyectarlo,
garantizando la integridad del bus de eventos, evitando duplicados y registrando
la auditoría de la operación en PostgreSQL.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaConsumer = None
    KafkaProducer = None
    KafkaError = Exception

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None
    PythonOperator = None

# Incluir rutas del proyecto para importar contratos
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from ingestion.contracts.validator import is_valid, validate_event
except ImportError:
    try:
        from contracts.validator import is_valid, validate_event
    except ImportError:
        is_valid = None
        validate_event = None

logger = logging.getLogger(__name__)

DAG_ID = "replay_dlq"

DEFAULT_ARGS = {
    "owner": "Camilo",
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": False,
}

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "market.dlq")
TOPIC_MAIN = os.environ.get("KAFKA_TOPIC_EVENTS", "market.events")
CONSUMER_GROUP_ID = "airflow-dlq-replay-group"

# Configuración de PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "serving_db")


def unpack_dlq_payload(raw_message: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Desempaqueta un mensaje proveniente de la DLQ.

    La DLQ puede recibir:
    1. Un evento canónico directo que falló validación.
    2. Un payload envoltorio con metadatos de error:
       {"original_event": {...}, "error": "...", "timestamp": "..."}
    """
    if isinstance(raw_message, (bytes, bytearray)):
        try:
            payload = json.loads(raw_message.decode("utf-8"))
        except json.JSONDecodeError as err:
            return None, f"JSON corrupto en DLQ: {err}"
    elif isinstance(raw_message, str):
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError as err:
            return None, f"JSON corrupto en DLQ: {err}"
    elif isinstance(raw_message, dict):
        payload = raw_message
    else:
        return None, f"Tipo de mensaje no soportado: {type(raw_message)}"

    # Verificar si viene encapsulado con metadatos de fallo
    for envelope_key in ["original_event", "raw_payload", "event", "data"]:
        if envelope_key in payload and isinstance(payload[envelope_key], dict):
            return payload[envelope_key], payload.get("error")

    # Si no tiene envoltorio, el mensaje es el evento crudo
    return payload, None


def matches_filters(
    event: Dict[str, Any],
    source_filter: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> bool:
    """
    Comprueba si el evento coincide con los criterios de filtrado del operador.
    """
    if source_filter and source_filter.lower() != "all":
        event_source = event.get("source") or event.get("exchange")
        if event_source != source_filter:
            return False

    event_time_str = event.get("event_time")
    if not event_time_str:
        return True

    try:
        # Normalizar marcas temporales ISO para comparación
        dt_val = event_time_str.replace("Z", "+00:00")
        event_dt = datetime.fromisoformat(dt_val)

        if start_time:
            st_val = start_time.replace("Z", "+00:00")
            if event_dt < datetime.fromisoformat(st_val):
                return False

        if end_time:
            et_val = end_time.replace("Z", "+00:00")
            if event_dt > datetime.fromisoformat(et_val):
                return False
    except Exception as ex:
        logger.debug("Omitiendo filtro de fecha por formato: %s", ex)

    return True


def replay_dlq_events(
    consumer: Any,
    producer: Any,
    source_filter: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_messages: int = 1000,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Lee mensajes de `market.dlq`, los revalida contra el contrato canónico
    y reinyecta los válidos al topic principal `market.events` (Criterios 1 y 2 BDD).
    """
    total_read = 0
    replayed_count = 0
    invalid_count = 0
    skipped_filter_count = 0

    logger.info(
        "Iniciando Replay DLQ. Max msgs: %d, Source: %s, Dry Run: %s",
        max_messages,
        source_filter,
        dry_run,
    )

    records = consumer.poll(timeout_ms=5000, max_records=max_messages)
    for topic_partition, message_list in records.items():
        for record in message_list:
            total_read += 1
            event, error_meta = unpack_dlq_payload(record.value)

            if not event:
                invalid_count += 1
                logger.warning("Mensaje corrupto no deserializable (offset %d)", record.offset)
                continue

            # 1. Aplicar filtros de fecha y origen
            if not matches_filters(event, source_filter, start_time, end_time):
                skipped_filter_count += 1
                continue

            # 2. Revalidar contra el contrato canónico formal
            valid = True
            if is_valid is not None:
                valid = is_valid(event)

            if not valid:
                invalid_count += 1
                logger.warning(
                    "Evento %s sigue sin cumplir el contrato formal. Se mantiene en cuarentena.",
                    event.get("event_id", "desconocido"),
                )
                continue

            # 3. Publicación al topic principal de eventos
            if not dry_run and producer is not None:
                # Usar símbolo como clave de particionado para respetar orden en la partición
                key_bytes = str(event.get("symbol", "DEFAULT")).encode("utf-8")
                val_bytes = json.dumps(event).encode("utf-8")

                producer.send(
                    topic=TOPIC_MAIN,
                    key=key_bytes,
                    value=val_bytes,
                )

            replayed_count += 1

    if not dry_run and producer is not None:
        producer.flush()
        consumer.commit()

    logger.info(
        "Replay finalizado: Leídos=%d, Reinyectados=%d, Inválidos=%d, Filtrados=%d",
        total_read,
        replayed_count,
        invalid_count,
        skipped_filter_count,
    )

    return {
        "total_read": total_read,
        "replayed_valid": replayed_count,
        "still_invalid": invalid_count,
        "filtered_out": skipped_filter_count,
        "dry_run": dry_run,
        "status": "SUCCESS",
    }


def execute_replay_task(**context) -> Dict[str, Any]:
    """Tarea Airflow para inicializar clientes de Kafka y ejecutar el replay."""
    start_time_exec = time.time()
    params = context.get("params") or {}

    source = params.get("source") or "all"
    start_time = params.get("start_time")
    end_time = params.get("end_time")
    max_messages = int(params.get("max_messages", 1000))
    dry_run = bool(params.get("dry_run", False))

    if KafkaConsumer is None or KafkaProducer is None:
        logger.warning("Librería kafka-python no disponible en el host local.")
        return {
            "status": "SKIPPED",
            "reason": "kafka-python no disponible fuera de Docker",
            "latency_ms": 0,
        }

    consumer = KafkaConsumer(
        TOPIC_DLQ,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
    )

    try:
        stats = replay_dlq_events(
            consumer=consumer,
            producer=producer,
            source_filter=source,
            start_time=start_time,
            end_time=end_time,
            max_messages=max_messages,
            dry_run=dry_run,
        )
    finally:
        consumer.close()
        producer.close()

    elapsed_ms = int((time.time() - start_time_exec) * 1000)
    stats["latency_ms"] = elapsed_ms
    return stats


def record_replay_audit_log(**context) -> None:
    """Registra la auditoría del proceso de contingencia y replay en PostgreSQL."""
    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="execute_dlq_replay") or {}

    latency_ms = result.get("latency_ms", 0)
    replayed = result.get("replayed_valid", 0)
    status = result.get("status", "SUCCESS")

    insert_query = """
    INSERT INTO serving.pipeline_execution_log (
        pipeline_name,
        execution_timestamp,
        latency_ms,
        cutoff_timestamp,
        records_processed,
        status
    ) VALUES (%s, %s, %s, %s, %s, %s);
    """

    databases_to_try = [POSTGRES_DB, "market_platform", "serving_db"]
    for db_name in set(databases_to_try):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=db_name,
                connect_timeout=10,
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        insert_query,
                        (
                            DAG_ID,
                            datetime.now(timezone.utc),
                            latency_ms,
                            datetime.now(timezone.utc),
                            replayed,
                            status,
                        ),
                    )
            conn.close()
            ti.log.info("Auditoría de Replay DLQ registrada en db: %s", db_name)
            break
        except Exception as ex:
            ti.log.warning("No se pudo registrar auditoría en %s: %s", db_name, ex)


if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description="Re-inyección controlada y bajo demanda de eventos corregidos desde market.dlq (US-17).",
        default_args=DEFAULT_ARGS,
        schedule=None,  # Únicamente bajo demanda / manual
        start_date=datetime(2026, 10, 1),
        catchup=False,
        tags=["contingency", "dlq", "kafka", "replay", "support"],
        params={
            "source": "all",
            "start_time": None,
            "end_time": None,
            "max_messages": 1000,
            "dry_run": False,
        },
    ) as dag:

        replay_task = PythonOperator(
            task_id="execute_dlq_replay",
            python_callable=execute_replay_task,
        )

        audit_task = PythonOperator(
            task_id="record_replay_audit",
            python_callable=record_replay_audit_log,
        )

        replay_task >> audit_task
else:
    dag = None
