"""
DAG de Airflow para la ingesta histórica diaria de datos de mercado (US-14).
Responsable: Camilo (Épica 5: Airflow DAGs)

Extrae periódicamente datos de mercado cerrados de APIs públicas (Binance REST),
los almacena de forma auditable e inmutable en el Data Lake de MinIO bajo el prefijo
funcional `s3://market-lake/historical/source=binance/date=YYYY-MM-DD/`, y registra
la auditoría del intervalo completado en PostgreSQL (`serving.pipeline_execution_log`).

Al culminar exitosamente, dispara de forma sincronizada el DAG `batch_recompute` (US-15).
"""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.client import Config
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Asegurar que el directorio de conectores sea importable tanto en Docker como en local
INGESTION_DIR = os.environ.get("INGESTION_DIR", "/opt/airflow/ingestion")
if INGESTION_DIR not in sys.path:
    sys.path.insert(0, INGESTION_DIR)
# También incluir la raíz del proyecto para ejecuciones locales
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from ingestion.connectors.binance_rest import extract_daily_market_data, DEFAULT_SYMBOLS
except ImportError:
    try:
        from connectors.binance_rest import extract_daily_market_data, DEFAULT_SYMBOLS
    except ImportError:
        extract_daily_market_data = None
        DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

logger = logging.getLogger(__name__)

DAG_ID = "ingest_historical"

DEFAULT_ARGS = {
    "owner": "Camilo",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# Configuración de almacenamiento MinIO (S3)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = "market-lake"
AWS_ACCESS_KEY_ID = (
    os.environ.get("AIRFLOW_MINIO_USER")
    or os.environ.get("AWS_ACCESS_KEY_ID")
    or "airflow_service"
)
AWS_SECRET_ACCESS_KEY = (
    os.environ.get("AIRFLOW_MINIO_PASSWORD")
    or os.environ.get("AWS_SECRET_ACCESS_KEY")
    or "airflow_secure_lake_pass123"
)

# Configuración de base de datos PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "serving_db")


def get_s3_client():
    """Inicializa el cliente de S3 para MinIO con soporte para endpoints locales."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def get_target_date(context: dict) -> date:
    """
    Determina la fecha objetivo a procesar.

    Por defecto toma el día anterior a la fecha lógica de ejecución (`logical_date - 1`).
    Permite sobrescribir la fecha en tiempo de ejecución mediante el parámetro `start_date`.
    """
    params = context.get("params") or {}
    custom_date = params.get("start_date")
    if custom_date:
        return datetime.strptime(custom_date, "%Y-%m-%d").date()

    logical_dt = context.get("logical_date") or context.get("execution_date")
    return (logical_dt - timedelta(days=1)).date()


def run_extract_and_load(**context) -> dict:
    """
    Tarea principal de extracción: descarga los eventos del día anterior desde
    Binance REST, los serializa en JSONL y los sube a MinIO en la ruta funcional.
    """
    start_time = time.time()
    target_date = get_target_date(context)
    params = context.get("params") or {}

    symbols_param = params.get("symbols")
    if symbols_param:
        symbols = [s.strip().upper() for s in symbols_param.split(",") if s.strip()]
    else:
        symbols = DEFAULT_SYMBOLS

    logger.info(
        "Iniciando ingesta histórica para fecha %s y símbolos %s",
        target_date.isoformat(),
        symbols,
    )

    if extract_daily_market_data is None:
        raise RuntimeError("No se pudo cargar el conector de Binance REST (ingestion/connectors/binance_rest.py)")

    events = extract_daily_market_data(target_date=target_date, symbols=symbols)
    records_count = len(events)
    logger.info("Eventos extraídos y validados con éxito: %d", records_count)

    # Persistencia en MinIO bajo el prefijo funcional estricto (Lambda Pattern)
    s3_key = f"historical/source=binance/date={target_date.isoformat()}/events.json"
    s3_client = get_s3_client()

    # Formato JSONL (un evento por línea) para ingesta directa y eficiente con Spark
    payload = "\n".join(json.dumps(event) for event in events).encode("utf-8")
    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=s3_key,
        Body=payload,
        ContentType="application/x-ndjson",
    )
    logger.info("Datos persistidos en MinIO: s3://%s/%s", MINIO_BUCKET, s3_key)

    elapsed_ms = int((time.time() - start_time) * 1000)

    # Pasar métricas al siguiente paso mediante XCom
    return {
        "target_date": target_date.isoformat(),
        "records_processed": records_count,
        "latency_ms": elapsed_ms,
        "status": "SUCCESS",
        "s3_path": f"s3://{MINIO_BUCKET}/{s3_key}",
    }


def record_audit_log(**context) -> None:
    """
    Registra el estado y las métricas de la ejecución en la tabla de auditoría
    `serving.pipeline_execution_log` de PostgreSQL (Criterio 3 BDD).
    """
    ti = context["task_instance"]
    ingest_result = ti.xcom_pull(task_ids="extract_and_load_minio")

    if not ingest_result:
        raise ValueError("No se encontraron resultados de la tarea previa en XCom.")

    target_date_str = ingest_result["target_date"]
    records_processed = ingest_result["records_processed"]
    latency_ms = ingest_result["latency_ms"]
    status = ingest_result.get("status", "SUCCESS")

    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    cutoff_timestamp = datetime.combine(
        target_date,
        datetime.max.time(),
        tzinfo=timezone.utc,
    )

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

    # Intentar conexión priorizando serving_db, con fallback a market_platform
    databases_to_try = [POSTGRES_DB, "market_platform", "serving_db"]
    connected = False
    last_error = None

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
                            cutoff_timestamp,
                            records_processed,
                            status,
                        ),
                    )
            conn.close()
            connected = True
            logger.info(
                "Auditoría registrada exitosamente en PostgreSQL (db: %s, records: %d, latency: %d ms)",
                db_name,
                records_processed,
                latency_ms,
            )
            break
        except Exception as ex:
            last_error = ex
            logger.warning("No se pudo registrar auditoría en db %s: %s", db_name, ex)

    if not connected:
        logger.error("Error crítico al registrar auditoría en PostgreSQL: %s", last_error)
        raise RuntimeError(f"Fallo al registrar auditoría en PostgreSQL: {last_error}")


with DAG(
    dag_id=DAG_ID,
    description=(
        "Extracción periódica de datos históricos desde Binance REST API, "
        "persistencia en MinIO (historical/) y auditoría en PostgreSQL."
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # Diario a las 00:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["ingestion", "binance", "historical", "minio", "lambda-architecture"],
    params={
        "start_date": None,
        "symbols": "BTCUSDT,ETHUSDT,SOLUSDT",
    },
) as dag:

    extract_and_load_minio = PythonOperator(
        task_id="extract_and_load_minio",
        python_callable=run_extract_and_load,
    )

    log_audit = PythonOperator(
        task_id="record_pipeline_audit",
        python_callable=record_audit_log,
    )

    # Disparar automáticamente el DAG de recálculo batch tras la ingesta exitosa
    trigger_batch = TriggerDagRunOperator(
        task_id="trigger_batch_recompute",
        trigger_dag_id="batch_recompute",
        conf={
            "start_date": "{{ ti.xcom_pull(task_ids='extract_and_load_minio')['target_date'] }}",
            "end_date": "{{ ti.xcom_pull(task_ids='extract_and_load_minio')['target_date'] }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Pipeline de dependencias
    extract_and_load_minio >> log_audit >> trigger_batch
