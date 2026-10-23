"""
DAG de Airflow para orquestar la reconciliación Batch/Speed de la Serving Layer (US-21).
Responsable: Andrés Blanco (Épica Spark / Serving Layer)

Ejecuta `spark/serving_reconcile.py` cada pocos minutos para que
`serving.ohlcv_reconciled` refleje tanto el histórico consolidado por
`batch_recompute.py` (MinIO) como el último snapshot del Speed Layer
(`serving.realtime_metrics`, poblado por `streaming_market.py`).

Sin este DAG, `serving_reconcile.py` solo se ejercita en los tests: la tabla
que lee `api/query_service.get_ohlcv()` nunca se actualiza y la API cae
siempre al dato sintético de `api/market_data.py`.
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None
    PythonOperator = None

logger = logging.getLogger(__name__)

DAG_ID = "serving_reconcile"

DEFAULT_ARGS = {
    "owner": "Andres Blanco",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_JOBS_DIR = os.environ.get("SPARK_JOBS_DIR", "/opt/airflow/spark")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1G")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1G")
SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": (
        os.environ.get("SPARK_MINIO_USER")
        or os.environ.get("AWS_ACCESS_KEY_ID")
        or "spark_service"
    ),
    "spark.hadoop.fs.s3a.secret.key": (
        os.environ.get("SPARK_MINIO_PASSWORD")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
        or "spark_secure_lake_pass123"
    ),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
}

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
SERVING_DB_NAME = os.environ.get("SERVING_DB_NAME", "serving_db")
SERVING_DB_USER = os.environ.get("SERVING_DB_USER", os.environ.get("POSTGRES_USER", "postgres"))
SERVING_DB_PASSWORD = os.environ.get(
    "SERVING_DB_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
)


def build_reconcile_command(**context) -> List[str]:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or {}
    params = context.get("params") or {}

    intervals = conf.get("intervals") or params.get("intervals") or "1m,5m,15m,1h,1d"
    symbols = conf.get("symbols") or params.get("symbols")

    command = [
        "spark-submit",
        "--master",
        SPARK_MASTER_URL,
        "--driver-memory",
        SPARK_DRIVER_MEMORY,
        "--executor-memory",
        SPARK_EXECUTOR_MEMORY,
        "--executor-cores",
        str(SPARK_EXECUTOR_CORES),
        "--packages",
        "org.postgresql:postgresql:42.7.3",
        "--name",
        "airflow-serving-reconcile",
    ]

    for key, value in S3A_CONF.items():
        command += ["--conf", f"{key}={value}"]

    command += [
        f"{SPARK_JOBS_DIR}/serving_reconcile.py",
        "--intervals",
        intervals,
        "--postgres-host",
        POSTGRES_HOST,
        "--postgres-port",
        str(POSTGRES_PORT),
        "--postgres-db",
        SERVING_DB_NAME,
        "--postgres-user",
        SERVING_DB_USER,
        "--postgres-password",
        SERVING_DB_PASSWORD,
    ]

    if symbols:
        command += ["--symbols", symbols]

    return command


def run_serving_reconcile(**context) -> Dict[str, Any]:
    start_time = time.time()
    task_instance = context["task_instance"]
    command = build_reconcile_command(**context)

    # No se loggea el comando completo: contiene la contraseña de Postgres.
    task_instance.log.info("Lanzando job Spark de reconciliación serving_reconcile.py")

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout:
        for line in process.stdout:
            task_instance.log.info(line.rstrip())

    process.wait()
    elapsed_ms = int((time.time() - start_time) * 1000)

    if process.returncode != 0:
        error_msg = f"serving_reconcile.py falló con código de salida {process.returncode}."
        task_instance.log.error(error_msg)
        raise RuntimeError(error_msg)

    task_instance.log.info("Reconciliación finalizada en %d ms.", elapsed_ms)
    return {"status": "SUCCESS", "latency_ms": elapsed_ms}


def handle_task_failure(context: dict) -> None:
    exception = context.get("exception")
    logger.error("ALERTA: Fallo en DAG serving_reconcile: %s", exception)


if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description=(
            "Reconciliación periódica Batch/Speed (US-21): escribe "
            "serving.ohlcv_reconciled a partir de MinIO y serving.realtime_metrics."
        ),
        default_args=DEFAULT_ARGS,
        schedule=timedelta(minutes=5),
        start_date=datetime(2026, 10, 1),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        tags=["serving", "spark", "reconciliation", "lambda-architecture"],
        on_failure_callback=handle_task_failure,
        params={
            "intervals": "1m,5m,15m,1h,1d",
            "symbols": None,
        },
    ) as dag:

        execute_reconcile = PythonOperator(
            task_id="execute_serving_reconcile",
            python_callable=run_serving_reconcile,
            retries=2,
            retry_delay=timedelta(minutes=1),
        )
else:
    dag = None
