"""
DAG de Airflow para orquestar el recálculo batch con Apache Spark (US-15).
Responsable: Camilo (Épica 5: Airflow DAGs)

Ejecuta el job distribuido `spark/batch_historical.py` contra el clúster Spark
local tras finalizar la ingesta histórica (US-14) o bajo demanda/programación.

Calcula de forma idempotente las velas OHLCV (1m, 5m, 1h, 1d), retornos y métricas
consolidadas sobre MinIO, y registra la auditoría de ejecución en PostgreSQL.
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

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

logger = logging.getLogger(__name__)

DAG_ID = "batch_recompute"

DEFAULT_ARGS = {
    "owner": "Camilo",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Configuración del entorno de Spark
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_JOBS_DIR = os.environ.get("SPARK_JOBS_DIR", "/opt/airflow/spark")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1G")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1G")
SPARK_EXECUTOR_CORES = os.environ.get("SPARK_EXECUTOR_CORES", "1")

# Configuración S3A para que el driver de Spark interactúe con MinIO
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

# Configuración de base de datos PostgreSQL para auditoría
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "serving_db")


def build_batch_command(**context) -> List[str]:
    """
    Construye el comando `spark-submit` con control estricto de recursos,
    memoria de driver/executor, configuraciones S3A y rango de fechas (Criterio 2 BDD).

    Prioriza parámetros transmitidos vía trigger downstream (`dag_run.conf`),
    luego parámetros de ejecución manual (`params`) y finalmente el día anterior.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or {}
    params = context.get("params") or {}

    logical_dt = context.get("logical_date") or context.get("execution_date")
    previous_day = (logical_dt - timedelta(days=1)).date().isoformat()

    start_date = conf.get("start_date") or params.get("start_date") or previous_day
    end_date = conf.get("end_date") or params.get("end_date") or previous_day
    symbols = conf.get("symbols") or params.get("symbols")
    intervals = conf.get("intervals") or params.get("intervals") or "1m,5m,1h,1d"
    metrics = conf.get("metrics") or params.get("metrics") or "ohlcv,returns"

    driver_mem = conf.get("driver_memory") or SPARK_DRIVER_MEMORY
    executor_mem = conf.get("executor_memory") or SPARK_EXECUTOR_MEMORY
    executor_cores = conf.get("executor_cores") or SPARK_EXECUTOR_CORES

    command = [
        "spark-submit",
        "--master",
        SPARK_MASTER_URL,
        "--driver-memory",
        driver_mem,
        "--executor-memory",
        executor_mem,
        "--executor-cores",
        str(executor_cores),
        "--name",
        f"airflow-batch-recompute-{start_date}",
    ]

    for key, value in S3A_CONF.items():
        command += ["--conf", f"{key}={value}"]

    command += [
        f"{SPARK_JOBS_DIR}/batch_historical.py",
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--intervals",
        intervals,
        "--metrics",
        metrics,
    ]

    if symbols:
        command += ["--symbols", symbols]

    return command


def run_batch_recompute(**context) -> Dict[str, Any]:
    """
    Ejecuta el job de Spark Batch transmitiendo la salida en tiempo real
    a los logs de Airflow y validando el código de terminación (Criterio 2 y 3 BDD).
    """
    start_time = time.time()
    task_instance = context["task_instance"]
    command = build_batch_command(**context)

    task_instance.log.info("Lanzando job Spark: %s", " ".join(command))

    # Ejecución con streaming de logs para visibilidad directa en la UI de Airflow
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    full_output = []
    if process.stdout:
        for line in process.stdout:
            task_instance.log.info(line.rstrip())
            full_output.append(line)

    process.wait()
    elapsed_ms = int((time.time() - start_time) * 1000)

    if process.returncode != 0:
        error_msg = f"El job batch_historical.py falló con código de salida {process.returncode}."
        task_instance.log.error(error_msg)
        raise RuntimeError(error_msg)

    task_instance.log.info("Job Spark finalizado exitosamente en %d ms.", elapsed_ms)

    return {
        "status": "SUCCESS",
        "latency_ms": elapsed_ms,
        "command": " ".join(command),
    }


def record_batch_audit_log(**context) -> None:
    """
    Registra el recálculo batch en la tabla de auditoría PostgreSQL (`serving.pipeline_execution_log`).
    """
    ti = context["task_instance"]
    batch_result = ti.xcom_pull(task_ids="execute_spark_batch") or {}

    latency_ms = batch_result.get("latency_ms", 0)
    status = batch_result.get("status", "SUCCESS")

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else {}) or {}
    params = context.get("params") or {}
    logical_dt = context.get("logical_date") or context.get("execution_date")
    previous_day = (logical_dt - timedelta(days=1)).date().isoformat()
    end_date_str = conf.get("end_date") or params.get("end_date") or previous_day

    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    cutoff_timestamp = datetime.combine(end_date, datetime.max.time(), tzinfo=timezone.utc)

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
                            cutoff_timestamp,
                            0,  # Procesamiento batch multiactivo
                            status,
                        ),
                    )
            conn.close()
            ti.log.info("Auditoría batch registrada en db: %s", db_name)
            break
        except Exception as ex:
            ti.log.warning("No se pudo registrar auditoría en %s: %s", db_name, ex)


def handle_task_failure(context: dict) -> None:
    """
    Callback de alerta y auditoría cuando falla el job de Spark tras agotar reintentos (Criterio 3 BDD).
    """
    exception = context.get("exception")
    task_instance = context.get("task_instance")
    logger.error("ALERTA CRÍTICA: Fallo en DAG batch_recompute: %s", exception)

    if task_instance:
        task_instance.log.error(
            "Alerta enviada para tarea %s en ejecución %s. Excepción: %s",
            task_instance.task_id,
            context.get("execution_date"),
            exception,
        )


if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description=(
            "Recálculo batch idempotente de velas OHLCV y retornos históricos "
            "con Apache Spark sobre MinIO (US-15)."
        ),
        default_args=DEFAULT_ARGS,
        schedule="0 2 * * *",  # 02:00 UTC consolidación diaria o activado por ingest_historical
        start_date=datetime(2026, 10, 1),
        catchup=False,
        is_paused_upon_creation=False,
        tags=["batch", "spark", "minio", "lambda-architecture", "serving"],
        on_failure_callback=handle_task_failure,
        params={
            "start_date": None,
            "end_date": None,
            "symbols": None,
            "intervals": "1m,5m,1h,1d",
            "metrics": "ohlcv,returns",
        },
    ) as dag:

        execute_spark_batch = PythonOperator(
            task_id="execute_spark_batch",
            python_callable=run_batch_recompute,
            retries=2,
            retry_delay=timedelta(minutes=2),
        )

        log_batch_audit = PythonOperator(
            task_id="record_batch_audit",
            python_callable=record_batch_audit_log,
        )

        execute_spark_batch >> log_batch_audit
else:
    dag = None
