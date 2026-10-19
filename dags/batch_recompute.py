"""
DAG de Airflow para orquestar el recálculo batch con Spark (US-15).

Lanza `spark/batch_historical.py` contra el clúster Spark local, lo que
permite programar backfills diarios y reprocesar periodos cerrados de forma
idempotente.

El DAG se activa manualmente o según el schedule configurado, y acepta
parámetros en tiempo de ejecución (`start_date`, `end_date` y `symbols`) para
disparar recálculos concretos sin tocar el código.
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Parámetros que el DAG admite en tiempo de ejecución.
DAG_ID = "batch_recompute"

# Job de Spark y sus argumentos por defecto. El rango de fechas se recalcula
# en cada ejecución: por defecto se procesa el día anterior.
DEFAULT_ARGS = {
    "owner": "Andrés Blanco",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Directorio donde el contenedor de Airflow monta el código de los jobs.
# Se puede sobrescribir con la variable de entorno `SPARK_JOBS_DIR` para
# ejecutar el DAG contra otra ruta durante las pruebas.
SPARK_JOBS_DIR = os.environ.get("SPARK_JOBS_DIR", "/opt/airflow/spark")

# Configuración S3A que el driver de Spark necesita para hablar con MinIO.
S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
}


def build_batch_command(**context):
    """
    Construye el comando de `spark-submit` con el rango de fechas de la run.

    Por defecto se recalcula el día anterior al de la ejecución, que es la
    ventana habitual de consolidación del batch. Los parámetros `start_date` y
    `end_date` permiten disparar backfills arbitrarios.
    """
    params = context["params"] or {}
    execution_date = context["logical_date"] or context["execution_date"]
    previous_day = (execution_date - timedelta(days=1)).date().isoformat()

    start_date = params.get("start_date") or previous_day
    end_date = params.get("end_date") or previous_day
    symbols = params.get("symbols")
    intervals = params.get("intervals") or "1m,5m,1h,1d"

    command = [
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
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
    ]
    if symbols:
        command += ["--symbols", symbols]

    return command


def run_batch_recompute(**context):
    """Ejecuta el job de Spark Batch y propaga su código de salida."""
    command = build_batch_command(**context)
    context["task_instance"].log.info("Ejecutando: %s", " ".join(command))

    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.stdout:
        context["task_instance"].log.info(completed.stdout)
    if completed.stderr:
        context["task_instance"].log.info(completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            f"El job batch_historical.py falló con código {completed.returncode}."
        )

    return completed.stdout


with DAG(
    dag_id=DAG_ID,
    description=(
        "Recálculo idempotente de velas OHLCV y retornos sobre el histórico "
        "de MinIO mediante Spark Batch."
    ),
    default_args=DEFAULT_ARGS,
    schedule="0 2 * * *",  # 02:00 UTC: consolidación del día anterior.
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["batch", "spark", "minio", "lambda-architecture"],
    params={
        "start_date": None,
        "end_date": None,
        "symbols": None,
        "intervals": "1m,5m,1h,1d",
    },
) as dag:
    batch_recompute = PythonOperator(
        task_id="batch_recompute",
        python_callable=run_batch_recompute,
        # El job es idempotente: un reintento sobrescribe las mismas
        # particiones sin duplicar filas.
        retries=1,
    )
