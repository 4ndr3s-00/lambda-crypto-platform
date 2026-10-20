"""
DAG de Airflow para la compactación periódica de archivos pequeños en MinIO (US-16).
Responsable: Camilo (Épica 5: Airflow DAGs)

Detecta particiones en `s3://market-lake/` que contengan más de 20 archivos
por debajo del umbral óptimo (< 16 MB) y ejecuta jobs Spark con `coalesce`
para consolidarlos en archivos de ~128 MB, garantizando la integridad de filas
y registrando auditoría en PostgreSQL.
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

try:
    import boto3
    from botocore.client import Config
except ImportError:
    boto3 = None
    Config = None

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

DAG_ID = "compact_files"

DEFAULT_ARGS = {
    "owner": "Camilo",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Configuración MinIO
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
LAKE_BUCKET = "market-lake"
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

# Configuración Spark
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_JOBS_DIR = os.environ.get("SPARK_JOBS_DIR", "/opt/airflow/spark")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "1G")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "1G")

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

# Configuración PostgreSQL
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "serving_db")


def get_s3_client():
    if boto3 is None:
        raise RuntimeError("boto3 no está disponible en este entorno.")
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4") if Config else None,
    )


def scan_small_file_partitions(**context) -> List[Dict[str, Any]]:
    """
    Identifica particiones con más de 20 archivos por debajo de 16MB (Criterio 1 BDD).
    """
    params = context.get("params") or {}
    bucket = params.get("bucket", LAKE_BUCKET)
    prefixes = params.get("prefixes", "historical/,batch-results/").split(",")
    min_files = int(params.get("min_file_count", 20))
    threshold_bytes = int(params.get("size_threshold_mb", 16)) * 1024 * 1024

    s3 = get_s3_client()
    partition_map: Dict[str, List[dict]] = {}

    for prefix in prefixes:
        prefix = prefix.strip()
        if not prefix:
            continue

        paginator = s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    size = obj["Size"]

                    # Omitir marcadores de directorio o metadatos
                    if key.endswith("/") or "/_" in key:
                        continue

                    # Extraer directorio de partición
                    partition_dir = key.rsplit("/", 1)[0]
                    if partition_dir not in partition_map:
                        partition_map[partition_dir] = []
                    partition_map[partition_dir].append({"key": key, "size": size})
        except Exception as ex:
            logger.warning("Error escaneando prefijo %s: %s", prefix, ex)

    candidates = []
    for part_dir, files in partition_map.items():
        small_files = [f for f in files if f["size"] < threshold_bytes]
        if len(small_files) >= min_files:
            # Determinar formato a partir de la extensión
            sample_key = files[0]["key"]
            fmt = "parquet" if sample_key.endswith(".parquet") else "json"

            candidates.append({
                "partition_dir": part_dir,
                "s3a_uri": f"s3a://{bucket}/{part_dir}/",
                "file_count": len(files),
                "small_file_count": len(small_files),
                "total_bytes": sum(f["size"] for f in files),
                "format": fmt,
            })

    logger.info("Escaneo finalizado: %d particiones candidatas a compactación", len(candidates))
    return candidates


def run_compaction(**context) -> Dict[str, Any]:
    """
    Ejecuta el job Spark de compactación para cada partición candidata (Criterio 2 BDD).
    """
    start_time = time.time()
    ti = context["task_instance"]
    candidates = ti.xcom_pull(task_ids="scan_small_file_partitions") or []

    if not candidates:
        ti.log.info("No se encontraron particiones con exceso de archivos pequeños. Proceso concluido.")
        return {"compacted_partitions": 0, "status": "SKIPPED", "latency_ms": 0}

    compacted_count = 0
    total_files_reduced = 0

    for candidate in candidates:
        s3a_uri = candidate["s3a_uri"]
        file_format = candidate["format"]

        ti.log.info("Compactando partición: %s (%d archivos)", s3a_uri, candidate["file_count"])

        command = [
            "spark-submit",
            "--master",
            SPARK_MASTER_URL,
            "--driver-memory",
            SPARK_DRIVER_MEMORY,
            "--executor-memory",
            SPARK_EXECUTOR_MEMORY,
            "--name",
            f"compact-{candidate['partition_dir'].replace('/', '_')}",
        ]

        for key, value in S3A_CONF.items():
            command += ["--conf", f"{key}={value}"]

        command += [
            f"{SPARK_JOBS_DIR}/compact_partition.py",
            "--partition-uri",
            s3a_uri,
            "--format",
            file_format,
            "--target-size-mb",
            "128",
        ]

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in process.stdout:
            ti.log.info(line.rstrip())

        process.wait()
        if process.returncode != 0:
            raise RuntimeError(f"Fallo en compactación de partición {s3a_uri} con código {process.returncode}")

        compacted_count += 1
        total_files_reduced += candidate["file_count"]

    elapsed_ms = int((time.time() - start_time) * 1000)
    return {
        "compacted_partitions": compacted_count,
        "files_reduced": total_files_reduced,
        "latency_ms": elapsed_ms,
        "status": "SUCCESS",
    }


def record_compaction_audit(**context) -> None:
    """Registra la auditoría en PostgreSQL."""
    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="execute_compaction") or {}

    latency_ms = result.get("latency_ms", 0)
    status = result.get("status", "SUCCESS")
    records_processed = result.get("compacted_partitions", 0)

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
                            records_processed,
                            status,
                        ),
                    )
            conn.close()
            ti.log.info("Auditoría de compactación registrada en db: %s", db_name)
            break
        except Exception as ex:
            ti.log.warning("No se pudo registrar auditoría en %s: %s", db_name, ex)


if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description="Fusión periódica de archivos pequeños en particiones de MinIO (US-16).",
        default_args=DEFAULT_ARGS,
        schedule="0 3 * * 0",  # Semanal: domingos a las 03:00 UTC (baja carga)
        start_date=datetime(2026, 10, 1),
        catchup=False,
        tags=["maintenance", "compaction", "spark", "minio"],
        params={
            "bucket": "market-lake",
            "prefixes": "historical/,batch-results/",
            "min_file_count": 20,
            "size_threshold_mb": 16,
        },
    ) as dag:

        scan_partitions = PythonOperator(
            task_id="scan_small_file_partitions",
            python_callable=scan_small_file_partitions,
        )

        compact_tasks = PythonOperator(
            task_id="execute_compaction",
            python_callable=run_compaction,
        )

        audit_task = PythonOperator(
            task_id="record_compaction_audit",
            python_callable=record_compaction_audit,
        )

        scan_partitions >> compact_tasks >> audit_task
else:
    dag = None
