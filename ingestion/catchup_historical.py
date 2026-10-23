"""
Catch-up de histórico real (US-14, extiende dags/ingest_historical.py).

Pensado para un proyecto académico donde Streaming NO corre 24/7: antes de
una demo, detecta cuál fue el último día ya guardado en
`historical/source=binance/` y trae de Binance REST solo los días que faltan
hasta ayer (ni más, ni menos), para que la capa Batch tenga datos frescos sin
depender de que el stack haya estado encendido todo el tiempo.

También mantiene una ventana móvil de `RETENTION_DAYS` días: lo que quede más
viejo que el corte se poda en las 3 capas (histórico crudo en MinIO, Parquet
de Batch en MinIO, y la tabla reconciliada en Postgres) para que el proyecto
no acumule datos indefinidamente demo tras demo.

Uso (dentro del contenedor de Airflow, que ya tiene internet y `ingestion/`
montado):

    python3 -m ingestion.catchup_historical

Es idempotente: si ya está al día y dentro de la ventana, no hace nada.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import date, timedelta

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    from ingestion.connectors.binance_rest import extract_daily_market_data, DEFAULT_SYMBOLS
except ImportError:
    from connectors.binance_rest import extract_daily_market_data, DEFAULT_SYMBOLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("catchup")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = "market-lake"
AWS_ACCESS_KEY_ID = os.environ.get("AIRFLOW_MINIO_USER") or os.environ.get("AWS_ACCESS_KEY_ID") or "airflow_service"
AWS_SECRET_ACCESS_KEY = os.environ.get("AIRFLOW_MINIO_PASSWORD") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "airflow_secure_lake_pass123"
MAX_CATCHUP_DAYS = int(os.environ.get("CATCHUP_MAX_DAYS", "120"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "100"))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")
SERVING_DB_NAME = os.environ.get("SERVING_DB_NAME", "serving_db")


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def _last_registered_date(s3) -> date | None:
    """Fecha más reciente ya presente en historical/source=binance/, o None si no hay nada."""
    paginator = s3.get_paginator("list_objects_v2")
    dates = []
    for page in paginator.paginate(
        Bucket=MINIO_BUCKET, Prefix="historical/source=binance/date=", Delimiter="/"
    ):
        for prefix in page.get("CommonPrefixes", []):
            raw = prefix["Prefix"].rsplit("date=", 1)[-1].rstrip("/")
            try:
                dates.append(date.fromisoformat(raw))
            except ValueError:
                continue
    return max(dates) if dates else None


def _missing_date_range(last_registered: date | None) -> list[date]:
    yesterday = date.today() - timedelta(days=1)
    if last_registered is None:
        start = yesterday - timedelta(days=MAX_CATCHUP_DAYS - 1)
    else:
        start = last_registered + timedelta(days=1)
    if start > yesterday:
        return []
    span = (yesterday - start).days + 1
    if span > MAX_CATCHUP_DAYS:
        logger.warning(
            "Faltan %d días, se limita a los últimos %d (CATCHUP_MAX_DAYS) para no saturar Binance.",
            span, MAX_CATCHUP_DAYS,
        )
        start = yesterday - timedelta(days=MAX_CATCHUP_DAYS - 1)
    return [start + timedelta(days=i) for i in range((yesterday - start).days + 1)]


def _trigger_dag(dag_id: str, conf: dict | None = None) -> None:
    cmd = ["airflow", "dags", "trigger", dag_id]
    if conf:
        cmd += ["--conf", json.dumps(conf)]
    subprocess.run(cmd, check=False)


def _prune_s3_prefix(s3, prefix: str, cutoff: date) -> int:
    """Borra todos los objetos bajo `prefix` cuyo date= sea anterior a `cutoff`."""
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix, Delimiter="/"):
        for sub in page.get("CommonPrefixes", []):
            raw = sub["Prefix"].rsplit("date=", 1)[-1].rstrip("/")
            try:
                day = date.fromisoformat(raw)
            except ValueError:
                continue
            if day >= cutoff:
                continue
            to_delete = [
                {"Key": obj["Key"]}
                for obj_page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=MINIO_BUCKET, Prefix=sub["Prefix"]
                )
                for obj in obj_page.get("Contents", [])
            ]
            if to_delete:
                s3.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": to_delete})
                deleted += len(to_delete)
            logger.info("Podado %s (%d objetos, más viejo que %s)", sub["Prefix"], len(to_delete), cutoff)
    return deleted


def _prune_postgres(cutoff: date) -> int:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, dbname=SERVING_DB_NAME, connect_timeout=5,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM serving.ohlcv_reconciled WHERE bucket_start < %s",
                    (cutoff,),
                )
                return cur.rowcount
    finally:
        conn.close()


def _prune_old_data(latest_date: date) -> None:
    """Mantiene una ventana móvil de RETENTION_DAYS días en las 3 capas."""
    cutoff = latest_date - timedelta(days=RETENTION_DAYS - 1)
    logger.info("Podando datos anteriores a %s (ventana de %d días)...", cutoff, RETENTION_DAYS)

    s3 = _s3_client()
    raw_deleted = _prune_s3_prefix(s3, "historical/source=binance/date=", cutoff)
    ohlcv_deleted = _prune_s3_prefix(s3, "batch-results/metric=ohlcv/date=", cutoff)
    returns_deleted = _prune_s3_prefix(s3, "batch-results/metric=returns/date=", cutoff)

    try:
        pg_deleted = _prune_postgres(cutoff)
    except Exception as exc:  # noqa: BLE001 — la poda es de limpieza, no debe tumbar el catch-up.
        logger.warning("No se pudo podar Postgres: %s", exc)
        pg_deleted = 0

    logger.info(
        "Poda completa: %d objetos crudos, %d ohlcv, %d returns en MinIO; %d filas en Postgres.",
        raw_deleted, ohlcv_deleted, returns_deleted, pg_deleted,
    )


def main() -> int:
    s3 = _s3_client()
    try:
        last = _last_registered_date(s3)
    except ClientError as exc:
        logger.error("No se pudo listar MinIO (¿está levantado?): %s", exc)
        return 1

    missing = _missing_date_range(last)
    if not missing:
        logger.info("Histórico ya al día (último día registrado: %s). Nada que traer.", last)
        if last is not None:
            _prune_old_data(last)
        return 0

    logger.info(
        "Último día registrado: %s. Trayendo %d día(s) faltante(s): %s -> %s",
        last, len(missing), missing[0], missing[-1],
    )

    total_events = 0
    for d in missing:
        events = extract_daily_market_data(target_date=d, symbols=DEFAULT_SYMBOLS)
        s3_key = f"historical/source=binance/date={d.isoformat()}/events.json"
        payload = "\n".join(json.dumps(e) for e in events).encode("utf-8")
        s3.put_object(Bucket=MINIO_BUCKET, Key=s3_key, Body=payload, ContentType="application/x-ndjson")
        total_events += len(events)
        logger.info("%s -> %d eventos escritos", d, len(events))

    logger.info("Catch-up completo: %d eventos en %d día(s).", total_events, len(missing))

    logger.info("Disparando batch_recompute para el rango traído...")
    _trigger_dag("batch_recompute", {"start_date": missing[0].isoformat(), "end_date": missing[-1].isoformat()})

    logger.info("Disparando serving_reconcile...")
    _trigger_dag("serving_reconcile")

    _prune_old_data(missing[-1])

    return 0


if __name__ == "__main__":
    sys.exit(main())
