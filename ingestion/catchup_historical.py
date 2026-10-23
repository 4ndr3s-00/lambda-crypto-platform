"""
Catch-up de histórico real (US-14, extiende dags/ingest_historical.py).

Pensado para un proyecto académico donde Streaming NO corre 24/7: antes de
una demo, detecta cuál fue el último día ya guardado en
`historical/source=binance/` y trae de Binance REST solo los días que faltan
hasta ayer (ni más, ni menos), para que la capa Batch tenga datos frescos sin
depender de que el stack haya estado encendido todo el tiempo.

Uso (dentro del contenedor de Airflow, que ya tiene internet y `ingestion/`
montado):

    python3 -m ingestion.catchup_historical

Es idempotente: si ya está al día, no hace ninguna llamada a Binance.
"""

import json
import logging
import os
import subprocess
import sys
from datetime import date, timedelta

import boto3
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
