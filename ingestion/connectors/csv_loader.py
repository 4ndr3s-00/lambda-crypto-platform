"""
Cargador de archivos históricos CSV/JSON (US-08).

Lee un archivo descargado a mano, convierte cada fila al contrato
`market_event` y lo envía a Kafka (ruta speed) o directo a MinIO bajo
`historical/` (ruta batch).

Columnas obligatorias: timestamp, symbol, price, quantity.
Opcionales: exchange (si falta se usa --source), trade_id, interval.

Uso:
    python -m ingestion.connectors.csv_loader --file datos.csv --source binance --target minio
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    from ingestion.connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, ms_to_iso, utc_iso,
    )
    from ingestion.contracts.dlq_message import build_dlq_message, serialize
    from ingestion.contracts.validator import validate_event
except ImportError:  # ejecución desde ingestion/
    from connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, ms_to_iso, utc_iso,
    )
    from contracts.dlq_message import build_dlq_message, serialize
    from contracts.validator import validate_event

from jsonschema.exceptions import ValidationError

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ("timestamp", "symbol", "price", "quantity")
MINIO_BUCKET = "market-lake"
QUARANTINE_BUCKET = "market-quarantine"


def parse_timestamp_ms(value: Any) -> int:
    """
    Acepta ISO-8601 (con o sin zona; sin zona se asume UTC) o epoch numérico.

    Un epoch mayor a 1e11 se interpreta como milisegundos: en segundos ese
    valor correspondería al año 5138, así que la ambigüedad no es real.
    """
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        number = None
    if number is not None:
        return int(number if number > 1e11 else number * 1000)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"timestamp inválido: {value!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _to_float(row: Dict[str, Any], column: str) -> float:
    try:
        return float(str(row[column]).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{column} no es numérico: {row[column]!r}")


class FileLoaderConnector(BaseConnector):
    source = "csv_backfill"
    component = "csv_loader"

    def __init__(self, publisher: EventPublisher, path: Path, origin: str):
        super().__init__(publisher)
        self.path = Path(path)
        #: nombre de la fuente original (binance, coingecko...); define el `exchange` por defecto
        self.origin = origin
        self.exchange = origin

    def fetch(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Entrega (número de fila, fila) en streaming, sin cargar el archivo completo."""
        suffix = self.path.suffix.lower()
        with open(self.path, newline="", encoding="utf-8-sig") as handle:
            if suffix == ".csv":
                reader = csv.DictReader(handle)
                missing = [c for c in REQUIRED_COLUMNS if c not in (reader.fieldnames or [])]
                if missing:
                    # Sin la columna no hay forma de que ninguna fila sea válida: se falla antes de leer
                    raise ValueError(f"Faltan columnas obligatorias: {', '.join(missing)}")
                for line_no, row in enumerate(reader, start=2):  # la fila 1 es el encabezado
                    yield line_no, row
            elif suffix in (".json", ".jsonl"):
                first = handle.read(1)
                handle.seek(0)
                if first == "[":
                    for line_no, row in enumerate(json.load(handle), start=1):
                        yield line_no, row
                else:
                    for line_no, line in enumerate(handle, start=1):
                        if line.strip():
                            yield line_no, json.loads(line)
            else:
                raise ValueError(f"Formato no soportado: {suffix} (use .csv, .json o .jsonl)")

    def normalize(self, raw: Tuple[int, Dict[str, Any]]) -> Dict[str, Any]:
        _, row = raw
        absent = [c for c in REQUIRED_COLUMNS if row.get(c) in (None, "")]
        if absent:
            raise ValueError(f"valores vacíos en: {', '.join(absent)}")
        symbol = str(row["symbol"]).strip().upper()
        ts_ms = parse_timestamp_ms(row["timestamp"])
        exchange = str(row.get("exchange") or self.origin).strip().lower()
        trade_id = str(row["trade_id"]).strip() if row.get("trade_id") not in (None, "") else None
        interval = str(row["interval"]).strip() if row.get("interval") not in (None, "") else None
        # Determinístico: recargar el mismo archivo produce los mismos ids y el batch deduplica
        event_id = f"csv_{exchange}_{symbol}_{ts_ms}" + (f"_{trade_id}" if trade_id else "")
        return {
            "event_id": event_id,
            "source": self.source,
            "exchange": exchange,
            "symbol": symbol,
            "event_time": ms_to_iso(ts_ms),
            "ingestion_time": utc_iso(),
            "price": _to_float(row, "price"),
            "quantity": _to_float(row, "quantity"),
            "trade_id": trade_id,
            "interval": interval,
            "schema_version": SCHEMA_VERSION,
        }

    def run(self) -> Dict[str, int]:
        self.stats["unparseable"] = 0
        for raw in self.fetch():
            self.stats["raw"] += 1
            line_no, row = raw
            try:
                event = self.normalize(raw)
            except ValueError as err:
                # Una fila mala no tumba la carga: va a la DLQ con su número de fila
                logger.warning("Fila %d rechazada: %s", line_no, err)
                self.publisher.reject(row, f"fila {line_no}: {err}", self.component, self.source)
                self.stats["unparseable"] += 1
                continue
            if self.publisher.publish(event, component=self.component):
                self.stats["published"] += 1
            else:
                self.stats["rejected"] += 1
        return dict(self.stats)


class MinioSink:
    """Escribe los eventos como JSONL en `historical/source=<fuente>/date=YYYY-MM-DD/`."""

    def __init__(self, source: str, file_stem: str, client: Optional[Any] = None):
        self.source = source
        self.file_stem = file_stem
        self._client = client
        self._by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._rejected: List[Dict[str, Any]] = []

    def _s3(self):
        if self._client is None:
            import boto3
            from botocore.client import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
                aws_access_key_id=os.environ.get("AIRFLOW_MINIO_USER", "airflow_service"),
                aws_secret_access_key=os.environ.get("AIRFLOW_MINIO_PASSWORD", "airflow_secure_lake_pass123"),
                region_name="us-east-1",
                config=Config(signature_version="s3v4"),
            )
        return self._client

    def publish(self, event: Dict[str, Any], component: str = "csv_loader") -> bool:
        try:
            validate_event(event)
        except ValidationError as err:
            self.reject(event, err.message, component)
            return False
        self._by_date[event["event_time"][:10]].append(event)
        return True

    def reject(self, original: Any, reason: str, component: str, source: Optional[str] = None) -> None:
        """Sin Kafka no hay DLQ: los rechazos se guardan en el bucket de cuarentena."""
        logger.warning("Rechazado por %s: %s", component, reason)
        self._rejected.append(build_dlq_message(original, reason, component, source or self.source))

    def flush(self) -> List[str]:
        keys = []
        for day, events in sorted(self._by_date.items()):
            # El nombre depende del archivo de origen: recargarlo sobrescribe, no duplica,
            # y no pisa el events.json que escribe el DAG de ingesta diaria.
            key = f"historical/source={self.source}/date={day}/{self.file_stem}.json"
            body = "\n".join(json.dumps(e) for e in events).encode("utf-8")
            self._s3().put_object(Bucket=MINIO_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")
            keys.append(key)
        self._by_date.clear()
        if self._rejected:
            today = datetime.now(timezone.utc).date().isoformat()
            key = f"source={self.source}/date={today}/{self.file_stem}.rejected.json"
            body = "\n".join(serialize(m) for m in self._rejected).encode("utf-8")
            self._s3().put_object(Bucket=QUARANTINE_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")
            keys.append(f"{QUARANTINE_BUCKET}/{key}")
            self._rejected.clear()
        return keys

    def close(self) -> None:
        pass


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga archivos históricos CSV/JSON a Kafka o MinIO")
    parser.add_argument("--file", required=True, help="Ruta del archivo .csv, .json o .jsonl")
    parser.add_argument("--source", required=True, help="Fuente de los datos (binance, coingecko...)")
    parser.add_argument("--target", required=True, choices=("kafka", "minio"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    args = parser.parse_args(argv)
    if not Path(args.file).is_file():
        parser.error(f"No existe el archivo: {args.file}")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    path = Path(args.file)

    if args.target == "kafka":
        try:
            from ingestion.connectors.kafka_producer import MarketEventProducer
        except ImportError:
            from connectors.kafka_producer import MarketEventProducer
        sink = MarketEventProducer(args.bootstrap)
    else:
        sink = MinioSink(args.source.lower(), path.stem)

    try:
        stats = FileLoaderConnector(sink, path, args.source.lower()).run()
        if args.target == "minio":
            for key in sink.flush():
                logger.info("Escrito s3://%s", key if key.startswith(QUARANTINE_BUCKET) else f"{MINIO_BUCKET}/{key}")
        else:
            sink.flush()
    finally:
        sink.close()

    print(f"Filas leídas: {stats['raw']} | publicadas: {stats['published']} | "
          f"rechazadas por contrato: {stats['rejected']} | filas ilegibles: {stats['unparseable']}")
    return 0 if stats["raw"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
