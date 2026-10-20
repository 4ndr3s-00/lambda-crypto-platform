"""Read-only data access for the serving layer.

Postgres is the source of truth when the Docker stack is up. Every method has
a deterministic demo fallback so `/api/*` and `/api/chat` work on a laptop
without Kafka, Spark or MinIO running.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from . import market_data
from .config import Settings, get_settings
from .sql_guardrails import validate_sql

try:  # optional at import time; the fallback keeps the API usable
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:  # noqa: BLE001
    psycopg2 = None  # type: ignore[assignment]
    RealDictCursor = None  # type: ignore[assignment]

LOGGER = logging.getLogger("iq_analyzer.query")


class QueryServiceError(RuntimeError):
    """Raised when a read-only query cannot be executed."""


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if value is None:
        return market_data.iso(market_data.utc_now())
    return str(value)


class QueryService:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._db_checked = False
        self._db_ok = False
        self._db_checked_at = 0.0

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _db(self) -> Iterator[Any]:
        if psycopg2 is None:
            raise QueryServiceError("psycopg2 no está instalado")
        connection = psycopg2.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
            dbname=self.settings.postgres_db,
            connect_timeout=2,
            options="-c statement_timeout=5000",
        )
        try:
            connection.set_session(readonly=True, autocommit=True)
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
        finally:
            connection.close()

    def database_available(self) -> bool:
        now = time.monotonic()
        if self._db_checked and now - self._db_checked_at < 30.0:
            return self._db_ok
        self._db_checked = True
        self._db_checked_at = now
        try:
            with self._db() as cursor:
                cursor.execute("SELECT 1 AS ok")
                self._db_ok = bool(cursor.fetchone())
        except Exception as exc:  # noqa: BLE001 - absence of DB is expected in demo mode
            LOGGER.info("Postgres no disponible, usando fallback determinista: %s", exc)
            self._db_ok = False
        return self._db_ok

    def _query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._db() as cursor:
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Public read endpoints
    # ------------------------------------------------------------------

    def get_assets(self) -> list[dict[str, Any]]:
        fallback = market_data.assets()
        if not self.database_available():
            return fallback
        try:
            rows = self._query(
                """
                SELECT symbol, base_asset, quote_asset, asset_type, is_active
                FROM serving.assets
                WHERE is_active = TRUE
                ORDER BY symbol
                """
            )
            if not rows:
                return fallback
            by_symbol = {item["symbol"]: item for item in fallback}
            return [
                {
                    **by_symbol.get(
                        row["symbol"],
                        {
                            "symbol": row["symbol"],
                            "base": row.get("base_asset") or row["symbol"][:-4],
                            "quote": row.get("quote_asset") or "USDT",
                            "source": "Postgres serving.assets",
                            "exchange": "binance",
                        },
                    ),
                    "active": bool(row.get("is_active", True)),
                }
                for row in rows
            ]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("get_assets fallback: %s", exc)
            return fallback

    def get_ohlcv(self, symbol: str, interval: str = "15m", limit: int = 96) -> dict[str, Any]:
        normalized_symbol = market_data.normalize_symbol(symbol)
        normalized_interval = market_data.normalize_interval(interval)
        normalized_limit = max(1, min(int(limit), 500))
        fallback_candles = market_data.generate_candles(
            normalized_symbol, normalized_interval, normalized_limit
        )
        fallback = {
            "symbol": normalized_symbol,
            "interval": normalized_interval,
            "limit": normalized_limit,
            "candles": fallback_candles,
            "source": market_data.SYMBOLS[normalized_symbol]["source"],
            "provenance": "SPEED_LAYER",
            "watermark": "event-time · 2s",
            "cutoff_time": fallback_candles[-1]["timestamp"],
        }
        if not self.database_available():
            return fallback
        try:
            rows = self._query(
                """
                SELECT bucket_start, open_price, high_price, low_price, close_price, volume
                FROM serving.ohlcv_reconciled
                WHERE symbol = %s AND timeframe = %s
                ORDER BY bucket_start DESC
                LIMIT %s
                """,
                (normalized_symbol, normalized_interval, normalized_limit),
            )
            if not rows:
                return fallback
            candles = [
                {
                    "timestamp": _iso(row["bucket_start"]),
                    "open": float(row["open_price"]),
                    "high": float(row["high_price"]),
                    "low": float(row["low_price"]),
                    "close": float(row["close_price"]),
                    "volume": float(row["volume"]),
                }
                for row in reversed(rows)
            ]
            return {
                **fallback,
                "candles": candles,
                "source": "serving.ohlcv_reconciled (Postgres)",
                "cutoff_time": candles[-1]["timestamp"],
            }
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("get_ohlcv fallback: %s", exc)
            return fallback

    def get_metrics(self, symbol: str, window: str = "24h") -> dict[str, Any]:
        # The synthesized series already mirrors the speed-layer metrics and
        # is deterministic per minute, which keeps the dashboard stable while
        # the realtime_metrics table is populated by Spark.
        return market_data.compute_metrics(symbol, window=window)

    def get_pipeline_status(self) -> dict[str, Any]:
        fallback = market_data.pipeline_status()
        if not self.database_available():
            return fallback
        try:
            rows = self._query(
                """
                SELECT component, last_run, status, consumer_lag, metrics_summary
                FROM serving.pipeline_status
                """
            )
            if not rows:
                return fallback
            by_component = {str(row["component"]).lower(): row for row in rows}
            merged = json.loads(json.dumps(fallback))
            if "kafka" in by_component:
                kafka = by_component["kafka"]
                merged["kafka"]["status"] = kafka.get("status") or merged["kafka"]["status"]
                merged["kafka"]["consumer_lag"] = int(kafka.get("consumer_lag") or 0)
            if "spark" in by_component:
                merged["spark"]["status"] = by_component["spark"].get("status") or merged["spark"]["status"]
            if "airflow" in by_component:
                airflow = by_component["airflow"]
                merged["airflow"]["status"] = airflow.get("status") or merged["airflow"]["status"]
                merged["airflow"]["last_run"] = _iso(airflow.get("last_run"))
            if "minio" in by_component:
                merged["minio"]["status"] = by_component["minio"].get("status") or merged["minio"]["status"]
            if "dlq" in by_component:
                metrics = by_component["dlq"].get("metrics_summary") or {}
                merged["dlq"]["rejected_events"] = int(metrics.get("rejected_events", 0))
            merged["timestamp"] = market_data.iso(market_data.utc_now())
            return merged
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("get_pipeline_status fallback: %s", exc)
            return fallback

    def get_catalog_metadata(self, dataset: str = "market_ohlcv") -> dict[str, Any]:
        path: Path = self.settings.catalog_path
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    datasets = payload.get("datasets", payload)
                    if isinstance(datasets, dict) and dataset in datasets:
                        return dict(datasets[dataset])
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("catalog JSON ilegible (%s), usando fallback: %s", path, exc)
        return market_data.catalog_metadata(dataset)

    # ------------------------------------------------------------------
    # Guarded read-only execution (used by tools / future SQL inspector)
    # ------------------------------------------------------------------

    def execute_readonly(self, sql: str) -> dict[str, Any]:
        result = validate_sql(sql)
        result.raise_for_error()
        if not self.database_available():
            raise QueryServiceError("Postgres no disponible para ejecutar SQL de lectura")
        try:
            rows = self._query(result.sql)
        except Exception as exc:  # noqa: BLE001
            raise QueryServiceError(f"fallo al ejecutar SQL: {exc}") from exc
        return {
            "sql": result.sql,
            "rows": rows,
            "row_count": len(rows),
            "limit": result.limit,
            "timeout_seconds": result.timeout_seconds,
            "tables": list(result.tables),
            "cutoff_time": market_data.iso(market_data.utc_now()),
        }

    def health(self) -> dict[str, Any]:
        db_ok = self.database_available()
        rag_ok = self.settings.rag_index_path.exists()
        return {
            "database": "UP" if db_ok else "FALLBACK",
            "rag_index": "READY" if rag_ok else "MISSING",
        }
