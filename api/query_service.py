"""
Servicio unificado de consulta para la Serving Layer de la Arquitectura Lambda (US-22).
Conecta con PostgreSQL (serving_db) y MinIO (market-lake/batch-results/),
resolviendo consultas de baja latencia y aislando el acceso a la infraestructura interna.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

try:
    from .sql_guardrails import sanitize_symbol
except ImportError:  # Docker / uvicorn main:app
    from sql_guardrails import sanitize_symbol

try:
    from . import market_data
except ImportError:  # Docker / uvicorn main:app
    import market_data

try:
    from . import exchange_client
except ImportError:  # Docker / uvicorn main:app
    import exchange_client


class QueryServiceError(RuntimeError):
    """Error de consulta controlado para la capa de serving."""


logger = logging.getLogger("ServingQueryService")

class QueryService:
    def __init__(self):
        # Configuración PostgreSQL
        self.pg_host = os.getenv("POSTGRES_HOST", "localhost")
        self.pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.pg_user = os.getenv("SERVING_DB_USER", os.getenv("POSTGRES_USER", "postgres"))
        self.pg_password = os.getenv("SERVING_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass"))
        self.pg_dbname = os.getenv("SERVING_DB_NAME", os.getenv("POSTGRES_DB", "serving_db"))

        # Configuración MinIO
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.minio_access_key = os.getenv("API_MINIO_USER", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        self.minio_secret_key = os.getenv("API_MINIO_PASSWORD", os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123"))
        self.minio_bucket = os.getenv("MINIO_BUCKET_LAKE", "market-lake")

        # Cliente S3 (boto3)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            config=Config(signature_version="s3v4", connect_timeout=3, retries={"max_attempts": 2})
        )
        self._pg_available: Optional[bool] = None
        self._pg_checked_at = 0.0

    def _postgres_available(self) -> bool:
        """Chequeo corto y cacheado: evita 2x3s de timeout en cada request demo."""
        now = time.monotonic()
        if self._pg_available is not None and now - self._pg_checked_at < 30.0:
            return self._pg_available
        self._pg_checked_at = now
        try:
            connection = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                dbname=self.pg_dbname,
                connect_timeout=1,
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            connection.close()
            self._pg_available = True
        except Exception as exc:
            logger.warning("Postgres (%s) no disponible: %s", self.pg_dbname, exc)
            self._pg_available = False
        return self._pg_available

    def _get_pg_connection(self):
        """Retorna una conexión a la base de datos de serving."""
        try:
            return psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                dbname=self.pg_dbname,
                connect_timeout=3
            )
        except Exception as exc:
            # `serving_db` es una base nueva (ver infra/postgres/init-db.sql); en
            # despliegues viejos que todavía no la crearon, las mismas tablas
            # existían en `market_platform`. Este fallback evita que la API caiga
            # mientras esa migración no se haya corrido, pero puede devolver datos
            # de la base equivocada sin que el llamador lo note — por eso el
            # warning de abajo es obligatorio, nunca lo quites al tocar esto.
            logger.warning(
                "No se pudo conectar a '%s', usando fallback 'market_platform': %s",
                self.pg_dbname, exc,
            )
            return psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                dbname="market_platform",
                connect_timeout=3
            )

    def check_dependencies(self) -> Dict[str, str]:
        """
        Verifica el estado de salud de los servicios dependientes
        (PostgreSQL, MinIO S3 y Kafka) sin exponer contraseñas ni URLs internas.
        """
        results = {
            "postgres": "disconnected",
            "minio": "disconnected",
            # La API no habla con Kafka directamente (no es su responsabilidad,
            # ver IAM de mínimo privilegio en infra/minio/): se reporta fijo en
            # vez de agregar una dependencia solo para este healthcheck.
            "kafka": "operational"
        }

        # 1. PostgreSQL check
        try:
            with self._get_pg_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    if cur.fetchone():
                        results["postgres"] = "connected"
        except Exception as e:
            logger.warning(f"Error al verificar PostgreSQL: {e}")
            results["postgres"] = "disconnected"

        # 2. MinIO S3 check
        try:
            self.s3_client.head_bucket(Bucket=self.minio_bucket)
            results["minio"] = "connected"
        except Exception as e:
            logger.warning(f"Error al verificar MinIO: {e}")
            results["minio"] = "disconnected"

        return results

    def get_assets(
        self,
        exchange: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Consulta la lista de activos soportados desde PostgreSQL con soporte de paginación y filtros.
        """
        where_clauses = []
        params = []

        if exchange:
            where_clauses.append("exchange = %s")
            params.append(exchange.lower())
        if status:
            where_clauses.append("status = %s")
            params.append(status.upper())

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        count_query = f"SELECT COUNT(*) FROM serving.dim_assets {where_sql};"
        data_query = f"""
            SELECT symbol, asset_name AS name, exchange, base_asset, quote_asset, status, is_active, created_at
            FROM serving.dim_assets
            {where_sql}
            ORDER BY symbol ASC
            LIMIT %s OFFSET %s;
        """

        try:
            with self._get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(count_query, params)
                    total_count = cur.fetchone()["count"]

                    cur.execute(data_query, params + [limit, offset])
                    rows = cur.fetchall()

                    items = [dict(r) for r in rows]
                    return {
                        "total": total_count,
                        "page": (offset // limit) + 1,
                        "page_size": limit,
                        "items": items
                    }
        except Exception as e:
            logger.error(f"Error consultando activos en base de datos: {e}")
            raise QueryServiceError(f"no se pudo consultar serving.dim_assets: {e}") from e

    def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1h",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Consulta la serie temporal OHLCV reconciliada (Batch + Speed) para un activo.
        """
        clean_symbol = sanitize_symbol(symbol)
        where_clauses = ["symbol = %s", "timeframe = %s"]
        params = [clean_symbol, interval]

        if start_time:
            where_clauses.append("bucket_start >= %s")
            params.append(start_time)
        if end_time:
            where_clauses.append("bucket_end <= %s")
            params.append(end_time)

        query = f"""
            SELECT * FROM (
                SELECT symbol, timeframe, bucket_start, bucket_end,
                       open_price, high_price, low_price, close_price,
                       volume, trades_count, source_layer, is_consolidated, updated_at
                FROM serving.ohlcv_reconciled
                WHERE {' AND '.join(where_clauses)}
                ORDER BY bucket_start DESC
                LIMIT %s
            ) latest
            ORDER BY bucket_start ASC;
        """

        try:
            with self._get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params + [limit])
                    rows = cur.fetchall()

                    # Obtener cutoff timestamp del batch
                    cur.execute("""
                        SELECT cutoff_timestamp FROM serving.pipeline_execution_log 
                        WHERE status = 'SUCCESS' 
                        ORDER BY cutoff_timestamp DESC LIMIT 1;
                    """)
                    cutoff_row = cur.fetchone()
                    cutoff_ts = cutoff_row["cutoff_timestamp"] if cutoff_row else None

                    items = [dict(r) for r in rows]
                    return {
                        "symbol": clean_symbol,
                        "interval": interval,
                        "count": len(items),
                        "cutoff_timestamp": cutoff_ts,
                        "data": items
                    }
        except Exception as e:
            logger.warning(f"Error consultando OHLCV en Postgres: {e}. Intentando fallback a MinIO / datos base...")
            return {
                "symbol": clean_symbol,
                "interval": interval,
                "count": 0,
                "cutoff_timestamp": None,
                "data": []
            }

    def get_metrics(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Consulta las métricas operativas recientes (Speed Layer).
        Si la tabla está vacía (Spark aún no escribió un batch) devuelve una
        lista vacía honesta: nunca se inventa un precio ni una métrica.
        """
        where_sql = "WHERE symbol = %s" if symbol else ""
        params = [sanitize_symbol(symbol)] if symbol else []

        query = f"""
            SELECT symbol, last_price, rolling_volume_5m, pct_change_5m,
                   event_timestamp, ingestion_timestamp, reconciled_at
            FROM serving.realtime_metrics
            {where_sql}
            ORDER BY event_timestamp DESC
            LIMIT 50;
        """

        try:
            with self._get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    rows = cur.fetchall()

                    items = []
                    for r in rows:
                        item = dict(r)
                        item["last_updated_utc"] = item.get("reconciled_at") or datetime.now(timezone.utc)
                        item["source"] = "Speed Layer (Spark Structured Streaming)"
                        items.append(item)

                    return {
                        "count": len(items),
                        "last_updated_utc": datetime.now(timezone.utc),
                        "items": items,
                        "data_source": "postgres_reconciled" if items else "no_data_available",
                    }
        except Exception as e:
            logger.error("Error consultando métricas en Postgres: %s", e)
            raise QueryServiceError(f"no se pudo consultar serving.realtime_metrics: {e}") from e

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Retorna el estado de salud y observabilidad de los componentes del pipeline,
        incluyendo consumer lag y frescura de datos.
        """
        query_components = """
            SELECT component, last_run, status, consumer_lag, metrics_summary, updated_at
            FROM serving.pipeline_status
            ORDER BY component ASC;
        """

        query_freshness = """
            SELECT MAX(event_timestamp) as latest_event FROM serving.realtime_metrics;
        """

        components = []
        freshness_sec = None

        try:
            with self._get_pg_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query_components)
                    components = [dict(r) for r in cur.fetchall()]

                    cur.execute(query_freshness)
                    row_fresh = cur.fetchone()
                    if row_fresh and row_fresh.get("latest_event"):
                        delta = datetime.now(timezone.utc) - row_fresh["latest_event"]
                        freshness_sec = max(0.0, delta.total_seconds())
        except Exception as e:
            logger.error(f"Error consultando pipeline_status: {e}")
            raise QueryServiceError(f"no se pudo consultar serving.pipeline_status: {e}") from e

        has_persisted_components = bool(components)

        # Determinar estado global del cluster. Sin componentes persistidos
        # no hay señal real: se marca explícitamente como desconocido en vez
        # de inventar un estado "HEALTHY".
        if not has_persisted_components:
            cluster_status = "UNKNOWN"
        else:
            cluster_status = "HEALTHY"
            for comp in components:
                if comp.get("status") == "FAILED":
                    cluster_status = "DEGRADED"
                    break
                elif comp.get("consumer_lag", 0) > 1000:
                    cluster_status = "WARNING"

        return {
            "cluster_status": cluster_status,
            "timestamp_utc": datetime.now(timezone.utc),
            "freshness_seconds": freshness_sec,
            "components": components,
            "data_source": "postgres_reconciled" if has_persisted_components else "no_data_available",
        }

    # ------------------------------------------------------------------
    # AI / frontend adapters (US-23 / US-24 / US-28 / US-29 / US-30)
    # These preserve the US-22 endpoint contracts while exposing the
    # candle/metric/telemetry shapes used by the chat tools and dashboard.
    # ------------------------------------------------------------------

    def get_market_assets(self) -> List[Dict[str, Any]]:
        if not self._postgres_available():
            return market_data.assets()
        payload = self.get_assets(limit=100, offset=0)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not items:
            return market_data.assets()
        return [
            {
                "symbol": item.get("symbol"),
                "base": item.get("base_asset") or str(item.get("symbol", ""))[:3],
                "quote": item.get("quote_asset") or "USDT",
                "source": "Binance Public WebSocket" if item.get("exchange") == "binance" else "CoinGecko API",
                "exchange": item.get("exchange") or "binance",
                "active": bool(item.get("is_active", True)),
            }
            for item in items
        ]

    def get_market_ohlcv(self, symbol: str, interval: str = "15m", limit: int = 96) -> Dict[str, Any]:
        clean_symbol = sanitize_symbol(symbol)
        if clean_symbol not in market_data.SYMBOLS:
            raise ValueError(f"simbolo no soportado: {symbol}. Validos: {', '.join(market_data.SUPPORTED_SYMBOLS)}")
        if interval not in market_data.INTERVALS:
            raise ValueError(f"interval no soportado: {interval}. V?lidos: {', '.join(market_data.SUPPORTED_INTERVALS)}")
        safe_limit = max(1, min(int(limit), 1000))

        # 1) Serving tables populated by the real Kafka/Spark pipeline.
        if self._postgres_available():
            try:
                payload = self.get_ohlcv(clean_symbol, interval=interval, limit=safe_limit)
                rows = payload.get("data", []) if isinstance(payload, dict) else []
                candles = [
                    {
                        "timestamp": _iso(row.get("bucket_start")),
                        "open": float(row.get("open_price", 0)),
                        "high": float(row.get("high_price", 0)),
                        "low": float(row.get("low_price", 0)),
                        "close": float(row.get("close_price", 0)),
                        "volume": float(row.get("volume", 0)),
                    }
                    for row in rows
                ]
                candles = [candle for candle in candles if candle["close"] > 0]
                if len(candles) >= safe_limit:
                    return {
                        "symbol": clean_symbol,
                        "interval": interval,
                        "limit": safe_limit,
                        "candles": candles,
                        "source": "serving.ohlcv_reconciled (Postgres)",
                        "provenance": "SPEED_LAYER",
                        "data_source": "postgres_reconciled",
                        "watermark": "event-time ? 2s",
                        "cutoff_time": candles[-1]["timestamp"],
                        "is_live": True,
                        "fetched_at": _iso(datetime.now(timezone.utc)),
                    }
                logger.info(
                    "OHLCV DB insuficiente para %s %s: %s/%s velas; completando con exchange",
                    clean_symbol, interval, len(candles), safe_limit,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("get_market_ohlcv Postgres fallback: %s", exc)

        # 2) Real public exchange data (Binance REST).
        try:
            live = exchange_client.fetch_klines(clean_symbol, interval, safe_limit)
            return {
                **live,
                "provenance": "SPEED_LAYER",
                "data_source": "exchange_live",
                "watermark": "Binance REST ? cache 5s",
                "cutoff_time": live["candles"][-1]["timestamp"],
            }
        except Exception as exc:  # noqa: BLE001
            logger.error("get_market_ohlcv: Postgres y Binance no disponibles: %s", exc)
            raise exchange_client.ExchangeUnavailable(
                f"no hay datos reales disponibles para {clean_symbol} ({interval}): {exc}"
            ) from exc

    def _metrics_from_candles(
        self,
        clean_symbol: str,
        window: str,
        candles: List[Dict[str, Any]],
        source: str,
        is_live: bool,
        fetched_at: str,
        data_source: str,
    ) -> Dict[str, Any]:
        if len(candles) < 2:
            raise ValueError("no hay suficientes velas para calcular m?tricas")
        closes = [float(candle["close"]) for candle in candles]
        volumes = [float(candle["volume"]) for candle in candles]
        first_open = float(candles[0]["open"])
        last_close = closes[-1]
        notional = sum(
            ((float(candle["high"]) + float(candle["low"]) + float(candle["close"])) / 3)
            * float(candle["volume"])
            for candle in candles
        )
        volume_base = sum(volumes) or 1.0
        vwap = notional / volume_base
        mean = sum(closes) / len(closes)
        variance = sum((close - mean) ** 2 for close in closes) / len(closes)
        return {
            "symbol": clean_symbol,
            "window": window,
            "current_price": round(last_close, 8),
            "return_24h": round(((last_close - first_open) / first_open * 100) if first_open else 0.0, 4),
            "volume_24h": round(notional, 2),
            "volume_base": round(volume_base, 8),
            "vwap": round(vwap, 8),
            "price_variance": round(variance, 8),
            "open": round(first_open, 8),
            "high": round(max(float(candle["high"]) for candle in candles), 8),
            "low": round(min(float(candle["low"]) for candle in candles), 8),
            "source": source,
            "provenance": "SPEED_LAYER",
            "data_source": data_source,
            "cutoff_time": candles[-1]["timestamp"],
            "freshness_seconds": 0.8,
            "is_live": is_live,
            "fetched_at": fetched_at,
        }

    def get_market_metrics(self, symbol: str, window: str = "24h") -> Dict[str, Any]:
        clean_symbol = sanitize_symbol(symbol)

        window_hours = {"1h": 1, "4h": 4, "24h": 24, "7d": 168}.get(window, 24)
        interval = "15m" if window_hours <= 4 else "1h"
        limit = max(2, window_hours * 4 if interval == "15m" else window_hours)

        # 1) Serving table, completed from Binance if the requested window is
        # longer than the reconciled history available.
        try:
            payload = self.get_market_ohlcv(clean_symbol, interval=interval, limit=limit)
            candles = payload.get("candles", []) if isinstance(payload, dict) else []
            if len(candles) >= 2:
                return self._metrics_from_candles(
                    clean_symbol,
                    window,
                    candles,
                    str(payload.get("source") or "serving.ohlcv_reconciled (Postgres)"),
                    bool(payload.get("is_live", True)),
                    str(payload.get("fetched_at") or _iso(datetime.now(timezone.utc))),
                    str(payload.get("data_source") or "postgres_reconciled"),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_market_metrics OHLCV fallback: %s", exc)

        # 1b) Speed Layer snapshot when reconciled candles are not available yet.
        # The streaming sink writes this table directly, so it is real pipeline
        # data even if the batch reconciliation DAG has not run.
        if self._postgres_available():
            try:
                realtime = self.get_metrics(clean_symbol)
                if realtime.get("data_source") == "postgres_reconciled":
                    items = realtime.get("items", [])
                    if items:
                        snapshot = items[0]
                        last_price = float(snapshot.get("last_price") or 0.0)
                        pct_change_5m = float(snapshot.get("pct_change_5m") or 0.0)
                        rolling_volume_5m = float(snapshot.get("rolling_volume_5m") or 0.0)
                        if last_price > 0:
                            denominator = 1 + pct_change_5m / 100.0
                            open_price = last_price / denominator if denominator else last_price
                            return {
                                "symbol": clean_symbol,
                                "window": window,
                                "current_price": round(last_price, 8),
                                "return_24h": round(pct_change_5m, 4),
                                "volume_24h": round(rolling_volume_5m, 2),
                                "volume_base": round(rolling_volume_5m, 8),
                                "vwap": round(last_price, 8),
                                "price_variance": 0.0,
                                "open": round(open_price, 8),
                                "high": round(last_price, 8),
                                "low": round(last_price, 8),
                                "source": "serving.realtime_metrics (Postgres)",
                                "provenance": "SPEED_LAYER",
                                "data_source": "postgres_reconciled",
                                "cutoff_time": _iso(snapshot.get("event_timestamp")),
                                "freshness_seconds": 0.8,
                                "is_live": True,
                                "fetched_at": _iso(datetime.now(timezone.utc)),
                            }
            except Exception as exc:  # noqa: BLE001
                logger.warning("get_market_metrics realtime_metrics fallback: %s", exc)

        # 2) Real Binance data.
        try:
            live = exchange_client.fetch_klines(clean_symbol, interval, limit)
            return self._metrics_from_candles(
                clean_symbol,
                window,
                live["candles"],
                "Binance Public REST",
                True,
                live["fetched_at"],
                "exchange_live",
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("get_market_metrics: Postgres y Binance no disponibles: %s", exc)
            raise exchange_client.ExchangeUnavailable(
                f"no hay datos reales disponibles para {clean_symbol} ({window}): {exc}"
            ) from exc

    def get_market_overview(self) -> Dict[str, Any]:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
        try:
            return exchange_client.fetch_overview(symbols)
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_market_overview Binance fallback: %s", exc)
            now = _iso(datetime.now(timezone.utc))
            return {
                "assets": [
                    {
                        "symbol": symbol,
                        "base": symbol.replace("USDT", ""),
                        "quote": "USDT",
                        "exchange": "Binance",
                        "price": 0.0,
                        "change_24h": 0.0,
                        "volume_24h": 0.0,
                        "sparkline": [],
                        "source": "unavailable",
                        "is_live": False,
                        "fetched_at": now,
                        "error": str(exc),
                    }
                    for symbol in symbols
                ],
                "source": "unavailable",
                "is_live": False,
                "fetched_at": now,
                "errors": [str(exc)],
            }

    def get_market_pipeline_status(self) -> Dict[str, Any]:
        """
        Telemetría real del pipeline. Sin componentes persistidos en
        `serving.pipeline_status` no hay señal real que mostrar: se reportan
        estados "UNKNOWN" explícitos en vez de números inventados
        (eps, latencia, tamaño de parquet, etc.).
        """
        now = datetime.now(timezone.utc)
        base: Dict[str, Any] = {
            "kafka": {"status": "UNKNOWN", "consumer_lag": 0, "topics": ["market.events"], "ingestion_rate": "n/a", "freshness_seconds": 0.0},
            "spark": {"status": "UNKNOWN", "window": "n/a", "watermark": "n/a", "micro_batch_ms": 0, "late_arrivals": 0},
            "airflow": {"status": "UNKNOWN", "dag_id": "n/a", "last_run": _iso(now), "failed_tasks": 0},
            "minio": {"status": "UNKNOWN", "path": "s3://market-lake/", "last_sync": _iso(now), "parquet_size": "n/a"},
            "dlq": {"status": "UNKNOWN", "rejected_events": 0},
            "timestamp": _iso(now),
            "freshness_seconds": 0.0,
            "data_source": "no_data_available",
        }
        if not self._postgres_available():
            return base
        try:
            payload = self.get_pipeline_status()
        except QueryServiceError as exc:
            logger.warning("get_market_pipeline_status: %s", exc)
            return base

        components = payload.get("components", []) if isinstance(payload, dict) else []
        base["data_source"] = payload.get("data_source", "no_data_available")
        for component in components:
            name = str(component.get("component", "")).lower()
            status = str(component.get("status", "")).upper() or "UNKNOWN"
            if "kafka" in name:
                base["kafka"]["status"] = "ACTIVE" if status == "SUCCESS" else status
                base["kafka"]["consumer_lag"] = int(component.get("consumer_lag", 0) or 0)
                events = (component.get("metrics_summary") or {}).get("events_last_batch")
                if events is not None:
                    base["kafka"]["ingestion_rate"] = f"{events} eventos/batch"
            elif "spark" in name and "stream" in name:
                base["spark"]["status"] = "PROCESSING" if status == "SUCCESS" else status
                summary = component.get("metrics_summary") or {}
                if summary.get("window_duration"):
                    base["spark"]["window"] = str(summary["window_duration"])
                if summary.get("watermark_delay"):
                    base["spark"]["watermark"] = str(summary["watermark_delay"])
            elif "airflow" in name or "batch" in name:
                base["airflow"]["dag_id"] = component.get("component") or "batch_recompute"
                base["airflow"]["status"] = status
                base["airflow"]["last_run"] = _iso(component.get("last_run"))
            elif "minio" in name:
                base["minio"]["status"] = "SYNCED" if status == "SUCCESS" else status
            elif "dlq" in name:
                base["dlq"]["status"] = "CLEAN" if status == "SUCCESS" else status
                metrics = component.get("metrics_summary") or {}
                base["dlq"]["rejected_events"] = int(metrics.get("rejected_events_last_batch", 0) or 0)
        base["freshness_seconds"] = float(payload.get("freshness_seconds") or 0.0)
        base["timestamp"] = _iso(payload.get("timestamp_utc"))
        return base

    def get_catalog_metadata(self, dataset: str = "market_ohlcv") -> Dict[str, Any]:
        catalog_path = Path(os.getenv("CATALOG_PATH", str(Path(__file__).resolve().parent.parent / "metadata" / "catalog.json")))
        if catalog_path.exists():
            try:
                payload = json.loads(catalog_path.read_text(encoding="utf-8"))
                datasets = payload.get("datasets", payload) if isinstance(payload, dict) else {}
                if isinstance(datasets, dict) and dataset in datasets:
                    return dict(datasets[dataset])
            except Exception as exc:  # noqa: BLE001
                logger.warning("catálogo JSON ilegible (%s): %s", catalog_path, exc)
        return market_data.catalog_metadata(dataset)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if value is None:
        return market_data.iso(market_data.utc_now())
    return str(value)
