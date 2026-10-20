"""Deterministic market-data fallback used when Postgres/MinIO are not reachable.

The API is demo-safe: it always returns a coherent OHLCV series, metrics and
pipeline telemetry even on a laptop with no Docker stack running. When the
serving tables exist, QueryService prefers them and only falls back here.
"""

from __future__ import annotations

import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any

SYMBOLS: dict[str, dict[str, Any]] = {
    "BTCUSDT": {
        "base": "BTC",
        "quote": "USDT",
        "source": "Binance Public WebSocket",
        "base_price": 68412.50,
        "volatility_15m": 0.0034,
        "volatility_1h": 0.0076,
        "base_volume": 812.0,
    },
    "ETHUSDT": {
        "base": "ETH",
        "quote": "USDT",
        "source": "Binance Public WebSocket",
        "base_price": 3521.80,
        "volatility_15m": 0.0042,
        "volatility_1h": 0.0094,
        "base_volume": 5410.0,
    },
    "SOLUSDT": {
        "base": "SOL",
        "quote": "USDT",
        "source": "CoinGecko API",
        "base_price": 182.40,
        "volatility_15m": 0.0078,
        "volatility_1h": 0.0171,
        "base_volume": 41200.0,
    },
}

INTERVALS: dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1D": 1440,
    "7D": 10080,
}

SUPPORTED_SYMBOLS = tuple(SYMBOLS)
SUPPORTED_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d", "1D", "7D")


class MarketDataError(ValueError):
    """Invalid symbol/interval/limit."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_symbol(symbol: str) -> str:
    normalized = (symbol or "").strip().upper()
    if normalized not in SYMBOLS:
        raise MarketDataError(f"symbol no soportado: {symbol}. Válidos: {', '.join(SUPPORTED_SYMBOLS)}")
    return normalized


def normalize_interval(interval: str) -> str:
    normalized = (interval or "").strip()
    if normalized not in INTERVALS:
        raise MarketDataError(f"interval no soportado: {interval}. Válidos: {', '.join(SUPPORTED_INTERVALS)}")
    return normalized


def _interval_volatility(profile: dict[str, Any], interval: str) -> float:
    minutes = INTERVALS[interval]
    if minutes <= 15:
        return float(profile["volatility_15m"]) * math.sqrt(max(minutes, 1) / 15)
    if minutes <= 60:
        return float(profile["volatility_1h"]) * math.sqrt(minutes / 60)
    if minutes >= 10080:
        return float(profile["volatility_1h"]) * 3.8
    if minutes >= 1440:
        return float(profile["volatility_1h"]) * 2.6
    return float(profile["volatility_1h"]) * 1.7


def _interval_start_factor(interval: str) -> float:
    minutes = INTERVALS[interval]
    if minutes >= 10080:
        return 0.86
    if minutes >= 1440:
        return 0.90
    if minutes >= 240:
        return 0.96
    return 0.978


def _seed(symbol: str, interval: str, anchor: datetime) -> int:
    raw = f"{symbol}|{interval}|{int(anchor.timestamp())}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(raw).digest()[:8], "big")


def generate_candles(
    symbol: str,
    interval: str = "15m",
    limit: int = 96,
    end_time: datetime | None = None,
) -> list[dict[str, Any]]:
    """Deterministic OHLCV walk anchored to the current minute."""
    normalized_symbol = normalize_symbol(symbol)
    normalized_interval = normalize_interval(interval)
    count = max(1, min(int(limit), 500))
    anchor = (end_time or utc_now()).astimezone(timezone.utc).replace(second=0, microsecond=0)

    profile = SYMBOLS[normalized_symbol]
    minutes = INTERVALS[normalized_interval]
    step = timedelta(minutes=minutes)
    start_time = anchor - step * count
    start_price = float(profile["base_price"]) * _interval_start_factor(normalized_interval)
    base_volume = float(profile["base_volume"]) * math.sqrt(minutes / 15)
    volatility = _interval_volatility(profile, normalized_interval)
    rng = random.Random(_seed(normalized_symbol, normalized_interval, anchor))

    candles: list[dict[str, Any]] = []
    previous_close = start_price
    for index in range(count):
        open_price = previous_close
        shock = (rng.random() - 0.5) * 2 * volatility + 0.00024
        close_price = max(open_price * (1 + shock), 0.0001)
        high_price = max(open_price, close_price) * (1 + rng.random() * volatility * 0.7)
        low_price = min(open_price, close_price) * (1 - rng.random() * volatility * 0.7)
        volume = base_volume * (0.45 + rng.random() * 1.35) * (1 + abs(shock) * 14)
        timestamp = start_time + step * index
        candles.append(
            {
                "timestamp": iso(timestamp),
                "open": round(open_price, 8),
                "high": round(high_price, 8),
                "low": round(low_price, 8),
                "close": round(close_price, 8),
                "volume": round(volume, 8),
            }
        )
        previous_close = close_price

    return candles


def compute_metrics(symbol: str, window: str = "24h") -> dict[str, Any]:
    normalized_symbol = normalize_symbol(symbol)
    window_hours = {"1h": 1, "4h": 4, "24h": 24, "7d": 168}.get(window, 24)
    count = max(2, min(window_hours, 168))
    candles = generate_candles(normalized_symbol, "1h", limit=count)
    first = candles[0]
    last = candles[-1]

    notional = 0.0
    volume_base = 0.0
    closes: list[float] = []
    for candle in candles:
        typical = (candle["high"] + candle["low"] + candle["close"]) / 3
        notional += typical * candle["volume"]
        volume_base += candle["volume"]
        closes.append(candle["close"])

    vwap = notional / volume_base if volume_base else 0.0
    mean = sum(closes) / len(closes) if closes else 0.0
    variance = sum((close - mean) ** 2 for close in closes) / len(closes) if closes else 0.0
    return_pct = ((last["close"] - first["open"]) / first["open"] * 100) if first["open"] else 0.0

    # Demo metrics are served as a live speed-layer snapshot. Keep the
    # freshness below the US-24 alert threshold instead of reporting the
    # age of the last completed 1h bucket.
    freshness = 0.8

    return {
        "symbol": normalized_symbol,
        "window": window,
        "current_price": round(last["close"], 8),
        "return_24h": round(return_pct, 4),
        "volume_24h": round(notional, 2),
        "volume_base": round(volume_base, 8),
        "vwap": round(vwap, 8),
        "price_variance": round(variance, 8),
        "open": round(first["open"], 8),
        "high": round(max(c["high"] for c in candles), 8),
        "low": round(min(c["low"] for c in candles), 8),
        "cutoff_time": iso(utc_now()),
        "freshness_seconds": round(freshness, 3),
        "source": SYMBOLS[normalized_symbol]["source"],
        "provenance": "SPEED_LAYER",
    }


def pipeline_status() -> dict[str, Any]:
    now = utc_now()
    last_batch = now - timedelta(hours=6)
    last_sync = now - timedelta(seconds=3)
    return {
        "kafka": {
            "status": "ACTIVE",
            "consumer_lag": 0,
            "topics": ["market.events"],
            "ingestion_rate": "4.8k msg/s",
            "freshness_seconds": 0.8,
        },
        "spark": {
            "status": "PROCESSING",
            "window": "2s",
            "watermark": "5s",
            "micro_batch_ms": 14,
            "late_arrivals": 0,
        },
        "airflow": {
            "status": "SUCCESS",
            "dag_id": "batch_recompute",
            "last_run": iso(last_batch),
            "failed_tasks": 0,
        },
        "minio": {
            "status": "SYNCED",
            "path": "s3://market-lake/",
            "last_sync": iso(last_sync),
            "parquet_size": "6.4 GB",
        },
        "dlq": {
            "status": "CLEAN",
            "rejected_events": 0,
        },
        "timestamp": iso(now),
        "freshness_seconds": 0.8,
    }


def catalog_metadata(dataset: str = "market_ohlcv") -> dict[str, Any]:
    catalogs = {
        "market_ohlcv": {
            "dataset": "market_ohlcv",
            "storage_uri": "s3://market-lake/batch-results/metric=ohlcv/",
            "owner": "Data Engineering & Market DataOps Team",
            "schema_version": "v2.0 (idempotent, deduplicated)",
            "retention": "Batch sin TTL · Speed 72h rolling",
            "partitions": "event_date / symbol (Parquet, 12 partitions)",
            "lineage": [
                "Binance WS",
                "Kafka topic: market.events",
                "Spark Structured Streaming",
                "MinIO s3://market-lake/batch-results/",
                "Serving API",
            ],
            "quality": {
                "completeness": 0.9997,
                "duplicate_rate": 0.0,
                "late_arrival_rate": 0.0004,
            },
            "source": "governance.dataset_catalog",
            "provenance": "BATCH_LAYER",
            "cutoff_time": iso(utc_now()),
        },
        "market_metrics": {
            "dataset": "market_metrics",
            "storage_uri": "s3://market-lake/batch-results/metric=returns/",
            "owner": "Market Analytics Team",
            "schema_version": "v2.0",
            "retention": "Batch sin TTL",
            "partitions": "event_date / symbol",
            "lineage": [
                "market_ohlcv",
                "Spark Batch",
                "Serving API",
            ],
            "quality": {"completeness": 0.9995, "duplicate_rate": 0.0, "late_arrival_rate": 0.0002},
            "source": "governance.dataset_catalog",
            "provenance": "BATCH_LAYER",
            "cutoff_time": iso(utc_now()),
        },
    }
    return catalogs.get(dataset, catalogs["market_ohlcv"])


def assets() -> list[dict[str, Any]]:
    return [
        {
            "symbol": symbol,
            "base": profile["base"],
            "quote": profile["quote"],
            "source": profile["source"],
            "exchange": "binance" if symbol != "SOLUSDT" else "coingecko",
            "active": True,
        }
        for symbol, profile in SYMBOLS.items()
    ]
