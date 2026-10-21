"""Public exchange data client (Binance REST).

This is the source of truth for market prices when the Kafka/Speed serving
tables are empty. Every response carries `source`, `is_live` and `fetched_at`
so the UI can distinguish real exchange data from a synthetic fallback.

No API key is required for the public endpoints used here.
"""

from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import httpx

BINANCE_BASE = os.getenv("BINANCE_API_BASE", "https://data-api.binance.vision")
CACHE_TTL_SECONDS = float(os.getenv("MARKET_CACHE_TTL_SECONDS", "5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("MARKET_REQUEST_TIMEOUT_SECONDS", "5"))

INTERVAL_MAP = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
    "1D": "1d",
    "7D": "1w",
}

_CACHE: dict[str, tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()


class ExchangeUnavailable(RuntimeError):
    """The public exchange API did not answer or returned an invalid payload."""


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _cache_get(key: str) -> Any | None:
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if entry and now - entry[0] < CACHE_TTL_SECONDS:
            return entry[1]
        if entry:
            _CACHE.pop(key, None)
    return None


def _cache_set(key: str, value: Any) -> Any:
    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic(), value)
    return value


def _request(path: str, params: dict[str, Any], ttl: float = CACHE_TTL_SECONDS) -> Any:
    key = f"{path}|{sorted(params.items())}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = client.get(f"{BINANCE_BASE}{path}", params=params)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:  # noqa: BLE001
        raise ExchangeUnavailable(f"Binance no respondiÃ³: {exc}") from exc
    return _cache_set(key, data)


def fetch_klines(symbol: str, interval: str = "15m", limit: int = 96) -> dict[str, Any]:
    binance_interval = INTERVAL_MAP.get(interval)
    if binance_interval is None:
        raise ValueError(f"interval no soportado por Binance: {interval}")
    safe_limit = max(1, min(int(limit), 1000))
    data = _request(
        "/api/v3/klines",
        {"symbol": symbol.upper(), "interval": binance_interval, "limit": safe_limit},
    )
    if not isinstance(data, list):
        raise ExchangeUnavailable("Binance devolviÃ³ klines invÃ¡lidas")
    candles = [
        {
            "timestamp": _iso(datetime.fromtimestamp(int(item[0]) / 1000, tz=timezone.utc)),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5]),
        }
        for item in data
        if isinstance(item, list) and len(item) >= 6
    ]
    if not candles:
        raise ExchangeUnavailable(f"Binance no devolviÃ³ velas para {symbol}")
    return {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": safe_limit,
        "candles": candles,
        "source": "Binance Public REST",
        "is_live": True,
        "fetched_at": _iso(datetime.now(timezone.utc)),
    }


def fetch_ticker(symbol: str) -> dict[str, Any]:
    data = _request("/api/v3/ticker/24hr", {"symbol": symbol.upper()})
    if not isinstance(data, dict):
        raise ExchangeUnavailable("Binance devolviÃ³ ticker invÃ¡lido")
    return {
        "symbol": symbol.upper(),
        "price": float(data.get("lastPrice", 0)),
        "change_24h": float(data.get("priceChangePercent", 0)),
        "volume_24h": float(data.get("quoteVolume", 0)),
        "high_24h": float(data.get("highPrice", 0)),
        "low_24h": float(data.get("lowPrice", 0)),
        "source": "Binance Public REST",
        "is_live": True,
        "fetched_at": _iso(datetime.now(timezone.utc)),
    }


def _overview_asset(symbol: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        ticker = fetch_ticker(symbol)
        series = fetch_klines(symbol, "1h", 24)
        return (
            {
                **ticker,
                "base": symbol.replace("USDT", ""),
                "quote": "USDT",
                "exchange": "Binance",
                "sparkline": [candle["close"] for candle in series["candles"]],
            },
            None,
        )
    except Exception as exc:  # noqa: BLE001
        return (
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
                "fetched_at": _iso(datetime.now(timezone.utc)),
                "error": str(exc),
            },
            f"{symbol}: {exc}",
        )


def fetch_overview(symbols: list[str]) -> dict[str, Any]:
    assets: list[dict[str, Any]] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, min(4, len(symbols)))) as executor:
        for asset, error in executor.map(_overview_asset, symbols):
            if asset is not None:
                assets.append(asset)
            if error is not None:
                errors.append(error)
    live = any(asset["is_live"] for asset in assets)
    return {
        "assets": assets,
        "source": "Binance Public REST" if live else "unavailable",
        "is_live": live,
        "fetched_at": _iso(datetime.now(timezone.utc)),
        "errors": errors,
    }


def fetch_depth(symbol: str, limit: int = 20) -> dict[str, Any]:
    data = _request("/api/v3/depth", {"symbol": symbol.upper(), "limit": max(5, min(limit, 100))})
    bids_raw = data.get("bids", []) if isinstance(data, dict) else []
    asks_raw = data.get("asks", []) if isinstance(data, dict) else []
    if not bids_raw or not asks_raw:
        raise ExchangeUnavailable(f"Binance no devolviÃ³ profundidad para {symbol}")

    bid_total = 0.0
    ask_total = 0.0
    bids = []
    asks = []
    for price, size in bids_raw:
        bid_total += float(size)
        bids.append({"price": float(price), "size": float(size), "total": bid_total})
    for price, size in asks_raw:
        ask_total += float(size)
        asks.append({"price": float(price), "size": float(size), "total": ask_total})

    best_bid = bids[0]["price"]
    best_ask = asks[0]["price"]
    spread = best_ask - best_bid
    obi = ((bid_total - ask_total) / max(bid_total + ask_total, 1e-9)) * 100
    return {
        "symbol": symbol.upper(),
        "bids": bids,
        "asks": asks,
        "spread": spread,
        "spread_pct": (spread / max(best_bid, 1e-9)) * 100,
        "obi": obi,
        "source": "Binance Public REST",
        "is_live": True,
        "fetched_at": _iso(datetime.now(timezone.utc)),
    }


def fetch_trades(symbol: str, limit: int = 50) -> dict[str, Any]:
    data = _request("/api/v3/trades", {"symbol": symbol.upper(), "limit": max(1, min(limit, 500))})
    if not isinstance(data, list):
        raise ExchangeUnavailable(f"Binance no devolviÃ³ trades para {symbol}")
    trades = []
    for item in data:
        price = float(item.get("price", 0))
        quantity = float(item.get("qty", 0))
        notional = price * quantity
        # isBuyerMaker=true means the buyer was maker, so the taker was a seller.
        side = "sell" if bool(item.get("isBuyerMaker")) else "buy"
        trades.append(
            {
                "id": str(item.get("id", f"{symbol}-{item.get('time', 0)}")),
                "timestamp": _iso(datetime.fromtimestamp(int(item.get("time", 0)) / 1000, tz=timezone.utc)),
                "price": price,
                "size": quantity,
                "notional": notional,
                "side": side,
                "whale": notional >= 100_000,
            }
        )
    return {
        "symbol": symbol.upper(),
        "trades": trades,
        "source": "Binance Public REST",
        "is_live": True,
        "fetched_at": _iso(datetime.now(timezone.utc)),
    }
