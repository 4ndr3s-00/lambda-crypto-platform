"""
Conectores REST de Binance (klines) y CoinGecko (precio spot) — US-07.

A diferencia de `binance_rest.py` (extracción diaria de US-14), estos
conectores heredan de `BaseConnector`, aplican token bucket, retroceso
exponencial ante 429/418/5xx y paginan con un cursor temporal explícito
para reanudar sin duplicar ni omitir velas.
"""

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests

try:
    from ingestion.connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, TokenBucket, backoff_delay, ms_to_iso, utc_iso,
    )
except ImportError:  # ejecución desde ingestion/
    from connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, TokenBucket, backoff_delay, ms_to_iso, utc_iso,
    )

logger = logging.getLogger(__name__)

BINANCE_API_BASE = "https://api.binance.com/api/v3"
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"

# Duración de cada granularidad que acepta el contrato, para avanzar el cursor
INTERVAL_MS = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000}
# 429: límite excedido; 418: IP baneada temporalmente por insistir tras un 429
RETRYABLE_STATUS = {418, 429, 500, 502, 503, 504}


class RateLimitedClient:
    """Cliente HTTP GET con token bucket y reintentos con retroceso exponencial."""

    def __init__(
        self,
        rate_per_sec: float,
        session: Optional[requests.Session] = None,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        timeout: float = 15.0,
        bucket: Optional[TokenBucket] = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.session = session or requests.Session()
        self.bucket = bucket or TokenBucket(rate_per_sec)
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.timeout = timeout
        self._sleep = sleep

    def get_json(self, url: str, params: Dict[str, Any]) -> Any:
        for attempt in range(self.max_retries + 1):
            self.bucket.acquire()
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as err:
                if attempt == self.max_retries:
                    raise
                delay = backoff_delay(attempt, self.base_delay, self.max_delay)
                logger.warning("Error de red (%s); reintento %d en %.1fs", err, attempt + 1, delay)
                self._sleep(delay)
                continue

            if response.status_code in RETRYABLE_STATUS:
                if attempt == self.max_retries:
                    response.raise_for_status()
                # Si el proveedor indica cuánto esperar, se obedece antes que nuestro cálculo
                retry_after = response.headers.get("Retry-After")
                delay = (
                    min(float(retry_after), self.max_delay)
                    if retry_after and retry_after.replace(".", "", 1).isdigit()
                    else backoff_delay(attempt, self.base_delay, self.max_delay)
                )
                logger.warning("HTTP %s en %s; reintento %d en %.1fs", response.status_code, url, attempt + 1, delay)
                self._sleep(delay)
                continue

            response.raise_for_status()
            return response.json()
        raise RuntimeError("inalcanzable")  # pragma: no cover


class BinanceRestConnector(BaseConnector):
    source = "binance_rest"
    exchange = "binance"

    def __init__(
        self,
        publisher: EventPublisher,
        symbols: List[str],
        start_ms: int,
        end_ms: int,
        interval: str = "1m",
        limit: int = 1000,
        client: Optional[RateLimitedClient] = None,
    ):
        super().__init__(publisher)
        if interval not in INTERVAL_MS:
            raise ValueError(f"Intervalo no soportado: {interval}")
        if start_ms > end_ms:
            raise ValueError("start_ms no puede ser mayor que end_ms")
        self.symbols = symbols
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.interval = interval
        self.limit = limit
        # ~10 req/s es holgado frente al límite de 1200 de peso/min de Binance
        self.client = client or RateLimitedClient(rate_per_sec=10)
        #: último cursor alcanzado por símbolo (open_time de la última vela), para reanudar
        self.cursors: Dict[str, int] = {}

    def fetch(self) -> Iterator[Dict[str, Any]]:
        for symbol in self.symbols:
            for kline in self.fetch_symbol(symbol):
                yield {"symbol": symbol, "kline": kline}

    def fetch_symbol(self, symbol: str) -> Iterator[list]:
        step = INTERVAL_MS[self.interval]
        # El cursor es el open_time de la próxima vela esperada; reanuda donde quedó
        cursor = self.cursors[symbol] + step if symbol in self.cursors else self.start_ms
        while cursor <= self.end_ms:
            data = self.client.get_json(
                f"{BINANCE_API_BASE}/klines",
                {"symbol": symbol, "interval": self.interval, "startTime": cursor,
                 "endTime": self.end_ms, "limit": self.limit},
            )
            if not data:
                break
            last_open = cursor - step
            for kline in data:
                open_time = int(kline[0])
                # Descarta repetidos o fuera de orden: el cursor garantiza que no hay duplicados
                if open_time <= last_open or open_time > self.end_ms:
                    continue
                last_open = open_time
                self.cursors[symbol] = open_time
                yield kline
            if len(data) < self.limit:
                break
            cursor = last_open + step

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        kline, symbol = raw["kline"], raw["symbol"]
        open_time = int(kline[0])
        return {
            # Mismo id que binance_rest.py (US-14): el batch deduplica ambas rutas
            "event_id": f"binance_{symbol}_{open_time}",
            "source": self.source,
            "exchange": self.exchange,
            "symbol": symbol,
            "event_time": ms_to_iso(open_time),
            "ingestion_time": utc_iso(),
            "price": float(kline[4]),
            "quantity": float(kline[5]),
            "trade_id": str(open_time),
            "interval": self.interval,
            "schema_version": SCHEMA_VERSION,
        }


class CoinGeckoRestConnector(BaseConnector):
    """Precio spot de referencia (CoinGecko no expone volumen por trade)."""

    source = "coingecko_rest"
    exchange = "coingecko"

    def __init__(
        self,
        publisher: EventPublisher,
        coins: Dict[str, str],
        vs_currency: str = "usd",
        client: Optional[RateLimitedClient] = None,
    ):
        """`coins` mapea id de CoinGecko -> símbolo canónico, p. ej. {"bitcoin": "BTCUSD"}."""
        super().__init__(publisher)
        self.coins = coins
        self.vs_currency = vs_currency
        # El plan público ronda 5-15 llamadas/min: 0.2 req/s es seguro
        self.client = client or RateLimitedClient(rate_per_sec=0.2, capacity=1)

    def fetch(self) -> Iterator[Dict[str, Any]]:
        data = self.client.get_json(
            f"{COINGECKO_API_BASE}/simple/price",
            {"ids": ",".join(self.coins), "vs_currencies": self.vs_currency,
             "include_last_updated_at": "true"},
        )
        for coin_id, symbol in self.coins.items():
            if coin_id in data:
                yield {"coin_id": coin_id, "symbol": symbol, "data": data[coin_id]}

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        data = raw["data"]
        price = data.get(self.vs_currency)
        updated = data.get("last_updated_at")
        if price is None or updated is None:
            return None
        return {
            "event_id": f"coingecko_{raw['symbol']}_{int(updated)}",
            "source": self.source,
            "exchange": self.exchange,
            "symbol": raw["symbol"],
            "event_time": ms_to_iso(int(updated) * 1000),
            "ingestion_time": utc_iso(),
            "price": float(price),
            "quantity": 0,
            "trade_id": None,
            "interval": None,
            "schema_version": SCHEMA_VERSION,
        }
