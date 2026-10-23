"""Productor REST continuo para entornos donde el WebSocket de Binance no llega.

Binance bloquea `stream.binance.com` con HTTP 451 en algunas regiones. El
espejo público `data-api.binance.vision` sí responde vía REST, así que este
conector hace polling de precios y publica eventos canónicos en `market.events`
para que Spark Structured Streaming mantenga viva la capa Speed.

Uso:
    python -m ingestion.connectors.market_rest_stream \
        --symbols BTCUSDT,ETHUSDT,SOLUSDT --bootstrap kafka:9092
"""

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

try:
    from ingestion.connectors.base_connector import SCHEMA_VERSION
    from ingestion.connectors.kafka_producer import MarketEventProducer
except ImportError:  # ejecución desde ingestion/
    from connectors.base_connector import SCHEMA_VERSION
    from connectors.kafka_producer import MarketEventProducer

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://data-api.binance.vision"
DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,SOLUSDT"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def price_url(symbol: str) -> str:
    base = os.environ.get("BINANCE_API_BASE", DEFAULT_API_BASE).rstrip("/")
    return f"{base}/api/v3/ticker/price"


def fetch_price(session: requests.Session, symbol: str) -> float:
    response = session.get(price_url(symbol), params={"symbol": symbol}, timeout=10)
    response.raise_for_status()
    return float(response.json()["price"])


def build_event(symbol: str, price: float, sequence: int) -> Dict[str, Any]:
    now = utc_iso()
    event_id = f"rest_stream_{symbol}_{int(time.time() * 1000)}_{sequence}"
    return {
        "event_id": event_id,
        "source": "binance_rest",
        "exchange": "binance",
        "symbol": symbol,
        "event_time": now,
        "ingestion_time": now,
        "price": price,
        "quantity": 0.0,
        "trade_id": event_id,
        "interval": "tick",
        "schema_version": SCHEMA_VERSION,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precios Binance REST continuos hacia Kafka")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    )
    parser.add_argument("--poll-interval", type=float, default=2.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols: List[str] = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    producer = MarketEventProducer(args.bootstrap)
    session = requests.Session()
    sequence = 0

    logger.info(
        "REST stream iniciado: symbols=%s bootstrap=%s poll=%.1fs base=%s",
        ",".join(symbols),
        args.bootstrap,
        args.poll_interval,
        os.environ.get("BINANCE_API_BASE", DEFAULT_API_BASE),
    )

    try:
        while True:
            for symbol in symbols:
                try:
                    price = fetch_price(session, symbol)
                    producer.publish(
                        build_event(symbol, price, sequence),
                        component="market_rest_stream",
                    )
                    sequence += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("No se pudo publicar %s: %s", symbol, exc)
            producer.flush()
            time.sleep(max(0.5, args.poll_interval))
    except KeyboardInterrupt:
        logger.info("REST stream detenido")
    finally:
        session.close()
        producer.close()


if __name__ == "__main__":
    main()
