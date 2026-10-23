"""
Cliente WebSocket persistente para los trades en vivo de Binance (US-07).

Se suscribe a `<symbol>@trade`, transforma cada mensaje al contrato canónico
y lo publica en `market.events`. Si la conexión cae (Binance corta cada 24 h
a propósito) reconecta con retroceso exponencial.

Uso:  python -m ingestion.connectors.market_websocket --symbols BTCUSDT,ETHUSDT
"""

import argparse
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

try:
    from ingestion.connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, backoff_delay, ms_to_iso, utc_iso,
    )
except ImportError:  # ejecución desde ingestion/
    from connectors.base_connector import (
        BaseConnector, EventPublisher, SCHEMA_VERSION, backoff_delay, ms_to_iso, utc_iso,
    )

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"


def _default_connect(url: str):
    # Import diferido: solo se necesita al conectar de verdad
    import websocket

    return websocket.create_connection(url, timeout=30)


class BinanceWebSocketConnector(BaseConnector):
    source = "binance_ws"
    exchange = "binance"
    component = "market_websocket"

    def __init__(
        self,
        publisher: EventPublisher,
        symbols: List[str],
        connect: Callable[[str], Any] = _default_connect,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        max_reconnects: Optional[int] = None,
        sleep: Callable[[float], None] = time.sleep,
        stop_event: Optional[threading.Event] = None,
    ):
        super().__init__(publisher)
        self.symbols = [s.upper() for s in symbols]
        self._connect = connect
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_reconnects = max_reconnects
        self._sleep = sleep
        self.stop_event = stop_event or threading.Event()
        self.reconnects = 0

    @property
    def url(self) -> str:
        streams = "/".join(f"{s.lower()}@trade" for s in self.symbols)
        return f"{BINANCE_WS_BASE}?streams={streams}"

    def fetch(self) -> Iterator[Any]:
        attempt = 0
        while not self.stop_event.is_set():
            try:
                ws = self._connect(self.url)
                logger.info("WebSocket conectado: %s", self.url)
                try:
                    while not self.stop_event.is_set():
                        message = ws.recv()
                        if not message:
                            raise ConnectionError("conexión cerrada por el servidor")
                        attempt = 0  # conexión sana: el próximo fallo vuelve a empezar desde base_delay
                        # Se entrega el texto crudo: si está corrupto lo rechaza normalize hacia la DLQ
                        yield message
                finally:
                    try:
                        ws.close()
                    except Exception:  # noqa: BLE001 - el cierre de un socket roto puede fallar
                        pass
            except Exception as err:  # noqa: BLE001
                # Cualquier fallo de socket o timeout se resuelve reconectando
                if self.max_reconnects is not None and self.reconnects >= self.max_reconnects:
                    raise
                self.reconnects += 1
                delay = backoff_delay(attempt, self.base_delay, self.max_delay)
                attempt += 1
                logger.warning("WebSocket caído (%s); reconectando en %.1fs", err, delay)
                self._sleep(delay)

    def normalize(self, raw: Any) -> Optional[Dict[str, Any]]:
        if isinstance(raw, (str, bytes, bytearray)):
            raw = json.loads(raw)  # ValueError si el JSON está corrupto
        if not isinstance(raw, dict):
            raise ValueError(f"se esperaba un objeto JSON y llegó {type(raw).__name__}")
        # Flujo combinado: {"stream": "...", "data": {...}}; flujo único: el trade directo
        trade = raw.get("data", raw)
        if trade.get("e") != "trade":
            return None
        symbol, trade_id = trade["s"], trade["t"]
        return {
            "event_id": f"binance_ws_{symbol}_{trade_id}",
            "source": self.source,
            "exchange": self.exchange,
            "symbol": symbol,
            "event_time": ms_to_iso(int(trade["T"])),
            "ingestion_time": utc_iso(),
            "price": float(trade["p"]),
            "quantity": float(trade["q"]),
            "trade_id": str(trade_id),
            "interval": "trade",
            "schema_version": SCHEMA_VERSION,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Trades de Binance en vivo hacia Kafka")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    try:
        from ingestion.connectors.kafka_producer import MarketEventProducer
    except ImportError:
        from connectors.kafka_producer import MarketEventProducer

    producer = MarketEventProducer(args.bootstrap)
    try:
        BinanceWebSocketConnector(producer, args.symbols.split(",")).run()
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == "__main__":
    main()
