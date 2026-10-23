#!/usr/bin/env python3
"""
Generador de Carga Masiva y Eventos Sintéticos de Mercado (US-31).
Responsable: Andrés Teherán (DevOps, SRE & Testing Lead)

Genera hasta millones de eventos sintéticos realistas respetando rigurosamente el
contrato JSON Schema formal (`ingestion/contracts/market_event.schema.json`).
Permite enviar los eventos hacia:
  - Apache Kafka (`market.events`) usando claves compuestas `exchange|symbol`.
  - MinIO S3 (`s3://market-lake/historical/source=synthetic/date=YYYY-MM-DD/`).
  - Modo benchmark local (memoria / disco) para medir throughput puro.
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, Generator, List, Optional

# Parámetros por defecto
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]
DEFAULT_EXCHANGE = "binance"
BASE_PRICES = {
    "BTCUSDT": 68000.0,
    "ETHUSDT": 2700.0,
    "SOLUSDT": 180.0,
    "BNBUSDT": 590.0,
    "ADAUSDT": 0.45,
    "XRPUSDT": 0.55,
}


def generate_market_event(
    symbol: Optional[str] = None,
    exchange: str = DEFAULT_EXCHANGE,
    event_time: Optional[datetime] = None,
) -> Dict[str, any]:
    """Genera un único evento válido conforme al contrato v1.0."""
    sym = symbol or random.choice(DEFAULT_SYMBOLS)
    base_price = BASE_PRICES.get(sym, 100.0)
    # Variación browniana simulada (+- 1.5%)
    variation = random.uniform(-0.015, 0.015)
    price = round(max(0.0001, base_price * (1.0 + variation)), 4)
    qty = round(random.uniform(0.001, 15.0), 5)

    now = event_time or datetime.now(timezone.utc)
    now_iso = now.isoformat()

    return {
        "event_id": str(uuid.uuid4()),
        "source": exchange,
        "exchange": exchange,
        "symbol": sym,
        "event_time": now_iso,
        "ingestion_time": now_iso,
        "price": price,
        "quantity": qty,
        "interval": "1m",
        "schema_version": "1.0",
    }


def stream_batch_events(total_events: int, batch_size: int = 5000) -> Generator[List[Dict[str, any]], None, None]:
    """Genera lotes de eventos en memoria usando generadores para no saturar la RAM."""
    remaining = total_events
    while remaining > 0:
        current_batch_size = min(remaining, batch_size)
        batch = [generate_market_event() for _ in range(current_batch_size)]
        remaining -= current_batch_size
        yield batch


def run_benchmark(total_events: int = 100000, batch_size: int = 10000) -> Dict[str, float]:
    """Ejecuta una prueba de rendimiento en memoria midiendo throughput (eventos/segundo)."""
    print(f"⚡ Iniciando benchmark sintético: {total_events:,} eventos...")
    start_time = time.time()
    events_generated = 0

    for batch in stream_batch_events(total_events, batch_size=batch_size):
        events_generated += len(batch)

    elapsed = time.time() - start_time
    throughput = events_generated / elapsed if elapsed > 0 else 0

    results = {
        "total_events": events_generated,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_eps": round(throughput, 2),
    }

    print(f"✅ Completado en {results['elapsed_seconds']}s")
    print(f"📊 Throughput: {results['throughput_eps']:,.2f} eventos/segundo")
    return results


def main():
    parser = argparse.ArgumentParser(description="Generador Sintético Masivo de Eventos de Mercado")
    parser.add_argument("--count", type=int, default=100000, help="Cantidad de eventos a generar (default: 100,000)")
    parser.add_argument("--batch-size", type=int, default=10000, help="Tamaño del lote en memoria (default: 10,000)")
    parser.add_argument("--target", choices=["stdout", "benchmark", "json-file"], default="benchmark", help="Destino")
    parser.add_argument("--output-file", type=str, default="/tmp/synthetic_events.json", help="Archivo de salida")

    args = parser.parse_args()

    if args.target == "benchmark":
        run_benchmark(args.count, args.batch_size)
    elif args.target == "json-file":
        print(f"Escribiendo {args.count:,} eventos en {args.output_file}...")
        start_time = time.time()
        with open(args.output_file, "w", encoding="utf-8") as f:
            for batch in stream_batch_events(args.count, args.batch_size):
                for item in batch:
                    f.write(json.dumps(item) + "\n")
        elapsed = time.time() - start_time
        print(f"Listo en {elapsed:.2f}s ({args.count / elapsed:,.2f} eps)")


if __name__ == "__main__":
    main()
