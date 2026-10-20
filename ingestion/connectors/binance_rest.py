"""
Conector REST para la extracción de datos históricos de Binance (US-14).

Extrae velas históricas (klines) de mercado desde la API pública de Binance,
normaliza cada observación al contrato canónico `market_event.schema.json`
y valida su conformidad antes de la persistencia en MinIO.
"""

import json
import logging
import time
from datetime import date, datetime, time as dtime, timezone
from typing import Any, Dict, List, Optional
import requests

try:
    from ingestion.contracts.validator import is_valid, validate_event
except ImportError:
    try:
        from contracts.validator import is_valid, validate_event
    except ImportError:
        is_valid = None
        validate_event = None

logger = logging.getLogger(__name__)

BINANCE_API_BASE = "https://api.binance.com/api/v3"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_INTERVAL = "1m"
SCHEMA_VERSION = "2.0.0"


def date_to_timestamps_ms(target_date: date) -> tuple[int, int]:
    """
    Convierte una fecha calendario en marcas de tiempo Unix (milisegundos UTC)
    para el inicio (00:00:00.000) y el final (23:59:59.999) del día.
    """
    start_dt = datetime.combine(target_date, dtime.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(target_date, dtime.max, tzinfo=timezone.utc)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    return start_ms, end_ms


def format_kline_to_event(
    kline: list,
    symbol: str,
    interval: str = DEFAULT_INTERVAL,
) -> Dict[str, Any]:
    """
    Transforma una vela cruda de la API de Binance al contrato canónico de eventos.

    Estructura de la respuesta kline de Binance:
    [
        0: Open time (ms),
        1: Open price,
        2: High price,
        3: Low price,
        4: Close price,
        5: Volume,
        6: Close time,
        ...
    ]
    """
    open_time_ms = int(kline[0])
    close_price = float(kline[4])
    volume = float(kline[5])
    
    event_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
    ingestion_dt = datetime.now(timezone.utc)

    # Identificador determinístico: evita duplicados si la ingesta se reintenta
    event_id = f"binance_{symbol}_{open_time_ms}"

    return {
        "event_id": event_id,
        "source": "binance_rest",
        "exchange": "binance",
        "symbol": symbol,
        "event_time": event_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "ingestion_time": ingestion_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "price": close_price,
        "quantity": volume,
        "trade_id": str(open_time_ms),
        "interval": interval,
        "schema_version": SCHEMA_VERSION,
    }


def fetch_binance_klines(
    symbol: str,
    target_date: date,
    interval: str = DEFAULT_INTERVAL,
    limit: int = 1000,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Descarga todas las velas de un símbolo para una fecha completa desde Binance REST API.

    Un día en intervalo de 1 minuto consta de 1440 observaciones. Como el límite
    máximo por petición es de 1000 elementos, se ejecutan iteraciones paginadas
    cubriendo el rango exacto desde las 00:00:00 hasta las 23:59:59 UTC.
    """
    start_ms, end_ms = date_to_timestamps_ms(target_date)
    http = session or requests.Session()
    events: List[Dict[str, Any]] = []

    current_start = start_ms
    endpoint = f"{BINANCE_API_BASE}/klines"

    while current_start <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": limit,
        }

        try:
            response = http.get(endpoint, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as err:
            logger.error("Error al consultar Binance para %s en %s: %s", symbol, target_date, err)
            raise

        if not data:
            break

        for item in data:
            event = format_kline_to_event(item, symbol, interval)
            # Validación estricta con el contrato si está disponible
            if validate_event is not None:
                validate_event(event)
            events.append(event)

        # Avanzar el inicio al siguiente milisegundo tras la última vela recibida
        last_close_time = int(data[-1][6])
        if last_close_time >= end_ms or len(data) < limit:
            break
        current_start = last_close_time + 1

        # Respetar rate limits de la API pública
        time.sleep(0.05)

    logger.info(
        "Descarga de Binance completada: %s, fecha %s, total eventos: %d",
        symbol,
        target_date.isoformat(),
        len(events),
    )
    return events


def extract_daily_market_data(
    target_date: date,
    symbols: Optional[List[str]] = None,
    interval: str = DEFAULT_INTERVAL,
) -> List[Dict[str, Any]]:
    """
    Extrae los eventos históricos del día para la lista de símbolos configurada.
    """
    target_symbols = symbols or DEFAULT_SYMBOLS
    all_events: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for symbol in target_symbols:
            symbol_events = fetch_binance_klines(
                symbol=symbol,
                target_date=target_date,
                interval=interval,
                session=session,
            )
            all_events.extend(symbol_events)

    return all_events

