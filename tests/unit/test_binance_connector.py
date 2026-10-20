"""
Pruebas unitarias para el conector REST de Binance y DAG ingest_historical (US-14).
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from ingestion.connectors.binance_rest import (
    date_to_timestamps_ms,
    format_kline_to_event,
    fetch_binance_klines,
    DEFAULT_SYMBOLS,
    SCHEMA_VERSION,
)
from ingestion.contracts.validator import is_valid, validate_event


def test_date_to_timestamps_ms():
    """Verifica que el rango diario en ms comience a las 00:00:00 y termine a las 23:59:59."""
    target = date(2026, 10, 18)
    start_ms, end_ms = date_to_timestamps_ms(target)

    # 2026-10-18 00:00:00 UTC
    expected_start = int(datetime(2026, 10, 18, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert start_ms == expected_start
    assert end_ms > start_ms
    # La diferencia debe ser exactamente 24 horas menos 1 milisegundo (o 999 ms)
    diff = end_ms - start_ms
    assert diff == (86400 * 1000) - 1


def test_format_kline_to_event():
    """Valida que la transformación de kline genere un evento conforme al contrato canónico."""
    # Kline sintético de Binance:
    # [open_time, open, high, low, close, volume, close_time, quote_volume, trades, ...]
    raw_kline = [
        1792281600000,   # open_time (ms)
        "68000.00",      # open
        "68500.00",      # high
        "67950.00",      # low
        "68450.50",      # close
        "12.4500",       # volume
        1792281659999,   # close_time
        "851000.00",     # quote_asset_volume
        150,             # number_of_trades
    ]

    event = format_kline_to_event(raw_kline, symbol="BTCUSDT", interval="1m")

    assert event["event_id"] == "binance_BTCUSDT_1792281600000"
    assert event["source"] == "binance_rest"
    assert event["exchange"] == "binance"
    assert event["symbol"] == "BTCUSDT"
    assert event["price"] == 68450.50
    assert event["quantity"] == 12.45
    assert event["interval"] == "1m"
    assert event["schema_version"] == SCHEMA_VERSION

    # Validación formal contra el contrato JSON Draft 2020-12
    assert is_valid(event) is True
    validate_event(event)


def test_fetch_binance_klines_mocked():
    """Valida la paginación y recolección de eventos mockeando la llamada HTTP."""
    raw_kline_1 = [
        1792281600000, "68000.00", "68500.00", "67950.00", "68450.50", "1.5",
        1792281659999, "100000.00", 10
    ]
    raw_kline_2 = [
        1792281660000, "68450.50", "68600.00", "68400.00", "68550.00", "2.0",
        1792281719999, "137000.00", 15
    ]

    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = [raw_kline_1, raw_kline_2]
    mock_response.raise_for_status.return_value = None
    mock_session.get.return_value = mock_response

    target = date(2026, 10, 18)
    events = fetch_binance_klines(
        symbol="BTCUSDT",
        target_date=target,
        session=mock_session,
    )

    assert len(events) == 2
    assert events[0]["symbol"] == "BTCUSDT"
    assert events[1]["price"] == 68550.00
    assert is_valid(events[0]) is True
    assert is_valid(events[1]) is True

