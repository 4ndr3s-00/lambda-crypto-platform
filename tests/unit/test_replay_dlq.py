"""
Pruebas unitarias para el DAG replay_dlq y desempaquetado de contingencia (US-17).
"""

import json
from unittest.mock import MagicMock

from dags.replay_dlq import (
    unpack_dlq_payload,
    matches_filters,
    replay_dlq_events,
)


def test_unpack_dlq_payload_raw_json():
    """Valida el desempaquetado de un evento directo sin envoltorio."""
    raw_event = {
        "event_id": "test_1",
        "symbol": "BTCUSDT",
        "price": 65000.0,
        "quantity": 1.0,
    }
    unpacked, err = unpack_dlq_payload(json.dumps(raw_event))
    assert unpacked == raw_event
    assert err is None


def test_unpack_dlq_payload_with_envelope():
    """Valida la extracción del evento original cuando viene envuelto con metadatos de error."""
    enveloped = {
        "original_event": {
            "event_id": "test_2",
            "symbol": "ETHUSDT",
            "price": 3500.0,
            "quantity": 2.5,
        },
        "error": "Timeout de validación previo",
        "timestamp": "2026-10-20T01:00:00Z",
    }
    unpacked, err = unpack_dlq_payload(json.dumps(enveloped))
    assert unpacked == enveloped["original_event"]
    assert err == "Timeout de validación previo"


def test_unpack_dlq_payload_corrupted_json():
    """Valida el manejo defensivo ante un JSON inválido o corrupto."""
    unpacked, err = unpack_dlq_payload("{esto no es un json valido")
    assert unpacked is None
    assert "JSON corrupto" in err


def test_matches_filters_by_source_and_date():
    """Valida los filtros por origen y rango temporal de la contingencia."""
    event_binance = {
        "symbol": "BTCUSDT",
        "source": "binance_ws",
        "event_time": "2026-10-20T03:30:00.000Z",
    }

    # Coincidencia exacta de fuente
    assert matches_filters(event_binance, source_filter="binance_ws") is True
    # Fuente distinta debe rechazarse
    assert matches_filters(event_binance, source_filter="coingecko_rest") is False
    # Filtro 'all' debe permitirlo
    assert matches_filters(event_binance, source_filter="all") is True

    # Filtro de rango temporal
    assert matches_filters(
        event_binance,
        start_time="2026-10-20T03:00:00Z",
        end_time="2026-10-20T04:00:00Z",
    ) is True

    # Evento anterior a start_time
    assert matches_filters(
        event_binance,
        start_time="2026-10-20T04:00:00Z",
    ) is False


def test_replay_dlq_events_dry_run_and_execution():
    """Valida la revalidación y re-inyección hacia Kafka en modos dry-run y real."""
    valid_event = {
        "event_id": "binance_BTCUSDT_1710500000000",
        "source": "binance_ws",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "2026-10-20T03:00:00.000Z",
        "ingestion_time": "2026-10-20T03:00:00.050Z",
        "price": 68000.0,
        "quantity": 1.25,
        "trade_id": "12345",
        "interval": "trade",
        "schema_version": "2.0.0",
    }

    invalid_event = {
        "event_id": "bad_event",
        "symbol": "BTCUSDT",
        "price": -50.0,  # Precio negativo inválido
        "schema_version": "2.0.0",
    }

    mock_record_valid = MagicMock()
    mock_record_valid.value = json.dumps(valid_event)
    mock_record_valid.offset = 101

    mock_record_invalid = MagicMock()
    mock_record_invalid.value = json.dumps(invalid_event)
    mock_record_invalid.offset = 102

    mock_consumer = MagicMock()
    mock_producer = MagicMock()

    mock_consumer.poll.return_value = {
        "partition_0": [mock_record_valid, mock_record_invalid]
    }

    # 1. Prueba en modo DRY-RUN (simulación sin envío real)
    stats_dry = replay_dlq_events(
        consumer=mock_consumer,
        producer=mock_producer,
        dry_run=True,
    )

    assert stats_dry["total_read"] == 2
    assert stats_dry["replayed_valid"] == 1
    assert stats_dry["still_invalid"] == 1
    mock_producer.send.assert_not_called()
    mock_consumer.commit.assert_not_called()

    # 2. Prueba en modo REAL
    stats_real = replay_dlq_events(
        consumer=mock_consumer,
        producer=mock_producer,
        dry_run=False,
    )

    assert stats_real["replayed_valid"] == 1
    assert stats_real["still_invalid"] == 1
    mock_producer.send.assert_called_once()
    mock_producer.flush.assert_called_once()
    mock_consumer.commit.assert_called_once()

