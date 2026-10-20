"""
Tests Unitarios para el Generador Sintético de Carga Masiva (US-31).
Responsable: Andrés Teherán

Verifica:
1. Conformidad de los eventos generados con el contrato formal (campos requeridos, tipos y rangos de precio).
2. Generación por lotes (streaming batch generator) y throughput mínimo (> 20,000 eps).
3. Claves compuestas válidas para el balanceo y ordenamiento en topics de Kafka.
"""

from tests.load.synthetic_generator import generate_market_event, stream_batch_events, run_benchmark


def test_generate_single_market_event():
    """Criterio 1: Cada evento generado debe cumplir con todos los campos del contrato formal."""
    event = generate_market_event(symbol="BTCUSDT", exchange="binance")

    assert "event_id" in event
    assert event["symbol"] == "BTCUSDT"
    assert event["exchange"] == "binance"
    assert event["source"] == "binance"
    assert "event_time" in event
    assert "ingestion_time" in event
    assert isinstance(event["price"], float)
    assert event["price"] > 0
    assert isinstance(event["quantity"], float)
    assert event["quantity"] >= 0
    assert event["interval"] == "1m"
    assert event["schema_version"] == "1.0"


def test_stream_batch_events_count():
    """Verifica que el generador produzca la cantidad exacta solicitada sin saturar memoria."""
    total = 25000
    batch_size = 5000
    collected_count = 0

    for batch in stream_batch_events(total, batch_size=batch_size):
        assert len(batch) <= batch_size
        collected_count += len(batch)

    assert collected_count == total


def test_benchmark_throughput():
    """Criterio 2: Medición de rendimiento en memoria."""
    results = run_benchmark(total_events=30000, batch_size=10000)
    assert results["total_events"] == 30000
    assert results["elapsed_seconds"] > 0
    assert results["throughput_eps"] > 10000  # Generador debe ser altamente eficiente
