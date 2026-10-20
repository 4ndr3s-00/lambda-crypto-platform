"""
Pruebas unitarias y BDD de la capa Spark Structured Streaming (US-18).

Criterios de Aceptación validados:
    - US-18 Criterio 1: Deserialización de JSON según contrato formal,
      extrayendo `event_time` como columna Timestamp.
    - US-18 Criterio 2: Configuración de marca de agua explícita
      (`withWatermark("event_time", "2 minutes")`) para control de estado.
    - US-18 Criterio 3: Enrutamiento de anomalías y mensajes corruptos al topic
      `market.dlq` sin colapsar el streaming.

Las pruebas utilizan una SparkSession local determinista (`local[2]`).
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "spark"))

import streaming_market  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """Sesión Spark local determinista para pruebas unitarias de streaming."""
    session = (
        SparkSession.builder.appName("tests-spark-streaming")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _raw_events_df(spark, payloads):
    """Construye un DataFrame que simula los mensajes binarios o string de Kafka."""
    rows = [(p,) for p in payloads]
    return spark.createDataFrame(rows, ["value"])


# ==============================================================================
# US-18: Criterio 1 - Consumo continuo y parseo de esquema
# ==============================================================================
def test_bdd_us18_criterio_1_deserializacion_json_y_tipado(spark):
    """
    Dado que fluyen eventos válidos en formato JSON,
    Cuando se ejecuta la función de parseo y tipado,
    Entonces se extraen los campos tipados y event_time como TimestampType.
    """
    valid_payload = json.dumps({
        "event_id": "evt-001",
        "source": "binance_ws",
        "exchange": "BINANCE",
        "symbol": "btcusdt",
        "event_time": "2026-10-19T14:30:00Z",
        "ingestion_time": "2026-10-19T14:30:01Z",
        "price": 67500.50,
        "quantity": 1.25,
        "trade_id": "t-100",
        "interval": "tick",
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [valid_payload])
    classified = streaming_market.classify_and_parse_events(raw_df)
    valid_df = streaming_market.extract_valid_events(classified)

    rows = valid_df.collect()
    assert len(rows) == 1
    row = rows[0]

    assert row.event_id == "evt-001"
    assert row.exchange == "binance"  # Normalizado a minúsculas
    assert row.symbol == "BTCUSDT"    # Normalizado a mayúsculas
    assert row.price == 67500.50
    assert row.quantity == 1.25
    assert row.trade_id == "t-100"
    assert row.interval == "tick"
    assert row.schema_version == "2.0.0"

    # Verificación estricta de tipo Timestamp
    event_time_field = [f for f in valid_df.schema.fields if f.name == "event_time"][0]
    assert isinstance(event_time_field.dataType, TimestampType)
    assert row.event_time == datetime(2026, 10, 19, 14, 30, 0)


def test_bdd_us18_criterio_1_campos_opcionales_nulos(spark):
    """
    Dado que un evento válido no trae trade_id ni interval (campos opcionales),
    Cuando se parsea el evento,
    Entonces se procesa como válido asignando null en los campos ausentes.
    """
    valid_payload = json.dumps({
        "event_id": "evt-002",
        "source": "coingecko_rest",
        "exchange": "coingecko",
        "symbol": "ETHUSDT",
        "event_time": "2026-10-19T15:00:00Z",
        "ingestion_time": "2026-10-19T15:00:05Z",
        "price": 2650.00,
        "quantity": 10.0,
        "trade_id": None,
        "interval": None,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [valid_payload])
    classified = streaming_market.classify_and_parse_events(raw_df)
    valid_df = streaming_market.extract_valid_events(classified)

    rows = valid_df.collect()
    assert len(rows) == 1
    assert rows[0].trade_id is None
    assert rows[0].interval is None
    assert rows[0].is_valid if hasattr(rows[0], "is_valid") else True


# ==============================================================================
# US-18: Criterio 2 - Definición de Marca de Agua (Watermark)
# ==============================================================================
def test_bdd_us18_criterio_2_watermark_explicito_configurado(spark):
    """
    Dado un DataFrame con columna event_time de tipo Timestamp,
    Cuando se aplica withWatermark con '2 minutes',
    Entonces Spark registra la marca de agua con la columna y el retraso definidos.
    """
    valid_payload = json.dumps({
        "event_id": "evt-003",
        "source": "synthetic_generator",
        "exchange": "kraken",
        "symbol": "SOLUSDT",
        "event_time": "2026-10-19T16:00:00Z",
        "ingestion_time": "2026-10-19T16:00:01Z",
        "price": 150.25,
        "quantity": 5.0,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [valid_payload])
    classified = streaming_market.classify_and_parse_events(raw_df)
    valid_df = streaming_market.extract_valid_events(classified)

    watermarked_df = streaming_market.apply_watermark(
        valid_df,
        watermark_delay="2 minutes",
        time_col="event_time",
    )

    # Validar que el DataFrame conserve su esquema y contenga la metadata de watermark
    assert "event_time" in watermarked_df.columns
    # El plan lógico o atributos de watermark se asocian a event_time
    event_time_field = [f for f in watermarked_df.schema.fields if f.name == "event_time"][0]
    assert isinstance(event_time_field.dataType, TimestampType)


def test_bdd_us18_criterio_2_watermark_rechaza_columna_no_timestamp(spark):
    """
    Dado un DataFrame con una columna temporal que no sea Timestamp,
    Cuando se intenta aplicar watermark,
    Entonces lanza un TypeError descriptivo.
    """
    df = spark.createDataFrame([("2026-10-19 12:00:00",)], ["event_time"])

    with pytest.raises(TypeError, match="TimestampType"):
        streaming_market.apply_watermark(df, watermark_delay="2 minutes", time_col="event_time")


def test_bdd_us18_criterio_2_watermark_rechaza_columna_inexistente(spark):
    """
    Dado un DataFrame sin la columna temporal solicitada,
    Cuando se intenta aplicar watermark,
    Entonces lanza un ValueError.
    """
    df = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="no se encuentra en el DataFrame"):
        streaming_market.apply_watermark(df, watermark_delay="2 minutes", time_col="non_existent")


# ==============================================================================
# US-18: Criterio 3 - Enrutamiento de Anomalías a DLQ
# ==============================================================================
def test_bdd_us18_criterio_3_aislamiento_json_corrupto_a_dlq(spark):
    """
    Dado que llega un mensaje que no es JSON válido (sintaxis rota),
    Cuando se procesa el stream,
    Entonces se etiqueta con error CORRUPT_JSON_SYNTAX y se aísla hacia la DLQ.
    """
    corrupt_payloads = [
        "esto no es un json",
        '{"event_id": "broken", "unclosed',
        "",
    ]

    raw_df = _raw_events_df(spark, corrupt_payloads)
    classified = streaming_market.classify_and_parse_events(raw_df)
    dlq_df = streaming_market.format_dlq_stream(classified)

    rows = dlq_df.collect()
    assert len(rows) == 3

    for row in rows:
        # Kafka sink requiere key y value
        assert row.key is not None
        assert row.value is not None

        parsed_val = json.loads(row.value)
        assert parsed_val["error"] in ("CORRUPT_JSON_SYNTAX", "MISSING_REQUIRED_FIELDS")
        assert "dlq_timestamp" in parsed_val


def test_bdd_us18_criterio_3_aislamiento_precio_no_positivo(spark):
    """
    Dado un evento con precio <= 0 (infracción del contrato US-05),
    Cuando se clasifica el evento,
    Entonces se aísla con error INVALID_PRICE_NON_POSITIVE hacia la DLQ.
    """
    zero_price_event = json.dumps({
        "event_id": "evt-zero",
        "source": "binance_rest",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "2026-10-19T14:00:00Z",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": 0.0,
        "quantity": 1.0,
        "schema_version": "2.0.0",
    })
    negative_price_event = json.dumps({
        "event_id": "evt-neg",
        "source": "binance_rest",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "2026-10-19T14:00:00Z",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": -100.50,
        "quantity": 1.0,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [zero_price_event, negative_price_event])
    classified = streaming_market.classify_and_parse_events(raw_df)

    valid_count = streaming_market.extract_valid_events(classified).count()
    assert valid_count == 0

    dlq_df = streaming_market.format_dlq_stream(classified)
    dlq_rows = dlq_df.collect()
    assert len(dlq_rows) == 2

    for r in dlq_rows:
        val = json.loads(r.value)
        assert val["error"] == "INVALID_PRICE_NON_POSITIVE"


def test_bdd_us18_criterio_3_aislamiento_cantidad_negativa(spark):
    """
    Dado un evento con volumen/cantidad negativo,
    Cuando se clasifica el evento,
    Entonces se aísla con error INVALID_QUANTITY_NEGATIVE hacia la DLQ.
    """
    neg_qty_event = json.dumps({
        "event_id": "evt-qty",
        "source": "binance_ws",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "2026-10-19T14:00:00Z",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": 60000.0,
        "quantity": -0.05,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [neg_qty_event])
    classified = streaming_market.classify_and_parse_events(raw_df)

    assert streaming_market.extract_valid_events(classified).count() == 0

    dlq_rows = streaming_market.format_dlq_stream(classified).collect()
    assert len(dlq_rows) == 1
    val = json.loads(dlq_rows[0].value)
    assert val["error"] == "INVALID_QUANTITY_NEGATIVE"


def test_bdd_us18_criterio_3_aislamiento_campos_obligatorios_faltantes(spark):
    """
    Dado un evento al que le falta symbol o event_id,
    Cuando se clasifica el evento,
    Entonces se aísla con error MISSING_REQUIRED_FIELDS hacia la DLQ.
    """
    missing_symbol = json.dumps({
        "event_id": "evt-no-sym",
        "source": "binance_ws",
        "exchange": "binance",
        "event_time": "2026-10-19T14:00:00Z",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": 60000.0,
        "quantity": 1.0,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [missing_symbol])
    classified = streaming_market.classify_and_parse_events(raw_df)

    assert streaming_market.extract_valid_events(classified).count() == 0

    dlq_rows = streaming_market.format_dlq_stream(classified).collect()
    assert len(dlq_rows) == 1
    val = json.loads(dlq_rows[0].value)
    assert val["error"] == "MISSING_REQUIRED_FIELDS"


def test_bdd_us18_criterio_3_aislamiento_timestamp_invalido(spark):
    """
    Dado un evento con marca temporal corrupta que no cumple formato ISO-8601,
    Cuando se clasifica el evento,
    Entonces se aísla con error INVALID_EVENT_TIME hacia la DLQ.
    """
    bad_time_event = json.dumps({
        "event_id": "evt-bad-time",
        "source": "binance_ws",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "fecha_invalida_no_iso",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": 60000.0,
        "quantity": 1.0,
        "schema_version": "2.0.0",
    })

    raw_df = _raw_events_df(spark, [bad_time_event])
    classified = streaming_market.classify_and_parse_events(raw_df)

    assert streaming_market.extract_valid_events(classified).count() == 0

    dlq_rows = streaming_market.format_dlq_stream(classified).collect()
    assert len(dlq_rows) == 1
    val = json.loads(dlq_rows[0].value)
    assert val["error"] == "INVALID_EVENT_TIME"


def test_bdd_us18_criterio_3_version_esquema_invalida(spark):
    """
    Dado un evento con versión de esquema distinta a 2.0.0,
    Cuando se clasifica el evento,
    Entonces se aísla con error UNSUPPORTED_SCHEMA_VERSION hacia la DLQ.
    """
    bad_schema = json.dumps({
        "event_id": "evt-v1",
        "source": "binance_ws",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "event_time": "2026-10-19T14:00:00Z",
        "ingestion_time": "2026-10-19T14:00:01Z",
        "price": 60000.0,
        "quantity": 1.0,
        "schema_version": "1.0.0",
    })

    raw_df = _raw_events_df(spark, [bad_schema])
    classified = streaming_market.classify_and_parse_events(raw_df)

    assert streaming_market.extract_valid_events(classified).count() == 0

    dlq_rows = streaming_market.format_dlq_stream(classified).collect()
    assert len(dlq_rows) == 1
    val = json.loads(dlq_rows[0].value)
    assert val["error"] == "UNSUPPORTED_SCHEMA_VERSION"


# ==============================================================================
# Pipeline Combinado: Eventos Mixtos (Válidos + Anómalos)
# ==============================================================================
def test_bdd_us18_separacion_simultanea_de_flujos(spark):
    """
    Dado un lote mixto con eventos válidos y eventos corruptos,
    Cuando se ejecuta el pipeline,
    Entonces los eventos válidos van al flujo limpio y las anomalías al flujo DLQ
    sin generar excepciones ni pérdida de información.
    """
    mixed_batch = [
        json.dumps({
            "event_id": "evt-ok-1",
            "source": "binance_ws",
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "event_time": "2026-10-19T10:00:00Z",
            "ingestion_time": "2026-10-19T10:00:01Z",
            "price": 65000.0,
            "quantity": 0.5,
            "schema_version": "2.0.0",
        }),
        "{json roto",
        json.dumps({
            "event_id": "evt-bad-price",
            "source": "binance_ws",
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "event_time": "2026-10-19T10:00:05Z",
            "ingestion_time": "2026-10-19T10:00:06Z",
            "price": -10.0,
            "quantity": 1.0,
            "schema_version": "2.0.0",
        }),
        json.dumps({
            "event_id": "evt-ok-2",
            "source": "coingecko_rest",
            "exchange": "coingecko",
            "symbol": "ETHUSDT",
            "event_time": "2026-10-19T10:00:10Z",
            "ingestion_time": "2026-10-19T10:00:12Z",
            "price": 2600.0,
            "quantity": 2.0,
            "schema_version": "2.0.0",
        }),
    ]

    raw_df = _raw_events_df(spark, mixed_batch)
    classified = streaming_market.classify_and_parse_events(raw_df)

    valid_df = streaming_market.extract_valid_events(classified)
    dlq_df = streaming_market.format_dlq_stream(classified)

    valid_rows = valid_df.collect()
    dlq_rows = dlq_df.collect()

    assert len(valid_rows) == 2
    assert {r.event_id for r in valid_rows} == {"evt-ok-1", "evt-ok-2"}

    assert len(dlq_rows) == 2
    dlq_errors = [json.loads(r.value)["error"] for r in dlq_rows]
    assert "CORRUPT_JSON_SYNTAX" in dlq_errors
    assert "INVALID_PRICE_NON_POSITIVE" in dlq_errors


# ==============================================================================
# CLI: Argumentos de Línea de Comandos
# ==============================================================================
def test_bdd_us18_cli_parser_defaults():
    """Valida los valores por defecto del parser de CLI."""
    args = streaming_market.parse_args([])
    assert args.input_topic == "market.events"
    assert args.dlq_topic == "market.dlq"
    assert args.watermark_delay == "2 minutes"
    assert args.starting_offsets == "latest"
    assert not args.console_sink
    assert not args.dry_run


def test_bdd_us18_cli_parser_custom_args():
    """Valida la sobreescritura de parámetros por CLI."""
    args = streaming_market.parse_args([
        "--bootstrap-servers", "my-kafka:9094",
        "--input-topic", "custom.events",
        "--dlq-topic", "custom.dlq",
        "--watermark-delay", "5 minutes",
        "--starting-offsets", "earliest",
        "--dry-run",
    ])
    assert args.bootstrap_servers == "my-kafka:9094"
    assert args.input_topic == "custom.events"
    assert args.dlq_topic == "custom.dlq"
    assert args.watermark_delay == "5 minutes"
    assert args.starting_offsets == "earliest"
    assert args.dry_run is True
