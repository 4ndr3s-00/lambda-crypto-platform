"""
Spark Structured Streaming: Ingesta continua en tiempo real con event-time,
marcas de agua (watermarks), enrutamiento a Dead-Letter Queue (DLQ),
deduplicación stateful y cálculo de métricas en ventanas deslizantes.

Historias de Usuario cubiertas:
    - US-18: Pipeline de Structured Streaming con event-time y watermarks
             (Criterio 1: parseo de esquema y event_time como Timestamp,
              Criterio 2: watermark explícito de retención de estado,
              Criterio 3: aislamiento de anomalías hacia topic market.dlq).
    - US-19: Deduplicación en tiempo real y cálculo de métricas en ventanas deslizantes
             (Criterio 1: deduplicación stateful con dropDuplicates(["event_id", "event_time"]),
              Criterio 2: agregaciones en ventanas deslizantes de 5 min / 1 min,
              Criterio 3: emisión a topic market.metrics con is_provisional = true).

Responsable Principal: Diego (Lead de la Capa de Velocidad / Speed Layer)

Ejemplo de ejecución con spark-submit:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        spark/streaming_market.py \\
        --bootstrap-servers kafka:9092 \\
        --input-topic market.events \\
        --metrics-topic market.metrics \\
        --dlq-topic market.dlq \\
        --watermark-delay "2 minutes" \\
        --window-duration "5 minutes" \\
        --slide-duration "1 minute"
"""

import argparse
import os
import sys

# Asegurar que el directorio de jobs esté en el path para importaciones
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_session import build_spark_session, checkpoints_path

# ==============================================================================
# Esquema Formal del Contrato Canónico (US-05 / market_event.schema.json)
# ==============================================================================
# Se definen las marcas temporales inicialmente como StringType para permitir que
# `from_json` en modo PERMISSIVE capture el texto crudo sin abortar la deserialización,
# permitiendo luego validar y reportar formatos inválidos a la DLQ.
MARKET_EVENT_SCHEMA = StructType([
    StructField("_corrupt_record", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("trade_id", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("schema_version", StringType(), True),
])

# Valores por defecto de configuración de streaming
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_INPUT_TOPIC = "market.events"
DEFAULT_DLQ_TOPIC = "market.dlq"
DEFAULT_METRICS_TOPIC = "market.metrics"
DEFAULT_WATERMARK_DELAY = "2 minutes"
DEFAULT_WINDOW_DURATION = "5 minutes"
DEFAULT_SLIDE_DURATION = "1 minute"
DEFAULT_METRICS_OUTPUT_MODE = "update"
DEFAULT_CHECKPOINT_DIR = checkpoints_path("spark/query=market-stream")


# ==============================================================================
# CLI (US-18 y US-19)
# ==============================================================================
def parse_args(argv=None) -> argparse.Namespace:
    """Define los parámetros de configuración de la CLI de Spark Streaming."""
    parser = argparse.ArgumentParser(
        prog="streaming_market.py",
        description=(
            "Job Spark Structured Streaming: consumo de Kafka con event-time, "
            "watermarks, deduplicación, métricas de ventana y DLQ."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Servidores bootstrap de Kafka (ej. kafka:9092 o localhost:29092).",
    )
    parser.add_argument(
        "--input-topic",
        type=str,
        default=DEFAULT_INPUT_TOPIC,
        help="Topic Kafka de entrada con los eventos de mercado crudos.",
    )
    parser.add_argument(
        "--dlq-topic",
        type=str,
        default=DEFAULT_DLQ_TOPIC,
        help="Topic Kafka de salida para eventos anómalos o corruptos (DLQ).",
    )
    parser.add_argument(
        "--metrics-topic",
        type=str,
        default=DEFAULT_METRICS_TOPIC,
        help="Topic Kafka de salida para métricas calculadas en tiempo real (US-19).",
    )
    parser.add_argument(
        "--watermark-delay",
        type=str,
        default=DEFAULT_WATERMARK_DELAY,
        help="Retraso de la marca de agua (ej. '2 minutes', '5 minutes').",
    )
    parser.add_argument(
        "--window-duration",
        type=str,
        default=DEFAULT_WINDOW_DURATION,
        help="Tamaño de la ventana temporal de agregación (ej. '5 minutes').",
    )
    parser.add_argument(
        "--slide-duration",
        type=str,
        default=DEFAULT_SLIDE_DURATION,
        help="Intervalo de deslizamiento de la ventana temporal (ej. '1 minute').",
    )
    parser.add_argument(
        "--metrics-output-mode",
        type=str,
        default=DEFAULT_METRICS_OUTPUT_MODE,
        choices=("update", "append"),
        help="Modo de salida para las métricas en Kafka: 'update' (provisionales rápidas) o 'append' (cierre de ventana).",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="latest",
        choices=("latest", "earliest"),
        help="Posición de lectura inicial en los topics de Kafka.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=DEFAULT_CHECKPOINT_DIR,
        help="Directorio de persistencia de checkpoints en MinIO o filesystem.",
    )
    parser.add_argument(
        "--console-sink",
        action="store_true",
        help="Emite eventos válidos y métricas por consola para depuración y monitoreo local.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida parámetros y configuración de sesión sin iniciar bucle infinito.",
    )

    return parser.parse_args(argv)


# ==============================================================================
# Parseo, Validación y Clasificación (US-18: Criterios 1 y 3)
# ==============================================================================
def classify_and_parse_events(raw_df: DataFrame) -> DataFrame:
    """
    Deserializa los mensajes JSON de Kafka y clasifica cada evento como válido
    o anómalo según el contrato canónico de datos (US-18, Criterio 1 y 3).

    Garantías de resiliencia:
    1. `mode='PERMISSIVE'` asegura que errores de sintaxis JSON no colapsen la JVM.
    2. Valida presencia de campos obligatorios, valores positivos de precio y
       formato parseable de marcas de tiempo UTC.
    3. Etiqueta la causa exacta del rechazo (`error_reason`) para trazabilidad en DLQ.
    """
    # Si la entrada proviene directamente de Kafka, la carga útil está en `value`
    if "value" in raw_df.columns:
        source_col = F.col("value").cast("string")
    elif "raw_payload" in raw_df.columns:
        source_col = F.col("raw_payload").cast("string")
    else:
        # Fallback a la primera columna disponible
        source_col = F.col(raw_df.columns[0]).cast("string")

    with_raw = raw_df.withColumn("raw_payload", source_col)

    # Deserialización con esquema estricto y modo permisivo capturando registros corruptos
    parsed = with_raw.withColumn(
        "_parsed",
        F.from_json(
            F.col("raw_payload"),
            MARKET_EVENT_SCHEMA,
            options={"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )

    # Detección determinista de errores de sintaxis JSON
    trimmed_raw = F.trim(F.col("raw_payload"))
    is_corrupt_json = (
        F.col("_parsed").isNull()
        | F.col("_parsed._corrupt_record").isNotNull()
        | (trimmed_raw == "")
        | (~trimmed_raw.startswith("{"))
        | (~trimmed_raw.endswith("}"))
    )

    # Parseo a Timestamp de event_time e ingestion_time
    event_timestamp_col = F.to_timestamp(F.col("_parsed.event_time"))
    ingestion_timestamp_col = F.to_timestamp(F.col("_parsed.ingestion_time"))

    # Clasificación de anomalías por reglas de negocio y contrato formal
    error_reason_expr = (
        F.when(is_corrupt_json, F.lit("CORRUPT_JSON_SYNTAX"))
        .when(
            F.col("_parsed.event_id").isNull()
            | (F.trim(F.col("_parsed.event_id")) == "")
            | F.col("_parsed.exchange").isNull()
            | (F.trim(F.col("_parsed.exchange")) == "")
            | F.col("_parsed.symbol").isNull()
            | (F.trim(F.col("_parsed.symbol")) == "")
            | F.col("_parsed.schema_version").isNull(),
            F.lit("MISSING_REQUIRED_FIELDS"),
        )
        .when(event_timestamp_col.isNull(), F.lit("INVALID_EVENT_TIME"))
        .when(ingestion_timestamp_col.isNull(), F.lit("INVALID_INGESTION_TIME"))
        .when(
            (F.col("_parsed.price").isNull()) | (F.col("_parsed.price") <= 0.0),
            F.lit("INVALID_PRICE_NON_POSITIVE"),
        )
        .when(
            (F.col("_parsed.quantity").isNull()) | (F.col("_parsed.quantity") < 0.0),
            F.lit("INVALID_QUANTITY_NEGATIVE"),
        )
        .when(
            F.col("_parsed.schema_version") != "2.0.0",
            F.lit("UNSUPPORTED_SCHEMA_VERSION"),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    classified = parsed.withColumn("error_reason", error_reason_expr)
    classified = classified.withColumn("is_valid", F.col("error_reason").isNull())

    # Normalización de columnas tipadas para eventos conformes
    normalized = (
        classified
        .withColumn("event_id", F.trim(F.col("_parsed.event_id")))
        .withColumn("source", F.coalesce(F.col("_parsed.source"), F.lit("unknown")))
        .withColumn("exchange", F.lower(F.trim(F.col("_parsed.exchange"))))
        .withColumn("symbol", F.upper(F.trim(F.col("_parsed.symbol"))))
        .withColumn("event_time", event_timestamp_col)
        .withColumn("ingestion_time", ingestion_timestamp_col)
        .withColumn("price", F.col("_parsed.price").cast("double"))
        .withColumn("quantity", F.col("_parsed.quantity").cast("double"))
        .withColumn("trade_id", F.col("_parsed.trade_id").cast("string"))
        .withColumn("interval", F.col("_parsed.interval").cast("string"))
        .withColumn("schema_version", F.col("_parsed.schema_version"))
        .drop("_parsed")
    )

    return normalized


def extract_valid_events(classified_df: DataFrame) -> DataFrame:
    """Filtra y devuelve exclusivamente los eventos conformes al contrato."""
    return (
        classified_df
        .filter(F.col("is_valid") == True)
        .select(
            "event_id",
            "source",
            "exchange",
            "symbol",
            "event_time",
            "ingestion_time",
            "price",
            "quantity",
            "trade_id",
            "interval",
            "schema_version",
        )
    )


# ==============================================================================
# Definición de Marca de Agua (US-18: Criterio 2)
# ==============================================================================
def apply_watermark(
    df: DataFrame,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
    time_col: str = "event_time",
) -> DataFrame:
    """
    Aplica una marca de agua (watermark) explícita sobre la columna de evento (US-18, Criterio 2).

    Efectos y justificación arquitectónica:
    1. Define el horizonte de tolerancia a datos desordenados o retrasados de red.
    2. Limita la acumulación indefinida del estado de agregación en la memoria del ejecutor,
       previniendo fugas de memoria (OOM) en el pipeline de tiempo real.
    """
    if time_col not in df.columns:
        raise ValueError(
            f"La columna temporal '{time_col}' no se encuentra en el DataFrame para aplicar watermark."
        )

    # Validar que la columna sea de tipo Timestamp
    field = [f for f in df.schema.fields if f.name == time_col][0]
    if not isinstance(field.dataType, TimestampType):
        raise TypeError(
            f"La columna '{time_col}' debe ser de tipo TimestampType, encontrada: {field.dataType}"
        )

    return df.withWatermark(time_col, watermark_delay)


# ==============================================================================
# Deduplicación Stateful en Tiempo Real (US-19: Criterio 1)
# ==============================================================================
def deduplicate_streaming_events(
    df: DataFrame,
    dedup_keys: list = None,
) -> DataFrame:
    """
    Aplica deduplicación de eventos en tiempo real preservando memoria (US-19, Criterio 1).

    En Structured Streaming con marcas de agua, `dropDuplicates(["event_id", "event_time"])`
    utiliza el StateStore de Spark para recordar los IDs ya vistos, pero evacúa
    automáticamente los registros cuya marca de tiempo cae por detrás del watermark actual.
    Esto garantiza deduplicación exacta tolerando reintentos del productor sin agotar la RAM.
    """
    keys = dedup_keys or ["event_id", "event_time"]
    return df.dropDuplicates(keys)


# ==============================================================================
# Métricas Operativas en Ventanas Deslizantes (US-19: Criterio 2)
# ==============================================================================
def compute_sliding_window_metrics(
    df: DataFrame,
    window_duration: str = DEFAULT_WINDOW_DURATION,
    slide_duration: str = DEFAULT_SLIDE_DURATION,
) -> DataFrame:
    """
    Calcula métricas de mercado sobre ventanas de tiempo deslizantes (US-19, Criterio 2).

    Agrupa los eventos por ventana temporal y símbolo:
        `groupBy(window(col("event_time"), "5 minutes", "1 minute"), col("symbol"))`

    Métricas calculadas:
        - `volume`: Volumen transaccionado acumulado en la ventana (sum(quantity))
        - `min_price`: Precio mínimo observado (low)
        - `max_price`: Precio máximo observado (high)
        - `first_price`: Precio inicial o de apertura de la ventana (min_by(price, event_time))
        - `last_price`: Último precio observado o cierre provisional (max_by(price, event_time))
        - `trades_count`: Cantidad de transacciones registradas en la ventana
        - `price_change_pct`: Variación porcentual ((last - first) / first) * 100
        - `is_provisional`: Indicador booleano `True` para Serving Layer (US-21)
        - `computed_at`: Marca de tiempo en que se generó el agregado
    """
    window_col = F.window(F.col("event_time"), window_duration, slide_duration)

    aggregated = (
        df.groupBy(window_col, F.col("symbol"))
        .agg(
            F.sum("quantity").alias("volume"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.min_by("price", "event_time").alias("first_price"),
            F.max_by("price", "event_time").alias("last_price"),
            F.count("event_id").alias("trades_count"),
        )
    )

    first_price_col = F.col("first_price")
    last_price_col = F.col("last_price")

    pct_change_expr = F.when(
        (first_price_col.isNotNull()) & (first_price_col > 0.0),
        F.round(((last_price_col - first_price_col) / first_price_col) * 100.0, 4),
    ).otherwise(F.lit(0.0))

    metrics_df = (
        aggregated
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("price_change_pct", pct_change_expr)
        .withColumn("is_provisional", F.lit(True))
        .withColumn("computed_at", F.current_timestamp())
        .drop("window")
    )

    return metrics_df


# ==============================================================================
# Formato de Salida para Dead-Letter Queue (US-18: Criterio 3)
# ==============================================================================
def format_dlq_stream(classified_df: DataFrame) -> DataFrame:
    """
    Prepara los eventos anómalos para su publicación en el topic Kafka `market.dlq`.

    Cumple con el estándar de productores de Kafka: genera columnas `key` y `value`.
    El payload incluye el mensaje crudo original, el diagnóstico del error y marca temporal.
    """
    invalid_df = classified_df.filter(F.col("is_valid") == False)

    dlq_payload = F.to_json(
        F.struct(
            F.coalesce(F.col("event_id"), F.lit("UNKNOWN_EVENT")).alias("event_id"),
            F.col("raw_payload").alias("raw_payload"),
            F.col("error_reason").alias("error"),
            F.current_timestamp().alias("dlq_timestamp"),
            # Estructura común de la DLQ (US-09): la misma que usan los conectores de ingesta.
            # Los campos de arriba se conservan para no romper a quien ya los lee.
            F.col("raw_payload").alias("original_event"),
            F.col("error_reason").alias("error_reason"),
            F.lit("spark_streaming").alias("failed_component"),
            F.date_format(
                F.to_utc_timestamp(F.current_timestamp(), F.current_timezone()),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            ).alias("timestamp_utc"),
            F.col("source").alias("source"),
        )
    )

    dlq_key = F.coalesce(F.col("event_id"), F.lit("corrupted")).cast("string")

    return invalid_df.select(
        dlq_key.alias("key"),
        dlq_payload.alias("value"),
    )


# ==============================================================================
# Formato de Salida para Métricas en Kafka (US-19: Criterio 3)
# ==============================================================================
def format_metrics_for_kafka(metrics_df: DataFrame) -> DataFrame:
    """
    Prepara las métricas calculadas para emitirse hacia Kafka topic `market.metrics` (US-19, Criterio 3).

    - `key`: Símbolo del activo (ej. BTCUSDT) para garantizar particionado determinista.
    - `value`: JSON serializado con las métricas de la ventana y el indicador `is_provisional = true`.
    """
    payload_struct = F.struct(
        F.col("symbol"),
        F.date_format(F.col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
        F.date_format(F.col("window_end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
        F.col("first_price"),
        F.col("last_price"),
        F.col("min_price"),
        F.col("max_price"),
        F.col("volume"),
        F.col("trades_count"),
        F.col("price_change_pct"),
        F.col("is_provisional"),
        F.date_format(F.col("computed_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("computed_at"),
    )

    return metrics_df.select(
        F.col("symbol").cast("string").alias("key"),
        F.to_json(payload_struct).alias("value"),
    )


# ==============================================================================
# Sinks de Salida y Consultas de Streaming
# ==============================================================================
def create_kafka_read_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest",
) -> DataFrame:
    """Inicia el streaming reader conectado al topic de Kafka."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def start_dlq_query(
    dlq_df: DataFrame,
    bootstrap_servers: str,
    dlq_topic: str = DEFAULT_DLQ_TOPIC,
    checkpoint_location: str = None,
):
    """Lanza la consulta de streaming que drena anomalías continuamente hacia Kafka DLQ."""
    writer = (
        dlq_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", dlq_topic)
        .outputMode("append")
    )
    if checkpoint_location:
        writer = writer.option("checkpointLocation", checkpoint_location)

    return writer.start()


def start_metrics_kafka_query(
    metrics_kafka_df: DataFrame,
    bootstrap_servers: str,
    metrics_topic: str = DEFAULT_METRICS_TOPIC,
    checkpoint_location: str = None,
    output_mode: str = DEFAULT_METRICS_OUTPUT_MODE,
):
    """
    Lanza la consulta de streaming que publica continuamente las métricas hacia Kafka `market.metrics`
    (US-19, Criterio 3). Admite modo 'update' (provisionales en tiempo real) y 'append' (cierre de ventana).
    """
    writer = (
        metrics_kafka_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", metrics_topic)
        .outputMode(output_mode)
    )
    if checkpoint_location:
        writer = writer.option("checkpointLocation", checkpoint_location)

    return writer.start()


def start_console_query(
    stream_df: DataFrame,
    checkpoint_location: str = None,
    query_name: str = "speed_metrics_events",
    output_mode: str = "update",
):
    """Consulta auxiliar de consola para depuración local o monitoreo."""
    writer = (
        stream_df.writeStream
        .format("console")
        .outputMode(output_mode)
        .queryName(query_name)
    )
    if checkpoint_location:
        writer = writer.option("checkpointLocation", checkpoint_location)

    return writer.start()


# ==============================================================================
# Pipeline Completo de Speed Layer
# ==============================================================================
def build_speed_layer_pipeline(
    raw_df: DataFrame,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
    window_duration: str = DEFAULT_WINDOW_DURATION,
    slide_duration: str = DEFAULT_SLIDE_DURATION,
):
    """
    Construye las etapas analíticas de la capa de velocidad (Speed Layer):
    1. Parseo y tipado estricto (US-18)
    2. Aislamiento y preparación de DLQ (US-18)
    3. Aplicación de watermark sobre event_time (US-18)
    4. Deduplicación stateful en tiempo real (US-19)
    5. Agregación en ventanas deslizantes (US-19)
    6. Formateo de salida para topic Kafka (US-19)

    Retorna la tupla `(dlq_stream, metrics_kafka_stream, metrics_raw_stream)`.
    """
    classified = classify_and_parse_events(raw_df)
    dlq_stream = format_dlq_stream(classified)

    valid = extract_valid_events(classified)
    watermarked = apply_watermark(valid, watermark_delay=watermark_delay)

    deduplicated = deduplicate_streaming_events(watermarked)
    metrics_stream = compute_sliding_window_metrics(
        deduplicated,
        window_duration=window_duration,
        slide_duration=slide_duration,
    )
    metrics_kafka = format_metrics_for_kafka(metrics_stream)

    return dlq_stream, metrics_kafka, metrics_stream


# ==============================================================================
# Punto de Entrada Principal (Main)
# ==============================================================================
def main(argv=None):
    """Punto de entrada de ejecución del job de streaming."""
    args = parse_args(argv)

    print("=" * 70)
    print("🚀 INICIANDO SPARK STRUCTURED STREAMING (US-18 / US-19: Speed Layer)")
    print(f"  Kafka Bootstrap:    {args.bootstrap_servers}")
    print(f"  Input Topic:        {args.input_topic}")
    print(f"  DLQ Topic:          {args.dlq_topic}")
    print(f"  Metrics Topic:      {args.metrics_topic}")
    print(f"  Watermark Delay:    {args.watermark_delay}")
    print(f"  Window Duration:    {args.window_duration}")
    print(f"  Slide Duration:     {args.slide_duration}")
    print(f"  Metrics Mode:       {args.metrics_output_mode}")
    print(f"  Starting Offsets:   {args.starting_offsets}")
    print(f"  Checkpoint Dir:     {args.checkpoint_dir}")
    print("=" * 70)

    spark = build_spark_session(
        app_name="streaming-market-speed",
        enable_kafka=True,
    )

    if args.dry_run:
        print("✅ Verificación en seco (dry-run) finalizada correctamente.")
        return 0

    # 1. Lectura continua de Kafka
    raw_stream = create_kafka_read_stream(
        spark=spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.input_topic,
        starting_offsets=args.starting_offsets,
    )

    # 2. Pipeline analítico completo (US-18 + US-19)
    dlq_stream, metrics_kafka_stream, metrics_raw = build_speed_layer_pipeline(
        raw_df=raw_stream,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        slide_duration=args.slide_duration,
    )

    # 3. Rama DLQ: enrutamiento de anomalías a Kafka market.dlq
    dlq_checkpoint = os.path.join(args.checkpoint_dir, "dlq")
    dlq_query = start_dlq_query(
        dlq_df=dlq_stream,
        bootstrap_servers=args.bootstrap_servers,
        dlq_topic=args.dlq_topic,
        checkpoint_location=dlq_checkpoint,
    )
    print(f"📡 Query DLQ activa hacia topic '{args.dlq_topic}' (Query ID: {dlq_query.id})")

    # 4. Rama de Métricas: emisión a Kafka market.metrics
    metrics_checkpoint = os.path.join(args.checkpoint_dir, "metrics")
    metrics_query = start_metrics_kafka_query(
        metrics_kafka_df=metrics_kafka_stream,
        bootstrap_servers=args.bootstrap_servers,
        metrics_topic=args.metrics_topic,
        checkpoint_location=metrics_checkpoint,
        output_mode=args.metrics_output_mode,
    )
    print(
        f"⚡ Query Métricas activa hacia topic '{args.metrics_topic}' "
        f"[modo: {args.metrics_output_mode}] (Query ID: {metrics_query.id})"
    )

    # 5. Monitoreo por consola si fue solicitado
    if args.console_sink:
        console_checkpoint = os.path.join(args.checkpoint_dir, "console")
        start_console_query(
            stream_df=metrics_raw,
            checkpoint_location=console_checkpoint,
            output_mode=args.metrics_output_mode,
        )

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo streaming ordenadamente...")
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("✅ Pipeline detenido.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
