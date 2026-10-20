"""
Spark Structured Streaming: Ingesta continua en tiempo real con event-time,
marcas de agua (watermarks) y enrutamiento a Dead-Letter Queue (DLQ).

Historia de Usuario: US-18 (Épica 6: Spark Streaming - Responsable: Diego)

Criterios de Aceptación (BDD):
    - Criterio 1: Consumo continuo y parseo de esquema
      Deserializa mensajes JSON desde Kafka (topic `market.events`) contra el
      contrato canónico (ingestion/contracts/market_event.schema.json) y tipa
      estrictamente la marca temporal `event_time` como columna Timestamp.
    - Criterio 2: Definición de marca de agua (Watermark)
      Configura un watermark explícito (por defecto '2 minutes') sobre `event_time`
      para delimitar la retención de estado en memoria y mitigar desbordes de RAM.
    - Criterio 3: Enrutamiento de anomalías a DLQ
      Ante mensajes corruptos o no conformes al contrato, aísla el registro
      y lo publica en el topic `market.dlq` sin colapsar el streaming.

Ejemplo de ejecución con spark-submit:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        spark/streaming_market.py \\
        --bootstrap-servers kafka:9092 \\
        --input-topic market.events \\
        --dlq-topic market.dlq \\
        --watermark-delay "2 minutes"
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
DEFAULT_WATERMARK_DELAY = "2 minutes"
DEFAULT_CHECKPOINT_DIR = checkpoints_path("spark/query=market-stream")


# ==============================================================================
# CLI (US-18)
# ==============================================================================
def parse_args(argv=None) -> argparse.Namespace:
    """Define los parámetros de configuración de la CLI de Spark Streaming."""
    parser = argparse.ArgumentParser(
        prog="streaming_market.py",
        description=(
            "Job Spark Structured Streaming: consumo de Kafka con event-time, "
            "watermarks y enrutamiento a Dead-Letter Queue (DLQ)."
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
        "--watermark-delay",
        type=str,
        default=DEFAULT_WATERMARK_DELAY,
        help="Retraso de la marca de agua (ej. '2 minutes', '5 minutes').",
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
        help="Emite eventos válidos por consola para depuración y monitoreo local.",
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
        )
    )

    dlq_key = F.coalesce(F.col("event_id"), F.lit("corrupted")).cast("string")

    return invalid_df.select(
        dlq_key.alias("key"),
        dlq_payload.alias("value"),
    )


# ==============================================================================
# Construcción del Pipeline de Structured Streaming
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


def start_console_query(
    stream_df: DataFrame,
    checkpoint_location: str = None,
    query_name: str = "valid_market_events",
):
    """Consulta auxiliar de consola para depuración local o monitoreo."""
    writer = (
        stream_df.writeStream
        .format("console")
        .outputMode("append")
        .queryName(query_name)
    )
    if checkpoint_location:
        writer = writer.option("checkpointLocation", checkpoint_location)

    return writer.start()


# ==============================================================================
# Punto de Entrada Principal (Main)
# ==============================================================================
def main(argv=None):
    """Punto de entrada de ejecución del job de streaming."""
    args = parse_args(argv)

    print("=" * 70)
    print("🚀 INICIANDO SPARK STRUCTURED STREAMING (US-18: Speed Layer)")
    print(f"  Kafka Bootstrap:    {args.bootstrap_servers}")
    print(f"  Input Topic:        {args.input_topic}")
    print(f"  DLQ Topic:          {args.dlq_topic}")
    print(f"  Watermark Delay:    {args.watermark_delay}")
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

    # 2. Parseo, tipado estricto y clasificación de anomalías
    classified_stream = classify_and_parse_events(raw_stream)

    # 3. Rama DLQ: enrutamiento de mensajes fallidos a Kafka
    dlq_stream = format_dlq_stream(classified_stream)
    dlq_checkpoint = os.path.join(args.checkpoint_dir, "dlq")
    dlq_query = start_dlq_query(
        dlq_df=dlq_stream,
        bootstrap_servers=args.bootstrap_servers,
        dlq_topic=args.dlq_topic,
        checkpoint_location=dlq_checkpoint,
    )
    print(f"📡 Query DLQ activa hacia topic '{args.dlq_topic}' (Query ID: {dlq_query.id})")

    # 4. Rama Válida: extracción y aplicación de Watermark explícito
    valid_stream = extract_valid_events(classified_stream)
    watermarked_stream = apply_watermark(
        valid_stream,
        watermark_delay=args.watermark_delay,
    )

    # 5. Salida principal (consola en dev/test o paso a agregaciones de US-19)
    valid_checkpoint = os.path.join(args.checkpoint_dir, "valid")
    valid_query = start_console_query(
        stream_df=watermarked_stream,
        checkpoint_location=valid_checkpoint,
    )
    print(f"⚡ Query Principal activa con watermark '{args.watermark_delay}' (Query ID: {valid_query.id})")

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
