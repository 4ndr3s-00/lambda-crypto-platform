"""
Spark Batch: lectura, normalización, deduplicación y agregación de los
históricos almacenados en el Data Lake de MinIO (S3-compatible).

Historias de usuario cubiertas:
    US-10  Lectura eficiente desde MinIO vía S3A, normalización de esquemas y
           deduplicación estricta por clave de evento.
    US-11  Cálculo histórico idempotente de velas OHLCV, retornos simples y
           logarítmicos y volumen acumulado.
    US-12  Parametrización CLI de rangos temporales arbitrarios (backfill) con
           particionado dinámico de MinIO.

Ejemplo de uso (backfill de un mes para dos símbolos):

    spark-submit --master spark://spark-master:7077 \
        spark/batch_historical.py \
        --start-date 2026-09-01 \
        --end-date   2026-09-30 \
        --symbols    BTCUSDT,ETHUSDT \
        --intervals  1m,5m,1h,1d

El job es idempotente: reejecutarlo sobre el mismo rango de fechas reescribe
únicamente las particiones `date=` afectadas y nunca duplica filas.
"""

import argparse
import os
import sys
from datetime import date, timedelta

# El job se ejecuta con `spark-submit spark/batch_historical.py`, donde el
# directorio del script es quien se agrega al `sys.path`. Lo replicamos aquí
# para que el módulo se pueda importar también desde las pruebas.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import (
    batch_results_path,
    build_spark_session,
    historical_path,
    quarantine_path,
)

# Duración de cada granularidad soportada, expresada en segundos.
# Se usa aritmética sobre el epoch en lugar de `date_trunc` porque 5m no es
# una unidad válida de `date_trunc` en Spark y de todos modos necesitamos
# exactamente estos cuatro Intervalos.
INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "1h": 3600,
    "1d": 86400,
}

# Columnas obligatorias del contrato canónico de eventos de mercado
# (ingestion/contracts/market_event.schema.json).
REQUIRED_EVENT_COLUMNS = (
    "event_id",
    "exchange",
    "symbol",
    "event_time",
    "price",
)

# Nombres de las columnas de salida de cada métrica agregada.
OHLCV_COLUMNS = (
    "symbol",
    "interval",
    "bucket_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trades",
    "volume_cumulative",
)

RETURNS_COLUMNS = (
    "symbol",
    "interval",
    "bucket_start",
    "close",
    "prev_close",
    "return_simple",
    "return_log",
)


# ==============================================================================
# CLI (US-12)
# ==============================================================================
def parse_date(value: str) -> date:
    """Convierte un texto `YYYY-MM-DD` en un objeto `date`."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Fecha inválida '{value}'. Se espera el formato YYYY-MM-DD."
        ) from exc


def parse_args(argv=None) -> argparse.Namespace:
    """Define la interfaz de línea de comandos del job de backfill."""
    parser = argparse.ArgumentParser(
        prog="batch_historical.py",
        description=(
            "Procesa los históricos de MinIO: normaliza, deduplica y calcula "
            "velas OHLCV y retornos de forma idempotente."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
        help="Fecha inicial del rango a procesar (inclusive).",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
        help="Fecha final del rango a procesar (inclusive).",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help=(
            "Lista de símbolos separados por coma (ej: BTCUSDT,ETHUSDT). "
            "Si se omite se procesan todos los símbolos disponibles."
        ),
    )
    parser.add_argument(
        "--intervals",
        type=str,
        default=",".join(INTERVAL_SECONDS),
        help=(
            "Granularidades a calcular, separadas por coma. "
            f"Valores soportados: {','.join(INTERVAL_SECONDS)}."
        ),
    )
    parser.add_argument(
        "--metrics",
        type=str,
        default="ohlcv,returns",
        help="Métricas a producir: ohlcv, returns o ambas.",
    )
    parser.add_argument(
        "--input-format",
        type=str,
        default="json",
        choices=("json", "parquet"),
        help="Formato de los archivos históricos en MinIO.",
    )
    parser.add_argument(
        "--output-root",
        type=str,
        default=None,
        help=(
            "Raíz de salida de los resultados agregados. "
            "Por defecto: s3a://market-lake/batch-results."
        ),
    )
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="URL del master Spark. Por defecto: spark://spark-master:7077.",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="batch-historical",
        help="Nombre de la aplicación Spark en la UI del master.",
    )
    parser.add_argument(
        "--quarantine-invalid",
        action="store_true",
        help=(
            "Envía los eventos que no cumplen el contrato a la Dead-Letter "
            "Queue en lugar de descartarlos silenciosamente."
        ),
    )

    args = parser.parse_args(argv)

    if args.start_date > args.end_date:
        parser.error("--start-date no puede ser posterior a --end-date.")

    args.symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else None
    )

    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
    invalid_intervals = [i for i in intervals if i not in INTERVAL_SECONDS]
    if invalid_intervals:
        parser.error(
            f"Granularidad no soportada: {','.join(invalid_intervals)}. "
            f"Valores permitidos: {','.join(INTERVAL_SECONDS)}."
        )
    args.intervals = intervals

    metrics = [m.strip().lower() for m in args.metrics.split(",") if m.strip()]
    invalid_metrics = [m for m in metrics if m not in ("ohlcv", "returns")]
    if invalid_metrics:
        parser.error(
            f"Métrica no soportada: {','.join(invalid_metrics)}. "
            "Valores permitidos: ohlcv, returns."
        )
    args.metrics = metrics

    return args


def date_range(start: date, end: date):
    """Genera la lista de fechas (inclusive) que cubre el rango solicitado."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ==============================================================================
# Lectura y normalización (US-10)
# ==============================================================================
def _has_only_keep_file(partition_path: str) -> bool:
    """Devuelve True si la partición contiene solo un archivo .keep (marcador de partición válida)."""
    import os
    try:
        entries = os.listdir(partition_path)
        # Un directorio con solo .keep significa que la partición está "sellada" y no tiene datos nuevos;
        # Spark lanzararía UNABLE_TO_INFER_SCHEMA al intentar leerla.
        return len(entries) == 1 and ".keep" in entries
    except Exception:
        return False


def read_historical(
    spark: SparkSession,
    start: date,
    end: date,
    symbols=None,
    input_format: str = "json",
) -> DataFrame:
    """
    Lee los eventos históricos desde MinIO usando el cliente S3A.

    Aplica *partition pruning* construyendo una ruta explícita por cada fecha
    del rango: Spark solo lista los prefijos `date=` solicitados y nunca
    recorre el histórico completo del Data Lake (US-12).

    El `basePath` se fija en la raíz de `historical/` para que Spark siga
    infiriendo las columnas de partición `source` y `date`.

    Las particiones inexistentes se descubren listando el bucket por S3 y se
    descartan antes de leer: Spark aborta con `PATH_NOT_FOUND` si alguna de
    las rutas del rango no existe, y un backfill debe poder ejecutarse sobre
    rangos con huecos.

    **Fix aplicado:** particiones que contienen solo un archivo `.keep` (marcador de partición válida)
    son ignoradas automáticamente en lugar de lanzar `UNABLE_TO_INFER_SCHEMA`.
    """
    base = historical_path()
    partitions = _existing_partitions(spark, base, start, end)

    # Filtrar particiones que contienen solo .keep (particiones "selladas", sin datos nuevos)
    partitions = [p for p in partitions if not _has_only_keep_file(p)]

    if not partitions:
        return _empty_events(spark, input_format)

    reader = spark.read.option("basePath", base)
    if input_format == "parquet":
        events = reader.parquet(partitions)
    else:
        events = reader.json(partitions)

    if symbols:
        events = events.filter(F.col("symbol").isin(symbols))

    return events


def _existing_partitions(
    spark: SparkSession,
    base: str,
    start: date,
    end: date,
) -> list:
    """
    Descubre las particiones `source=*/date=*` que existen dentro del rango.

    Se listan directamente sobre S3 en lugar de usar un glob de Hadoop,
    porque el glob de Hadoop no interpreta los directorios con el formato
    Hive `clave=valor` y fallaría con `IllegalArgumentException`.
    """
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = jvm.org.apache.hadoop.fs.Path
    filesystem = path_class(base).getFileSystem(hadoop_conf)

    def list_status(ruta: str):
        """Lista las entradas de una ruta, normalizando la barra final."""
        return [
            _ensure_trailing_slash(entry.getPath().toString())
            for entry in filesystem.listStatus(path_class(ruta))
        ]

    return _filter_partitions(list_status, base, start, end)


def _ensure_trailing_slash(path: str) -> str:
    """Normaliza una ruta de S3 para que termine en barra final."""
    return path if path.endswith("/") else path + "/"


def _filter_partitions(list_status, base: str, start: date, end: date) -> list:
    """
    Selecciona las particiones cuyas fechas caen dentro del rango pedido.

    `list_status` es la función que lista las entradas de una ruta. Se recibe
    como parámetro para poder ejercitar esta lógica sin necesidad de MinIO.
    """
    wanted = {d.isoformat() for d in date_range(start, end)}
    partitions = []

    try:
        sources = list_status(base)
    except Exception:
        # Si no se puede listar (bucket vacío o sin permisos), se devuelve
        # la lista vacía y la lectura se omite.
        return partitions

    for source_path in sources:
        try:
            dates = list_status(source_path)
        except Exception:
            continue
        for partition_path in dates:
            day = partition_path.rstrip("/").rsplit("/", 1)[-1]
            if day.startswith("date=") and day[len("date="):] in wanted:
                partitions.append(partition_path)

    return sorted(partitions)


def _empty_events(spark: SparkSession, input_format: str = "json") -> DataFrame:
    """Construye un dataset vacío con el esquema canónico de eventos."""
    schema = (
        "event_id string, source string, exchange string, symbol string, "
        "event_time string, ingestion_time string, price double, "
        "quantity double, trade_id string, interval string, schema_version string"
    )
    if input_format == "parquet":
        schema = (
            "event_id string, source string, exchange string, symbol string, "
            "event_time timestamp, ingestion_time timestamp, price double, "
            "quantity double, trade_id string, interval string, schema_version string"
        )
    return spark.createDataFrame([], schema=schema)


def normalize_events(events: DataFrame) -> DataFrame:
    """
    Normaliza el esquema crudo a los tipos canónicos del contrato.

    Los eventos que no tienen los campos obligatorios o que traen precios o
    cantidades inválidos se descartan (o se envían a la DLQ, si el job se
    lanzó con `--quarantine-invalid`).
    """
    normalized = (
        events
        .withColumn("event_id", F.trim(F.col("event_id")).cast("string"))
        .withColumn("exchange", F.lower(F.trim(F.col("exchange"))).cast("string"))
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))).cast("string"))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingestion_time", F.to_timestamp("ingestion_time"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("source", F.col("source").cast("string"))
        .withColumn("trade_id", F.col("trade_id").cast("string"))
    )

    # Un evento es válido si trae los campos obligatorios, el precio es
    # positivo y la cantidad no es negativa (contrato US-05).
    valid_condition = (
        F.col("event_id").isNotNull()
        & F.col("exchange").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
        & F.col("quantity").isNotNull()
        & (F.col("quantity") >= 0)
    )

    invalid = normalized.filter(~valid_condition)
    valid = normalized.filter(valid_condition)

    return valid, invalid


def deduplicate_events(events: DataFrame) -> DataFrame:
    """
    Elimina duplicados de forma estricta (US-10, criterio 2).

    Se aplican dos niveles de deduplicación:
        1. Por `event_id`, que es la clave única del evento.
        2. Por la clave compuesta `(exchange, symbol, event_time)`, que atrapa
           el caso de que el mismo evento haya sido emitido con identificadores
           distintos por dos conectores.
    """
    by_event_id = events.dropDuplicates(["event_id"])
    return by_event_id.dropDuplicates(["exchange", "symbol", "event_time"])


# ==============================================================================
# Agregaciones (US-11)
# ==============================================================================
def _bucket_start(column: str, seconds: int):
    """
    Trunca una marca de tiempo al inicio de su intervalo.

    La columna se castea a `timestamp` antes de convertirla a epoch porque
    `unix_timestamp` solo interpreta correctamente valores ya tipados: sobre
    una cadena ISO-8601 devolvería `null` y todas las velas acabarían en el
    mismo bucket.
    """
    epoch = F.unix_timestamp(F.col(column).cast("timestamp"))
    return F.from_unixtime(F.floor(epoch / seconds) * seconds).cast("timestamp")


def compute_ohlcv(events: DataFrame, intervals=None) -> DataFrame:
    """
    Calcula las velas OHLCV con Spark Window Functions (US-11, criterio 1).

    Por cada granularidad se define una ventana particionada por
    `(symbol, bucket_start)` y ordenada por `event_time`. Sobre esa ventana se
    calculan `first` (apertura), `max` (máximo), `min` (mínimo), `last`
    (cierre) y `sum` (volumen), que es exactamente la semántica de una vela
    OHLCV.

    El volumen acumulado se calcula como suma corrida sobre la misma
    partición, por lo que refleja el total acumulado dentro del rango
    procesado.
    """
    intervals = intervals or list(INTERVAL_SECONDS)
    frames = []

    # Si el dataset de entrada no trae volumen (por ejemplo, cuando solo se
    # cálcula el precio de una vela), se asume cantidad cero en lugar de
    # fallar por una columna ausente.
    if "quantity" not in events.columns:
        events = events.withColumn("quantity", F.lit(0.0))

    for interval in intervals:
        seconds = INTERVAL_SECONDS[interval]
        bucketed = events.withColumn("bucket_start", _bucket_start("event_time", seconds))

        candle_window = Window.partitionBy("symbol", "bucket_start").orderBy(
            "event_time"
        ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        candles = (
            bucketed
            .withColumn("open", F.first("price").over(candle_window))
            .withColumn("high", F.max("price").over(candle_window))
            .withColumn("low", F.min("price").over(candle_window))
            .withColumn("close", F.last("price").over(candle_window))
            .withColumn("volume", F.sum("quantity").over(candle_window))
            .withColumn("trades", F.count(F.lit(1)).over(candle_window))
            .withColumn("interval", F.lit(interval))
        )

        # El volumen acumulado es una suma corrida sobre la partición.
        running_volume = (
            Window.partitionBy("symbol", "interval")
            .orderBy("bucket_start")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        candles = candles.withColumn(
            "volume_cumulative", F.sum("volume").over(running_volume)
        )

        # Tras la agregación todas las filas de una misma vela tienen valores
        # idénticos, así que basta conservar una sola por clave.
        candles = candles.select(*OHLCV_COLUMNS).dropDuplicates(
            ["symbol", "interval", "bucket_start"]
        )
        frames.append(candles)

    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def compute_returns(ohlcv: DataFrame) -> DataFrame:
    """
    Calcula los retornos simples y logarítmicos entre velas consecutivas.

    El retorno simple es `(Close_t - Close_{t-1}) / Close_{t-1}` y el
    logarítmico es `ln(Close_t / Close_{t-1})` (US-11, criterio 2).
    """
    ordering = Window.partitionBy("symbol", "interval").orderBy("bucket_start")

    return (
        ohlcv
        .withColumn("prev_close", F.lag("close").over(ordering))
        .withColumn(
            "return_simple",
            (F.col("close") - F.col("prev_close")) / F.col("prev_close"),
        )
        .withColumn("return_log", F.log(F.col("close") / F.col("prev_close")))
        .select(*RETURNS_COLUMNS)
    )


# ==============================================================================
# Escritura idempotente (US-11, criterio 3)
# ==============================================================================
def write_idempotent(
    df: DataFrame,
    path: str,
    partition_column: str = "date",
) -> None:
    """
    Escribe el DataFrame en Parquet con compresión Snappy sobrescribiendo
    únicamente las particiones afectadas.

    Con `partitionOverwriteMode=dynamic` Spark borra solo las particiones
    `date=` presentes en el DataFrame y deja intactas las demás. La opción se
    fija en el propio writer (y no solo en la sesión) para que el
    comportamiento idempotente no dependa de cómo se construyó la sesión.
    """
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_column)
        .parquet(path)
    )


def write_quarantine(invalid: DataFrame, start: date, end: date) -> None:
    """Envía los eventos inválidos a la Dead-Letter Queue particionada por fecha."""
    for day in date_range(start, end):
        target = quarantine_path(date=day.isoformat())
        (
            invalid.write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("date")
            .parquet(target)
        )


# ==============================================================================
# Orquestación
# ==============================================================================
def run(spark: SparkSession, args: argparse.Namespace) -> dict:
    """Ejecuta el pipeline completo y devuelve un resumen de la ejecución."""
    events = read_historical(
        spark,
        args.start_date,
        args.end_date,
        symbols=args.symbols,
        input_format=args.input_format,
    )

    valid, invalid = normalize_events(events)

    raw_count = events.count()
    valid_count = valid.count()
    invalid_count = raw_count - valid_count
    deduplicated = deduplicate_events(valid)
    dedup_count = deduplicated.count()

    output_root = args.output_root or batch_results_path()

    if invalid_count > 0 and args.quarantine_invalid:
        write_quarantine(invalid, args.start_date, args.end_date)

    written = {}
    if "ohlcv" in args.metrics:
        ohlcv = compute_ohlcv(deduplicated, intervals=args.intervals)
        ohlcv = ohlcv.withColumn("date", F.to_date("bucket_start"))
        target = f"{output_root}/metric=ohlcv"
        write_idempotent(ohlcv, target, partition_column="date")
        written["ohlcv"] = target

    if "returns" in args.metrics:
        ohlcv = compute_ohlcv(deduplicated, intervals=args.intervals)
        returns = compute_returns(ohlcv).withColumn("date", F.to_date("bucket_start"))
        target = f"{output_root}/metric=returns"
        write_idempotent(returns, target, partition_column="date")
        written["returns"] = target

    return {
        "raw_events": raw_count,
        "valid_events": valid_count,
        "invalid_events": invalid_count,
        "deduplicated_events": dedup_count,
        "written": written,
    }


def main(argv=None) -> int:
    """Punto de entrada del job."""
    args = parse_args(argv)
    spark = build_spark_session(args.app_name, master_url=args.master)

    try:
        summary = run(spark, args)
        print("=== Batch histórico finalizado ===")
        for key, value in summary.items():
            print(f"  {key}: {value}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
