"""
Capa Serving: reconciliación entre la ruta Speed y la ruta Batch (US-21).

Este módulo implementa el algoritmo de reconciliación Lambda que combina:

    * La ruta **Batch**: resultados históricos consolidados, ya cerrados y
      definitivos, escritos por la capa Batch en
      `s3://market-lake/batch-results/`.
    * La ruta **Speed**: resultados provisionales recientes, escritos por la
      capa Speed mientras la ventana aún no ha sido consolidada.

Especificación matemática
--------------------------
Sea `B` el conjunto de filas de la ruta Batch y `S` el de la ruta Speed, y sea
`cutoff` (`batch_cutoff_timestamp`) la marca de tiempo hasta la cual la capa
Batch ya consolidó el histórico. La función de reconciliación se define como:

    R(cutoff) = { b ∈ B : b.event_time <= cutoff }
              ∪ { s ∈ S : s.event_time > cutoff }

Propiedades que se pueden leer directamente de la definición:

1. **Precedencia del Batch en ventanas cerradas.** Toda fila de `B` con
   `event_time <= cutoff` se conserva y desplaza a cualquier fila de `S` del
   mismo periodo, porque el corte de la unión es inclusivo (`<=`) para Batch
   y exclusivo (`>`) para Speed.

2. **Dominancia de Speed en la ventana reciente.** Toda fila de `S` con
   `event_time > cutoff` se conserva, marking el resultado como *provisional*.

3. **Ausencia total de doble conteo.** Los dos conjuntos son disjuntos: una
   fila no puede cumplir a la vez `event_time <= cutoff` y `event_time >
   cutoff`. Por tanto `|R| = |{b <= cutoff}| + |{s > cutoff}|` y ninguna fila
   queda contada dos veces, aunque Speed y Batch hayan calculado la misma
   ventana.

4. **Monotonía.** Al avanzar `cutoff`, la función solo puede mover filas del
   conjunto Speed al conjunto Batch, nunca al revés.

La unicidad de la clave `(symbol, interval, bucket_start)` garantiza que el
resultado consolidado tenga exactamente una fila por vela.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark_session import batch_results_path, build_spark_session, pg_conn_params_from_args

# Columna que identifica de forma única una vela dentro del dataset
# consolidado. La reconciliación se apoya en ella para descartar las filas
# de Speed que el Batch ya consolidó.
CANDLE_KEY = ["symbol", "interval", "bucket_start"]

# Marcador que identifica el origen de cada fila del resultado unificado.
SOURCE_BATCH = "batch"
SOURCE_SPEED = "speed"

# Epoch usado como corte cuando la ruta Batch todavía no tiene datos: en ese
# caso no hay ninguna ventana consolidada y todo el histórico de Speed queda
# marcado como provisional.
EPOCH_CUTOFF = "TIMESTAMP '1970-01-01 00:00:00'"

# Nombre de la columna auxiliar que transporta el corte durante el cruce.
_CUTOFF_COLUMN = "__batch_cutoff__"


def compute_batch_cutoff(batch_df: DataFrame) -> DataFrame:
    """
    Deriva el `batch_cutoff_timestamp` a partir de la propia ruta Batch.

    El corte es la marca de tiempo máxima ya consolidada por la capa Batch:
    todo lo anterior o igual a ese instante está cerrado y es definitivo;
    todo lo posterior todavía pertenece a la ruta Speed.

    Se devuelve como DataFrame de una sola fila porque Spark no admite
    funciones de agregación (`max`) dentro de un `WHERE`: el corte se
    calcula por separado y luego se cruza con cada ruta.
    """
    return batch_df.agg(F.max(F.col("event_time")).alias(_CUTOFF_COLUMN))


def resolve_cutoff(batch_df: DataFrame, cutoff=None) -> DataFrame:
    """
    Devuelve el corte como un DataFrame de una fila con la columna auxiliar.

    Si no se entrega `cutoff`, se deriva del máximo consolidado por la capa
    Batch. Si la ruta Batch está vacía, se usa el epoch, lo que deja toda la
    ruta Speed como provisional.
    """
    if cutoff is not None:
        value = cutoff if isinstance(cutoff, F.Column) else F.lit(cutoff)
        return batch_df.sparkSession.range(1).select(value.alias(_CUTOFF_COLUMN))

    if batch_df.isEmpty():
        return batch_df.sparkSession.range(1).select(
            F.expr(EPOCH_CUTOFF).alias(_CUTOFF_COLUMN)
        )

    return compute_batch_cutoff(batch_df)


def reconcile(
    batch_df: DataFrame,
    speed_df: DataFrame,
    cutoff=None,
) -> DataFrame:
    """
    Combina la ruta Batch y la ruta Speed en un único dataset unificado.

    Parámetros
    ----------
    batch_df : DataFrame
        Resultados consolidados de la capa Batch.
    speed_df : DataFrame
        Resultados provisionales de la capa Speed.
    cutoff : timestamp, opcional
        `batch_cutoff_timestamp`. Si se omite, se deriva de la ruta Batch.

    Devuelve
    -------
    DataFrame
        Dataset unificado con las columnas comunes más `source` (origen de la
        fila) e `is_provisional` (indica si el valor todavía puede cambiar).
    """
    cutoff_frame = resolve_cutoff(batch_df, cutoff)
    cutoff_col = F.col(_CUTOFF_COLUMN)

    # --- Lado Batch: ventanas cerradas, resultado definitivo ---------------
    closed_batch = (
        batch_df.crossJoin(cutoff_frame)
        .filter(F.col("event_time") <= cutoff_col)
        .withColumn("source", F.lit(SOURCE_BATCH))
        .withColumn("is_provisional", F.lit(False))
        .drop(_CUTOFF_COLUMN)
    )

    # --- Lado Speed: ventana reciente, resultado provisional ---------------
    recent_speed = (
        speed_df.crossJoin(cutoff_frame)
        .filter(F.col("event_time") > cutoff_col)
        .withColumn("source", F.lit(SOURCE_SPEED))
        .withColumn("is_provisional", F.lit(True))
        .drop(_CUTOFF_COLUMN)
    )

    return closed_batch.unionByName(recent_speed)


def reconcile_deduplicated(
    batch_df: DataFrame,
    speed_df: DataFrame,
    cutoff=None,
    key: list = None,
) -> DataFrame:
    """
    Reconcilia y garantiza que exista como máximo una fila por clave de vela.

    Es la versión que debe consumir la API de servicio: además de la
    precedencia temporal, aplica la precedencia de clave, de modo que si el
    Batch y el Speed calculan la misma vela, sobrevive la del Batch (por ser
    la definitiva) y la del Speed se descarta.
    """
    key = key or CANDLE_KEY
    unified = reconcile(batch_df, speed_df, cutoff)

    # La precedencia se resuelve ordenando por `is_provisional` ascendente
    # (False = Batch, que va primero) y tomando la primera fila de cada clave.
    winner = (
        F.row_number()
        .over(
            Window
            .partitionBy(*key)
            .orderBy(F.col("is_provisional").asc(), F.col("event_time").desc())
        )
    )

    return unified.withColumn("_rn", winner).filter(F.col("_rn") == 1).drop("_rn")


def unified_ohlcv_view(
    batch_df: DataFrame,
    speed_df: DataFrame,
    cutoff=None,
) -> DataFrame:
    """
    Vista unificada de velas OHLCV lista para ser consultada por la API.

    Si no se entrega `cutoff`, se deriva de la ruta Batch.
    """
    return reconcile_deduplicated(batch_df, speed_df, cutoff)


def reconciliation_stats(
    batch_df: DataFrame,
    speed_df: DataFrame,
    cutoff=None,
) -> dict:
    """
    Calcula las métricas de auditoría de la reconciliación.

    Sirve para certificar en las pruebas que la unión no introduce doble
    conteo: `overlapping_keys` debe ser cero y `unified_rows` debe ser igual a
    la suma de `batch_rows` y `speed_rows` que efectivamente entran en la
    unión.
    """
    unified = reconcile(batch_df, speed_df, cutoff)
    key = CANDLE_KEY

    cutoff_frame = resolve_cutoff(batch_df, cutoff)
    cutoff_col = F.col(_CUTOFF_COLUMN)

    batch_selected = (
        batch_df.crossJoin(cutoff_frame)
        .filter(F.col("event_time") <= cutoff_col)
        .drop(_CUTOFF_COLUMN)
    )
    speed_selected = (
        speed_df.crossJoin(cutoff_frame)
        .filter(F.col("event_time") > cutoff_col)
        .drop(_CUTOFF_COLUMN)
    )

    batch_keys = set(batch_selected.select(*key).distinct().collect())
    speed_keys = set(speed_selected.select(*key).distinct().collect())
    overlapping = batch_keys & speed_keys

    return {
        "batch_rows": batch_selected.count(),
        "speed_rows": speed_selected.count(),
        "unified_rows": unified.count(),
        "overlapping_keys": len(overlapping),
    }


# ==============================================================================
# Persistencia en Postgres y CLI (ejecución programada, p.ej. desde Airflow)
# ==============================================================================
# Hasta aquí este módulo era una librería pura de DataFrames (solo tests). Lo
# que sigue cierra el circuito real: lee la ruta Batch desde MinIO, la ruta
# Speed desde `serving.realtime_metrics` (lo único que el Speed Layer deja en
# Postgres hoy: el último valor conocido por símbolo) y escribe el resultado
# reconciliado en `serving.ohlcv_reconciled`, que es la tabla que de verdad
# consulta `api/query_service.py`.

JDBC_URL_TEMPLATE = "jdbc:postgresql://{host}:{port}/{dbname}"


def _jdbc_options(pg_conn_params: dict) -> dict:
    return {
        "url": JDBC_URL_TEMPLATE.format(
            host=pg_conn_params["host"],
            port=pg_conn_params["port"],
            dbname=pg_conn_params["dbname"],
        ),
        "user": pg_conn_params["user"],
        "password": pg_conn_params["password"],
        "driver": "org.postgresql.Driver",
    }


_BATCH_OHLCV_SCHEMA = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("interval", T.StringType()),
    T.StructField("bucket_start", T.TimestampType()),
    T.StructField("bucket_end", T.TimestampType()),
    T.StructField("open", T.DoubleType()),
    T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()),
    T.StructField("close", T.DoubleType()),
    T.StructField("volume", T.DoubleType()),
    T.StructField("trades_count", T.LongType()),
    T.StructField("event_time", T.TimestampType()),
])


def read_batch_ohlcv(spark, interval: str, symbols: list = None) -> DataFrame:
    """
    Lee las velas consolidadas por la capa Batch (US-11) para un `interval` dado.

    `batch_historical.py` escribe todas las granularidades juntas bajo
    `batch-results/metric=ohlcv` (particionado solo por `date`), con `interval`
    como columna de datos, no de partición — por eso se filtra en memoria.

    Si todavía no existe ningún Parquet (Batch nunca corrió, o corrió sin
    datos en el rango pedido), se devuelve un DataFrame vacío con el esquema
    esperado en vez de propagar la excepción: `resolve_cutoff()` ya sabe
    tratar un Batch vacío como "todo Speed es provisional" (decisión de
    diseño #3), así que la reconciliación no debe saltarse el intervalo
    completo solo porque la capa Batch aún no escribió nada.
    """
    path = batch_results_path("ohlcv")
    try:
        df = spark.read.parquet(path)
    except Exception:  # noqa: BLE001 — Parquet sin archivos: schema no inferible.
        return spark.createDataFrame([], _BATCH_OHLCV_SCHEMA)

    df = (
        df.filter(F.col("interval") == interval)
        .withColumn("event_time", F.col("bucket_start"))
        .withColumn("trades_count", F.coalesce(F.col("trades_count"), F.lit(0)))
    )
    # `batch-results/` está particionado por `date`; esa columna es de
    # almacenamiento, no del contrato lógico, y rompe el unionByName contra
    # la ruta Speed.
    if "date" in df.columns:
        df = df.drop("date")
    if symbols:
        df = df.filter(F.col("symbol").isin(symbols))
    return df


def read_speed_snapshot(spark, pg_conn_params: dict, interval: str, symbols: list = None) -> DataFrame:
    """
    Proyecta `serving.realtime_metrics` (último valor conocido por símbolo) como
    una "vela en formación" del intervalo solicitado, para poder reconciliarla
    contra la ruta Batch con el mismo esquema `(symbol, interval, bucket_start, ...)`.

    Es una aproximación honesta de lo que hay disponible hoy: el Speed Layer
    (`streaming_market.py`) solo persiste el último snapshot por símbolo, no un
    historial de velas; por eso la fila Speed siempre representa "ahora mismo".
    """
    query = (
        "(SELECT symbol, last_price, rolling_volume_5m, event_timestamp "
        "FROM serving.realtime_metrics) AS speed_snapshot"
    )
    raw = spark.read.format("jdbc").options(**_jdbc_options(pg_conn_params), dbtable=query).load()
    if symbols:
        raw = raw.filter(F.col("symbol").isin(symbols))

    seconds = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}.get(interval, 300)
    bucket_start = F.from_unixtime(
        (F.unix_timestamp("event_timestamp") / seconds).cast("long") * seconds
    ).cast("timestamp")

    return (
        raw.withColumn("bucket_start", bucket_start)
        .withColumn("bucket_end", F.col("event_timestamp"))
        .withColumn("interval", F.lit(interval))
        .withColumn("open", F.col("last_price"))
        .withColumn("high", F.col("last_price"))
        .withColumn("low", F.col("last_price"))
        .withColumn("close", F.col("last_price"))
        .withColumn("volume", F.coalesce(F.col("rolling_volume_5m"), F.lit(0.0)))
        .withColumn("trades_count", F.lit(0))
        .withColumn("event_time", F.col("event_timestamp"))
        .select(
            "symbol", "interval", "bucket_start", "bucket_end",
            "open", "high", "low", "close", "volume", "trades_count", "event_time",
        )
    )


def write_ohlcv_reconciled(unified_df: DataFrame, pg_conn_params: dict) -> int:
    """
    Hace upsert del resultado reconciliado en `serving.ohlcv_reconciled`.

    Se materializa un batch pequeño (una fila por vela por símbolo) con
    `collect()` y se escribe con `INSERT ... ON CONFLICT` vía psycopg2: Spark
    JDBC no soporta upsert nativo y el volumen por ejecución es mínimo
    (símbolos activos x intervalos), así que no se justifica un MERGE distribuido.
    """
    rows = unified_df.select(
        "symbol", "interval", "bucket_start", "bucket_end",
        "open", "high", "low", "close", "volume", "trades_count",
        "source", "is_provisional",
    ).collect()

    if not rows:
        return 0

    import psycopg2
    from psycopg2.extras import execute_values

    query = """
        INSERT INTO serving.ohlcv_reconciled (
            symbol, timeframe, bucket_start, bucket_end,
            open_price, high_price, low_price, close_price,
            volume, trades_count, source_layer, is_consolidated
        ) VALUES %s
        ON CONFLICT (symbol, timeframe, bucket_start) DO UPDATE SET
            bucket_end = EXCLUDED.bucket_end,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            trades_count = EXCLUDED.trades_count,
            source_layer = EXCLUDED.source_layer,
            is_consolidated = EXCLUDED.is_consolidated,
            updated_at = CURRENT_TIMESTAMP
        -- Nunca dejar que una fila provisional (Speed) pise una ya consolidada
        -- (Batch) para la misma vela: eso rompería la precedencia de US-21.
        WHERE serving.ohlcv_reconciled.is_consolidated = FALSE
           OR EXCLUDED.is_consolidated = TRUE;
    """
    values = [
        (
            r["symbol"], r["interval"], r["bucket_start"], r["bucket_end"],
            r["open"], r["high"], r["low"], r["close"],
            r["volume"], r["trades_count"],
            r["source"], (not r["is_provisional"]),
        )
        for r in rows
    ]

    conn = psycopg2.connect(connect_timeout=5, **pg_conn_params)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, query, values)
    finally:
        conn.close()

    return len(values)


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="serving_reconcile.py",
        description=(
            "Reconcilia la ruta Batch (MinIO) y la ruta Speed (serving.realtime_metrics) "
            "y escribe el resultado en serving.ohlcv_reconciled (US-21)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--intervals", type=str, default="1m,5m,15m,1h,1d")
    parser.add_argument("--symbols", type=str, default=None, help="Lista separada por comas, ej. BTCUSDT,ETHUSDT")
    parser.add_argument("--postgres-host", type=str, default=os.environ.get("POSTGRES_HOST", "postgres"))
    parser.add_argument("--postgres-port", type=str, default=os.environ.get("POSTGRES_PORT", "5432"))
    parser.add_argument(
        "--postgres-db", type=str,
        default=os.environ.get("SERVING_DB_NAME", os.environ.get("POSTGRES_DB", "serving_db")),
    )
    parser.add_argument(
        "--postgres-user", type=str,
        default=os.environ.get("SERVING_DB_USER", os.environ.get("POSTGRES_USER", "postgres")),
    )
    parser.add_argument(
        "--postgres-password", type=str,
        default=os.environ.get("SERVING_DB_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "postgres_secure_pass")),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    intervals = [i.strip() for i in args.intervals.split(",") if i.strip()]
    symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    pg_conn_params = pg_conn_params_from_args(args)

    spark = build_spark_session(app_name="serving-reconcile", enable_postgres=True)

    total_written = 0
    for interval in intervals:
        try:
            batch_df = read_batch_ohlcv(spark, interval, symbols)
        except Exception as exc:  # noqa: BLE001
            print(f"⚠️  Sin datos Batch para interval={interval}: {exc}")
            continue

        speed_df = read_speed_snapshot(spark, pg_conn_params, interval, symbols)
        unified = reconcile_deduplicated(batch_df, speed_df)
        written = write_ohlcv_reconciled(unified, pg_conn_params)
        total_written += written
        print(f"✅ interval={interval}: {written} velas upsert en serving.ohlcv_reconciled")

    print(f"🏁 Reconciliación completa: {total_written} velas escritas en total.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
