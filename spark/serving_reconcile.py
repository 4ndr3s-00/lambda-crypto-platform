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

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
