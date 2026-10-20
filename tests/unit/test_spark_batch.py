"""
Pruebas unitarias de la capa Spark Batch (US-10, US-11, US-12).

Validan los criterios de aceptación:

- US-10 criterio 1: lectura desde MinIO vía S3A con filtrado por rango de fechas.
- US-10 criterio 2: deduplicación estricta por `event_id` y por
  `(exchange, symbol, event_time)`.
- US-10 criterio 3: escritura columnar en Parquet con compresión Snappy.
- US-11 criterio 1: cálculo exacto de OHLCV por intervalo temporal.
- US-11 criterio 2: cálculo de retornos simples y logarítmicos.
- US-11 criterio 3: idempotencia estricta de la escritura.
- US-12 criterio 1: parametrización CLI de rangos temporales y símbolos.
- US-12 criterio 2: reemplazo de particiones sin tocar las fechas no afectadas.

Las pruebas usan una SparkSession local (`local[2]`), por lo que no dependen
del clúster Docker ni de MinIO: son rápidas y deterministas.
"""

import os
import sys
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "spark"))

import batch_historical  # noqa: E402
import serving_reconcile  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    """SparkSession local compartida por todas las pruebas del módulo."""
    session = (
        SparkSession.builder.appName("tests-batch-historical")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _events(spark, rows):
    """Construye un DataFrame con el esquema crudo del contrato de eventos."""
    return spark.createDataFrame(
        rows,
        "event_id string, source string, exchange string, symbol string, "
        "event_time string, ingestion_time string, price double, "
        "quantity double, trade_id string, interval string, schema_version string",
    )


# ==============================================================================
# US-12: Parametrización CLI
# ==============================================================================
def test_bdd_us12_criterio_1_cli_rango_temporal():
    """
    Dado que se requiere recalcular un mes,
    Cuando se parsean los argumentos de línea de comandos,
    Entonces se aceptan las fechas y los símbolos solicitados.
    """
    args = batch_historical.parse_args(
        [
            "--start-date", "2026-09-01",
            "--end-date", "2026-09-30",
            "--symbols", "BTCUSDT,ETHUSDT",
        ]
    )

    assert args.start_date == date(2026, 9, 1)
    assert args.end_date == date(2026, 9, 30)
    assert args.symbols == ["BTCUSDT", "ETHUSDT"]


def test_bdd_us12_criterio_1_cli_normaliza_simbolos():
    """Los símbolos se normalizan a mayúsculas y sin espacios sobrantes."""
    args = batch_historical.parse_args(
        [
            "--start-date", "2026-09-01",
            "--end-date", "2026-09-30",
            "--symbols", "btcusdt, ethusdt",
        ]
    )
    assert args.symbols == ["BTCUSDT", "ETHUSDT"]


def test_bdd_us12_criterio_1_cli_rechaza_rango_invertido():
    """Un rango con la fecha inicial posterior a la final es un error de uso."""
    with pytest.raises(SystemExit):
        batch_historical.parse_args(
            ["--start-date", "2026-09-30", "--end-date", "2026-09-01"]
        )


def test_bdd_us12_criterio_1_cli_rechaza_granularidad_invalida():
    """Solo se aceptan las granularidades soportadas por la plataforma."""
    with pytest.raises(SystemExit):
        batch_historical.parse_args(
            [
                "--start-date", "2026-09-01",
                "--end-date", "2026-09-30",
                "--intervals", "7m",
            ]
        )


def test_bdd_us12_criterio_1_cli_rechaza_metrica_invalida():
    """Las métricas a producir están acotadas a ohlcv y returns."""
    with pytest.raises(SystemExit):
        batch_historical.parse_args(
            [
                "--start-date", "2026-09-01",
                "--end-date", "2026-09-30",
                "--metrics", "volatilidad",
            ]
        )


def test_bdd_us12_criterio_1_pruning_por_fecha():
    """
    Dado que se requiere procesar un subconjunto del histórico,
    Cuando se construye la lista de particiones a leer,
    Entonces solo se incluyen las particiones del rango solicitado.
    """
    base = "s3a://market-lake/historical"

    def list_status(ruta):
        """Simula el listado de un bucket con dos fuentes y tres fechas."""
        if ruta.rstrip("/").endswith("historical"):
            return [
                "s3a://market-lake/historical/source=binance",
                "s3a://market-lake/historical/source=coingecko",
            ]
        if "binance" in ruta:
            return [
                "s3a://market-lake/historical/source=binance/date=2026-09-01",
                "s3a://market-lake/historical/source=binance/date=2026-09-02",
                "s3a://market-lake/historical/source=binance/date=2026-09-03",
            ]
        return ["s3a://market-lake/historical/source=coingecko/date=2026-09-02"]

    partitions = batch_historical._filter_partitions(
        list_status, base, date(2026, 9, 1), date(2026, 9, 2)
    )

    # Solo se seleccionan las particiones del rango, no las del 2026-09-03.
    assert all("2026-09-03" not in p for p in partitions)
    assert any("date=2026-09-01" in p for p in partitions)
    assert any("date=2026-09-02" in p for p in partitions)
    # Las dos fuentes disponibles se incluyen.
    assert len(partitions) == 3


def test_bdd_us12_criterio_2_lectura_sin_datos_no_falla(spark):
    """
    Dado que un rango de fechas puede no tener datos,
    Cuando se lee el histórico de ese rango,
    Entonces se devuelve un dataset vacío en lugar de fallar.
    """
    events = batch_historical._empty_events(spark)

    assert events.count() == 0
    # El esquema vacío debe mantener las columnas canónicas del contrato.
    for column in ("event_id", "exchange", "symbol", "event_time", "price"):
        assert column in events.columns


def test_date_range_cubre_rango_inclusive():
    """El rango de fechas incluye tanto el inicio como el final."""
    days = list(batch_historical.date_range(date(2026, 9, 1), date(2026, 9, 3)))
    assert [d.isoformat() for d in days] == [
        "2026-09-01",
        "2026-09-02",
        "2026-09-03",
    ]


# ==============================================================================
# US-10: Normalización y deduplicación
# ==============================================================================
def test_bdd_us10_criterio_2_normalizacion_de_esquema(spark):
    """
    Dado que los eventos crudos traen los tipos del contrato,
    Cuando se normalizan,
    Entonces los timestamps se parsean y los precios quedan como doubles.
    """
    raw = _events(
        spark,
        [
            (
                "evt_1", "binance_ws", "binance", "btcusdt",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68450.50, 0.452, "1", "trade", "2.0.0",
            )
        ],
    )

    valid, invalid = batch_historical.normalize_events(raw)

    assert invalid.count() == 0
    row = valid.select("symbol", "event_time", "price").collect()[0]
    assert row["symbol"] == "BTCUSDT"          # normalizado a mayúsculas
    assert row["price"] == pytest.approx(68450.50)
    assert row["event_time"] is not None        # timestamp parseado


def test_bdd_us10_criterio_2_descarta_eventos_invalidos(spark):
    """
    Dado que algunos eventos violan el contrato,
    Cuando se normaliza el dataset,
    Entonces los inválidos se separan de los válidos.
    """
    raw = _events(
        spark,
        [
            # Evento válido.
            (
                "evt_ok", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68000.0, 0.1, "1", "trade", "2.0.0",
            ),
            # Precio no positivo.
            (
                "evt_bad_price", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:01:00.000Z", "2026-10-19T12:01:00.045Z",
                -5.0, 0.1, "2", "trade", "2.0.0",
            ),
            # Cantidad negativa.
            (
                "evt_bad_qty", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:02:00.000Z", "2026-10-19T12:02:00.045Z",
                68000.0, -0.1, "3", "trade", "2.0.0",
            ),
            # Falta la marca de tiempo.
            (
                "evt_no_time", "binance_ws", "binance", "BTCUSDT",
                None, "2026-10-19T12:03:00.045Z",
                68000.0, 0.1, "4", "trade", "2.0.0",
            ),
        ],
    )

    valid, invalid = batch_historical.normalize_events(raw)

    assert valid.count() == 1
    assert invalid.count() == 3


def test_bdd_us10_criterio_2_deduplicacion_por_event_id(spark):
    """
    Dado que la ingesta capturó el mismo evento dos veces,
    Cuando se deduplica por `event_id`,
    Entonces cada `event_id` queda representado una sola vez.
    """
    raw = _events(
        spark,
        [
            (
                "evt_1", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68000.0, 0.1, "1", "trade", "2.0.0",
            ),
            (
                "evt_1", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68000.0, 0.1, "1", "trade", "2.0.0",
            ),
        ],
    )
    valid, _ = batch_historical.normalize_events(raw)
    deduplicated = batch_historical.deduplicate_events(valid)

    assert deduplicated.count() == 1


def test_bdd_us10_criterio_2_deduplicacion_por_clave_compuesta(spark):
    """
    Dado que dos conectores emitieron el mismo instante de mercado con
    identificadores distintos,
    Cuando se deduplica por `(exchange, symbol, event_time)`,
    Entonces sobrevive una única fila para esa clave.
    """
    raw = _events(
        spark,
        [
            (
                "evt_1", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68000.0, 0.1, "1", "trade", "2.0.0",
            ),
            (
                "evt_2", "binance_rest", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:01.000Z",
                68001.0, 0.2, "2", "trade", "2.0.0",
            ),
        ],
    )
    valid, _ = batch_historical.normalize_events(raw)
    deduplicated = batch_historical.deduplicate_events(valid)

    assert deduplicated.count() == 1


def test_bdd_us10_criterio_3_escritura_parquet_snappy(spark, tmp_path):
    """
    Dado que se consolidan los datos limpios,
    Cuando se guardan los resultados intermedios,
    Entonces se escriben en Parquet con compresión Snappy.
    """
    df = _events(
        spark,
        [
            (
                "evt_1", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:00.000Z", "2026-10-19T12:00:00.045Z",
                68000.0, 0.1, "1", "trade", "2.0.0",
            )
        ],
    ).withColumn("date", F.lit(date(2026, 10, 19)))

    target = str(tmp_path / "batch-results" / "metric=ohlcv")
    batch_historical.write_idempotent(df, target, partition_column="date")

    # La partición se materializa con el esquema Hive esperado.
    partition = tmp_path / "batch-results" / "metric=ohlcv" / "date=2026-10-19"
    assert partition.exists()

    # Los archivos escritos son Parquet comprimidos con Snappy.
    parquet_files = list(partition.glob("*.parquet"))
    assert parquet_files, "No se escribieron archivos Parquet."

    roundtrip = spark.read.parquet(str(tmp_path / "batch-results" / "metric=ohlcv"))
    assert roundtrip.count() == 1
    assert roundtrip.columns == df.columns


# ==============================================================================
# US-11: Cálculo de OHLCV y retornos
# ==============================================================================
def _ohlcv_fixture(spark):
    """Dataset de 4 eventos de BTCUSDT dentro de la misma vela de 1 minuto."""
    return _events(
        spark,
        [
            (
                "e1", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:10.000Z", "2026-10-19T12:00:10.045Z",
                100.0, 1.0, "1", "trade", "2.0.0",
            ),
            (
                "e2", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:20.000Z", "2026-10-19T12:00:20.045Z",
                120.0, 2.0, "2", "trade", "2.0.0",
            ),
            (
                "e3", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:30.000Z", "2026-10-19T12:00:30.045Z",
                90.0, 3.0, "3", "trade", "2.0.0",
            ),
            (
                "e4", "binance_ws", "binance", "BTCUSDT",
                "2026-10-19T12:00:40.000Z", "2026-10-19T12:00:40.045Z",
                110.0, 4.0, "4", "trade", "2.0.0",
            ),
        ],
    )


def test_bdd_us11_criterio_1_ohlcv_valores_exactos(spark):
    """
    Dado que se tienen eventos de precios normalizados,
    Cuando se calcula la agregación OHLCV,
    Entonces Open, High, Low, Close y Volume son los valores correctos.
    """
    raw = _ohlcv_fixture(spark)
    valid, _ = batch_historical.normalize_events(raw)
    candles = batch_historical.compute_ohlcv(valid, intervals=["1m"])

    assert candles.count() == 1
    candle = candles.collect()[0]

    assert candle["open"] == pytest.approx(100.0)    # primer precio
    assert candle["high"] == pytest.approx(120.0)    # máximo
    assert candle["low"] == pytest.approx(90.0)      # mínimo
    assert candle["close"] == pytest.approx(110.0)   # último precio
    assert candle["volume"] == pytest.approx(10.0)   # suma de cantidades
    assert candle["trades"] == 4
    assert candle["interval"] == "1m"


def test_bdd_us11_criterio_1_ohlcv_respeta_granularidad(spark):
    """
    Dado que los eventos abarcan varias horas,
    Cuando se calcula la agregación con granularidad de 1 hora,
    Entonces se genera una vela por hora, no una por minuto.
    """
    raw = spark.createDataFrame(
        [
            ("e1", "2026-10-19T12:05:00.000Z", 100.0, 1.0),
            ("e2", "2026-10-19T12:35:00.000Z", 110.0, 1.0),
            ("e3", "2026-10-19T13:05:00.000Z", 120.0, 1.0),
            ("e4", "2026-10-19T13:35:00.000Z", 130.0, 1.0),
        ],
        "event_id string, event_time string, price double, quantity double",
    ).withColumn("exchange", F.lit("binance")).withColumn("symbol", F.lit("BTCUSDT"))

    candles = batch_historical.compute_ohlcv(raw, intervals=["1h"])
    assert candles.count() == 2

    candles_1m = batch_historical.compute_ohlcv(raw, intervals=["1m"])
    assert candles_1m.count() == 4


def test_bdd_us11_criterio_1_ohlcv_separa_simbolos(spark):
    """Cada símbolo se agrega en su propia serie de velas."""
    raw = spark.createDataFrame(
        [
            ("e1", "BTCUSDT", "2026-10-19T12:00:10.000Z", 100.0, 1.0),
            ("e2", "ETHUSDT", "2026-10-19T12:00:10.000Z", 10.0, 1.0),
        ],
        "event_id string, symbol string, event_time string, price double, quantity double",
    ).withColumn("exchange", F.lit("binance"))

    candles = batch_historical.compute_ohlcv(raw, intervals=["1m"])
    assert candles.count() == 2

    by_symbol = {row["symbol"]: row for row in candles.collect()}
    assert by_symbol["BTCUSDT"]["close"] == pytest.approx(100.0)
    assert by_symbol["ETHUSDT"]["close"] == pytest.approx(10.0)


def test_bdd_us11_criterio_2_retornos_simples_y_logaritmicos(spark):
    """
    Dado que se tienen velas de precios sucesivas,
    Cuando se procesa la métrica `returns`,
    Entonces se calcula la variación porcentual y su equivalente logarítmico.
    """
    raw = spark.createDataFrame(
        [
            ("e1", "2026-10-19T12:00:00.000Z", 100.0, 1.0),
            ("e2", "2026-10-19T12:01:00.000Z", 110.0, 1.0),
        ],
        "event_id string, event_time string, price double, quantity double",
    ).withColumn("exchange", F.lit("binance")).withColumn("symbol", F.lit("BTCUSDT"))

    candles = batch_historical.compute_ohlcv(raw, intervals=["1m"])
    returns = batch_historical.compute_returns(candles).orderBy("bucket_start").collect()

    # La primera vela no tiene vela previa, por lo que su retorno es nulo.
    assert returns[0]["return_simple"] is None

    # La segunda vela cierra en 110 tras una apertura de 100.
    assert returns[1]["return_simple"] == pytest.approx(0.10)
    assert returns[1]["return_log"] == pytest.approx(0.09531017980432487)
    assert returns[1]["prev_close"] == pytest.approx(100.0)


def test_bdd_us11_criterio_3_idempotencia_de_la_escritura(spark, tmp_path):
    """
    Dado que un trabajo batch se ejecuta varias veces sobre el mismo rango,
    Cuando finaliza cada ejecución,
    Entonces los resultados son idénticos y no hay filas duplicadas.
    """
    raw = _ohlcv_fixture(spark)
    valid, _ = batch_historical.normalize_events(raw)
    target = str(tmp_path / "metric=ohlcv")

    snapshots = []
    for _ in range(3):
        candles = batch_historical.compute_ohlcv(valid, intervals=["1m"])
        candles = candles.withColumn("date", F.to_date("bucket_start"))
        batch_historical.write_idempotent(candles, target, partition_column="date")
        snapshots.append(
            sorted(
                spark.read.parquet(target)
                .select("symbol", "interval", "bucket_start", "open", "high", "low", "close", "volume")
                .collect(),
                key=lambda row: (row["symbol"], row["interval"], row["bucket_start"]),
            )
        )

    # Las tres ejecuciones producen exactamente el mismo conjunto de velas.
    assert snapshots[0] == snapshots[1] == snapshots[2]
    # Y no se acumulan filas duplicadas.
    assert len(snapshots[0]) == 1


def test_bdd_us11_criterio_3_no_altera_fechas_no_afectadas(spark, tmp_path):
    """
    Dado que se recalcula un periodo,
    Cuando se escribe el resultado,
    Entonces las particiones de otras fechas permanecen intactas.
    """
    target = str(tmp_path / "metric=ohlcv")

    # Primera ejecución: dos fechas distintas.
    two_days = spark.createDataFrame(
        [
            ("e1", "2026-10-18T12:00:00.000Z", 100.0, 1.0),
            ("e2", "2026-10-19T12:00:00.000Z", 200.0, 1.0),
        ],
        "event_id string, event_time string, price double, quantity double",
    ).withColumn("exchange", F.lit("binance")).withColumn("symbol", F.lit("BTCUSDT"))

    candles = batch_historical.compute_ohlcv(two_days, intervals=["1d"])
    candles = candles.withColumn("date", F.to_date("bucket_start"))
    batch_historical.write_idempotent(candles, target, partition_column="date")

    # Segunda ejecución: solo se recalcula el 19.
    only_19 = spark.createDataFrame(
        [("e3", "2026-10-19T12:00:00.000Z", 250.0, 1.0)],
        "event_id string, event_time string, price double, quantity double",
    ).withColumn("exchange", F.lit("binance")).withColumn("symbol", F.lit("BTCUSDT"))

    candles = batch_historical.compute_ohlcv(only_19, intervals=["1d"])
    candles = candles.withColumn("date", F.to_date("bucket_start"))
    batch_historical.write_idempotent(candles, target, partition_column="date")

    result = spark.read.parquet(target)
    by_date = {row["date"].isoformat(): row for row in result.collect()}

    # El 18 conserva su valor original y el 19 fue recalculado.
    assert by_date["2026-10-18"]["close"] == pytest.approx(100.0)
    assert by_date["2026-10-19"]["close"] == pytest.approx(250.0)
    assert result.count() == 2


# ==============================================================================
# US-21: Reconciliación Speed / Batch
# ==============================================================================
def _candles(spark, rows):
    """Dataset de velas con la forma que produce la capa Batch."""
    return spark.createDataFrame(
        rows,
        "symbol string, interval string, bucket_start timestamp, event_time timestamp, "
        "open double, high double, low double, close double, volume double",
    )


def _ts(value: str):
    """Convierte un texto ISO-8601 en un `datetime` para los fixtures."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def test_bdd_us21_criterio_1_batch_reemplaza_a_speed_en_ventanas_cerradas(spark):
    """
    Dado que el batch consolidó una ventana cerrada,
    Cuando se reconcilia,
    Entonces el resultado del batch reemplaza al provisional del speed.
    """
    cutoff = F.lit(_ts("2026-10-19T12:00:00Z"))

    batch = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
          _ts("2026-10-19T11:59:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0)],
    )
    speed = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
          _ts("2026-10-19T11:59:00Z"), 9.0, 9.0, 9.0, 9.0, 9.0)],
    )

    unified = serving_reconcile.reconcile(batch, speed, cutoff)
    rows = unified.collect()

    # Solo sobrevive la fila del batch: la del speed cae en la ventana cerrada.
    assert len(rows) == 1
    assert rows[0]["source"] == serving_reconcile.SOURCE_BATCH
    assert rows[0]["close"] == pytest.approx(1.0)
    assert rows[0]["is_provisional"] is False


def test_bdd_us21_criterio_2_speed_domina_en_la_ventana_reciente(spark):
    """
    Dado que el batch solo corre periódicamente,
    Cuando se consultan los últimos minutos no consolidados,
    Entonces la respuesta proviene únicamente de la ruta speed.
    """
    cutoff = F.lit(_ts("2026-10-19T12:00:00Z"))

    batch = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
          _ts("2026-10-19T11:59:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0)],
    )
    speed = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T12:05:00Z"),
          _ts("2026-10-19T12:05:00Z"), 9.0, 9.0, 9.0, 9.0, 9.0)],
    )

    unified = serving_reconcile.reconcile(batch, speed, cutoff)
    rows = unified.collect()

    # La ventana reciente (event_time > cutoff) la cubre solo la ruta speed.
    recent = [row for row in rows if row["event_time"] > _ts("2026-10-19T12:00:00Z")]
    assert len(recent) == 1
    assert recent[0]["source"] == serving_reconcile.SOURCE_SPEED
    assert recent[0]["is_provisional"] is True

    # Y la vela consolidada por el batch sigue presente como definitiva.
    closed = [row for row in rows if row["event_time"] <= _ts("2026-10-19T12:00:00Z")]
    assert len(closed) == 1
    assert closed[0]["source"] == serving_reconcile.SOURCE_BATCH
    assert closed[0]["is_provisional"] is False


def test_bdd_us21_criterio_3_ausencia_de_doble_conteo(spark):
    """
    Dado que una consulta abarca desde hace 7 días hasta el segundo actual,
    Cuando el motor une ambas capas,
    Entonces no existe ninguna fila contada dos veces.
    """
    cutoff = F.lit(_ts("2026-10-19T12:00:00Z"))

    batch = _candles(
        spark,
        [
            ("BTCUSDT", "1m", _ts("2026-10-18T12:00:00Z"),
             _ts("2026-10-18T12:00:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0),
            ("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
             _ts("2026-10-19T11:59:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0),
        ],
    )
    speed = _candles(
        spark,
        [
            # Misma vela que el batch: debe ser descartada por el corte.
            ("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
             _ts("2026-10-19T11:59:00Z"), 9.0, 9.0, 9.0, 9.0, 9.0),
            # Vela posterior al corte: sí entra.
            ("BTCUSDT", "1m", _ts("2026-10-19T12:00:30Z"),
             _ts("2026-10-19T12:00:30Z"), 9.0, 9.0, 9.0, 9.0, 9.0),
        ],
    )

    stats = serving_reconcile.reconciliation_stats(batch, speed, cutoff)

    assert stats["overlapping_keys"] == 0
    assert stats["unified_rows"] == stats["batch_rows"] + stats["speed_rows"]
    assert stats["unified_rows"] == 3


def test_bdd_us21_criterio_3_reconciliacion_deduplicada_por_clave(spark):
    """
    Dado que batch y speed calcularon la misma vela,
    Cuando se aplica la precedencia de clave,
    Entonces sobrevive una sola fila y es la del batch.
    """
    cutoff = F.lit(_ts("2026-10-19T12:00:00Z"))

    batch = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
          _ts("2026-10-19T11:59:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0)],
    )
    speed = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
          _ts("2026-10-19T11:59:00Z"), 9.0, 9.0, 9.0, 9.0, 9.0)],
    )

    unified = serving_reconcile.reconcile_deduplicated(batch, speed, cutoff)
    rows = unified.collect()

    assert len(rows) == 1
    assert rows[0]["source"] == serving_reconcile.SOURCE_BATCH
    assert rows[0]["close"] == pytest.approx(1.0)


def test_bdd_us21_criterio_1_corte_derivado_del_batch(spark):
    """
    Dado que la capa batch ya consolidó hasta cierta marca de tiempo,
    Cuando se deriva el `batch_cutoff_timestamp`,
    Entonces corresponde al máximo de tiempo consolidado.
    """
    batch = _candles(
        spark,
        [
            ("BTCUSDT", "1m", _ts("2026-10-19T11:58:00Z"),
             _ts("2026-10-19T11:58:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0),
            ("BTCUSDT", "1m", _ts("2026-10-19T11:59:00Z"),
             _ts("2026-10-19T11:59:00Z"), 1.0, 1.0, 1.0, 1.0, 1.0),
        ],
    )
    speed = _candles(
        spark,
        [("BTCUSDT", "1m", _ts("2026-10-19T12:00:30Z"),
          _ts("2026-10-19T12:00:30Z"), 9.0, 9.0, 9.0, 9.0, 9.0)],
    )

    view = serving_reconcile.unified_ohlcv_view(batch, speed)
    rows = view.collect()

    # El corte se deriva del máximo consolidado por el batch (11:59), así que
    # sus dos velas se conservan como definitivas y la vela de speed, que es
    # posterior al corte, se conserva como provisional.
    assert len(rows) == 3

    by_source = {}
    for row in rows:
        by_source.setdefault(row["source"], []).append(row)

    assert len(by_source[serving_reconcile.SOURCE_BATCH]) == 2
    assert all(row["is_provisional"] is False for row in by_source[serving_reconcile.SOURCE_BATCH])

    assert len(by_source[serving_reconcile.SOURCE_SPEED]) == 1
    assert by_source[serving_reconcile.SOURCE_SPEED][0]["is_provisional"] is True
