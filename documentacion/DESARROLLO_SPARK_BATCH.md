# DESARROLLO CAPA SPARK BATCH - RESUMEN COMPLETO

## Historia a cargo de Andrés Blanco

| Historia | Descripción | Estado |
|----------|-------------|--------|
| **US-10** | Spark Batch: lectura, normalización y deduplicación | ✅ Implementado |
| **US-11** | Spark Batch: OHLCV idempotente y retornos | ✅ Implementado |
| **US-12** | Spark Batch: backfill masivo y recálculo | ✅ Implementado |
| **US-21** | Serving: reconciliación Speed vs Batch | ✅ Implementado |

## Contexto General

El trabajo de la capa Spark Batch fue implementado en la **rama `feature/spark-batch-ohlcv`** y mergeado al PR **#37** hacia `develop`. Este trabajo incluye:

### Archivos principales incluidos en el PR #37:

| Archivo | Para qué sirve |
|---------|----------------|
| `spark/batch_historical.py` | Job principal de la capa Batch - lectura S3A, normalización, deduplicación, cálculo de OHLCV y retornos, escritura idempotente |
| `spark/spark_session.py` | Configuración de sesión Spark compartida con dependencias Hadoop-AWS y cliente S3A |
| `spark/serving_reconcile.py` | Reconciliación entre la ruta Speed y la ruta Batch (US-21) |
| `dags/batch_recompute.py` | DAG de Airflow para recálculo batch |
| `tests/unit/test_spark_batch.py` | 44 pruebas unitarias que cubren US-10, US-11, US-12 y US-21 |

### Comandos clave:

```bash
# Levantar la infraestructura completa (MinIO, Kafka, Postgres, Spark, Airflow)
make up

# Verificar estado de contenedores
make ps

# Ejecutar el job Spark contra MinIO
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT,ETHUSDT'

# Verificar UIs
# Master: http://localhost:8080
# Worker: http://localhost:8081
# MinIO Console: http://localhost:9001
```

## Flujo de trabajo completado:

1. **Lectura S3A**: Spark lee el directorio `s3a://market-lake/historical/` con formato de particiones Hive
2. **Normalización**: Los eventos se normalizan al contrato canonical de `market_event`
3. **Deduplicación**: Se eliminan duplicados estrictamente por `event_id` y por clave compuesta `(exchange, symbol, event_time)`
4. **Cálculo OHLCV**: Ventanas de Spark con funciones Window para granularidades 1m, 5m, 1h, 1d
5. **Cálculo de retornos**: `ln(Close_t / Close_{t-1})` retornos simples y logarítmicos
6. ** Escritura idempotente**: `partitionOverwriteMode=dynamic` sobrescribe solo las particiones `date=` afectadas
7. **Backfill parametrizado**: CLI con `--start-date`, `--end-date` y `--symbols`

## Estado actual (19/10/2026):

- **Rama develop**: Contiene el trabajo mergeado del PR #37 y siguientes PRs (40, 41)
- **PR #37**: Mergeado - cubre US-10, US-11, US-12, US-21
- **Tests**: 44 pruebas unitarias en `tests/unit/test_spark_batch.py`
- **MinIO Buckets**: `market-lake`, `market-checkpoints`, `market-quarantine` creados
- **Kafka Topics**: `market.events`, `market.ohlcv`, `market.metrics`, `market.dlq`

## Para los compañeros que vienen:

1. **Clonar repositorio**: `git clone <url> && cd lambda-crypto-platform`
2. **Cambiar a develop**: `git checkout develop` (el trabajo ya está aquí)
3. **Levantar infra**: `make up`
4. **Configurar worker** (opcional): `make scale-spark WORKERS=2`
5. **Ejecutar tests**: `python -m pytest tests -v` (dentro del container Spark)
6. **Probar job**: Ver comando spark-submit arriba

## Recursos adicionales:

- `AGENTS.md`: Contexto general del proyecto, decisiones de diseño, errores ya resueltos
- `SPARK_WORKER_GUIDE.md`: Guía para configurar worker en otro computador
- `documentacion/CONTINUIDAD.md`: Guía de continuidad entre máquinas