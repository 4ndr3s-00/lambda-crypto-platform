# Cómo Levantar el Entorno Lambda Crypto Platform

## Requisitos previos

- Docker y Docker Compose instalados
- Repositorio clonado: `git clone <url> && cd lambda-crypto-platform`
- Rama: `develop` (el trabajo ya está mergeado, no está en `main`)
- Archivo `.env` configurado (o copiar desde `.env.example`)

## Paso 1: Configurar entorno

```bash
# Copiar ejemplo si no existe .env
cp .env.example .env

# Opcional: Revisar/ajustar credenciales S3 críticas
# - SPARK_MINIO_USER=spark_service
# - SPARK_MINIO_PASSWORD=spark_secure_lake_pass123
# Estas son obligatorias para que Spark se conecte a MinIO
```

## Paso 2: Levantar toda la infraestructura

```bash
make up
```

### Lo que levanta este comando:

| Servicio | Contenedor | Puertos |
|----------|------------|---------|
| **MinIO** | `lambda-minio` | 9000 (S3), 9001 (Console) |
| **Kafka** | `lambda-kafka` | 9092 |
| **PostgreSQL** | `lambda-postgres` | 5432 |
| **Spark Master** | `lambda-spark-master` | 7077 (API), 8080 (UI) |
| **Spark Worker** (1 por defecto) | `lambda-spark-worker` | 8081 (UI) |
| **Airflow** | `lambda-airflow` | 8085 (Web) |
| **API Serving** | `lambda-serving-api` | 8000 |

### Verificar que todo levantado:

```bash
make ps
```

Deberías ver todos los contenedores con status `Up`.

## Paso 3: Escalar el clúster Spark (opcional)

Si querés más de un worker:

```bash
make scale-spark WORKERS=3
```

Esto modificará temporalmente el docker-compose para añadir 2 workers más (total 3).

### Para volver a 1 worker:

```bash
make scale-spark WORKERS=1
```

### O con docker compose directo:

```bash
docker compose -f infra/docker-compose.yml \
  -f infra/docker-compose.scale.yml \
  --env-file .env \
  up -d --scale spark-worker=3
```

## Paso 4: Verificar que Spark funcione

```bash
# Ejecutar job de prueba
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT,ETHUSDT' 2>&1 | head -20
```

Deberías ver salida con `raw_events`, `valid_events`, `invalid_events`, `deduplicated_events` y `written`.

### Verificar UIs:

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001

## Paso 5: Probar la ruta Speed (Streaming)

```bash
# Desplegar streaming job
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    streaming_market.py \
    --bootstrap-servers kafka:9092 \
    --input-topic market.events \
    --dlq-topic market.dlq \
    --metrics-topic market.metrics \
    --watermark-delay 2 minutes \
    --window-duration 5 minutes \
    --slide-duration 1 minute \
    --checkpoint-dir s3a://market-checkpoints/spark/query=market-stream \
    --console-sink' 2>&1 | head -30
```

## Paso 6: Verificación final

```bash
# Todos los contenedores Running?
make ps

# Tests unitarios (dentro container Spark)
docker exec lambda-spark-master bash -c '
  cd /opt/bitnami/spark/jobs && python -m pytest tests/unit/test_spark_batch.py -v' 2>&1 | tail -20
```

## Para detener todo:

```bash
make down
```

Esto detiene y elimina todos los contenedores y volúmenes.

## Para reiniciar sin perder configuración:

```bash
make restart
```

## Notas importantes

1. **`make up` es idempotente**: Vuelve a crear buckets, topics, esquema BD si ya existen
2. **`.env` persiste entre levantamientos**: No se pierde la configuración de credenciales
3. **`make up` recrea**: Buckets MinIO, topics Kafka y esquema de base de datos automáticamente
4. **Trabajo de Andrés Blanco**: Ya está en `develop`, no necesitámergear PR #37 manualmente
5. **Número de workers por defecto**: 1 (us `make scale-spark WORKERS=3` para más)

## Problemas comunes

| Problema | Solución |
|----------|----------|
| `make up` falla en MinIO | Revisar que Docker tenga suficiente recursos |
| `403 Forbidden` en MinIO | Revisar `.env` credenciales y fix en `infra/minio/` |
| `No module named 'py4j'` | Integrar `wip/infra-airflow-spark-client` branch en develop |
| Tests fallan | Ejecutar dentro del container Spark (ver comando arriba) |
| Worker no se conecta | Revisar red Docker y `make scale-spark` |

## Estructura de comandos resumida:

```
# 1. Configurar
cp .env.example .env

# 2. Levantar
make up

# 2b. Escalar workers (opcional)
make scale-spark WORKERS=3

# 3. Verificar
make ps

# 4. Probar job Spark
spark-submit --master spark://spark-master:7077 ... (ver comando arriba)

# 5. Probar streaming
spark-submit streaming_market.py ... (ver comando arriba)

# 6. Apagar
make down
```