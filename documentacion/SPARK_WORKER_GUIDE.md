# Guía para Configurar Spark Worker en Otro Computador

## Propósito
Este documento explica cómo configurar un segundo computador para funcionar como **worker** del clúster Spark, conectándose al master que está en el computador principal. Esto permite distribuir el procesamiento de datos entre varios nodos.

## Estado Actual (Computador Principal - Master)

El clúster Spark ya funciona en el computador actual con:
- **Master**: `lambda-spark-master` (puertos 7077 API, 8080 UI)
- **Worker**: `lambda-spark-worker` (puerto 8081 UI)
- **MinIO**: `lambda-minio` con buckets `market-lake`, `market-checkpoints`, `market-quarantine`
- **Kafka**: `lambda-kafka` en puerto 9092
- **PostgreSQL**: Ya corriendo en el host (puerto 5432)
- **Airflow**: `http://localhost:8085`

---

## OPCIÓN A: Mismo Docker Compose (recomendado si ambos PCs comparten la red)

Si ambos computadores están en la misma red local y pueden acceder al mismo Docker Compose, sigue estos pasos:

### 1. En el computador principal (ya hecho):
```bash
# El master ya está levantado
make up

# Verificar contenedores
make ps
```

### 2. En el computador worker:
```bash
# 1. Clonar el repositorio
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
cd lambda-crypto-platform

# 2. Ir a la rama develop (el trabajo de Andrés Blanco ya está mergeado)
git checkout develop

# 3. Levantar el Docker Compose con el override de scaling
#    Esto añade workers al master ya en ejecución
make scale-spark WORKERS=2

# 4. Verificar estado
make ps
```

El `make scale-spark WORKERS=2` modificará temporalmente `docker-compose.yml` para añadir un segundo worker. Cuando termines, `make up` otra vez vuelve al estado original.

### 3. Verificar que el worker se unió al clúster

Desde el master, verifica en la UI:
- http://localhost:8080 (Spark Master UI)
- Debe aparecer el worker conectado mostrando sus recursos

---

## OPCIÓN B: Docker Compose Distribuido (para PCs reales separados)

Si los computadores son físicamente distintos y no comparten el mismo Docker Daemon, necesitas configurar ambos con el mismo `docker-compose.yml` y unirles mediante red.

### 1. En AMBOS computadores:

**A. Igualar el archivo `.env`:**
```bash
# En cada computador
cp .env.example .env
# Verificar que estas variables sean consistentes:
# SPARK_MINIO_USER=spark_service
# SPARK_MINIO_PASSWORD=spark_secure_lake_pass123
# KAFKA_CLUSTER_ID=4L622nShT-2xMVuLqW6q3w
```

**B. Levantar MinIO en ambos (opcional, si necesitan acceso local a datos):**
```bash
make up
# O manualmente:
docker compose -f infra/docker-compose.yml --env-file .env up -d minio
```

**C. La red debe ser compartida o replicada:**
- Si usan una red local común (misma subnet), los containers se descubrirán por nombres
- Si son redes aisladas, necesitarías configurar IPs estáticas o usar Swarm/K8s

### 2. En el computador worker específicamente:

**A. Unirse a la red existente:**
```bash
# Si la red lambda-platform-network ya existe en la red local:
docker network create lambda-platform-network 2>/dev/null || true

# O unirte a la red del master si es accesible:
docker run -d --name lambda-spark-worker \
  --network lambda-platform-network \
  -p 8081:8081 \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://<IP-O-NOMBRE-del-master>:7077 \
  -e AWS_ACCESS_KEY_ID=spark_service \
  -e AWS_SECRET_ACCESS_KEY=spark_secure_lake_pass123 \
  -e S3_ENDPOINT=http://minio:9000 \
  bitnamilegacy/spark:3.5.1
```

**B. Verificar conexión:**
```bash
# Desde cualquier computador, probar conexión Spark:
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://<master-ip>:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT'
```

---

## 🔧 Configuración Crítica (ambas opciones)

### 1. Credenciales S3A (OBLIGATORIO)
Spark necesita estas 5 flags para conectarse a MinIO:

```bash
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Las credenciales son las del `.env`:
- `SPARK_MINIO_USER=spark_service`
- `SPARK_MINIO_PASSWORD=spark_secure_lake_pass123`

### 2. Puertos necesarios

| Servicio | Master | Worker |
|----------|--------|--------|
| Spark Master API | 7077 | - |
| Spark Master UI | 8080 | - |
| Spark Worker UI | - | 8081 |
| MinIO S3 | 9000 | 9000 |
| MinIO Console | 9001 | 9001 |

### 3. Volúmenes compartidos
El directorio `spark/` debe estar montado en ambos computadores con los mismos scripts:

```bash
# En docker-compose.yml (ya está configurado):
- ../spark:/opt/bitnami/spark/jobs:ro
```

Esto permite que ambos computadores lean los mismos scripts.

---

## 🛠️ Solución de Problemas

### Error: `UnknownHostException: spark-master`
- **Causa**: El nombre `spark-master` no se resuelve en la red
- **Solución**: Usar la IP en lugar del nombre, o asegurar que ambos computadores estén en la misma red Docker

### Error: `403 Forbidden` al leer MinIO
- **Causa**: Credenciales S3A incorrectas o permisos insuficientes
- **Solución**: Verificar que `--conf spark.hadoop.fs.s3a.aws.credentials.provider=...` esté presente y que las credenciales del `.env` sean correctas

### Error: `No module named 'py4j'`
- **Causa**: El worker no encuentra la versión correcta de PySpark
- **Solución**: Asegurarse de usar `bitnamilegacy/spark:3.5.1` o integrar el cambio `wip/infra-airflow-spark-client` en develop

### Error: Worker no se conecta al master
- **Causa**: Puertos bloqueados o red incompatible
- **Solución**: Verificar puertos 7077 y 8081 están abiertos, y ambos computadores usan la misma red Docker

---

## 🚀 Comandos Útiles

### Ver estado del clúster
```bash
make ps  # Ver contenedores
make scale-spark WORKERS=3  # Escalar workers
```

### Probar job Spark
```bash
# Desde el master (dentro del container):
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT,ETHUSDT'

# O desde la CLI host (usando la IP en lugar de nombre):
spark-submit \
  --master spark://192.168.1.50:7077 \
  ...
```

### Ver UIs
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081
- MinIO Console: http://localhost:9001

---

## 📋 Resumen Rápido

1. **Master**: `make up` + `make scale-spark WORKERS=2` (o distribución manual)
2. **Worker**: Mismo docker-compose o `docker run` con --network lambda-platform-network
3. **Siempre**: Las 5 `--conf` de spark-submit para MinIO
4. **Datos**: Buckets de MinIO ya creados (market-lake, market-checkpoints, market-quarantine)
5. **Credenciales**: Del archivo `.env` (spark_service / spark_secure_lake_pass123)
6. **Spark jobs directory**: ./spark/ directory (mismo en ambos PCs)

---

## 📦 Para tu Compañero: Instrucciones Paso a Paso

### Paso 1: Preparar el computador

```bash
# 1. Abrir terminal en el otro computador
# 2. Clonar el repositorio
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git

# 3. Entrar al directorio
cd lambda-crypto-platform

# 4. Ir a la rama develop (el trabajo ya está mergeado en develop)
git checkout develop

# 5. Copiar el archivo de entorno
cp .env.example .env

# 6. Editar .env y asegurar que estas variables sean consistentes:
#    SPARK_MINIO_USER=spark_service
#    SPARK_MINIO_PASSWORD=spark_secure_lake_pass123
#    KAFKA_CLUSTER_ID=4L622nShT-2xMVuLqW6q3w
```

### Paso 2: Levantar la infraestructura

```bash
# 7. Levantar MinIO, Kafka, Postgres, Spark base
make up

# 8. Verificar estado
make ps
# Deberías ver: lambda-minio, lambda-kafka, lambda-postgres corriendo
```

### Paso 3: Configurar el worker

**Opción A (si comparten docker-compose):**
```bash
make scale-spark WORKERS=2
# Verificar con: make ps
```

**Opción B (distribución manual):**
```bash
docker run -d --name lambda-spark-worker \
  --network lambda-platform-network \
  -p 8081:8081 \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://192.168.1.50:7077 \  # Usar IP del master
  -e AWS_ACCESS_KEY_ID=spark_service \
  -e AWS_SECRET_ACCESS_KEY=spark_secure_lake_pass123 \
  -e S3_ENDPOINT=http://minio:9000 \
  bitnamilegacy/spark:3.5.1
```

### Paso 4: Probar que funciona

```bash
# 9. Probar job Spark desde el master:
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://192.168.1.50:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT'

# 10. Verificar en la UI:
#   Master: http://localhost:8080
#   Worker: http://localhost:8081
```

### Paso 5: Qué NO tocar en el computador worker

**Estrictamente prohibido tocar:**
- ❌ Archivos en `infra/docker-compose.yml` (a menos que sea `make scale-spark`)
- ❌ Las credenciales `.env` (a menos que sepas lo que haces)
- ❌ Borrar contenedores lambda-*
- ❌ Cambiar puertos publicados sin razón
- ❌ Modificar `airflow/` o `api/` sin necesidad

**SÍ puedes tocar:**
- ✅ Ejecutar jobs Spark (`spark-submit`)
- ✅ Ver logs (`make logs-spark`)
- ✅ Ver UIs (`http://localhost:8080`, `http://localhost:8081`)
- ✅ Ejecutar `make ps` para ver estado
- ✅ Ejecutar `make scale-spark WORKERS=N` para cambiar cantidad de workers

---

## 🔍 Verificación de Versiones

Asegúrate de que ambos computadores tengan lo mismo:

```bash
# En cada computador, verificar:
java -version  # Debe ser Java 17 (o compatible con Spark 3.5.1)
python3 --version  # Python 3. compatible
docker --version  # Mismo Docker ayuda mucho

# En el computador con Spark:
docker exec lambda-spark-master bash -c 'java -version'
# output esperado: openjdk version "17.x.x"
```

---

## 🎯 checklist FINAL para el otro PC

[X] Clonar repositorio: `git clone ...`
[X] Cambiar a rama: `git checkout develop`
[X] Copiar .env: `cp .env.example .env`
[X] Verificar credenciales S3 consistentes
[X] Levantar infra: `make up`
[X] Configurar worker: `make scale-spark WORKERS=2` O `docker run ...`
[X] Probar job: spark-submit con las 5 --conf
[X] Verificar UI: http://localhost:8080 y http://localhost:8081
[X] NO tocar: infra/, credenciales, contenedores innecesarios

---

## ❓ Preguntas Frecuentes

**¿Necesito instalar Java y Python en el worker?**
No si usas Docker. El container `bitnamilegacy/spark:3.5.1` ya trae todo incluido. Solo necesitas Docker y Docker Compose.

**¿Puedo tener el master y worker en redes distintas?**
Solo si configuras redes puenteadas manualmente o usas Swarm/K8s. Lo más sencillo es mantener ambas en la misma red `lambda-platform-network`.

**¿Qué pasa si el master se cae?**
El worker dejará de responder. Para recuperar, levanta un nuevo master con el mismo `docker-compose.yml` y los workers se re-conectarán automáticamente.

**¿Necesito Airflow en el worker?**
No. Airflow corre en el computador principal (o donde tengas la base de datos PostgreSQL). El worker solo necesita Spark y MinIO.