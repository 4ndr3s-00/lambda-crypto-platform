# 📄 Guía Rápida: Cómo Apagar y Retomar el Proyecto Lambda Crypto

## 🌟 Paso 1: Hoy (Antes de apagar)

```bash
# Verificar estado
make ps

# Si hay commits pending y quieres guardarlos:
git add .
git commit -m "tema: descripción breve"
git push origin feature/spark-batch-ohlcv

# Si no hay cambios, solo asegurarse que esté limpio
git status  # Debería decir "En la rama develop" y "actualizada con origin/develop"
```

---

## 🌟 Paso 2: Apagar el computador

✅ **Total tranquilidad**: Todo se guarda solo.
- **Git**: Los commits y branches están en GitHub
- **Docker**: Los containers guardan su estado en los volúmenes
- **MinIO**: Los datos persisten en el almacenamiento configurado

---

## 🌟 Paso 3: Mañana (Al encender)

### A. Clonar (solo si es máquina nueva o limpia)
```bash
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
cd lambda-crypto-platform
git checkout develop && git pull origin develop
```

### B. Levantar el entorno
```bash
cp .env.example .env  # solo primera vez o si se borró
make up               # levantar MinIO, Kafka, Spark, Airflow, etc.
make ps               # verificar que todo esté "Up" / "Running"
```

### C. Puertos clave a verificar
- `http://10.1.20.20:9001` → MinIO Console (minioadmin / miniopassword123)
- `http://10.1.20.20:8080/` → Spark Master UI
- `http://10.1.20.20:8085/` → Airflow Web (admin / admin)
- `http://10.1.20.20:8000/` → Serving API FastAPI
- `http://10.1.20.20:29092/` → Kafka

### C. Probar los tests (validan que la lógica está bien)
```bash
python3 -m pytest tests/unit/test_spark_batch.py -v
# Deberías ver: 24 passed in ~21s
```

### D. Verificar estado de issues/PR
```bash
gh pr view 37 --web           # Ver PR #37
gh issue list                 # Ver issues abiertas
```

### D. Ejecutar job batch con datos reales
```bash
# Verificar primero que hay datos en MinIO
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://10.1.20.20:9000',
                  aws_access_key_id='spark_service',
                  aws_secret_access_key='spark_secure_lake_pass123')
response = s3.list_objects_v2(Bucket='market-lake', Prefix='historical/')
print('Eventos en MinIO:', len(response.get('Contents', [])))
"

# Ejecutar el job
docker exec lambda-spark-master bash -c '
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/bitnami/spark/jobs/batch_historical.py \
  --start-date 2026-09-01 \
  --end-date 2026-09-30 \
  --symbols BTCUSDT,ETHUSDT \
  --metrics ohlcv,returns
'

# Ver resultados
python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://10.1.20.20:9000',
                  aws_access_key_id='spark_service',
                  aws_secret_access_key='spark_secure_lake_pass123')
objs = s3.list_objects_v2(Bucket='market-lake', Prefix='batch-results/')
for o in objs.get('Contents', []):
    print(f'  {o[\"Key\"]}')
# Deberías ver archivos parquet por fecha y métrica
"
```

### E. (Opcional) Streaming en tiempo real
```bash
docker exec lambda-spark-master bash -c '
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/bitnami/spark/jobs/streaming_market.py \
  --bootstrap-servers localhost:29092 \
  --input-topic market.events \
  --metrics-topic market.metrics \
  --dlq-topic market.dlq \
  --watermark-delay "2 minutes" \
  --window-duration "5 minutes" \
  --slide-duration "1 minute" \
  --console-sink
# Presionar Ctrl+C para detener
'
```

---

## 🔧 Comandos de emergencia si algo falla

```bash
# Reiniciar todo el stack
make down    # detener todos los containers
make up      # volver a levantarlos

# Limpiar aplicaciones antiguas en Spark UI
curl -sS -X DELETE http://10.1.20.20:8080/api/v1/applications/

# Cambiar número de workers
make scale-spark WORKERS=3   # de 1 a 3 workers
make scale-spark WORKERS=1   # de 3 a 1 worker (default)
```

---

## ✅ Checklist Mínimo "Mañana Lists"

Solo necesitas estos 3 pasos para retomar:

1. **`make up`** - Levantar todo el stack Docker
2. **`make ps`** - Confirmar que todo está verde
3. **`python3 -m pytest tests/unit/test_spark_batch.py -v`** - 24/24 passing

---

## 💡 Consejos Adicionales

- **No necesitas exportar nada manualmente**: GitHub tiene todo el código, Docker tiene el estado, MinIO tiene los datos en el bucket `market-lake`.
- **Si cambias de computadora**: Solo `git clone` + `make up` y todo está.
- **Los issues se cierran solo**: Cuando el PR #37 se mergea, las issues US-10, US-11, US-12, US-21 se cierran automáticamente.
- **Para ver el progreso**: `make ps` muestra el estado de todos los servicios.

---

**¡Listo! Con esto puedes apagar el computador hoy y retomar todo mañana sin depender de nadie.**