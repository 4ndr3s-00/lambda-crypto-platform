# ERRORES YA RESUELTOS - No reintroducirlos

## Tabla de errores comunes y soluciones

| Síntoma | Causa | Solución |
|---------|-------|----------|
| `403 Forbidden` al leer MinIO | IAM sin permisos de traversal | `fix(minio)` en `infra/minio/` - ampliar permisos de traversal |
| `PATH_NOT_FOUND` en backfill | Ruta de fecha sin partición | `_existing_partitions()` filtra antes de leer |
| `No module named 'py4j'` | El worker no encuentra py4j | Integrar `wip/infra-airflow-spark-client` en `develop` |
| `max() in WHERE` inválido | Agregado dentro de un filtro | Usar `crossJoin` con el corte en lugar de `max()` dentro de `WHERE` |
| Todas las velas en un solo bucket | `unix_timestamp` sobre string | Castrar a `timestamp` antes de convertir a epoch |
| No se pueden escalar workers | `container_name` fijo | Usar `infra/docker-compose.scale.yml` (override) |
| `403 Forbidden` al HEAD S3A | Permisos traversal no cubiertos por `/*` | Fix en `infra/minio/policies/` - MinIO responde 403 en lugar de 404 |

## Configuración crítica (Siempre necesaria)

### 5 flags obligatorias para spark-submit (para conectarse a MinIO):

```bash
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

**Las credenciales vienen del `.env`:**

```
SPARK_MINIO_USER=spark_service
SPARK_MINIO_PASSWORD=spark_secure_lake_pass123
```

### Si falta alguna flag:

- Falta `--conf spark.hadoop.fs.s3a.endpoint=...` → Spark no encuentra MinIO
- Falta `--conf spark.hadoop.fs.s3a.path.style.access=true` → MinIO lanza error de formato de ruta
- Falta `--conf spark.hadoop.fs.s3a.aws.credentials.provider=...` → 403 Forbidden
- Falta `--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false` → Error de conexión SSL

## Credenciales de servicio (Mínimo Privilegio - US-03)

| Servicio | Usuario | Contraseña |
|----------|---------|------------|
| **Spark** | `spark_service` | `spark_secure_lake_pass123` |
| **Airflow** | `airflow_service` | `airflow_secure_lake_pass123` |
| **API** | `api_service` | `api_secure_serving_pass123` |

**Importante**: La API es solo lectura. Spark y Airflow tienen escritura.

## Comandos de verificación rápida

```bash
# 1. Verificar credenciales S3
python3 -c "
from minio import Minio
mc = Minio('localhost:9000', 
           access_key='minioadmin', 
           secret_key='miniopassword123', 
           secure=False)
buckets = [b.name for b in mc.list_buckets()]
print('Buckets:', buckets)
print('✅ market-lake' in buckets)
print('✅ market-checkpoints' in buckets)
print('✅ market-quarantine' in buckets)
"

# 2. Verificar función checkpoints_path
python3 << 'EOF'
import sys
sys.path.insert(0, '/home/cohorte5/Descargas/lambda-crypto-platform')
from spark_session import checkpoints_path
print('checkpoints_path():', checkpoints_path())
print('✅ US-20 implementada')
EOF

# 3. Probar spark-submit básico
docker exec lambda-spark-master bash -c '
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT' 2>&1 | head -10
```

## Si algo sale mal - Guía rápida

| Problema | Qué revisar primero |
|----------|---------------------|
| `403 Forbidden` accediendo a MinIO | Credenciales S3 en `.env`, permissions traversal |
| `PATH_NOT_FOUND` error | Particiones de fecha, `_existing_partitions()` |
| `No module named 'py4j'` | Integrar `wip/infra-airflow-spark-client` |
| Todas las velas en un bucket | Revisar `unix_timestamp` y `timestamp` casting |
| No se escalan workers | Usar `make scale-spark` en lugar de modificar compose directo |
| Error en tests | Ejecutar `python -m pytest tests -v` dentro del container Spark |

## Recursos de ayuda

- `AGENTS.md`: Contexto completo, decisiones de diseño, errores ya resueltos
- `SPARK_WORKER_GUIDE.md`: Configurar worker en otro computador
- `docs/CONTINUIDAD.md`: Guía de continuidad entre máquinas
- `Makefile`: Comandos de ayuda (`make up`, `make ps`, `make scale-spark`)