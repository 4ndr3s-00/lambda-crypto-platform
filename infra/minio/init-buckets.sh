#!/bin/sh
# Script para inicializar buckets de MinIO organizados por función (Patrón Lambda sin Medallion)
set -e

echo "[MinIO Init] Esperando a que el servicio MinIO esté listo..."
until /usr/bin/mc alias set local http://minio:9000 "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-miniopassword123}"; do
  echo "[MinIO Init] Reintentando conexión con MinIO..."
  sleep 2
done

echo "[MinIO Init] Creando buckets funcionales..."

# 1. Bucket de almacenamiento duradero para históricos y resultados batch
/usr/bin/mc mb --ignore-existing local/market-lake
# 2. Bucket de checkpoints duraderos para Spark Structured Streaming
/usr/bin/mc mb --ignore-existing local/market-checkpoints
# 3. Bucket de cuarentena para eventos corruptos o no conformes (DLQ sink)
/usr/bin/mc mb --ignore-existing local/market-quarantine

# Estructura funcional en prefijos
echo "[MinIO Init] Creando estructura funcional dentro de buckets..."
echo "historical directory" | /usr/bin/mc pipe local/market-lake/historical/.keep
echo "batch-results directory" | /usr/bin/mc pipe local/market-lake/batch-results/.keep
echo "reference directory" | /usr/bin/mc pipe local/market-lake/reference/.keep
echo "checkpoints directory" | /usr/bin/mc pipe local/market-checkpoints/spark/.keep
echo "quarantine directory" | /usr/bin/mc pipe local/market-quarantine/dead-letter/.keep

echo "[MinIO Init] Configuración de MinIO completada exitosamente."
