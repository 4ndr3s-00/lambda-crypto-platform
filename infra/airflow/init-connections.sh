#!/usr/bin/env bash
# ==============================================================================
# Script de Inicialización Automatizada de Conexiones de Airflow (US-13)
# Responsable: Andrés Teherán (DevOps & SCM)
#
# Configura las conexiones predefinidas requeridas por los DAGs de la plataforma:
#   - minio_s3_conn: Conexión S3 para el Data Lake MinIO
#   - postgres_serving_conn: Conexión PostgreSQL para metastores y serving_db
#   - spark_default: Conexión Spark para ejecuciones distribuidas con SparkSubmit
# ==============================================================================

set -euo pipefail

echo "=================================================="
echo "🚀 [US-13] Inicializando Conexiones de Airflow..."
echo "=================================================="

# Variables con valores por defecto acordes a la arquitectura
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-${MINIO_ROOT_USER:-admin}}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-${MINIO_ROOT_PASSWORD:-admin12345}}"

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-admin}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-admin12345}"
POSTGRES_DB="${POSTGRES_DB:-serving_db}"

SPARK_MASTER_HOST="${SPARK_MASTER_HOST:-spark-master}"
SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"

# 1. Conexión S3 / MinIO (minio_s3_conn)
echo "📦 Configurando conexión 'minio_s3_conn'..."
airflow connections delete minio_s3_conn || true
airflow connections add minio_s3_conn \
    --conn-type aws \
    --conn-login "${MINIO_ACCESS_KEY}" \
    --conn-password "${MINIO_SECRET_KEY}" \
    --conn-extra "{\"endpoint_url\": \"${MINIO_ENDPOINT}\", \"aws_access_key_id\": \"${MINIO_ACCESS_KEY}\", \"aws_secret_access_key\": \"${MINIO_SECRET_KEY}\"}"

# 2. Conexión PostgreSQL (postgres_serving_conn)
echo "🐘 Configurando conexión 'postgres_serving_conn'..."
airflow connections delete postgres_serving_conn || true
airflow connections add postgres_serving_conn \
    --conn-type postgres \
    --conn-host "${POSTGRES_HOST}" \
    --conn-login "${POSTGRES_USER}" \
    --conn-password "${POSTGRES_PASSWORD}" \
    --conn-schema "${POSTGRES_DB}" \
    --conn-port "${POSTGRES_PORT}"

# 3. Conexión Spark (spark_default)
echo "⚡ Configurando conexión 'spark_default'..."
airflow connections delete spark_default || true
airflow connections add spark_default \
    --conn-type spark \
    --conn-host "spark://${SPARK_MASTER_HOST}" \
    --conn-port "${SPARK_MASTER_PORT}"

echo "✅ [US-13] Conexiones predefinidas configuradas exitosamente:"
airflow connections list
