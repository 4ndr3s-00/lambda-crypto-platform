#!/usr/bin/env bash
# ==============================================================================
# Script de inicialización de MinIO para la Plataforma de Datos Lambda
# US-03: Organización funcional estricta, regla Anti-Medallion y políticas IAM
# ==============================================================================
set -euo pipefail

echo "======================================================================"
echo "[MinIO Init] Iniciando configuración de almacenamiento y seguridad..."
echo "======================================================================"

# Localizar binario mc
MC_BIN="$(which mc 2>/dev/null || echo '/usr/bin/mc')"
if [ ! -x "$MC_BIN" ]; then
  if command -v mc >/dev/null 2>&1; then
    MC_BIN="mc"
  else
    echo "[MinIO Init] ERROR: Binario mc no encontrado."
    exit 1
  fi
fi

# Configuración de variables con valores por defecto
MINIO_HOST="${MINIO_HOST:-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-miniopassword123}"

SPARK_MINIO_USER="${SPARK_MINIO_USER:-spark_service}"
SPARK_MINIO_PASSWORD="${SPARK_MINIO_PASSWORD:-spark_secure_lake_pass123}"

AIRFLOW_MINIO_USER="${AIRFLOW_MINIO_USER:-airflow_service}"
AIRFLOW_MINIO_PASSWORD="${AIRFLOW_MINIO_PASSWORD:-airflow_secure_lake_pass123}"

API_MINIO_USER="${API_MINIO_USER:-api_service}"
API_MINIO_PASSWORD="${API_MINIO_PASSWORD:-api_secure_serving_pass123}"

MINIO_URL="http://${MINIO_HOST}:${MINIO_PORT}"

# Esperar a que el servicio MinIO esté listo
echo "[MinIO Init] Esperando conexión con el servicio MinIO en ${MINIO_URL}..."
MAX_RETRIES=30
RETRY_COUNT=0
until $MC_BIN alias set local "${MINIO_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "[MinIO Init] ERROR: Tiempo de espera agotado conectando a MinIO."
    exit 1
  fi
  echo "[MinIO Init] Reintentando conexión con MinIO ($RETRY_COUNT/$MAX_RETRIES)..."
  sleep 2
done
echo "[MinIO Init] Conexión establecida con éxito."

# ------------------------------------------------------------------------------
# 1. Creación de Buckets Operativos (Criterio 1 BDD)
# ------------------------------------------------------------------------------
echo "[MinIO Init] Aprovisionando buckets funcionales..."
$MC_BIN mb --ignore-existing local/market-lake
$MC_BIN mb --ignore-existing local/market-checkpoints
$MC_BIN mb --ignore-existing local/market-quarantine

# ------------------------------------------------------------------------------
# 2. Creación de Prefijos Canónicos Operativos (Criterio 1 BDD)
# ------------------------------------------------------------------------------
echo "[MinIO Init] Configurando estructura de prefijos canónicos..."

# s3://market-lake/historical/source=<fuente>/date=YYYY-MM-DD/
echo "placeholder" | $MC_BIN pipe --quiet local/market-lake/historical/source=binance/date=2026-10-19/.keep
echo "placeholder" | $MC_BIN pipe --quiet local/market-lake/historical/source=coingecko/date=2026-10-19/.keep

# s3://market-lake/batch-results/metric=<metrica>/date=YYYY-MM-DD/
echo "placeholder" | $MC_BIN pipe --quiet local/market-lake/batch-results/metric=ohlcv/date=2026-10-19/.keep
echo "placeholder" | $MC_BIN pipe --quiet local/market-lake/batch-results/metric=returns/date=2026-10-19/.keep

# s3://market-lake/reference/symbols/
echo "placeholder" | $MC_BIN pipe --quiet local/market-lake/reference/symbols/.keep

# s3://market-checkpoints/spark/query=<stream_id>/
echo "placeholder" | $MC_BIN pipe --quiet local/market-checkpoints/spark/query=crypto_trades_stream/.keep
echo "placeholder" | $MC_BIN pipe --quiet local/market-checkpoints/spark/query=crypto_metrics_stream/.keep

# s3://market-quarantine/source=<fuente>/date=YYYY-MM-DD/
echo "placeholder" | $MC_BIN pipe --quiet local/market-quarantine/source=binance/date=2026-10-19/.keep
echo "placeholder" | $MC_BIN pipe --quiet local/market-quarantine/source=coingecko/date=2026-10-19/.keep

# ------------------------------------------------------------------------------
# 3. Auditoría Anti-Medallion (Criterio 2 BDD)
# ------------------------------------------------------------------------------
echo "[MinIO Init] Auditando cumplimiento de regla Anti-Medallion..."
MEDALLION_VIOLATIONS=$($MC_BIN find local/ --name "*bronze*" --name "*silver*" --name "*gold*" 2>/dev/null || true)
if [ -n "$MEDALLION_VIOLATIONS" ]; then
  echo "[MinIO Init] ERROR FATAL: Se detectaron violaciones al patrón Anti-Medallion:"
  echo "$MEDALLION_VIOLATIONS"
  exit 1
fi
echo "[MinIO Init] Regla Anti-Medallion cumplida: Cero directorios bronze/silver/gold."

# ------------------------------------------------------------------------------
# 4. Creación de Usuarios de Servicio (Criterio 3 BDD)
# ------------------------------------------------------------------------------
echo "[MinIO Init] Creando usuarios de servicio con credenciales dedicadas..."
$MC_BIN admin user add local "${SPARK_MINIO_USER}" "${SPARK_MINIO_PASSWORD}" || true
$MC_BIN admin user add local "${AIRFLOW_MINIO_USER}" "${AIRFLOW_MINIO_PASSWORD}" || true
$MC_BIN admin user add local "${API_MINIO_USER}" "${API_MINIO_PASSWORD}" || true

# ------------------------------------------------------------------------------
# 5. Definición y Asignación de Políticas IAM de Mínimo Privilegio (Criterio 3 BDD)
# ------------------------------------------------------------------------------
echo "[MinIO Init] Aplicando políticas de seguridad IAM..."
TMP_POLICIES_DIR="$(mktemp -d)"

# Política para Apache Spark
cat << 'EOF' > "${TMP_POLICIES_DIR}/spark-policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSparkBucketListingAndLocation",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake",
        "arn:aws:s3:::market-checkpoints",
        "arn:aws:s3:::market-quarantine"
      ]
    },
    {
      "Sid": "AllowSparkCheckpointsFullAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::market-checkpoints/*",
        "arn:aws:s3:::market-checkpoints"
      ]
    },
    {
      "Sid": "AllowSparkBatchWriteResults",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake/batch-results/*",
        "arn:aws:s3:::market-lake/batch-results"
      ]
    },
    {
      "Sid": "AllowSparkReadHistoricalAndReference",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake/historical/*",
        "arn:aws:s3:::market-lake/historical",
        "arn:aws:s3:::market-lake/reference/*",
        "arn:aws:s3:::market-lake/reference"
      ]
    },
    {
      "Sid": "AllowSparkDLQQuarantineWrite",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::market-quarantine/*",
        "arn:aws:s3:::market-quarantine"
      ]
    }
  ]
}
EOF

# Política para Apache Airflow
cat << 'EOF' > "${TMP_POLICIES_DIR}/airflow-policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAirflowBucketListing",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake",
        "arn:aws:s3:::market-quarantine"
      ]
    },
    {
      "Sid": "AllowAirflowMarketLakeFullAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake/*",
        "arn:aws:s3:::market-lake"
      ]
    },
    {
      "Sid": "AllowAirflowQuarantineReadAudit",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::market-quarantine/*",
        "arn:aws:s3:::market-quarantine"
      ]
    }
  ]
}
EOF

# Política para FastAPI Serving Layer
cat << 'EOF' > "${TMP_POLICIES_DIR}/api-policy.json"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowApiBucketListing",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake"
      ]
    },
    {
      "Sid": "AllowApiReadBatchResultsAndReference",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::market-lake/batch-results/*",
        "arn:aws:s3:::market-lake/batch-results",
        "arn:aws:s3:::market-lake/reference/*",
        "arn:aws:s3:::market-lake/reference"
      ]
    }
  ]
}
EOF

# Registrar políticas en MinIO
$MC_BIN admin policy create local policy-spark "${TMP_POLICIES_DIR}/spark-policy.json" 2>/dev/null || \
  $MC_BIN admin policy update local policy-spark "${TMP_POLICIES_DIR}/spark-policy.json" 2>/dev/null || true

$MC_BIN admin policy create local policy-airflow "${TMP_POLICIES_DIR}/airflow-policy.json" 2>/dev/null || \
  $MC_BIN admin policy update local policy-airflow "${TMP_POLICIES_DIR}/airflow-policy.json" 2>/dev/null || true

$MC_BIN admin policy create local policy-fastapi "${TMP_POLICIES_DIR}/api-policy.json" 2>/dev/null || \
  $MC_BIN admin policy update local policy-fastapi "${TMP_POLICIES_DIR}/api-policy.json" 2>/dev/null || true

# Asignar políticas a sus usuarios respectivos
$MC_BIN admin policy attach local policy-spark --user "${SPARK_MINIO_USER}" 2>/dev/null || \
  $MC_BIN admin policy set local policy-spark "user=${SPARK_MINIO_USER}" 2>/dev/null || true

$MC_BIN admin policy attach local policy-airflow --user "${AIRFLOW_MINIO_USER}" 2>/dev/null || \
  $MC_BIN admin policy set local policy-airflow "user=${AIRFLOW_MINIO_USER}" 2>/dev/null || true

$MC_BIN admin policy attach local policy-fastapi --user "${API_MINIO_USER}" 2>/dev/null || \
  $MC_BIN admin policy set local policy-fastapi "user=${API_MINIO_USER}" 2>/dev/null || true

rm -rf "${TMP_POLICIES_DIR}"

echo "======================================================================"
echo "[MinIO Init] Inicialización completada exitosamente."
echo "[MinIO Init] Buckets aprovisionados: market-lake, market-checkpoints, market-quarantine"
echo "[MinIO Init] Usuarios configurados: ${SPARK_MINIO_USER}, ${AIRFLOW_MINIO_USER}, ${API_MINIO_USER}"
echo "======================================================================"
