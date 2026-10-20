"""
Construcción de la sesión Spark compartida por la capa Batch y la capa Serving.

US-10: Configurar sesión de Spark con dependencias Hadoop-AWS y cliente S3A
       para leer y escribir en MinIO (Data Lake S3-compatible).

El módulo centraliza la configuración de S3A (endpoint, estilo de ruta y
credenciales del servicio `spark_service`) para que todos los jobs de la
plataforma hablen con MinIO de la misma forma y no se duplique la lógica
de conexión en cada script.

Variables de entorno Reconocidas:
    SPARK_MASTER_URL     URL del master Spark (por defecto: spark://spark-master:7077)
    S3_ENDPOINT          Endpoint S3 de MinIO (por defecto: http://minio:9000)
    AWS_ACCESS_KEY_ID    Credencial de acceso del servicio Spark
    AWS_SECRET_ACCESS_KEY Secreto de acceso del servicio Spark
    AWS_REGION           Región AWS (por defecto: us-east-1)
"""

import os

from pyspark.sql import SparkSession

# Valores por defecto alineados con `.env.example` y con el servicio IAM
# `spark_service` creado por `infra/minio/init-buckets.sh`.
DEFAULT_MASTER_URL = "spark://spark-master:7077"
DEFAULT_S3_ENDPOINT = "http://minio:9000"
DEFAULT_REGION = "us-east-1"

# URI canónico del Data Lake. La organización es funcional (no Medallion):
# `historical/` guarda los eventos crudos, `batch-results/` los agregados.
LAKE_BUCKET = "market-lake"
QUARANTINE_BUCKET = "market-quarantine"
CHECKPOINTS_BUCKET = "market-checkpoints"


def build_spark_session(
    app_name: str,
    master_url: str = None,
    enable_kafka: bool = False,
) -> SparkSession:
    """
    Crea (o reutiliza) la `SparkSession` conectada al clúster y a MinIO.

    Se usa el constructor de Session en vez de `SparkSession.builder.getOrCreate()`
    directo para poder inyectar la configuración de S3A en un solo lugar.
    """
    master = master_url or os.environ.get("SPARK_MASTER_URL", DEFAULT_MASTER_URL)
    endpoint = os.environ.get("S3_ENDPOINT", DEFAULT_S3_ENDPOINT)
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "spark_service")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "spark_secure_lake_pass123")
    region = os.environ.get("AWS_REGION", DEFAULT_REGION)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # --- Cliente S3A (Hadoop-AWS) -------------------------------------
        # `path.style.access` es obligatorio en MinIO: S3A por defecto usa
        # virtual-host style (bucket.endpoint), que MinIO no resuelve.
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.aws.region", region)
        # Evita reintentos largos cuando un prefijo no existe todavía
        # (típico en la primera ejecución de un backfill).
        .config("spark.hadoop.fs.s3a.connection.maximum", "40")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        # --- Comportamiento de escritura idempotente ---------------------
        # `dynamic` sobrescribe solo las particiones presentes en el
        # DataFrame escrito, sin tocar las fechas no afectadas (US-11).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
    )

    # Añadir soporte de conectores Kafka si se solicita o vía entorno
    if enable_kafka or os.environ.get("SPARK_ENABLE_KAFKA", "").lower() in ("true", "1", "yes"):
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )

    # Añadir soporte de OpenLineage listener (US-26)
    if os.environ.get("OPENLINEAGE_ENABLED", "").lower() in ("true", "1", "yes"):
        builder = (
            builder.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
            .config("spark.openlineage.appName", app_name)
            .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "lambda-crypto-platform"))
        )

    return builder.getOrCreate()


def lake_path(prefix: str = "", bucket: str = LAKE_BUCKET) -> str:
    """Construye una URI `s3a://` canónica apuntando al Data Lake."""
    base = f"s3a://{bucket}"
    if not prefix:
        return base
    return f"{base}/{prefix.lstrip('/')}"


def historical_path() -> str:
    """Prefijo de eventos crudos ingeridos."""
    return lake_path("historical")


def batch_results_path(metric: str = None) -> str:
    """Prefijo de resultados agregados de la capa Batch."""
    base = lake_path("batch-results")
    return f"{base}/metric={metric}" if metric else base


def quarantine_path(source: str = None, date: str = None) -> str:
    """Prefijo de la Dead-Letter Queue (aislamiento de errores)."""
    base = lake_path("", bucket=QUARANTINE_BUCKET)
    parts = [base]
    if source:
        parts.append(f"source={source}")
    if date:
        parts.append(f"date={date}")
    return "/".join(parts)


def checkpoints_path(prefix: str = "") -> str:
    """Prefijo de checkpoints duraderos en MinIO (US-20)."""
    return lake_path(prefix, bucket=CHECKPOINTS_BUCKET)
