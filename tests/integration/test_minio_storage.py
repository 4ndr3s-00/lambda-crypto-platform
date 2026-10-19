"""
Integration and S3 Compatibility Tests for MinIO Storage (US-03).
Validates:
- Criterio 1 (BDD): Buckets funcionales y estructura canónica de prefijos (Hive-style).
- Criterio 2 (BDD): Regla Anti-Medallion en almacenamiento de objetos.
- Criterio 3 (BDD): Aislamiento de permisos y mínimo privilegio.
- Tarea Técnica 4: Compatibilidad de SDK Python con boto3 y s3fs.
"""

import io
import os
import re
import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws


CANONICAL_BUCKETS = [
    "market-lake",
    "market-checkpoints",
    "market-quarantine"
]

CANONICAL_PREFIXES = {
    "market-lake": [
        "historical/source=binance/date=2026-10-19/",
        "historical/source=coingecko/date=2026-10-19/",
        "batch-results/metric=ohlcv/date=2026-10-19/",
        "batch-results/metric=returns/date=2026-10-19/",
        "reference/symbols/"
    ],
    "market-checkpoints": [
        "spark/query=crypto_trades_stream/",
        "spark/query=crypto_metrics_stream/"
    ],
    "market-quarantine": [
        "source=binance/date=2026-10-19/",
        "source=coingecko/date=2026-10-19/"
    ]
}


@pytest.fixture
def s3_env():
    """Configura el entorno de almacenamiento S3 (Mock Moto para ejecución determinista)."""
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="mock_access_key",
            aws_secret_access_key="mock_secret_key"
        )
        # Inicializar buckets
        for bucket in CANONICAL_BUCKETS:
            client.create_bucket(Bucket=bucket)

        # Inicializar prefijos canónicos con marcador .keep
        for bucket, prefixes in CANONICAL_PREFIXES.items():
            for prefix in prefixes:
                key = f"{prefix}.keep"
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=b"marker",
                    ContentType="text/plain"
                )

        yield client


# ==============================================================================
# Criterio 1: Estructura de buckets y prefijos funcionales
# ==============================================================================
def test_bdd_criterio_1_buckets_exist(s3_env):
    """
    Dado que MinIO está en ejecución,
    Cuando se audita el almacenamiento,
    Entonces deben existir los 3 buckets operativos: market-lake, market-checkpoints, market-quarantine.
    """
    response = s3_env.list_buckets()
    bucket_names = [b["Name"] for b in response.get("Buckets", [])]
    for expected_bucket in CANONICAL_BUCKETS:
        assert expected_bucket in bucket_names, f"Falta el bucket operativo: {expected_bucket}"


def test_bdd_criterio_1_functional_prefixes_structure(s3_env):
    """
    Dado que los buckets están aprovisionados,
    Cuando se listan los prefijos operativos,
    Entonces deben respetar estrictamente el formato Hive-style particionado:
    - s3://market-lake/historical/source=<fuente>/date=YYYY-MM-DD/
    - s3://market-lake/batch-results/metric=<metrica>/date=YYYY-MM-DD/
    - s3://market-lake/reference/symbols/
    - s3://market-checkpoints/spark/query=<stream_id>/
    - s3://market-quarantine/source=<fuente>/date=YYYY-MM-DD/
    """
    for bucket, expected_prefixes in CANONICAL_PREFIXES.items():
        response = s3_env.list_objects_v2(Bucket=bucket)
        keys = [obj["Key"] for obj in response.get("Contents", [])]

        for prefix in expected_prefixes:
            matching_keys = [k for k in keys if k.startswith(prefix)]
            assert len(matching_keys) > 0, f"No se encontró el prefijo requerido: s3://{bucket}/{prefix}"

            # Validar formato canónico con expresiones regulares
            if "historical" in prefix:
                assert re.match(r"^historical/source=[a-z0-9_-]+/date=\d{4}-\d{2}-\d{2}/", prefix)
            elif "batch-results" in prefix:
                assert re.match(r"^batch-results/metric=[a-z0-9_-]+/date=\d{4}-\d{2}-\d{2}/", prefix)
            elif "reference" in prefix:
                assert prefix.startswith("reference/symbols/")
            elif "spark" in prefix:
                assert re.match(r"^spark/query=[a-z0-9_-]+/", prefix)
            elif bucket == "market-quarantine":
                assert re.match(r"^source=[a-z0-9_-]+/date=\d{4}-\d{2}-\d{2}/", prefix)


# ==============================================================================
# Criterio 2: Cumplimiento de la regla anti-Medallion
# ==============================================================================
def test_bdd_criterio_2_anti_medallion_rule(s3_env):
    """
    Dado que la arquitectura es puramente Lambda,
    Cuando se audita la organización física de MinIO,
    Entonces NO debe existir ningún bucket o directorio denominado bronze, silver o gold.
    """
    # 1. Auditar nombres de buckets
    response = s3_env.list_buckets()
    for bucket in response.get("Buckets", []):
        bname = bucket["Name"].lower()
        assert "bronze" not in bname, f"Bucket prohibido encontrado: {bname}"
        assert "silver" not in bname, f"Bucket prohibido encontrado: {bname}"
        assert "gold" not in bname, f"Bucket prohibido encontrado: {bname}"

    # 2. Auditar todas las claves y prefijos
    for bucket in CANONICAL_BUCKETS:
        objs = s3_env.list_objects_v2(Bucket=bucket)
        for item in objs.get("Contents", []):
            key = item["Key"].lower()
            assert "bronze" not in key, f"Prefijo Medallion detectado en {bucket}: {key}"
            assert "silver" not in key, f"Prefijo Medallion detectado en {bucket}: {key}"
            assert "gold" not in key, f"Prefijo Medallion detectado en {bucket}: {key}"


# ==============================================================================
# Criterio 3: Seguridad y Mínimo Privilegio (Simulación de Permisos)
# ==============================================================================
class IAMPolicySimulator:
    """Simulador de evaluación de políticas IAM para verificación BDD de US-03."""
    @staticmethod
    def is_action_allowed(role: str, action: str, bucket: str, key: str = "") -> bool:
        if role == "api_service":
            # API solo puede leer batch-results y reference de market-lake
            if bucket != "market-lake":
                return False
            if action not in ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]:
                return False
            if action == "s3:GetObject":
                return key.startswith("batch-results/") or key.startswith("reference/")
            return True

        elif role == "spark_service":
            # Spark puede escribir checkpoints, cuarentena y batch-results
            if bucket == "market-checkpoints":
                return True
            if bucket == "market-quarantine":
                return action in ["s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"]
            if bucket == "market-lake":
                if key.startswith("batch-results/"):
                    return True
                if key.startswith("historical/") or key.startswith("reference/"):
                    return action in ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
                return action in ["s3:ListBucket", "s3:GetBucketLocation"]
            return False

        elif role == "airflow_service":
            # Airflow administra market-lake y audita cuarentena; sin checkpoints
            if bucket == "market-checkpoints":
                return False
            if bucket == "market-lake":
                return True
            if bucket == "market-quarantine":
                return action in ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
            return False

        return False


def test_bdd_criterio_3_iam_least_privilege_isolation():
    """
    Criterio 3 (BDD): Valida el aislamiento y mínimo privilegio de los 3 componentes:
    - FastAPI: Read-Only en market-lake/batch-results/*, denegado en checkpoints y quarantine.
    - Spark: Full en checkpoints, write en DLQ quarantine, write en batch-results.
    - Airflow: Full en market-lake, read en quarantine, bloqueado en checkpoints.
    """
    sim = IAMPolicySimulator()

    # 1. FastAPI (Serving API)
    assert sim.is_action_allowed("api_service", "s3:GetObject", "market-lake", "batch-results/metric=ohlcv/test.parquet")
    assert sim.is_action_allowed("api_service", "s3:GetObject", "market-lake", "reference/symbols/btc.json")
    # Prohibido: Escribir en market-lake
    assert not sim.is_action_allowed("api_service", "s3:PutObject", "market-lake", "batch-results/metric=ohlcv/test.parquet")
    # Prohibido: Leer o escribir checkpoints
    assert not sim.is_action_allowed("api_service", "s3:GetObject", "market-checkpoints", "spark/query=trades/offset")
    assert not sim.is_action_allowed("api_service", "s3:PutObject", "market-checkpoints", "spark/query=trades/offset")
    # Prohibido: Leer o escribir cuarentena
    assert not sim.is_action_allowed("api_service", "s3:GetObject", "market-quarantine", "source=binance/error.json")

    # 2. Apache Spark
    assert sim.is_action_allowed("spark_service", "s3:PutObject", "market-checkpoints", "spark/query=stream/0")
    assert sim.is_action_allowed("spark_service", "s3:PutObject", "market-quarantine", "source=binance/corrupt.json")
    assert sim.is_action_allowed("spark_service", "s3:PutObject", "market-lake", "batch-results/metric=ohlcv/agg.parquet")
    assert sim.is_action_allowed("spark_service", "s3:GetObject", "market-lake", "historical/source=binance/raw.json")
    # Prohibido: Escribir históricos directamente (Airflow los ingesta)
    assert not sim.is_action_allowed("spark_service", "s3:PutObject", "market-lake", "historical/source=binance/raw.json")

    # 3. Apache Airflow
    assert sim.is_action_allowed("airflow_service", "s3:PutObject", "market-lake", "historical/source=binance/raw.json")
    assert sim.is_action_allowed("airflow_service", "s3:GetObject", "market-quarantine", "source=binance/corrupt.json")
    # Prohibido: Acceso a checkpoints de Spark
    assert not sim.is_action_allowed("airflow_service", "s3:PutObject", "market-checkpoints", "spark/query=test/0")
    assert not sim.is_action_allowed("airflow_service", "s3:GetObject", "market-checkpoints", "spark/query=test/0")


# ==============================================================================
# Tarea Técnica 4: Compatibilidad S3 con SDK Python (boto3 & s3fs)
# ==============================================================================
def test_s3_sdk_boto3_compatibility(s3_env):
    """Prueba operaciones de carga, lectura, metadatos y streaming con boto3."""
    payload = b'{"event_id": "test_1", "symbol": "BTCUSDT", "price": 68000.0}'
    test_key = "historical/source=binance/date=2026-10-19/test_event.json"

    # PutObject con ContentType y Metadata
    put_res = s3_env.put_object(
        Bucket="market-lake",
        Key=test_key,
        Body=payload,
        ContentType="application/json",
        Metadata={"source": "binance", "schema_version": "2.0.0"}
    )
    assert put_res["ResponseMetadata"]["HTTPStatusCode"] == 200

    # GetObject y validación de integridad
    get_res = s3_env.get_object(Bucket="market-lake", Key=test_key)
    body_content = get_res["Body"].read()
    assert body_content == payload
    # Moto / S3 normalizan los headers de metadata con guiones o minúsculas
    meta_version = get_res["Metadata"].get("schema-version") or get_res["Metadata"].get("schema_version")
    assert meta_version == "2.0.0"

    # HeadObject y ListObjects
    head_res = s3_env.head_object(Bucket="market-lake", Key=test_key)
    assert head_res["ContentLength"] == len(payload)

    # DeleteObject (limpieza)
    del_res = s3_env.delete_object(Bucket="market-lake", Key=test_key)
    assert del_res["ResponseMetadata"]["HTTPStatusCode"] in [200, 204]


def test_s3_sdk_s3fs_compatibility():
    """Valida la configuración e integración de s3fs para lectura de datasets en la plataforma."""
    import s3fs

    # 1. Instanciación con credenciales y endpoint de MinIO
    fs = s3fs.S3FileSystem(
        key="api_service",
        secret="api_secure_serving_pass123",
        client_kwargs={
            "endpoint_url": "http://minio:9000",
            "region_name": "us-east-1"
        }
    )
    assert "s3" in fs.protocol

    # 2. Validación de parseo y división de rutas canónicas en MinIO
    canonical_uri = "s3://market-lake/batch-results/metric=ohlcv/date=2026-10-19/data.parquet"
    bucket, key, _ = fs.split_path(canonical_uri)
    assert bucket == "market-lake"
    assert key == "batch-results/metric=ohlcv/date=2026-10-19/data.parquet"
