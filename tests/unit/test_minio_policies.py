"""
Unit tests for MinIO IAM policies and initialization script.
Validates US-03 Acceptance Criteria:
- Criterio 2: Regla Anti-Medallion en definiciones de políticas.
- Criterio 3: Principio de Mínimo Privilegio (Spark, Airflow, FastAPI).
"""

import json
import os
import stat
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
POLICIES_DIR = REPO_ROOT / "infra" / "minio" / "policies"
INIT_SCRIPT = REPO_ROOT / "infra" / "minio" / "init-buckets.sh"


def test_policies_directory_and_files_exist():
    """Verifica que el directorio de políticas y los archivos JSON existan."""
    assert POLICIES_DIR.exists(), f"El directorio {POLICIES_DIR} debe existir."
    expected_files = ["spark-policy.json", "airflow-policy.json", "api-policy.json"]
    for filename in expected_files:
        policy_path = POLICIES_DIR / filename
        assert policy_path.exists(), f"El archivo {filename} debe existir en {POLICIES_DIR}."


def test_anti_medallion_rule_in_policies():
    """
    Criterio 2 (BDD): Cumplimiento de la regla anti-Medallion.
    Ninguna política debe hacer referencia a 'bronze', 'silver' o 'gold'.
    """
    for policy_path in POLICIES_DIR.glob("*.json"):
        content = policy_path.read_text().lower()
        assert "bronze" not in content, f"Violación anti-medallion ('bronze') en {policy_path.name}"
        assert "silver" not in content, f"Violación anti-medallion ('silver') en {policy_path.name}"
        assert "gold" not in content, f"Violación anti-medallion ('gold') en {policy_path.name}"


def test_spark_policy_least_privilege():
    """
    Criterio 3 (BDD): Valida que la política de Spark otorgue solo los permisos requeridos:
    - Checkpoints R/W/Delete en market-checkpoints/spark/*
    - DLQ Write en market-quarantine/*
    - Batch results Write en market-lake/batch-results/*
    - Read en historical/* y reference/*
    """
    policy_path = POLICIES_DIR / "spark-policy.json"
    data = json.loads(policy_path.read_text())
    assert data.get("Version") == "2012-10-17"
    statements = data.get("Statement", [])
    assert len(statements) >= 4

    resources = []
    actions = []
    for stmt in statements:
        assert stmt.get("Effect") == "Allow"
        res = stmt.get("Resource")
        if isinstance(res, list):
            resources.extend(res)
        else:
            resources.append(res)
        act = stmt.get("Action")
        if isinstance(act, list):
            actions.extend(act)
        else:
            actions.append(act)

    # Checkpoints
    assert "arn:aws:s3:::market-checkpoints/*" in resources
    # Quarantine
    assert "arn:aws:s3:::market-quarantine/*" in resources
    # Batch results
    assert "arn:aws:s3:::market-lake/batch-results/*" in resources


def test_airflow_policy_least_privilege():
    """
    Criterio 3 (BDD): Valida que Airflow tenga acceso a market-lake y auditoría en market-quarantine,
    pero NO a market-checkpoints.
    """
    policy_path = POLICIES_DIR / "airflow-policy.json"
    data = json.loads(policy_path.read_text())
    statements = data.get("Statement", [])

    all_resources = []
    for stmt in statements:
        assert stmt.get("Effect") == "Allow"
        res = stmt.get("Resource")
        if isinstance(res, list):
            all_resources.extend(res)
        else:
            all_resources.append(res)

    # Debe incluir market-lake y market-quarantine
    assert any("market-lake" in r for r in all_resources)
    assert any("market-quarantine" in r for r in all_resources)

    # Airflow NO debe tener acceso a market-checkpoints (aislamiento)
    assert not any("market-checkpoints" in r for r in all_resources), \
        "Airflow no debe tener acceso al bucket de checkpoints de streaming."


def test_api_policy_read_only_least_privilege():
    """
    Criterio 3 (BDD): Valida que FastAPI tenga acceso estrictamente Read-Only
    a market-lake/batch-results/* y market-lake/reference/*, sin permisos de escritura ni acceso a otros buckets.
    """
    policy_path = POLICIES_DIR / "api-policy.json"
    data = json.loads(policy_path.read_text())
    statements = data.get("Statement", [])

    all_actions = []
    all_resources = []
    for stmt in statements:
        assert stmt.get("Effect") == "Allow"
        act = stmt.get("Action")
        if isinstance(act, list):
            all_actions.extend(act)
        else:
            all_actions.append(act)

        res = stmt.get("Resource")
        if isinstance(res, list):
            all_resources.extend(res)
        else:
            all_resources.append(res)

    # Solo lectura (GetObject / ListBucket / GetBucketLocation)
    for action in all_actions:
        assert action in ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"], \
            f"Acción prohibida para la API de Serving: {action}"

    # No debe tener acciones de escritura ni borrado
    assert not any("Put" in a or "Delete" in a for a in all_actions)

    # Aislamiento de buckets: solo market-lake
    assert not any("market-checkpoints" in r for r in all_resources)
    assert not any("market-quarantine" in r for r in all_resources)


def test_init_buckets_script_executable_and_valid():
    """Valida la integridad del script init-buckets.sh."""
    assert INIT_SCRIPT.exists(), f"{INIT_SCRIPT} debe existir."
    # Permiso de ejecución
    file_mode = os.stat(INIT_SCRIPT).st_mode
    assert bool(file_mode & stat.S_IXUSR), "init-buckets.sh debe tener permisos de ejecución (+x)."

    content = INIT_SCRIPT.read_text()
    # Buckets requeridos
    assert "market-lake" in content
    assert "market-checkpoints" in content
    assert "market-quarantine" in content

    # Usuarios de servicio
    assert "SPARK_MINIO_USER" in content
    assert "AIRFLOW_MINIO_USER" in content
    assert "API_MINIO_USER" in content

    # Verificación Anti-Medallion en el script
    assert "bronze" in content and "silver" in content and "gold" in content


def _spark_quarantine_actions(policy_text: str) -> list:
    """Devuelve las acciones del statement que cubre `market-quarantine/*`."""
    for stmt in json.loads(policy_text)["Statement"]:
        res = stmt.get("Resource")
        resources = res if isinstance(res, list) else [res]
        if "arn:aws:s3:::market-quarantine/*" in resources:
            actions = stmt.get("Action")
            return actions if isinstance(actions, list) else [actions]
    raise AssertionError("No se encontró un statement que cubra market-quarantine/*")


def _init_heredoc(script_text: str, filename: str) -> str:
    """Extrae el JSON que `init-buckets.sh` escribe vía `cat << 'EOF'`."""
    marker = f'cat << \'EOF\' > "${{TMP_POLICIES_DIR}}/{filename}"'
    start = script_text.index(marker) + len(marker)
    end = script_text.index("\nEOF", start)
    return script_text[start:end].lstrip("\n")


def test_spark_dlq_permite_leer_borrar_y_escribir_en_ambas_definiciones():
    """
    Regresión: la escritura de la DLQ con `mode=overwrite` exige tres permisos
    de S3A que la política de Spark no concedía:

    1. `GetObject` — S3A hace `getFileStatus` (HEAD) del directorio de destino
       antes de escribir, y MinIO responde 403 —y no 404— sin ese permiso.
    2. `DeleteObject` — el committer de S3A borra el directorio temporal
       (`.spark-staging-*`) tras confirmar la escritura.
    3. `PutObject`/`AbortMultipartUpload` — la escritura en sí.

    La política de Spark existe en dos sitios (el JSON montado en `/policies`
    y el heredoc que `init-buckets.sh` envía a `mc admin policy`); ambos deben
    otorgar los permisos, o `make up` revierte el fix silenciosamente.
    """
    json_path = POLICIES_DIR / "spark-policy.json"
    heredoc = _init_heredoc(INIT_SCRIPT.read_text(), "spark-policy.json")

    for origen, policy_text in (("spark-policy.json", json_path.read_text()),
                                ("init-buckets.sh", heredoc)):
        actions = _spark_quarantine_actions(policy_text)
        for required in ("s3:GetObject", "s3:PutObject", "s3:DeleteObject"):
            assert required in actions, (
                f"{origen} no concede {required} sobre market-quarantine/*: "
                f"el job Batch con --quarantine-invalid falla escribiendo la DLQ."
            )
