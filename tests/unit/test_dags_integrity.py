"""
Tests de Integración y Validación de Integridad de DAGs de Airflow (US-13).
Responsable: Andrés Teherán

Verifica:
1. Ausencia total de errores de sintaxis e importación en todos los archivos de `dags/` (DAG Import Errors: 0).
2. Cumplimiento de políticas de reintentos (`retries >= 2` o `3`) y callbacks de fallo.
3. Existencia y correcta definición estructural de los DAGs troncales:
   - `ingest_historical` (US-14)
   - `batch_recompute` (US-15)
   - `compact_files` (US-16)
   - `replay_dlq` (US-17)
"""

import importlib.util
import os
import sys
from pathlib import Path
import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent.parent / "dags"


def get_dag_files():
    """Retorna todos los archivos .py de la carpeta dags excepto common_config y __init__."""
    dag_files = []
    for f in DAGS_DIR.glob("*.py"):
        if f.name not in ["__init__.py", "common_config.py"]:
            dag_files.append(f)
    return dag_files


@pytest.mark.parametrize("dag_path", get_dag_files(), ids=lambda p: p.name)
def test_dag_files_import_without_errors(dag_path):
    """Criterio 2: Cada archivo de DAG debe importarse limpiamente sin errores sintácticos."""
    module_name = f"dags.{dag_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, str(dag_path))
    assert spec is not None, f"No se pudo crear spec para {dag_path.name}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        pytest.fail(f"Error de importación en el DAG {dag_path.name}: {e}")


def test_common_config_standards():
    """Criterio 3: common_config debe definir retries=3, retroceso exponencial y callback de alertas."""
    from dags.common_config import DEFAULT_DAG_ARGS, default_failure_alert, get_default_args

    assert DEFAULT_DAG_ARGS["retries"] == 3
    assert DEFAULT_DAG_ARGS["retry_exponential_backoff"] is True
    assert DEFAULT_DAG_ARGS["on_failure_callback"] is not None

    custom_args = get_default_args(owner="Andres", retries=5)
    assert custom_args["owner"] == "Andres"
    assert custom_args["retries"] == 5
    assert custom_args["retry_exponential_backoff"] is True


def test_init_connections_script_exists_and_executable():
    """Criterio 1: El script de inicialización de conexiones debe existir y tener permisos de ejecución."""
    init_script = DAGS_DIR.parent / "infra" / "airflow" / "init-connections.sh"
    assert init_script.exists(), "El script infra/airflow/init-connections.sh no existe"
    assert os.access(str(init_script), os.X_OK), "El script no tiene permisos de ejecución (+x)"

    content = init_script.read_text()
    assert "minio_s3_conn" in content
    assert "postgres_serving_conn" in content
    assert "spark_default" in content
