"""
Pruebas unitarias para el DAG batch_recompute (US-15).
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

try:
    import pytest
except ImportError:
    pytest = None

from dags.batch_recompute import build_batch_command, run_batch_recompute


def test_build_batch_command_defaults():
    """Valida la construcción del comando spark-submit con valores por defecto."""
    mock_context = {
        "params": {},
        "dag_run": None,
        "logical_date": datetime(2026, 10, 19, 2, 0, 0, tzinfo=timezone.utc),
    }

    command = build_batch_command(**mock_context)

    assert command[0] == "spark-submit"
    assert "--master" in command
    assert "--driver-memory" in command
    assert "--executor-memory" in command
    assert "--executor-cores" in command
    assert "--start-date" in command
    # El día anterior a 2026-10-19 es 2026-10-18
    start_idx = command.index("--start-date")
    assert command[start_idx + 1] == "2026-10-18"
    end_idx = command.index("--end-date")
    assert command[end_idx + 1] == "2026-10-18"


def test_build_batch_command_from_trigger_conf():
    """Valida que los parámetros pasados desde el trigger de ingesta (conf) tomen precedencia."""
    mock_dag_run = MagicMock()
    mock_dag_run.conf = {
        "start_date": "2026-09-01",
        "end_date": "2026-09-15",
        "symbols": "BTCUSDT,ETHUSDT",
        "driver_memory": "2G",
    }
    mock_context = {
        "params": {"start_date": "2026-10-01"},
        "dag_run": mock_dag_run,
        "logical_date": datetime(2026, 10, 19, 2, 0, 0, tzinfo=timezone.utc),
    }

    command = build_batch_command(**mock_context)

    start_idx = command.index("--start-date")
    assert command[start_idx + 1] == "2026-09-01"
    end_idx = command.index("--end-date")
    assert command[end_idx + 1] == "2026-09-15"
    assert "--symbols" in command
    sym_idx = command.index("--symbols")
    assert command[sym_idx + 1] == "BTCUSDT,ETHUSDT"
    driver_idx = command.index("--driver-memory")
    assert command[driver_idx + 1] == "2G"


@patch("subprocess.Popen")
def test_run_batch_recompute_success(mock_popen):
    """Valida la ejecución exitosa de Spark submit con streaming de logs."""
    mock_process = MagicMock()
    mock_process.stdout = ["Iniciando Spark Batch...\n", "Procesadas 1440 velas.\n"]
    mock_process.wait.return_value = None
    mock_process.returncode = 0
    mock_popen.return_value = mock_process

    mock_ti = MagicMock()
    mock_context = {
        "task_instance": mock_ti,
        "params": {},
        "dag_run": None,
        "logical_date": datetime(2026, 10, 19, 2, 0, 0, tzinfo=timezone.utc),
    }

    result = run_batch_recompute(**mock_context)
    assert result["status"] == "SUCCESS"
    assert result["latency_ms"] >= 0


@patch("subprocess.Popen")
def test_run_batch_recompute_failure_raises(mock_popen):
    """Valida que un código de salida distinto de 0 lance una excepción para forzar reintento."""
    mock_process = MagicMock()
    mock_process.stdout = ["OutOfMemoryError in executor...\n"]
    mock_process.wait.return_value = None
    mock_process.returncode = 137  # Kill por OOM
    mock_popen.return_value = mock_process

    mock_ti = MagicMock()
    mock_context = {
        "task_instance": mock_ti,
        "params": {},
        "dag_run": None,
        "logical_date": datetime(2026, 10, 19, 2, 0, 0, tzinfo=timezone.utc),
    }

    error_raised = False
    try:
        run_batch_recompute(**mock_context)
    except RuntimeError as ex:
        error_raised = True
        assert "falló con código de salida 137" in str(ex)

    assert error_raised is True

