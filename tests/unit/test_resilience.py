"""
Tests Unitarios y de Integración para Escenarios de Caos y Resiliencia (US-32).
Responsable: Andrés Teherán

Verifica:
1. Recuperación exacta desde checkpoints duraderos tras la caída abrupta de un worker.
2. Comportamiento resiliente ante particiones de red o indisponibilidad transitoria de almacenamiento S3.
3. Existencia y completitud del Manual de Recuperación ante Desastres (DRP).
"""

from pathlib import Path
from tests.resilience.chaos_runner import ChaosExperiment


def test_spark_checkpoint_recovery_simulation():
    """Criterio 1: Simulación de caída abrupta y recuperación sin duplicados."""
    exp = ChaosExperiment("spark_worker_kill", "Caída forzada de worker de streaming")
    result = exp.run_spark_worker_restart_simulation()

    assert result["status"] == "SUCCESS"
    assert result["recovered_offset"] == 10500
    assert result["duplicate_events"] == 0
    assert result["data_loss"] is False
    assert len(result["audit_log"]) >= 4


def test_storage_backoff_and_retry_simulation():
    """Criterio 2: Reintentos con backoff exponencial ante corte temporal de MinIO."""
    exp = ChaosExperiment("minio_disconnect", "Fallo transitorio de MinIO S3")
    result = exp.run_storage_backoff_simulation(max_retries=3)

    assert result["status"] == "SUCCESS"
    assert result["service_recovered"] is True
    assert result["retries_exhausted"] == 2


def test_disaster_recovery_manual_exists():
    """Verifica que el Manual de Operaciones y DRP esté documentado en documentacion/."""
    drp_path = Path(__file__).resolve().parent.parent.parent / "documentacion" / "MANUAL_RECUPERACION_DESASTRES.md"
    assert drp_path.exists(), "El archivo documentacion/MANUAL_RECUPERACION_DESASTRES.md no existe"

    content = drp_path.read_text(encoding="utf-8")
    assert "RTO" in content
    assert "RPO" in content
    assert "Exactly-Once" in content
    assert "market.dlq" in content
