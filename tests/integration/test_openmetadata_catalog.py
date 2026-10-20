"""
Pruebas de Integración para OpenMetadata Catálogo, Profiling y Calidad de Datos (US-25).
Responsable: Samuel Roncancio (Data Architect & Storage Lead)

Criterios de Aceptación Validados:
- Criterio 1 (BDD): Registro completo de datasets con owners asignados, descripciones de columnas,
  frecuencia de actualización y SLA en MinIO, Postgres y Kafka.
- Criterio 2 (BDD): Generación de perfiles estadísticos (mínimos, máximos, medias, recuento de nulos
  y cardinalidad de símbolos).
- Criterio 3 (BDD): Reglas de pruebas de calidad de datos automatizadas:
  * No nulidad en event_time
  * No nulidad en price
  * Unicidad de event_id
  * Valores de price > 0
- Tareas Técnicas 1 y 2: Conectores declarativos y aprovisionamiento de infraestructura.
"""

import os
import json
import pytest
import yaml

from metadata.openmetadata.scripts.register_and_profile import (
    GOVERNANCE_METADATA,
    calculate_data_profile,
    run_data_quality_tests,
    export_catalog_report
)


# ------------------------------------------------------------------------------
# Pruebas de Criterio 1: Registro Completo de Datasets, Owners, SLAs y Columnas
# ------------------------------------------------------------------------------

def test_bdd_criterio_1_catalog_datasets_registration():
    """Valida que estén catalogados los datasets clave de MinIO, PostgreSQL y Kafka."""
    datasets = GOVERNANCE_METADATA["catalog_datasets"]
    dataset_names = {d["name"] for d in datasets}

    assert "market_events_raw" in dataset_names, "Dataset MinIO raw no catalogado"
    assert "batch_ohlcv_consolidated" in dataset_names, "Dataset MinIO batch OHLCV no catalogado"
    assert "postgres_dim_assets" in dataset_names, "Dataset PostgreSQL dim_assets no catalogado"
    assert "kafka_market_events_topic" in dataset_names, "Topic Kafka market.events no catalogado"


def test_bdd_criterio_1_owners_and_slas_assigned():
    """Valida que cada dataset tenga asignado su Owner técnico/negocio, frecuencia y SLA."""
    datasets = GOVERNANCE_METADATA["catalog_datasets"]

    for d in datasets:
        assert d.get("owner"), f"Dataset {d['name']} no tiene owner asignado"
        assert d.get("sla"), f"Dataset {d['name']} no tiene SLA definido"
        assert d.get("frequency"), f"Dataset {d['name']} no tiene frecuencia definida"
        assert len(d.get("columns", [])) > 0, f"Dataset {d['name']} no tiene columnas documentadas"


def test_bdd_criterio_1_connectors_configuration_files():
    """Valida que existan los manifiestos YAML de los conectores de MinIO, Postgres y Kafka."""
    connectors_dir = "metadata/openmetadata/connectors"
    required_connectors = [
        "minio_s3_connector.yaml",
        "postgres_connector.yaml",
        "kafka_connector.yaml"
    ]

    for conn_file in required_connectors:
        full_path = os.path.join(connectors_dir, conn_file)
        assert os.path.isfile(full_path), f"Conector {conn_file} no encontrado"
        with open(full_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
            assert "source" in content, f"Estructura inválida en {conn_file}"
            assert "serviceConnection" in content["source"], f"Falta serviceConnection en {conn_file}"


# ------------------------------------------------------------------------------
# Pruebas de Criterio 2: Perfiles Estadísticos de Datos (Data Profiling)
# ------------------------------------------------------------------------------

def test_bdd_criterio_2_profiling_statistics_generation():
    """Valida la generación de métricas de distribución: min, max, mean, nulos y cardinalidad."""
    sample_records = [
        {"event_id": "e1", "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.5, "event_time": "2026-10-20T00:00:01Z"},
        {"event_id": "e2", "symbol": "BTCUSDT", "price": 62000.0, "quantity": 0.5, "event_time": "2026-10-20T00:00:02Z"},
        {"event_id": "e3", "symbol": "ETHUSDT", "price": 2500.0, "quantity": 10.0, "event_time": "2026-10-20T00:00:03Z"},
        {"event_id": "e4", "symbol": "SOLUSDT", "price": 150.0, "quantity": 20.0, "event_time": "2026-10-20T00:00:04Z"},
    ]

    profile = calculate_data_profile(sample_records)

    assert profile["row_count"] == 4
    price_metrics = profile["columns"]["price"]
    assert price_metrics["min"] == 150.0
    assert price_metrics["max"] == 62000.0
    assert price_metrics["mean"] == (60000.0 + 62000.0 + 2500.0 + 150.0) / 4
    assert price_metrics["null_count"] == 0

    symbol_metrics = profile["columns"]["symbol"]
    assert symbol_metrics["cardinality"] == 3  # BTCUSDT, ETHUSDT, SOLUSDT
    assert set(symbol_metrics["distinct_values"]) == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


def test_bdd_criterio_2_profiling_null_count_detection():
    """Valida la detección correcta de valores nulos en el profiler."""
    sample_records_with_nulls = [
        {"event_id": "e1", "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.0, "event_time": "2026-10-20T00:00:01Z"},
        {"event_id": "e2", "symbol": None, "price": None, "quantity": None, "event_time": None},
    ]

    profile = calculate_data_profile(sample_records_with_nulls)
    assert profile["columns"]["price"]["null_count"] == 1
    assert profile["columns"]["event_time"]["null_count"] == 1
    assert profile["columns"]["symbol"]["null_count"] == 1


# ------------------------------------------------------------------------------
# Pruebas de Criterio 3: Reglas de Calidad Automatizadas (Data Quality Tests)
# ------------------------------------------------------------------------------

def test_bdd_criterio_3_quality_tests_passing_on_valid_data():
    """Valida que un dataset válido apruebe todas las reglas de calidad."""
    valid_records = [
        {"event_id": "id-001", "event_time": "2026-10-20T01:00:00Z", "price": 50000.0, "quantity": 1.0},
        {"event_id": "id-002", "event_time": "2026-10-20T01:00:01Z", "price": 50050.0, "quantity": 2.5},
    ]

    result = run_data_quality_tests(valid_records)
    assert result["all_passed"] is True, "Dataset válido falló las pruebas de calidad"
    
    test_statuses = {t["test_name"]: t["status"] for t in result["tests"]}
    assert test_statuses["columnValuesToBeNotNull_event_time"] == "PASSED"
    assert test_statuses["columnValuesToBeNotNull_price"] == "PASSED"
    assert test_statuses["columnValuesToBeUnique_event_id"] == "PASSED"
    assert test_statuses["columnValuesToBeGreaterThan_price_0"] == "PASSED"


def test_bdd_criterio_3_quality_tests_detect_null_event_time_or_price():
    """Valida que se detecten fallos ante valores nulos en event_time o price."""
    invalid_records = [
        {"event_id": "id-001", "event_time": None, "price": 50000.0},
        {"event_id": "id-002", "event_time": "2026-10-20T01:00:00Z", "price": None},
    ]

    result = run_data_quality_tests(invalid_records)
    assert result["all_passed"] is False

    test_statuses = {t["test_name"]: t["status"] for t in result["tests"]}
    assert test_statuses["columnValuesToBeNotNull_event_time"] == "FAILED"
    assert test_statuses["columnValuesToBeNotNull_price"] == "FAILED"


def test_bdd_criterio_3_quality_tests_detect_duplicate_event_id():
    """Valida que se detecte duplicidad de event_id."""
    duplicate_records = [
        {"event_id": "duplicate-uuid", "event_time": "2026-10-20T01:00:00Z", "price": 100.0},
        {"event_id": "duplicate-uuid", "event_time": "2026-10-20T01:00:01Z", "price": 102.0},
    ]

    result = run_data_quality_tests(duplicate_records)
    assert result["all_passed"] is False

    test_statuses = {t["test_name"]: t["status"] for t in result["tests"]}
    assert test_statuses["columnValuesToBeUnique_event_id"] == "FAILED"


def test_bdd_criterio_3_quality_tests_detect_negative_or_zero_price():
    """Valida que se detecte price <= 0."""
    invalid_price_records = [
        {"event_id": "id-001", "event_time": "2026-10-20T01:00:00Z", "price": 0.0},
        {"event_id": "id-002", "event_time": "2026-10-20T01:00:01Z", "price": -15.5},
    ]

    result = run_data_quality_tests(invalid_price_records)
    assert result["all_passed"] is False

    test_statuses = {t["test_name"]: t["status"] for t in result["tests"]}
    assert test_statuses["columnValuesToBeGreaterThan_price_0"] == "FAILED"


def test_bdd_report_export_and_persistence(tmp_path):
    """Valida la generación y persistencia del informe JSON de catálogo y calidad."""
    report_file = export_catalog_report(str(tmp_path))
    assert os.path.isfile(report_file)

    with open(report_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert "metadata_header" in data
        assert "profiling" in data
        assert "quality_tests" in data
        assert data["quality_tests"]["all_passed"] is True
