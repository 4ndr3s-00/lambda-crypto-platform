"""
Pruebas unitarias y BDD de Gobierno de Datos en Apache Atlas (US-27).

Criterios de Aceptación validados:
    - US-27 Criterio 1: Definición del glosario de negocio unificado con términos:
      Vela Japonesa (OHLCV), Precio de Cierre, Volumen Ponderado, Event-Time,
      Retorno Porcentual y Watermark.
    - US-27 Criterio 2: Clasificación y taxonomía de entidades aplicando
      Datos_Publicos_Mercado, Metadatos_Sensibles y Datos_Provisionales_Speed.
    - US-27 Criterio 3: Delimitación formal de responsabilidades Atlas vs OpenMetadata.
"""

import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
METADATA_DIR = REPO_ROOT / "metadata"
sys.path.insert(0, str(METADATA_DIR))

import import_atlas_governance  # noqa: E402


@pytest.fixture
def governance_files():
    """Carga los archivos de gobierno para las aserciones de prueba."""
    return import_atlas_governance.load_governance_files(METADATA_DIR)


# ==============================================================================
# US-27: Criterio 1 - Definición del Glosario de Negocio Unificado
# ==============================================================================
def test_bdd_us27_criterio_1_glosario_terminos_obligatorios(governance_files):
    """
    Dado que se consulta el archivo de glosario de Apache Atlas,
    Cuando se evalúan los términos presentes,
    Entonces se encuentran formalmente definidos: Vela Japonesa (OHLCV),
    Precio de Cierre, Volumen Ponderado, Event-Time, Retorno Porcentual y Watermark.
    """
    glossary_data, _, _ = governance_files
    terms = {t["name"]: t for t in glossary_data["terms"]}

    mandatory_terms = [
        "Vela_Japonesa_OHLCV",
        "Precio_de_Cierre",
        "Volumen_Ponderado",
        "Event_Time",
        "Retorno_Porcentual",
        "Watermark",
    ]

    for term_name in mandatory_terms:
        assert term_name in terms, f"El término obligatorio '{term_name}' no se encuentra en el glosario."
        term = terms[term_name]
        assert term["displayName"], f"El término '{term_name}' carece de displayName legible."
        assert len(term["shortDescription"]) > 10, f"Descripción corta insuficiente para '{term_name}'."
        assert len(term["longDescription"]) > 20, f"Descripción larga insuficiente para '{term_name}'."
        assert "formula" in term, f"Fórmula o definición técnica ausente en '{term_name}'."
        assert term["steward"], f"Steward responsable no asignado en '{term_name}'."


def test_bdd_us27_criterio_1_glosario_conceptos_adicionales_financieros(governance_files):
    """
    Dado que la plataforma procesa criptoactivos y microestructura de mercado,
    Cuando se audita el glosario,
    Entonces también se encuentran definidos Slippage y Spread.
    """
    glossary_data, _, _ = governance_files
    term_names = {t["name"] for t in glossary_data["terms"]}

    assert "Slippage" in term_names
    assert "Spread" in term_names


def test_bdd_us27_criterio_1_metadatos_del_glosario_raiz(governance_files):
    """Valida que el contenedor raíz del glosario tenga identificador y descripción formal."""
    glossary_data, _, _ = governance_files
    root = glossary_data.get("glossary", {})
    assert root.get("name") == "CryptoFinance_Enterprise_Glossary"
    assert root.get("language") == "es"
    assert "Glosario Empresarial" in root.get("shortDescription", "")


# ==============================================================================
# US-27: Criterio 2 - Clasificación y Taxonomía de Entidades
# ==============================================================================
def test_bdd_us27_criterio_2_clasificaciones_normativas_definidas(governance_files):
    """
    Dado que se inspecciona la taxonomía de Apache Atlas,
    Cuando se validan las definiciones de clasificación,
    Entonces están presentes Datos_Publicos_Mercado, Metadatos_Sensibles y Datos_Provisionales_Speed.
    """
    _, types_data, _ = governance_files
    classifications = {c["name"]: c for c in types_data.get("classificationDefs", [])}

    expected_classes = [
        "Datos_Publicos_Mercado",
        "Metadatos_Sensibles",
        "Datos_Provisionales_Speed",
    ]

    for class_name in expected_classes:
        assert class_name in classifications, f"Clasificación mandatoria '{class_name}' ausente."
        c_def = classifications[class_name]
        assert len(c_def.get("description", "")) > 15
        assert len(c_def.get("attributeDefs", [])) > 0, f"'{class_name}' debe tener atributos definidos."


def test_bdd_us27_criterio_2_atributos_de_seguridad_en_clasificaciones(governance_files):
    """
    Dado que las clasificaciones regulan la gobernanza y confidencialidad,
    Cuando se analizan sus atributos,
    Entonces Metadatos_Sensibles exige nivel_criticidad y requiere_cifrado,
    mientras Datos_Publicos_Mercado define es_publico.
    """
    _, types_data, _ = governance_files
    classifications = {c["name"]: c for c in types_data.get("classificationDefs", [])}

    sensibles_attrs = {a["name"]: a for a in classifications["Metadatos_Sensibles"]["attributeDefs"]}
    assert "nivel_criticidad" in sensibles_attrs
    assert "requiere_cifrado" in sensibles_attrs

    publicos_attrs = {a["name"]: a for a in classifications["Datos_Publicos_Mercado"]["attributeDefs"]}
    assert "es_publico" in publicos_attrs

    speed_attrs = {a["name"]: a for a in classifications["Datos_Provisionales_Speed"]["attributeDefs"]}
    assert "capa_origen" in speed_attrs
    assert "reconciliado_por_batch" in speed_attrs


def test_bdd_us27_criterio_2_mapeo_entidades_plataforma(governance_files):
    """
    Dado el inventario de entidades de la plataforma (MinIO, Kafka, Postgres),
    Cuando se evalúa su asignación de clasificaciones,
    Entonces cada entidad está catalogada con taxonomías y términos existentes.
    """
    glossary_data, types_data, entities_data = governance_files
    report = import_atlas_governance.validate_governance_definitions(
        glossary_data, types_data, entities_data
    )

    assert report["entities_count"] >= 8

    entities = {e["name"]: e for e in entities_data["entities"]}

    # Kafka
    assert "market.events" in entities
    assert "Datos_Publicos_Mercado" in entities["market.events"]["classifications"]

    assert "market.metrics" in entities
    assert "Datos_Provisionales_Speed" in entities["market.metrics"]["classifications"]

    assert "market.dlq" in entities
    assert "Datos_Cuarentena_DLQ" in entities["market.dlq"]["classifications"]

    # Postgres Serving
    assert "serving.pipeline_execution_log" in entities
    assert "Metadatos_Sensibles" in entities["serving.pipeline_execution_log"]["classifications"]

    assert "serving.realtime_metrics" in entities
    assert "Datos_Provisionales_Speed" in entities["serving.realtime_metrics"]["classifications"]

    # MinIO
    assert "market-lake-batch-results" in entities
    assert "Datos_Historicos_Inmutables" in entities["market-lake-batch-results"]["classifications"]


# ==============================================================================
# US-27: Criterio 3 - Delimitación de Responsabilidades Atlas vs OpenMetadata
# ==============================================================================
def test_bdd_us27_criterio_3_documento_delimitacion_fuentes_de_verdad():
    """
    Dado que ambos sistemas manejan metadatos en la arquitectura empresarial,
    Cuando se audita la documentación de gobernanza (ATLAS_VS_OPENMETADATA.md),
    Entonces queda formalmente delimitado que Atlas es la fuente de verdad de clasificaciones
    y glosario empresarial, mientras OpenMetadata lo es para catálogo operativo y calidad.
    """
    doc_path = METADATA_DIR / "ATLAS_VS_OPENMETADATA.md"
    assert doc_path.exists(), "El documento ATLAS_VS_OPENMETADATA.md no existe."

    content = doc_path.read_text(encoding="utf-8")

    # Validación de términos clave de la delimitación
    assert "Apache Atlas" in content
    assert "OpenMetadata" in content
    assert "Fuente de Verdad" in content
    assert "Glosario de Negocio" in content or "Business Glossary" in content
    assert "Calidad de Datos" in content or "Data Profiling" in content
    assert "Matriz de Responsabilidades" in content or "RACI" in content


# ==============================================================================
# Validación de la Herramienta de Importación y Resiliencia
# ==============================================================================
def test_import_atlas_governance_dry_run():
    """Valida que la herramienta de importación ejecute satisfactoriamente en modo dry-run."""
    exit_code = import_atlas_governance.main(["--dry-run", "--metadata-dir", str(METADATA_DIR)])
    assert exit_code == 0


def test_import_atlas_governance_rechaza_termino_faltante():
    """Valida que el validador falle si se elimina un término obligatorio de Criterio 1."""
    glossary = {
        "glossary": {"name": "Test"},
        "terms": [{"name": "Solo_Uno", "displayName": "Solo"}],
    }
    types = {"classificationDefs": [{"name": "Datos_Publicos_Mercado"}, {"name": "Metadatos_Sensibles"}, {"name": "Datos_Provisionales_Speed"}]}
    entities = {"entities": []}

    with pytest.raises(ValueError, match="Faltan términos obligatorios"):
        import_atlas_governance.validate_governance_definitions(glossary, types, entities)


def test_import_atlas_governance_rechaza_clasificacion_faltante():
    """Valida que el validador falle si se elimina una clasificación obligatoria de Criterio 2."""
    glossary = {
        "glossary": {"name": "Test"},
        "terms": [
            {"name": "Vela_Japonesa_OHLCV"},
            {"name": "Precio_de_Cierre"},
            {"name": "Volumen_Ponderado"},
            {"name": "Event_Time"},
            {"name": "Retorno_Porcentual"},
            {"name": "Watermark"},
        ],
    }
    types = {"classificationDefs": []}
    entities = {"entities": []}

    with pytest.raises(ValueError, match="Faltan clasificaciones normativas"):
        import_atlas_governance.validate_governance_definitions(glossary, types, entities)
