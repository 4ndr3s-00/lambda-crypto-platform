"""
Integration Tests for Kafka Topics Provisioning (US-06).
Valida la configuración declarada en infra/kafka/init-topics.sh:
- Criterio 1 (BDD): Creación de los topics troncales.
- Criterio 2 (BDD): Estrategia de clave compuesta exchange|symbol documentada.
- Criterio 3 (BDD): Retención por tiempo y tamaño, y monitoreo de consumer lag.
"""

import os
import re
import pytest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INIT_TOPICS_SCRIPT = os.path.join(REPO_ROOT, "infra", "kafka", "init-topics.sh")
LAG_SCRIPT = os.path.join(REPO_ROOT, "infra", "kafka", "check-consumer-lag.sh")
KAFKA_README = os.path.join(REPO_ROOT, "infra", "kafka", "README.md")

CANONICAL_TOPICS = ["market.events", "market.ohlcv", "market.metrics", "market.dlq"]
MIN_PARTITIONS = 3


@pytest.fixture(scope="module")
def init_topics_script():
    with open(INIT_TOPICS_SCRIPT, "r", encoding="utf-8") as f:
        return f.read()


def _extract_topic_blocks(script: str) -> dict:
    """Divide el script en bloques, uno por cada comando kafka-topics.sh --create."""
    blocks = {}
    command_starts = [m.start() for m in re.finditer(r"/opt/kafka/bin/kafka-topics\.sh", script)]
    for i, start in enumerate(command_starts):
        end = command_starts[i + 1] if i + 1 < len(command_starts) else len(script)
        block = script[start:end]
        topic_match = re.search(r"--topic\s+(market\.\w+)", block)
        if topic_match:
            blocks[topic_match.group(1)] = block
    return blocks


# ==============================================================================
# Criterio 1: Creación de topics troncales
# ==============================================================================
def test_bdd_criterio_1_canonical_topics_declared(init_topics_script):
    """
    Dado que Kafka está operativo,
    Cuando se ejecuta el script de aprovisionamiento de topics,
    Entonces deben crearse: market.events, market.ohlcv, market.metrics y market.dlq.
    """
    blocks = _extract_topic_blocks(init_topics_script)
    for topic in CANONICAL_TOPICS:
        assert topic in blocks, f"Falta la declaración del topic: {topic}"
        assert "--create" in blocks[topic]
        assert "--if-not-exists" in blocks[topic]


# ==============================================================================
# Criterio 2: Estrategia de claves y particionado
# ==============================================================================
def test_bdd_criterio_2_minimum_partitions(init_topics_script):
    """
    Dado que se requiere paralelismo balanceado,
    Cuando se configuran los topics,
    Entonces cada topic debe tener al menos 3 particiones.
    """
    blocks = _extract_topic_blocks(init_topics_script)
    for topic, block in blocks.items():
        match = re.search(r"--partitions\s+(\d+)", block)
        assert match, f"El topic {topic} no declara --partitions"
        partitions = int(match.group(1))
        assert partitions >= MIN_PARTITIONS, (
            f"El topic {topic} tiene {partitions} particiones, se requieren al menos {MIN_PARTITIONS}"
        )


def test_bdd_criterio_2_partition_key_strategy_documented():
    """
    Dado que se requiere procesar eventos preservando orden temporal por activo,
    Cuando los productores publican en market.events,
    Entonces debe estar documentada la clave compuesta exchange|symbol.
    """
    with open(KAFKA_README, "r", encoding="utf-8") as f:
        readme = f.read()
    assert "{exchange}|{symbol}" in readme
    assert "binance|BTCUSDT" in readme


# ==============================================================================
# Criterio 3: Retención y métricas de consumer lag
# ==============================================================================
def test_bdd_criterio_3_retention_time_and_size(init_topics_script):
    """
    Dado que los eventos crudos tienen una vida útil operativa,
    Cuando se configuran los topics,
    Entonces la retención debe fijarse por tiempo (retention.ms) y por tamaño (retention.bytes).
    """
    blocks = _extract_topic_blocks(init_topics_script)
    for topic, block in blocks.items():
        assert re.search(r"retention\.ms=\d+", block), f"{topic} no define retention.ms"
        assert re.search(r"retention\.bytes=\d+", block), f"{topic} no define retention.bytes"


def test_bdd_criterio_3_consumer_lag_script_exists():
    """
    Dado que Spark Streaming consumirá los topics,
    Cuando se requiere monitorear su avance,
    Entonces debe existir un script para inspeccionar el consumer lag.
    """
    assert os.path.isfile(LAG_SCRIPT), "Falta infra/kafka/check-consumer-lag.sh"
    with open(LAG_SCRIPT, "r", encoding="utf-8") as f:
        content = f.read()
    assert "kafka-consumer-groups.sh" in content
    assert "--describe" in content
