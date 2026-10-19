#!/bin/bash
# Script de inicialización de topics de Apache Kafka para la plataforma Lambda
set -e

BOOTSTRAP_SERVER=${KAFKA_BROKER_LIST:-kafka:9092}

echo "[Kafka Init] Esperando a que el broker Kafka esté disponible en $BOOTSTRAP_SERVER..."
while ! /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list > /dev/null 2>&1; do
    echo "[Kafka Init] Broker aún no disponible, reintentando en 3 segundos..."
    sleep 3
done

echo "[Kafka Init] Creando topics de eventos, streaming y DLQ..."

# 1. Topic de eventos crudos de mercado (ticks, trades, libro)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic market.events \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

# 2. Topic de agregaciones de velas OHLCV en tiempo real (Speed Layer)
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic market.ohlcv \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=259200000

# 3. Topic de métricas calculadas en streaming
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic market.metrics \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=86400000

# 4. Dead Letter Queue (DLQ) para eventos inválidos o fuera de contrato
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic market.dlq \
    --partitions 2 \
    --replication-factor 1 \
    --config retention.ms=1209600000

echo "[Kafka Init] Topics de Kafka creados y verificados exitosamente:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list
