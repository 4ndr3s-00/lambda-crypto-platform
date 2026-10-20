#!/bin/bash
# Muestra el lag (retraso) de un grupo de consumidores de Kafka.
# Uso: ./check-consumer-lag.sh <nombre-del-grupo-consumidor>
set -e

BOOTSTRAP_SERVER=${KAFKA_BROKER_LIST:-kafka:9092}
GROUP=$1

if [ -z "$GROUP" ]; then
    echo "Uso: $0 <nombre-del-grupo-consumidor>"
    echo ""
    echo "Grupos de consumidores activos actualmente:"
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list
    exit 1
fi

echo "[Kafka Lag] Estado del grupo '$GROUP':"
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe --group "$GROUP"
