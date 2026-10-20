# Configuración de Kafka

## Topics

| Topic | Particiones | Retención | Propósito |
| :--- | :--- | :--- | :--- |
| `market.events` | 4 | 7 días / 5 GB | Eventos crudos de mercado (ticks, trades) |
| `market.ohlcv` | 4 | 3 días / 2 GB | Velas OHLCV calculadas en streaming |
| `market.metrics` | 3 | 1 día / 1 GB | Métricas agregadas en tiempo real |
| `market.dlq` | 3 | 14 días / 2 GB | Eventos inválidos o fuera de contrato |

## Estrategia de clave de partición

Todo productor que publique en `market.events` debe usar como **clave del mensaje**
el valor compuesto:

```
{exchange}|{symbol}
```

Ejemplo: `binance|BTCUSDT`

Kafka usa esta clave para decidir en qué partición cae cada evento. Al usar siempre
la misma clave para el mismo activo, todos sus eventos caen en la misma partición y
se procesan en el orden en que llegaron (orden por event-time), evitando que el
mismo símbolo se reparta entre carriles distintos y se desordene.

Esta regla aplica a los conectores REST/WebSocket/CSV (US-07, US-08) cuando publiquen
hacia Kafka.

## Monitoreo de consumer lag

Usar `check-consumer-lag.sh` para inspeccionar cuánto retraso tiene un grupo de
consumidores (por ejemplo, el job de Spark Streaming) respecto a los eventos
publicados:

```bash
docker exec lambda-kafka /check-consumer-lag.sh <nombre-del-grupo-consumidor>
```

Un lag alto o creciente indica que el consumidor no está procesando al mismo ritmo
en que llegan los eventos (recursos insuficientes, error en el procesamiento, etc.).
