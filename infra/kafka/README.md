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

## Formato de los mensajes de `market.dlq`

Ningún evento rechazado se descarta en silencio: los conectores de ingesta
(WebSocket, REST, CSV) y Spark Streaming lo publican en `market.dlq` con este
sobre JSON, que conserva el payload original intacto:

```json
{
  "original_event":   "<payload tal como llegó: objeto JSON o texto crudo>",
  "error_reason":     "por qué se rechazó",
  "failed_component": "market_websocket | market_rest | csv_loader | spark_streaming",
  "timestamp_utc":    "2026-10-20T15:00:00.000Z",
  "source":           "binance_ws | binance_rest | csv_backfill | ..."
}
```

La clave del mensaje es `exchange|symbol` cuando el payload la tiene, y `unknown` si no.
El formato lo construye `ingestion/contracts/dlq_message.py`. Spark conserva además los
campos anteriores (`error`, `dlq_timestamp`) por compatibilidad.

### Inspeccionar la DLQ

```bash
python -m ingestion.dlq_inspect              # conteo por componente, fuente y motivo + últimos 5
python -m ingestion.dlq_inspect --last 20    # más mensajes recientes
```

Cuando se carga un archivo con `--target minio` (sin Kafka), los rechazos se guardan en
`s3://market-quarantine/source=<fuente>/date=YYYY-MM-DD/<archivo>.rejected.json`.
