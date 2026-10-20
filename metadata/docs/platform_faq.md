# IQ Analyzer · Preguntas frecuentes de la plataforma

## ¿Qué calcula la capa Batch?
La capa Batch recalcula velas OHLCV y métricas históricas completas sobre Parquet en MinIO usando Spark Batch, orquestado por Airflow. Es idempotente y puede reprocesar correcciones de la fuente sin duplicar filas.

## ¿Qué calcula la capa Speed?
La capa Speed consume `market.events` desde Kafka con Spark Structured Streaming, aplica ventanas por event-time, watermark de 5 segundos y deduplicación. Publica métricas recientes para el Serving Layer y deriva eventos inválidos a la DLQ.

## ¿Cómo se calcula el retorno?
El retorno porcentual de una ventana es `(close_final - open_inicial) / open_inicial * 100`. Se calcula sobre velas reconciliadas por event-time. El VWAP se calcula como `sum(precio_típico * volumen) / sum(volumen)`.

## ¿Qué es la frescura de datos?
Es la diferencia en segundos entre el timestamp del evento más reciente (`event_time`) y el reloj UTC actual. El dashboard muestra la frescura y activa una alerta cuando el lag supera 60 segundos.

## ¿Qué servicios monitorea el panel de salud?
Kafka (ingesta y consumer lag), Spark Structured Streaming (ventana y watermark), Airflow (último DAG batch_recompute) y MinIO (sincronización de Parquet). La DLQ se muestra como conteo de rechazos.

## ¿Qué es el catálogo de datos?
OpenMetadata y Apache Atlas documentan owners, esquemas, retención, particiones y linaje de los datasets `market_ohlcv` y `market_metrics`. El linaje completo va de Binance WS a Kafka, Spark, MinIO y la Serving API.

## ¿Qué guardrails protege el asistente?
El asistente solo puede invocar `get_catalog_metadata`, `query_metrics` y `query_timeseries`. El validador SQL permite únicamente SELECT, tablas de una allowlist, `LIMIT <= 100`, timeout de 5 segundos y bloquea DROP, DELETE, UPDATE, ALTER, EXEC y funciones peligrosas.

## ¿Cómo se evita que el modelo invente cifras?
El prompt del sistema prohíbe inventar números. Toda cifra de mercado debe provenir de una herramienta tipada. Si no hay herramienta ni dato disponible, el asistente debe decirlo y no estimar.

## ¿Qué modelo local se usa?
Qwen2.5 Instruct servido por Ollama en `http://ollama:11434`. El modelo por defecto es `qwen2.5:3b-instruct` y puede cambiarse con `OLLAMA_MODEL`. Si Ollama no está disponible, la API responde con un fallback determinista basado en herramientas.
