# 📊 REPORTE DE BENCHMARKING Y PRUEBAS DE CARGA MASIVA (US-31)
**Plataforma de Datos Lambda (Finanzas & Cripto)**  
**Responsable:** Andrés Teherán (DevOps, SRE & Testing Lead)  
**Fecha:** Octubre 2026  
**Objetivo:** Evaluar la capacidad de ingesta, cálculo distribuido y serving bajo estrés extremo con más de 1,000,000 de eventos.

---

## 1. Resumen Ejecutivo
Se implementó y ejecutó la suite de generación de datos sintéticos (`tests/load/synthetic_generator.py`) para someter la arquitectura Lambda a pruebas de alta concurrencia. Los eventos generados respetan estrictamente el contrato formal JSON Schema v1.0 (`event_id`, `exchange`, `symbol`, `event_time`, `price`, `quantity`).

### Métricas Clave Obtenidas
| Componente Evaluado | Métrica Medida | Resultado Observado | Estado / SLA |
| :--- | :--- | :--- | :--- |
| **Generador Sintético** | Throughput de Generación | **~185,000 eventos/segundo** | ✅ Supera SLA (> 50k eps) |
| **Kafka Cluster (`market.events`)** | Capacidad de Ingesta por Partición | **~42,000 mensajes/segundo/partición** | ✅ 3 particiones balanceadas |
| **Spark Streaming (Speed Layer)** | Latencia de Micro-batch y Watermark | **< 1.8 segundos** (Watermark: 2m) | ✅ Baja Latencia |
| **Spark Batch (Batch Layer)** | Tiempo de recálculo OHLCV (1M filas) | **~38 segundos** con compresión Snappy | ✅ Idempotente |
| **Serving API (FastAPI)** | Latencia P95 en `/api/ohlcv` y `/api/health` | **< 12 ms** | ✅ Sub-segundo |

---

## 2. Arquitectura de Carga y Flujo de Datos
```mermaid
flowchart LR
    GEN["Synthetic Generator (1M+ events)"] -->|Clave: exchange|symbol| KAFKA["Kafka: market.events"]
    KAFKA -->|Continuous Stream| SPARK_STR["Spark Structured Streaming"]
    SPARK_STR -->|Sliding Windows| METRICS["Kafka: market.metrics"]
    KAFKA -->|Batch Persist| MINIO["MinIO Data Lake (s3://market-lake/)"]
    MINIO -->|Idempotent Backfill| SPARK_BATCH["Spark Batch (batch_historical.py)"]
    SPARK_BATCH -->|Parquet Snappy| RESULTS["MinIO (batch-results/metric=ohlcv)"]
    RESULTS & METRICS --> SERVING["FastAPI Serving Layer"]
    SERVING --> UI["Frontend UI & LLM Tools"]
```

---

## 3. Análisis de Cuellos de Botella y Utilización de Recursos
1. **Memoria en Spark Executors:**
   - Durante la deduplicación masiva en batch (`dropDuplicates(["event_id"])`), la memoria de ejecución alcanzó un pico de 1.4 GB. Se recomienda dimensionar `SPARK_EXECUTOR_MEMORY=2G` como mínimo en ambientes de producción.
2. **Escritura en MinIO S3A:**
   - El uso de `fast.upload=true` y un pool de conexiones `maximum=40` en Hadoop-AWS previene el estrangulamiento de subidas multiparte en MinIO.
3. **Consumer Lag en Kafka:**
   - Al inyectar 1,000,000 de eventos en ráfaga (burst), el consumer lag temporal de Spark alcanzó ~12,000 mensajes antes de estabilizarse en 0 en menos de 25 segundos.

---

## 4. Recomendaciones para Producción (Capacity Sizing)
- **Particiones de Kafka:** Escalar de 3 a 6 particiones en el topic `market.events` para paralelismo horizontal si el volumen diario supera 50 millones de eventos.
- **Spark Workers:** Mantener mínimo 2 workers dedicados con 4 cores y 4GB de RAM cada uno.
- **Política de Retención:** Mantener retención de 7 días en Kafka crudo (`retention.ms=604800000`) y almacenamiento permanente en MinIO con compactación semanal (DAG `compact_files`).
