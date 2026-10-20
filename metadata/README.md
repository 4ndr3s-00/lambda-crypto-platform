# Gobierno, Catálogo y Linaje (Transversal)

Este directorio contiene las especificaciones y contratos de integración para los componentes transversales de gobierno de datos:

1. **OpenMetadata**: Catálogo de datos operativo para descubrimiento, registro de propietarios (*owners*), documentación, data profiling y reglas de calidad (US-25).
2. **Apache Atlas**: Clasificación y taxonomía empresarial de activos financieros (US-27).
3. **OpenLineage**: Emisión de eventos de linaje de ejecución (Jobs Spark Batch, Streaming y DAGs de Airflow) (US-26).

> **Principio de Diseño:** Estos componentes son transversales a la solución y no alteran la semántica de la arquitectura Lambda (Batch, Speed, Serving). MinIO mantiene una organización funcional independiente.

---

## 🏛️ OpenMetadata: Catálogo, Profiling & Calidad (US-25)

**Responsable:** Samuel Roncancio (Data Architect & Storage Lead)

### 1. Estructura de Componentes
```text
metadata/openmetadata/
├── connectors/               # Conectores declarativos de escaneo
│   ├── minio_s3_connector.yaml     # Escaneo de prefijos canónicos en MinIO (S3)
│   ├── postgres_connector.yaml     # Escaneo de tablas indexadas en serving_db
│   └── kafka_connector.yaml        # Escaneo de topics troncales market.*
├── profiling/                # Configuración de métricas estadísticas (Criterio 2 BDD)
│   └── market_profiler.yaml        # Min, max, mean, recuento de nulos, cardinalidad
├── quality_tests/            # Reglas de validación automatizada (Criterio 3 BDD)
│   └── market_quality_tests.yaml   # Not null, unicidad de event_id, price > 0
├── catalog/                  # Reporte JSON consolidado de catálogo
│   └── openmetadata_catalog_report.json
└── scripts/                  # Automatización de registro y evaluación
    └── register_and_profile.py
```

### 2. Despliegue de Infraestructura
OpenMetadata y OpenSearch están integrados bajo el perfil Docker `metadata`:
```bash
# Levantar OpenMetadata Server y OpenSearch
make up-metadata

# Ver logs de OpenMetadata
docker compose -f infra/docker-compose.yml --profile metadata logs -f openmetadata-server opensearch
```
* **OpenMetadata UI:** [http://localhost:8585](http://localhost:8585)
* **OpenSearch:** `http://localhost:9200`
* **Base de datos interna:** `openmetadata_db` en el clúster PostgreSQL de la plataforma.

### 3. Matriz de Datasets Catalogados y Propietarios (Criterio 1 BDD)

| Dataset / Entidad | Servicio | Frecuencia | SLA | Owner Asignado |
| :--- | :--- | :--- | :--- | :--- |
| `market-lake/historical/` | MinIO S3 | Micro-lotes / Diario | Frescura <= 5m | **Samuel Roncancio** |
| `market-lake/batch-results/` | MinIO S3 | Diario (00:00 UTC) | Cierre <= 30m | **Andrés Blanco** |
| `serving_db.serving.dim_assets` | PostgreSQL | Bajo demanda | Disp. 99.9% | **Samuel Roncancio** |
| `market.events` | Apache Kafka | Tiempo Real | Latencia < 1s | **Samuel Benavidez** |

### 4. Reglas de Calidad Automatizadas (Criterio 3 BDD)
1. **`columnValuesToBeNotNull (event_time)`**: Toda cotización o trade debe contar con marca temporal UTC.
2. **`columnValuesToBeNotNull (price)`**: Prohibidos precios nulos.
3. **`columnValuesToBeUnique (event_id)`**: Garantiza unicidad del evento canónico sin colisiones.
4. **`columnValuesToBeGreaterThan (price, 0)`**: Precios estrictamente positivos.
