# Gobierno, Catálogo y Linaje (Transversal)

Este directorio contiene las especificaciones y contratos de integración para los componentes transversales de gobierno de datos:

1. **OpenMetadata**: Catálogo de datos operativo para descubrimiento, registro de propietarios (*owners*), documentación y reglas de calidad.
2. **Apache Atlas**: Clasificación y taxonomía empresarial de activos financieros.
3. **OpenLineage**: Emisión de eventos de linaje de ejecución (Jobs Spark Batch, Streaming y DAGs de Airflow).

> **Principio de Diseño:** Estos componentes son transversales a la solución y no alteran la semántica de la arquitectura Lambda (Batch, Speed, Serving). MinIO mantiene una organización funcional independiente.
