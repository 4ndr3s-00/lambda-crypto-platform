# ESTADO ACTUAL DEL REPOSITORIO

## Rama actual: `develop`

Ubicación actual: `develop`

### Últimos commits mergeados:

| Commit | PR | Descripción |
|--------|-----|-------------|
| `de95260` | #41 | Merge pull request #41 from 4ndr3s-00/feature/governance-metadata-lineage |
| `7e6e065` | #40 | Merge pull request #40 from 4ndr3s-00/feature/speed-layer-spark-streaming |
| `1a6b039` | #39 | Merge pull request #39 from 4ndr3s-00/feature/kafka-topics-config |
| `0d0866e` | #38 fix | fix(kafka): subir particiones a minimo 3 y agregar limite de retencion por tamano |
| `b18a81c` | #27 | feat(governance): definir glosario empresarial, clasificaciones y delimitacion Atlas vs OpenMetadata |
| `49ad570` | #38 | feat(streaming): deduplicacion en tiempo real y metricas en ventanas deslizantes (US-19) |
| `6d31f10` | #38 | feat(streaming): implementar pipeline de Structured Streaming con event-time, watermarks y DLQ (US-18) |
| `164cb3c` | #37 | **Merge pull request #37 from 4ndr3s-00/feature/spark-batch-ohlcv** |
| `99c1c1a` | #37 | feat(spark): implementar capa Spark Batch y reconciliación Speed/Batch |

### Historias implementadas por Andrés Blanco:

| Historia | Commit | Descripción |
|----------|---------|-------------|
| **US-10** | `99c1c1a` | Spark Batch: lectura, normalización y deduplicación |
| **US-11** | `99c1c1a` | Spark Batch: OHLCV idempotente y retornos |
| **US-12** | `99c1c1a` | Spark Batch: backfill masivo y recálculo |
| **US-21** | `99c1c1a` / `6d31f10` | Serving: reconciliación Speed vs Batch |

## Estructura del repositorio actual:

```
/lambda-crypto-platform/
├── .env              # Configuración de entorno (credenciales S3, puertos, etc.)
├── .env.example      # Plantilla de .env
├── AGENTS.md         # Contexto del proyecto, decisiones, errores resueltos
├── SPARK_WORKER_GUIDE.md  # Guía para worker en otro computador
├── DESARROLLO_SPARK_BATCH.md  # Resumen del trabajo de Andrés Blanco
├── ESTADO_ACTUAL_REPOSITORIO.md  # Este archivo
├── infra/            # Configuración Docker Compose
│   ├── docker-compose.yml
│   └── docker-compose.scale.yml
├── spark/            # Código Spark Batch & Streaming
│   ├── batch_historical.py
│   ├── serving_reconcile.py
│   ├── spark_session.py
│   └── streaming_market.py
├── dags/             # Airflow DAGs
│   └── batch_recompute.py
├── tests/            # Pruebas unitarias
│   └── unit/
│       └── test_spark_batch.py (44 tests)
├── documentacion/    # Documentación
│   ├── CONTINUIDAD.md
│   └── ...
├── infra/minio/      # Configuración MinIO
│   ├── init-buckets.sh
│   └── policies/
├── Makefile          # Comandos de ayuda (make up, make ps, make scale-spark, etc.)
└── README.md         # Vista general del proyecto
```

## Comandos de ayuda principales:

| Comando | Qué hace |
|---------|----------|
| `make up` | Levanta toda la infraestructura (MinIO, Kafka, Postgres, Spark, Airflow, API) |
| `make ps` | Muestra estado de contenedores |
| `make scale-spark WORKERS=N` | Escala clúster Spark a N workers (override) |
| `make down` | Detiene y elimina todos los contenedores |
| `make restart` | Reinicia todos los servicios |

## Contenedores corriendo actualmente:

- `lambda-spark-master`: Master Spark (puertos 7077, 8080)
- `lambda-spark-worker`: Worker Spark (puerto 8081)
- `lambda-minio`: MinIO Data Lake (puertos 9000, 9001)
- `lambda-kafka`: Kafka Bus de Eventos (puerto 9092)
- `lambda-postgres`: PostgreSQL (puerto 5432)
- `lambda-airflow`: Airflow Web (puerto 8085)

## Buckets MinIO creados:

- `market-lake`: Datos crudos y resultados batch
- `market-checkpoints`: Checkpoints duraderos para streaming (US-20)
- `market-quarantine`: Dead Letter Queue para eventos erroróneos

## Para los compañeros que se incorporan:

1. **Repositorio**: Clonar y checkout a `develop` (el trabajo ya está mergeado)
2. **Entorno**: `cp .env.example .env` (o usar el .env existente)
3. **Levantar**: `make up`
4. **Verificar**: `make ps`
5. **Probar**: Ejecutar job Spark con credenciales S3 correctas
6. **Documentación**: Revisar `AGENTS.md` y `DESARROLLO_SPARK_BATCH.md` para contexto

## Rama `feature/spark-batch-ohlcv`:

- **Estado**: Ya fue mergeada a `develop`, historia completada
- **PR #37**: Integraba US-10, US-11, US-12, US-21
- **Posteriormente**: Se integraron PR #40 (streaming US-18/19) y #41 (governance metadata)

## Consideraciones importantes:

- **Las issues US-10, US-11, US-12, US-21**: Están abiertas a propósito en el repositorio. En este flujo de trabajo, las issues se cierran cuando su PR se mergea. El PR #37 ya fue mergeado, por lo que estas deberían estar cerradas o reabiertas con aviso.
- **El PR #37**: No se debe mergear nuevamente, ya está en `develop`.
- **Trabajo pendiente**: Integrar `wip/infra-airflow-spark-client` en `develop` para evitar error `No module named 'py4j'`.
- **Ruta Speed Layer (US-18/19/20)**: Implementada parcialmente, el hueco restante es la componente streaming completa vista desde el computador del usuario.