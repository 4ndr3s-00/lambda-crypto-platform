# Plataforma de Datos Financieros y Criptomonedas (Arquitectura Lambda)

[![Arquitectura](https://img.shields.io/badge/Patrón-Lambda%20v2.0-blue.svg)](#decisión-arquitectónica)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2.20+-2496ED.svg?logo=docker)](#guía-rápida-de-despliegue-quickstart)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%20(KRaft)-231F20.svg?logo=apachekafka)](#matriz-de-servicios-y-puertos)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark)](#matriz-de-servicios-y-puertos)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow)](#matriz-de-servicios-y-puertos)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-C72C48.svg?logo=minio)](#matriz-de-servicios-y-puertos)
[![Licencia](https://img.shields.io/badge/Licencia-MIT-green.svg)](LICENSE)

Plataforma distribuida y extensible para la ingesta, procesamiento, reconciliación y visualización de datos financieros y de criptomonedas a gran escala, implementando estrictamente el **patrón Lambda** sin capas Medallion.

---

## 1. Decisión Arquitectónica

Esta plataforma implementa **Arquitectura Lambda** clásica de tres capas fundamentales:

```
                               ┌────────────────────────────────┐
                               │  Fuentes de Datos (Mercado)    │
                               │  APIs REST · WebSockets · CSV  │
                               └───────────────┬────────────────┘
                                               │
                                               â–¼
                               ┌────────────────────────────────┐
                               │  Bus de Eventos (Apache Kafka) │
                               │  Topics · Particiones · DLQ    │
                               └───────┬────────────────┬───────┘
                                       │                │
                     eventos históricos│                │eventos recientes
                               / replay│                │
                                       â–¼                â–¼
┌──────────────────────────────────────────────┐  ┌──────────────────────────────────────────────┐
│                 BATCH LAYER                  │  │                 SPEED LAYER                  │
│ • Airflow orquesta extracciones y backfills  │  │ • Spark Structured Streaming (baja latencia) │
│ • Spark Batch recalcula históricos completos │  │ • Ventanas por event-time y watermarking     │
│ • MinIO conserva datos y resultados batch    │  │ • Checkpoints duraderos y desvío a DLQ       │
└──────────────────────┬───────────────────────┘  └──────────────────────┬───────────────────────┘
                       │                                                 │
                       │ resultados históricos                           │ métricas recientes
                       │ consolidados                                    │ (ventanas cortas)
                       └───────────────────────┬─────────────────────────┘
                                               │
                                               â–¼
                               ┌────────────────────────────────┐
                               │         SERVING LAYER          │
                               │  Reconciliación por Event-Time │
                               │  Deduplicación & Precedencia   │
                               └───────────────┬────────────────┘
                                               │
                                               â–¼
                               ┌────────────────────────────────┐
                               │    FastAPI + Dashboard HTML    │
                               │  + Asistente Local (Qwen 2.5)  │
                               └────────────────────────────────┘
```

### ¿Por qué NO capas Medallion?
La arquitectura Medallion (Bronze/Silver/Gold) es un patrón alternativo centrado en niveles progresivos de refinamiento en almacenamiento lakehouse. En este proyecto se optó por el **patrón Lambda puro**, caracterizado por:
1. **Separación operativa estricta**: Batch Layer (precisión, inmutabilidad, reprocesamiento) y Speed Layer (baja latencia, ventanas temporales recientes).
2. **Organización funcional en MinIO**: Almacenamiento organizado por propósito funcional (`historical/`, `batch-results/`, `reference/`, `checkpoints/`, `quarantine/`), no por niveles Bronze/Silver/Gold.
3. **Reconciliación en Serving**: La capa de servicio resuelve colisiones y solapamientos con base en marcas temporales de evento (*event-time*), no por hora de llegada.

> **¿Retomas el trabajo en otra máquina?** Consulta la
> [guía de continuidad](docs/CONTINUIDAD.md): explica qué se conserva, qué se
> reconstruye y los pasos exactos para dejar la plataforma operativa de nuevo.

---

## 2. Estructura del Repositorio

El proyecto sigue una estructura modular orientada a desacoplar infraestructura, procesamiento y consumo:

```text
lambda-crypto-platform/
├── infra/                           # Orquestación de infraestructura local
│   ├── docker-compose.yml           # Stack multicontenedor (Kafka, MinIO, Spark, Airflow, Postgres, API)
│   ├── minio/                       # Inicialización de buckets funcionales
│   │   └── init-buckets.sh
│   ├── kafka/                       # Inicialización de topics, particiones y retención
│   │   └── init-topics.sh
│   ├── postgres/                    # Esquema relacional para Airflow y Serving Layer
│   │   └── init.sql
│   └── airflow/                     # Imagen base para Airflow con dependencias PySpark y conectores
│       └── Dockerfile
├── ingestion/                       # Conectores y contratos de datos
│   ├── connectors/                  # Adaptadores REST, WebSocket y CSV
│   │   └── .gitkeep
│   └── contracts/                   # Esquema canónico de validación de eventos
│       └── market_event.schema.json
├── spark/                           # Lógica de procesamiento distribuido
│   ├── spark_session.py             # Sesión Spark con cliente S3A (Hadoop-AWS)
│   ├── batch_historical.py          # Spark Batch: normalización, dedup, OHLCV y retornos
│   └── serving_reconcile.py         # Reconciliación Lambda entre Speed y Batch
├── dags/                            # Orquestación de flujos batch con Apache Airflow
│   └── batch_recompute.py           # DAG de recálculo batch con Spark (US-15)
├── metadata/                        # Catálogo (OpenMetadata), Linaje (OpenLineage) y Gobierno (Atlas)
│   └── README.md
├── api/                             # Serving Layer (FastAPI)
│   ├── Dockerfile
│   ├── main.py                      # Punto de entrada de la API
│   └── requirements.txt
├── frontend/                        # Dashboard web interactivo
│   └── .gitkeep
├── tests/                           # Batería de pruebas
│   ├── unit/                        # Pruebas unitarias de transformaciones y contratos
│   ├── integration/                 # Pruebas de integración MinIO/Kafka/Postgres
│   ├── load/                        # Generador sintético de estrés (millones de eventos)
│   └── requirements.txt             # Dependencias de las pruebas
├── docs/                            # Documentación operativa
│   └── CONTINUIDAD.md               # Guía de traspaso entre máquinas
├── AGENTS.md                        # Contexto del proyecto para asistentes de IA
├── Makefile                         # Comandos rápidos de operación (make up, make down)
├── .env.example                     # Variables de entorno documentadas
├── .gitignore                       # Exclusiones de secretos, datos y temporales
└── README.md                        # Documentación maestra
```

---

## 3. Estrategia de Ramas (Git Branching Model)

Para asegurar un desarrollo ordenado y concurrente entre los colaboradores del equipo, el repositorio utiliza la siguiente convención de ramas:

| Rama | Propósito | Responsable / Capa |
| :--- | :--- | :--- |
| `main` | Rama principal de producción. Solo código validado, documentado y listo para despliegue. | Core / Prod |
| `develop` | Rama de integración continua donde convergen todos los módulos antes de release. | Integración |
| `feature/infra-docker-setup` | Configuración del entorno Docker, redes, volúmenes, scripts de inicio y variables `.env`. | Infraestructura |
| `feature/ingestion-connectors` | Desarrollo de conectores REST y WebSockets (Binance, CoinGecko, CSV) y validación de contrato JSON. | Ingesta & Contratos |
| `feature/speed-layer-spark-streaming` | Job Spark Structured Streaming, consumo de topics Kafka, ventanas de tiempo, DLQ y checkpoints. | Speed Layer |
| `feature/batch-layer-spark-airflow` | Jobs Spark Batch idempotentes, cálculo de agregaciones históricas y DAGs de orquestación en Airflow. | Batch Layer |
| `feature/serving-reconciliation-api` | Desarrollo de endpoints FastAPI, reglas de precedencia de datos (Batch vs Speed) y deduplicación. | Serving Layer |
| `feature/frontend-dashboard` | Interfaz gráfica HTML5/CSS/JS con series temporales (OHLCV), volumen, indicadores y panel de control. | Consumo / UI |
| `feature/governance-metadata-lineage` | Registro en catálogo OpenMetadata, taxonomía en Atlas y emisión de eventos OpenLineage. | Gobierno |
| `feature/qwen-llm-chat-rag` | Integración del modelo local Qwen2.5 (Ollama) con guardrails de solo lectura y RAG sobre metadatos. | LLM / Analítica |

---

## 4. Guía Rápida de Despliegue (`Quickstart`)

### Requisitos Previos
* **Docker Engine** v24+ y **Docker Compose** v2.20+
* **Git** 2.30+
* Mínimo **6 GB de RAM** libre (el stack está optimizado con Kafka KRaft sin ZooKeeper y límites de memoria en workers).

### 1. Clonar el repositorio
```bash
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
cd lambda-crypto-platform
```

### 2. Configurar variables de entorno
```bash
make env
# O manualmente: cp .env.example .env
```

### 3. Levantar la infraestructura base
Levanta los servicios esenciales (MinIO, Kafka en KRaft, PostgreSQL, Spark Master/Worker, Airflow y Serving API):
```bash
make up
# O manualmente: docker compose -f infra/docker-compose.yml up -d --build
```

### 4. Verificar el estado de los contenedores
```bash
make ps
```

### 5. (Opcional) Escalar el clúster Spark a varios workers
Por defecto la plataforma levanta un único worker. Para añadir capacidad al
clúster (útil para jobs batch pesados o en pruebas de carga) se puede escalar:
```bash
make scale-spark WORKERS=3
```
Cada worker adicional aporta `SPARK_WORKER_CORES` cores y `SPARK_WORKER_MEMORY`
de memoria. Las Web UI de los workers quedan disponibles en los puertos
`8081`, `8082`, `8083`... y el master muestra el total de cores del clúster en
[http://localhost:8080](http://localhost:8080).

Para volver a la configuración de un solo worker:
```bash
make scale-down
```

### 6. (Opcional) Levantar con LLM local Qwen2.5 (Ollama)
Si tu equipo cuenta con al menos 12 GB de RAM o GPU dedicada:
```bash
make up-all
```

### 7. Ejecutar el Spark Batch (capa Batch)

El job `spark/batch_historical.py` lee el histórico de MinIO vía S3A,
normaliza y deduplica los eventos, y calcula las velas OHLCV y los retornos
escribiéndolos de forma idempotente en `s3://market-lake/batch-results/`.

```bash
# Recálculo de un mes para dos símbolos
docker exec lambda-spark-master bash -c '
  cd /opt/bitnami/spark/jobs && spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py \
    --start-date 2026-09-01 --end-date 2026-09-30 \
    --symbols BTCUSDT,ETHUSDT \
    --intervals 1m,5m,1h,1d'
```

Opciones disponibles:

| Flag | Descripción |
| :--- | :--- |
| `--start-date` / `--end-date` | Rango a procesar (inclusive). Ambos son obligatorios. |
| `--symbols` | Lista de símbolos separados por coma. Si se omite, se procesan todos. |
| `--intervals` | Granularidades: `1m`, `5m`, `1h`, `1d` (por defecto, todas). |
| `--metrics` | Métricas a producir: `ohlcv`, `returns` (por defecto, ambas). |
| `--quarantine-invalid` | Envía los eventos que violan el contrato a la DLQ. |

El mismo recálculo puede dispararse desde Airflow con el DAG
`batch_recompute`, que por defecto procesa el día anterior a las 02:00 UTC.

### 8. Ejecutar las pruebas

Las pruebas de la capa Spark usan una SparkSession local, por lo que no
necesitan el clúster en marcha:

```bash
docker exec lambda-spark-master bash -c '
  pip install --no-cache-dir pytest==8.3.4 && \
  cd /opt/bitnami/spark/jobs && python -m pytest . -v'
```

---

## 5. Matriz de Servicios y Puertos

| Servicio | Puerto Interno | Puerto Host | URL de Acceso | Credenciales por Defecto |
| :--- | :--- | :--- | :--- | :--- |
| **Serving API / Dashboard** | 8000 | `8000` | [http://localhost:8000](http://localhost:8000) / [/docs](http://localhost:8000/docs) | N/A |
| **MinIO Console** | 9001 | `9001` | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `miniopassword123` |
| **MinIO S3 Endpoint** | 9000 | `9000` | `http://localhost:9000` | `minioadmin` / `miniopassword123` |
| **Apache Airflow Web** | 8080 | `8085` | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` |
| **Apache Spark Master** | 8080 | `8080` | [http://localhost:8080](http://localhost:8080) | N/A |
| **Apache Spark Worker** | 8081 | `8081` | [http://localhost:8081](http://localhost:8081) | N/A |
| **Kafka Broker (KRaft)** | 9092 | `29092` | `localhost:29092` | N/A |
| **PostgreSQL 16** | 5432 | `5432` | `localhost:5432` | `postgres` / `postgres_secure_pass` |
| **Ollama (Perfil llm)** | 11434 | `11434` | [http://localhost:11434](http://localhost:11434) | N/A |

---

## 6. Organización de Datos en MinIO

La arquitectura organiza los datos por función operativa en prefijos determinísticos:

```text
s3://market-lake/
├── historical/source={exchange}/date=YYYY-MM-DD/     # Respuestas y eventos históricos inmutables
├── batch-results/metric=ohlcv/symbol={symbol}/       # Agregaciones periódicas consolidadas (Parquet)
├── batch-results/metric=returns/symbol={symbol}/     # Retornos porcentuales y volatilidad batch
└── reference/symbols/                                # Metadatos de símbolos, pares y contratos

s3://market-checkpoints/
└── spark/query={query_name}/                         # Checkpoints duraderos de streaming para recuperación

s3://market-quarantine/
└── dead-letter/source={exchange}/date=YYYY-MM-DD/    # Eventos que violaron contrato o corruptos
```

---

## 7. Contrato Canónico de Eventos

Todo evento emitido por los conectores REST, WebSockets o CSV hacia Kafka (`market.events`) debe validar contra [`ingestion/contracts/market_event.schema.json`](ingestion/contracts/market_event.schema.json):

```json
{
  "event_id": "binance_BTCUSDT_1710500000000",
  "source": "binance_ws",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "event_time": "2026-10-19T12:00:00.000Z",
  "ingestion_time": "2026-10-19T12:00:00.045Z",
  "price": 68450.50,
  "quantity": 0.4520,
  "trade_id": "98421045",
  "interval": "trade",
  "schema_version": "2.0.0"
}
```

---

## 8. Guía para Contribuir

1. **Crear una rama desde `develop`** con el prefijo correspondiente (`feature/`, `fix/`, `docs/`):
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/nombre-de-la-tarea
   ```
2. **Formatear commits bajo el estándar [Conventional Commits](https://www.conventionalcommits.org/):**
   * `feat: descripción de nueva funcionalidad`
   * `fix: corrección de error`
   * `docs: actualización de documentación`
   * `chore: configuración o mantenimiento`
3. **Validar antes de abrir Pull Request:**
   * La infraestructura debe levantar limpia con `make up`.
   * Los schemas JSON deben ser válidos.
4. **Abrir Pull Request hacia `develop`** para revisión técnica por pares.

---

## 9. Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo [LICENSE](LICENSE) para más información.

---

## 10. IA local Qwen2.5, RAG y guardrails (US-28 / US-29 / US-30)

La capa de IA vive en `api/` y tiene dos modos:

1. **Con Ollama** (`make up-all`): Qwen2.5 con function calling sobre herramientas de solo lectura.
2. **Sin Ollama**: fallback determinista con las mismas herramientas, para que el dashboard y el chat funcionen sin GPU.

| Archivo | Responsabilidad |
| :--- | :--- |
| `api/rag_indexer.py` | Indexa README, `metadata/docs/`, `metadata/catalog.json` y `metadata/glossary.json`. ChromaDB opcional; TF-IDF local si no está instalado. |
| `api/llm_service.py` | Prompt + contexto RAG + tool calls de Ollama + fallback. |
| `api/sql_guardrails.py` | SELECT-only, allowlist, `LIMIT <= 100`, timeout 5s, bloqueo de DDL/EXEC/funciones peligrosas. |
| `api/tools.py` | Solo `get_catalog_metadata`, `query_metrics`, `query_timeseries`. Sin shell ni SQL libre. |

```bash
make rag-index                 # reconstruye metadata/rag_index.json
make up-all                    # Ollama + ollama-init (pull del modelo)
pip install -r api/requirements-llm.txt   # ChromaDB opcional
make test                      # 48 tests
```

## 11. Dashboard V7 · IQ Analyzer & Crypto Streaming Suite

El frontend es una app **Next.js** (`frontend/`) que se exporta estático y FastAPI la sirve en `http://localhost:8000/` (Docker). También se puede desarrollar con `npm run dev` en `http://localhost:3000`.

Incluye:

- **View 1:** consola AI con partículas, input flotante, streaming de respuesta, SQL inspector y artefactos.
- **View 2:** card deck multi-activo (`BTC`, `ETH`, `SOL`, `BNB`) con precio streaming, 24h, sparkline, funding y OBI.
- **Deep dive** al hacer click en una card: velas OHLCV + volumen, Order Book L2 con gauge OBI, execution tape con filtro de ballenas, matriz de arbitraje cross-exchange, liquidation radar y on-chain/macro (netflow, OI, long/short, MVRV).
- Telemetría global: latencia WS, throughput, funding countdown, gas y Fear & Greed.
- Chat con `/api/chat` / SSE, fuentes, fecha de corte y disclaimer.

### Desarrollo local

```bash
# API en Docker (o uvicorn en :8000)
docker compose -f infra/docker-compose.yml -p infra --env-file .env --profile llm up -d

# Frontend Next.js en :3000
cd frontend
npm ci
npm run dev
```

El frontend detecta automáticamente `http://localhost:8000` cuando corre en `:3000`. Para otro host:

```bash
NEXT_PUBLIC_API_BASE_URL=http://mi-api:8000 npm run dev
```

### Docker (flujo real)

`api/Dockerfile` hace un build multi-stage: compila Next.js (`output: "export"`) y copia `out/` a `/app/static`. La API sirve el dashboard en `:8000`.

```bash
docker compose -f infra/docker-compose.yml -p infra --env-file .env build api
docker compose -f infra/docker-compose.yml -p infra --env-file .env up -d api
```

Los endpoints originales de US-22 (`/api/health`, `/api/assets`, `/api/ohlcv`, `/api/metrics`, `/api/pipeline/status`) se mantienen intactos para no romper su contrato ni sus tests.

## 12. Troubleshooting en Windows

### Scripts `.sh` con CRLF
Los scripts de `infra/` corren dentro de contenedores Linux. Si un checkout en Windows los convierte a CRLF, `minio-init` y `kafka-init` fallan con `set: pipefail: invalid option name` o `$'\r': command not found`.

Este repo incluye `.gitattributes` con `*.sh text eol=lf`. Si clonaste antes del fix:

```bash
git add --renormalize .
git checkout -- .
# o, si tenés dos2unix:
dos2unix infra/kafka/*.sh infra/minio/*.sh
```

### Puerto 8081 ocupado
Docker Desktop en Windows puede estar usando `8081`. Configurá en `.env`:

```env
SPARK_WORKER_WEBUI_PORT=8082
```

y recreá el worker:

```bash
docker compose -f infra/docker-compose.yml -p infra --env-file .env up -d --force-recreate spark-worker
```

### "Mi frontend se ve distinto"
Verificá primero que estén en el mismo commit y en el mismo repo:

```bash
git log -1 --oneline
git remote -v
git branch --show-current
```

El dashboard de este repo es `frontend/` (HTML/CSS/JS estático) y se sirve en `http://localhost:8000/`. No es el proyecto Next.js `iq-analyzer`: si uno corre ese otro frontend, se ve completamente distinto. Antes del merge del PR #45, `frontend/` estaba vacío en `develop`; un checkout viejo no mostraba el dashboard. También conviene abrir `/` (no `/static/index.html`) y hacer un hard reload para descartar caché.
