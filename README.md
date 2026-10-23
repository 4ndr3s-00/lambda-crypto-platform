# Lambda Crypto Platform

[![Arquitectura](https://img.shields.io/badge/Patrón-Lambda-blue.svg)](#arquitectura)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2.20+-2496ED.svg?logo=docker)](#puesta-en-marcha)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%20(KRaft)-231F20.svg?logo=apachekafka)](#servicios-y-puertos)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark)](#servicios-y-puertos)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow)](#servicios-y-puertos)
[![Next.js](https://img.shields.io/badge/Next.js-15-black.svg?logo=next.js)](#frontend)
[![Licencia](https://img.shields.io/badge/Licencia-MIT-green.svg)](LICENSE)

Plataforma de datos financieros y de criptomonedas que implementa, de punta a
punta, el **patrón Lambda** (Batch + Speed + Serving): ingiere eventos de
mercado en tiempo real, los reprocesa en lote para obtener resultados
históricos exactos, reconcilia ambas rutas sin doble conteo, y los expone a
través de una API y un dashboard web con un asistente conversacional.

No usa capas Medallion (Bronze/Silver/Gold): el almacenamiento se organiza por
función (`historical/`, `batch-results/`, `reference/`, `checkpoints/`,
`quarantine/`), y la precedencia Batch-vs-Speed se resuelve en la Serving
Layer por *event-time*, no por orden de llegada.

---

## Tabla de contenido

1. [Arquitectura](#arquitectura)
2. [Flujo de datos](#flujo-de-datos)
3. [Stack tecnológico](#stack-tecnológico)
4. [Estructura del repositorio](#estructura-del-repositorio)
5. [Puesta en marcha](#puesta-en-marcha)
6. [Servicios y puertos](#servicios-y-puertos)
7. [Cómo probar que todo funciona](#cómo-probar-que-todo-funciona)
8. [Contrato de datos](#contrato-de-datos)
9. [Organización de datos en MinIO](#organización-de-datos-en-minio)
10. [API](#api)
11. [Asistente de IA (chat)](#asistente-de-ia-chat)
12. [Frontend](#frontend)
13. [Gobierno y catálogo de datos](#gobierno-y-catálogo-de-datos)
14. [Convenciones y cómo contribuir](#convenciones-y-cómo-contribuir)
15. [Solución de problemas](#solución-de-problemas)
16. [Licencia](#licencia)

---

## Arquitectura

```
                      ┌──────────────────────────────┐
                      │   Fuentes de mercado real     │
                      │   REST · WebSocket · CSV      │
                      └───────────────┬────────────────┘
                                      │
                                      ▼
                      ┌──────────────────────────────┐
                      │  Apache Kafka (bus de eventos) │
                      │  topics · particiones · DLQ    │
                      └───────┬──────────────────┬─────┘
                  eventos     │                  │ eventos
                  históricos  │                  │ recientes
                              ▼                  ▼
          ┌───────────────────────────┐  ┌───────────────────────────┐
          │       BATCH LAYER         │  │       SPEED LAYER         │
          │ Airflow orquesta jobs     │  │ Spark Structured Streaming│
          │ Spark recalcula histórico │  │ ventanas + watermark      │
          │ MinIO guarda resultados   │  │ checkpoints + DLQ         │
          └─────────────┬─────────────┘  └─────────────┬─────────────┘
                        │ velas consolidadas            │ snapshot reciente
                        └───────────────┬────────────────┘
                                        ▼
                      ┌──────────────────────────────┐
                      │        SERVING LAYER          │
                      │  Reconciliación por event-time│
                      │  (sin doble conteo)            │
                      └───────────────┬────────────────┘
                                      ▼
                      ┌──────────────────────────────┐
                      │   FastAPI + Dashboard Next.js  │
                      │   + asistente conversacional   │
                      └──────────────────────────────┘
```

**Por qué Lambda y no Medallion:** Medallion organiza el dato por niveles
progresivos de refinamiento (Bronze → Silver → Gold) dentro de un único lago.
Lambda separa las cosas por **propósito operativo**: una ruta optimizada para
exactitud y reprocesamiento (Batch) y otra para baja latencia (Speed), que
convergen en un único punto de reconciliación (Serving). Esa separación es la
que permite recalcular todo el histórico sin tocar la ruta en tiempo real, y
viceversa.

## Flujo de datos

1. **Ingesta** (`ingestion/connectors/`): conectores REST, WebSocket y CSV
   normalizan cada evento al [contrato canónico](#contrato-de-datos) y lo
   publican en Kafka (`market.events`). Lo que no cumple el contrato va a una
   Dead-Letter Queue, nunca se descarta en silencio.
2. **Speed Layer** (`spark/streaming_market.py`): un job de Spark Structured
   Streaming consume Kafka, deduplica con estado, calcula métricas en
   ventanas deslizantes y escribe el último valor por símbolo en
   `serving.realtime_metrics` (Postgres).
3. **Batch Layer** (`spark/batch_historical.py`, orquestado por
   `dags/batch_recompute.py`): lee el histórico crudo de MinIO, deduplica,
   calcula velas OHLCV y retornos, y los escribe de forma idempotente en
   `s3://market-lake/batch-results/`.
4. **Serving Layer** (`spark/serving_reconcile.py`): combina ambas rutas — lo
   anterior al corte de consolidación del Batch es definitivo, lo posterior
   viene de Speed y se marca como provisional — y escribe el resultado único
   en `serving.ohlcv_reconciled`.
5. **Consumo**: la API (`api/`) lee esa tabla reconciliada (nunca inventa un
   precio si no hay dato real) y la expone al dashboard y al asistente de IA.

## Stack tecnológico

| Capa | Tecnología | Rol |
| :--- | :--- | :--- |
| Ingesta de eventos | Apache Kafka 3.7 (modo KRaft, sin ZooKeeper) | Bus de eventos, particionado y Dead-Letter Queue |
| Procesamiento distribuido | Apache Spark 3.5 (Batch + Structured Streaming) | Cálculo de OHLCV, retornos, deduplicación y reconciliación |
| Orquestación | Apache Airflow 2.8 | Programación de recálculos batch, compactación y replay de la DLQ |
| Almacenamiento de objetos | MinIO (S3-compatible) | Data lake: históricos, resultados batch, checkpoints, cuarentena |
| Base de datos relacional | PostgreSQL 16 | Metastore de Airflow, métricas en tiempo real, catálogo de activos |
| API / Serving | FastAPI (Python 3.11) | Reconciliación expuesta por HTTP, guardrails SQL, orquestación del chat |
| Frontend | Next.js 15 + React 19 + TypeScript + Tailwind | Dashboard con velas OHLCV, order book y consola de chat |
| IA conversacional | Groq (LLM remoto) + Ollama (`qwen2.5`, `bge-m3` local) | Router semántico local + RAG + herramientas de solo lectura |
| Gobierno de datos | OpenMetadata, OpenSearch, Apache Atlas (perfiles opcionales) | Catálogo, calidad de datos, linaje y taxonomía |
| Infraestructura | Docker Compose | Todo el stack se levanta y reconstruye con un solo comando |

## Estructura del repositorio

```text
lambda-crypto-platform/
├── ingestion/              # Conectores (REST, WebSocket, CSV) y contrato de eventos
│   ├── connectors/
│   └── contracts/          # market_event.schema.json + validador
├── spark/                  # Jobs de Spark Batch y Speed
│   ├── spark_session.py    # Sesión Spark compartida (cliente S3A)
│   ├── batch_historical.py # Batch: dedup, OHLCV, retornos
│   ├── streaming_market.py # Speed: Structured Streaming
│   └── serving_reconcile.py# Reconciliación Batch ↔ Speed
├── dags/                   # DAGs de Airflow (recálculo, compactación, replay DLQ)
├── api/                    # Serving Layer: FastAPI, guardrails SQL, orquestación del chat
├── frontend/               # Dashboard Next.js (se exporta estático y la API lo sirve)
├── infra/                  # docker-compose y scripts de inicialización por servicio
│   ├── minio/ kafka/ postgres/ airflow/ ingestion/
├── metadata/                # Catálogo, glosario, linaje (OpenLineage) y gobierno (Atlas)
├── tests/                  # unit/ integration/ load/ resilience/
├── documentacion/           # Guías operativas y de continuidad entre máquinas
├── AGENTS.md                # Contexto del proyecto para asistentes de IA
├── Makefile                 # make up / make test / make scale-spark ...
└── .env.example              # Variables de entorno documentadas
```

## Puesta en marcha

### Requisitos

- Docker Engine 24+ y Docker Compose v2.20+
- Git 2.30+
- 6 GB de RAM libres como mínimo (más si usas los perfiles opcionales de IA
  local o gobierno de datos)

### Pasos

```bash
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
cd lambda-crypto-platform

make env     # copia .env.example -> .env
make up      # levanta MinIO, Kafka, Postgres, Spark, Airflow y la API
make ps      # verifica que todo esté healthy
```

`make up` es idempotente: los contenedores de inicialización recrean buckets,
políticas IAM, topics y el esquema de base de datos cada vez. No hay estado
que sincronizar a mano entre máquinas.

### Perfiles opcionales

| Comando | Qué levanta | Cuándo usarlo |
| :--- | :--- | :--- |
| `make up-all` | Todo lo anterior + Ollama (Qwen2.5 local) | Si tienes ≥12 GB de RAM o GPU y quieres el asistente sin depender de Groq |
| `make up-metadata` | OpenMetadata + OpenSearch | Si vas a trabajar en catálogo/gobierno de datos |
| `make scale-spark WORKERS=3` | Escala el clúster Spark a N workers | Jobs batch pesados o pruebas de carga |

## Servicios y puertos

| Servicio | Puerto host | URL | Credenciales por defecto |
| :--- | :--- | :--- | :--- |
| Serving API / Dashboard | `8000` | http://localhost:8000 (`/docs` para OpenAPI) | — |
| MinIO Console | `9001` | http://localhost:9001 | `minioadmin` / `miniopassword123` |
| MinIO S3 | `9000` | http://localhost:9000 | — |
| Apache Airflow | `8085` | http://localhost:8085 | `admin` / `admin` |
| Spark Master UI | `8080` | http://localhost:8080 | — |
| Spark Worker UI | `8081`, `8082`, ... | http://localhost:8081 | — |
| Kafka (externo) | `29092` | `localhost:29092` | — |
| PostgreSQL 16 | `5432` | `localhost:5432` | `postgres` / `postgres_secure_pass` |
| Ollama (perfil `llm`) | `11434` | http://localhost:11434 | — |
| OpenMetadata (perfil `metadata`) | `8585` | http://localhost:8585 | — |

Todos los valores son configurables en `.env` (ver `.env.example`).

## Cómo probar que todo funciona

```bash
# Suite completa de tests (260+), contra Kafka/Postgres/MinIO reales
docker exec lambda-spark-master bash -c '
  pip install --no-cache-dir -r /opt/bitnami/spark/repo/tests/requirements.txt &&
  cd /opt/bitnami/spark/repo &&
  PYTHONPATH=/opt/bitnami/spark/repo:/opt/bitnami/spark/python python -m pytest tests -v'

# Health check de la API
curl http://localhost:8000/api/health

# Métricas de mercado en vivo
curl http://localhost:8000/api/market/overview
```

Más comandos de verificación (ejecución real de los jobs de Spark contra
MinIO, reconciliación Batch/Speed) en [`AGENTS.md §4`](AGENTS.md).

## Contrato de datos

Todo evento que entra al sistema —por REST, WebSocket o CSV— valida contra
[`ingestion/contracts/market_event.schema.json`](ingestion/contracts/market_event.schema.json)
antes de publicarse en Kafka. Lo que no cumple el contrato se aísla en la
Dead-Letter Queue, nunca se descarta silenciosamente ni se deja pasar.

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
  "schema_version": "2.0.0"
}
```

## Organización de datos en MinIO

```text
s3://market-lake/
├── historical/source={exchange}/date=YYYY-MM-DD/   # eventos crudos, inmutables
├── batch-results/metric=ohlcv/symbol={symbol}/      # velas consolidadas (Parquet)
├── batch-results/metric=returns/symbol={symbol}/    # retornos batch
└── reference/symbols/                                # metadatos de símbolos y pares

s3://market-checkpoints/spark/query={query}/          # checkpoints duraderos de streaming
s3://market-quarantine/dead-letter/source={exchange}/date=YYYY-MM-DD/  # eventos rechazados
```

## API

La Serving API (FastAPI) expone, entre otros:

| Endpoint | Qué devuelve |
| :--- | :--- |
| `GET /api/health` | Estado de Postgres, MinIO y Kafka |
| `GET /api/assets`, `GET /api/ohlcv`, `GET /api/metrics` | Catálogo y series reconciliadas |
| `GET /api/market/overview` | Precios en vivo desde el exchange (sparkline incluido) |
| `GET /api/pipeline/status` 🔒 | Observabilidad del pipeline completo (requiere auth) |
| `GET /api/catalog/{dataset}` | Metadatos de gobierno del dataset |
| `POST /api/chat`, `GET /api/chat/stream` | Asistente conversacional (ver abajo) |

La documentación interactiva completa (OpenAPI/Swagger) está siempre
disponible en **`/docs`** con el stack levantado. Los endpoints marcados 🔒
requieren `Authorization: Bearer <ADMIN_API_KEY>` o cabecera `X-API-Key`
(ver `API_SECURITY_ENABLED`/`ADMIN_API_KEY` en `.env.example`).

## Asistente de IA (chat)

El chat combina tres piezas, cada una con un rol acotado:

1. **Router semántico local** (`api/semantic_router.py`, embeddings
   `bge-m3` vía Ollama): responde localmente saludos, despedidas y preguntas
   fuera de alcance, sin llamar al LLM externo.
2. **Groq** (`api/llm_service.py`): solo se activa para preguntas analíticas
   reales, con RAG sobre la documentación (`api/rag_indexer.py`) y
   herramientas de **solo lectura** (`api/tools.py`).
3. **Guardrails SQL** (`api/sql_guardrails.py`): todo SQL generado es
   `SELECT`-only, contra una allowlist de tablas, con `LIMIT` forzado y
   timeout — nunca ejecuta DDL ni funciones peligrosas.

El asistente nunca inventa un precio o una métrica: si el dato real no está
disponible, lo dice explícitamente en vez de fabricarlo.

## Frontend

Dashboard en **Next.js 15 / React 19 / TypeScript**, exportado como sitio
estático (`output: "export"`) y servido por la propia API en `:8000`.

```bash
# Desarrollo local del frontend (hot reload en :3000)
cd frontend
npm ci
npm run dev
```

El frontend en `:3000` detecta automáticamente la API en `localhost:8000`.
Para apuntar a otro host: `NEXT_PUBLIC_API_BASE_URL=http://mi-api:8000 npm run dev`.

Todo lo que muestra el dashboard es un dato real o está explícitamente
etiquetado como `SIMULADO` — nunca se presenta un número inventado como si
fuera en vivo.

## Gobierno y catálogo de datos

Con el perfil `metadata` (`make up-metadata`) se levanta OpenMetadata +
OpenSearch para catálogo, calidad de datos y profiling; el linaje de extremo
a extremo se emite vía OpenLineage (`metadata/openlineage/`) y la taxonomía
de clasificación vive en Apache Atlas. Detalles en
[`metadata/README.md`](metadata/README.md).

## Convenciones y cómo contribuir

- **Ramas**: desde `develop`, con prefijo `feature/`, `fix/`, `docs/` o
  `chore/`. Pull Request hacia `develop`, nunca commits directos.
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/)
  en español (`feat:`, `fix:`, `docs:`, `chore:`).
- **Antes de abrir el PR**: `make up` debe levantar limpio y
  `make test` (o la suite completa, ver arriba) debe pasar.
- **Issues**: se gestionan en GitHub con criterios de aceptación en BDD; la
  issue se cierra cuando se mergea su PR, no antes.

Contexto completo del proyecto (decisiones de diseño, errores ya resueltos,
estado actual) en [`AGENTS.md`](AGENTS.md).

## Solución de problemas

**Scripts `.sh` con CRLF en Windows:** los scripts de `infra/` corren en
contenedores Linux. Si un checkout en Windows los convierte a CRLF,
`minio-init`/`kafka-init` fallan. El repo ya fuerza `*.sh text eol=lf` en
`.gitattributes`; si clonaste antes de eso:

```bash
git add --renormalize .
git checkout -- .
```

**Puerto `8081` ocupado:** cambia `SPARK_WORKER_WEBUI_PORT` en `.env` y
recrea el worker con `docker compose ... up -d --force-recreate spark-worker`.

**El frontend no refleja tus cambios:** confirma que estás en el commit y la
rama correctos (`git log -1 --oneline`), y haz un hard reload — el dashboard
se sirve estático y el navegador puede tener una versión en caché.

## Licencia

MIT — ver [LICENSE](LICENSE).
