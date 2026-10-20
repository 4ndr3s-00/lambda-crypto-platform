# Plataforma de Datos Financieros y Criptomonedas (Arquitectura Lambda)

[![Arquitectura](https://img.shields.io/badge/PatrÃ³n-Lambda%20v2.0-blue.svg)](#decisiÃ³n-arquitectÃ³nica)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2.20+-2496ED.svg?logo=docker)](#guÃ­a-rÃ¡pida-de-despliegue-quickstart)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%20(KRaft)-231F20.svg?logo=apachekafka)](#matriz-de-servicios-y-puertos)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C.svg?logo=apachespark)](#matriz-de-servicios-y-puertos)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE.svg?logo=apacheairflow)](#matriz-de-servicios-y-puertos)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-C72C48.svg?logo=minio)](#matriz-de-servicios-y-puertos)
[![Licencia](https://img.shields.io/badge/Licencia-MIT-green.svg)](LICENSE)

Plataforma distribuida y extensible para la ingesta, procesamiento, reconciliaciÃ³n y visualizaciÃ³n de datos financieros y de criptomonedas a gran escala, implementando estrictamente el **patrÃ³n Lambda** sin capas Medallion.

---

## 1. DecisiÃ³n ArquitectÃ³nica

Esta plataforma implementa **Arquitectura Lambda** clÃ¡sica de tres capas fundamentales:

```
                               â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                               â”‚  Fuentes de Datos (Mercado)    â”‚
                               â”‚  APIs REST Â· WebSockets Â· CSV  â”‚
                               â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                               â”‚
                                               â–¼
                               â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                               â”‚  Bus de Eventos (Apache Kafka) â”‚
                               â”‚  Topics Â· Particiones Â· DLQ    â”‚
                               â””â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”˜
                                       â”‚                â”‚
                     eventos histÃ³ricosâ”‚                â”‚eventos recientes
                               / replayâ”‚                â”‚
                                       â–¼                â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚                 BATCH LAYER                  â”‚  â”‚                 SPEED LAYER                  â”‚
â”‚ â€¢ Airflow orquesta extracciones y backfills  â”‚  â”‚ â€¢ Spark Structured Streaming (baja latencia) â”‚
â”‚ â€¢ Spark Batch recalcula histÃ³ricos completos â”‚  â”‚ â€¢ Ventanas por event-time y watermarking     â”‚
â”‚ â€¢ MinIO conserva datos y resultados batch    â”‚  â”‚ â€¢ Checkpoints duraderos y desvÃ­o a DLQ       â”‚
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                       â”‚                                                 â”‚
                       â”‚ resultados histÃ³ricos                           â”‚ mÃ©tricas recientes
                       â”‚ consolidados                                    â”‚ (ventanas cortas)
                       â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                               â”‚
                                               â–¼
                               â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                               â”‚         SERVING LAYER          â”‚
                               â”‚  ReconciliaciÃ³n por Event-Time â”‚
                               â”‚  DeduplicaciÃ³n & Precedencia   â”‚
                               â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                               â”‚
                                               â–¼
                               â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                               â”‚    FastAPI + Dashboard HTML    â”‚
                               â”‚  + Asistente Local (Qwen 2.5)  â”‚
                               â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
```

### Â¿Por quÃ© NO capas Medallion?
La arquitectura Medallion (Bronze/Silver/Gold) es un patrÃ³n alternativo centrado en niveles progresivos de refinamiento en almacenamiento lakehouse. En este proyecto se optÃ³ por el **patrÃ³n Lambda puro**, caracterizado por:
1. **SeparaciÃ³n operativa estricta**: Batch Layer (precisiÃ³n, inmutabilidad, reprocesamiento) y Speed Layer (baja latencia, ventanas temporales recientes).
2. **OrganizaciÃ³n funcional en MinIO**: Almacenamiento organizado por propÃ³sito funcional (`historical/`, `batch-results/`, `reference/`, `checkpoints/`, `quarantine/`), no por niveles Bronze/Silver/Gold.
3. **ReconciliaciÃ³n en Serving**: La capa de servicio resuelve colisiones y solapamientos con base en marcas temporales de evento (*event-time*), no por hora de llegada.

---

## 2. Estructura del Repositorio

El proyecto sigue una estructura modular orientada a desacoplar infraestructura, procesamiento y consumo:

```text
lambda-crypto-platform/
â”œâ”€â”€ infra/                           # OrquestaciÃ³n de infraestructura local
â”‚   â”œâ”€â”€ docker-compose.yml           # Stack multicontenedor (Kafka, MinIO, Spark, Airflow, Postgres, API)
â”‚   â”œâ”€â”€ minio/                       # InicializaciÃ³n de buckets funcionales
â”‚   â”‚   â””â”€â”€ init-buckets.sh
â”‚   â”œâ”€â”€ kafka/                       # InicializaciÃ³n de topics, particiones y retenciÃ³n
â”‚   â”‚   â””â”€â”€ init-topics.sh
â”‚   â”œâ”€â”€ postgres/                    # Esquema relacional para Airflow y Serving Layer
â”‚   â”‚   â””â”€â”€ init.sql
â”‚   â””â”€â”€ airflow/                     # Imagen base para Airflow con dependencias PySpark y conectores
â”‚       â””â”€â”€ Dockerfile
â”œâ”€â”€ ingestion/                       # Conectores y contratos de datos
â”‚   â”œâ”€â”€ connectors/                  # Adaptadores REST, WebSocket y CSV
â”‚   â”‚   â””â”€â”€ .gitkeep
â”‚   â””â”€â”€ contracts/                   # Esquema canÃ³nico de validaciÃ³n de eventos
â”‚       â””â”€â”€ market_event.schema.json
â”œâ”€â”€ spark/                           # LÃ³gica de procesamiento distribuido
â”‚   â”œâ”€â”€ .gitkeep                     # Jobs de Spark Batch, Structured Streaming y ReconciliaciÃ³n
â”œâ”€â”€ dags/                            # OrquestaciÃ³n de flujos batch con Apache Airflow
â”‚   â””â”€â”€ .gitkeep                     # DAGs de ingesta, recÃ¡lculo, DLQ y calidad
â”œâ”€â”€ metadata/                        # CatÃ¡logo (OpenMetadata), Linaje (OpenLineage) y Gobierno (Atlas)
â”‚   â””â”€â”€ README.md
â”œâ”€â”€ api/                             # Serving Layer (FastAPI)
â”‚   â”œâ”€â”€ Dockerfile
â”‚   â”œâ”€â”€ main.py                      # Punto de entrada de la API
â”‚   â””â”€â”€ requirements.txt
â”œâ”€â”€ frontend/                        # Dashboard web interactivo
â”‚   â””â”€â”€ .gitkeep
â”œâ”€â”€ tests/                           # BaterÃ­a de pruebas
â”‚   â”œâ”€â”€ unit/                        # Pruebas unitarias de transformaciones y contratos
â”‚   â”œâ”€â”€ integration/                 # Pruebas de integraciÃ³n MinIO/Kafka/Postgres
â”‚   â””â”€â”€ load/                        # Generador sintÃ©tico de estrÃ©s (millones de eventos)
â”œâ”€â”€ Makefile                         # Comandos rÃ¡pidos de operaciÃ³n (make up, make down)
â”œâ”€â”€ .env.example                     # Variables de entorno documentadas
â”œâ”€â”€ .gitignore                       # Exclusiones de secretos, datos y temporales
â””â”€â”€ README.md                        # DocumentaciÃ³n maestra
```

---

## 3. Estrategia de Ramas (Git Branching Model)

Para asegurar un desarrollo ordenado y concurrente entre los colaboradores del equipo, el repositorio utiliza la siguiente convenciÃ³n de ramas:

| Rama | PropÃ³sito | Responsable / Capa |
| :--- | :--- | :--- |
| `main` | Rama principal de producciÃ³n. Solo cÃ³digo validado, documentado y listo para despliegue. | Core / Prod |
| `develop` | Rama de integraciÃ³n continua donde convergen todos los mÃ³dulos antes de release. | IntegraciÃ³n |
| `feature/infra-docker-setup` | ConfiguraciÃ³n del entorno Docker, redes, volÃºmenes, scripts de inicio y variables `.env`. | Infraestructura |
| `feature/ingestion-connectors` | Desarrollo de conectores REST y WebSockets (Binance, CoinGecko, CSV) y validaciÃ³n de contrato JSON. | Ingesta & Contratos |
| `feature/speed-layer-spark-streaming` | Job Spark Structured Streaming, consumo de topics Kafka, ventanas de tiempo, DLQ y checkpoints. | Speed Layer |
| `feature/batch-layer-spark-airflow` | Jobs Spark Batch idempotentes, cÃ¡lculo de agregaciones histÃ³ricas y DAGs de orquestaciÃ³n en Airflow. | Batch Layer |
| `feature/serving-reconciliation-api` | Desarrollo de endpoints FastAPI, reglas de precedencia de datos (Batch vs Speed) y deduplicaciÃ³n. | Serving Layer |
| `feature/frontend-dashboard` | Interfaz grÃ¡fica HTML5/CSS/JS con series temporales (OHLCV), volumen, indicadores y panel de control. | Consumo / UI |
| `feature/governance-metadata-lineage` | Registro en catÃ¡logo OpenMetadata, taxonomÃ­a en Atlas y emisiÃ³n de eventos OpenLineage. | Gobierno |
| `feature/qwen-llm-chat-rag` | IntegraciÃ³n del modelo local Qwen2.5 (Ollama) con guardrails de solo lectura y RAG sobre metadatos. | LLM / AnalÃ­tica |

---

## 4. GuÃ­a RÃ¡pida de Despliegue (`Quickstart`)

### Requisitos Previos
* **Docker Engine** v24+ y **Docker Compose** v2.20+
* **Git** 2.30+
* MÃ­nimo **6 GB de RAM** libre (el stack estÃ¡ optimizado con Kafka KRaft sin ZooKeeper y lÃ­mites de memoria en workers).

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

### 5. (Opcional) Levantar con LLM local Qwen2.5 (Ollama)
Si tu equipo cuenta con al menos 12 GB de RAM o GPU dedicada:
```bash
make up-all
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

## 6. OrganizaciÃ³n de Datos en MinIO

La arquitectura organiza los datos por funciÃ³n operativa en prefijos determinÃ­sticos:

```text
s3://market-lake/
â”œâ”€â”€ historical/source={exchange}/date=YYYY-MM-DD/     # Respuestas y eventos histÃ³ricos inmutables
â”œâ”€â”€ batch-results/metric=ohlcv/symbol={symbol}/       # Agregaciones periÃ³dicas consolidadas (Parquet)
â”œâ”€â”€ batch-results/metric=returns/symbol={symbol}/     # Retornos porcentuales y volatilidad batch
â””â”€â”€ reference/symbols/                                # Metadatos de sÃ­mbolos, pares y contratos

s3://market-checkpoints/
â””â”€â”€ spark/query={query_name}/                         # Checkpoints duraderos de streaming para recuperaciÃ³n

s3://market-quarantine/
â””â”€â”€ dead-letter/source={exchange}/date=YYYY-MM-DD/    # Eventos que violaron contrato o corruptos
```

---

## 7. Contrato CanÃ³nico de Eventos

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

## 8. GuÃ­a para Contribuir

1. **Crear una rama desde `develop`** con el prefijo correspondiente (`feature/`, `fix/`, `docs/`):
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/nombre-de-la-tarea
   ```
2. **Formatear commits bajo el estÃ¡ndar [Conventional Commits](https://www.conventionalcommits.org/):**
   * `feat: descripciÃ³n de nueva funcionalidad`
   * `fix: correcciÃ³n de error`
   * `docs: actualizaciÃ³n de documentaciÃ³n`
   * `chore: configuraciÃ³n o mantenimiento`
3. **Validar antes de abrir Pull Request:**
   * La infraestructura debe levantar limpia con `make up`.
   * Los schemas JSON deben ser vÃ¡lidos.
4. **Abrir Pull Request hacia `develop`** para revisiÃ³n tÃ©cnica por pares.

---

## 9. IA local Qwen2.5, RAG y guardrails (US-28 / US-29 / US-30)

La capa de IA vive en `api/` y funciona en dos modos:

1. **Con Ollama** (`make up-all`): Qwen2.5 responde con function calling sobre herramientas de solo lectura.
2. **Sin Ollama**: la API devuelve un fallback determinista con las mismas herramientas y metadatos de auditorÃ­a, para que el dashboard nunca quede bloqueado.

### Componentes

| Archivo | Responsabilidad |
| :--- | :--- |
| `api/rag_indexer.py` | Indexa `README.md`, `metadata/docs/`, `metadata/catalog.json` y `metadata/glossary.json`. Usa ChromaDB si estÃ¡ instalado; si no, TF-IDF/coseno local. |
| `api/llm_service.py` | Orquesta el prompt, el contexto RAG, las tool calls de Ollama y el fallback. |
| `api/sql_guardrails.py` | Valida SQL de solo lectura con AST (`sqlglot`) y fallback regex. |
| `api/tools.py` | Expone Ãºnicamente `get_catalog_metadata`, `query_metrics` y `query_timeseries`. |
| `api/query_service.py` | Consulta Postgres (`serving.*`) o genera datos deterministas de demo. |

### Guardrails SQL

Toda sentencia pasa por:

* Solo una sentencia `SELECT` (se permiten CTE).
* Allowlist de tablas: `serving.ohlcv_reconciled`, `serving.realtime_metrics`, `serving.pipeline_status`, `serving.assets`, catÃ¡logo y telemetrÃ­a autorizada.
* `LIMIT` obligatorio y `<= 100` (se inyecta `LIMIT 100` si falta).
* Timeout de 5 segundos en la conexiÃ³n Postgres de solo lectura.
* Bloqueo de `DROP`, `DELETE`, `UPDATE`, `ALTER`, `INSERT`, `EXEC`, `COPY`, `MERGE`, `pg_sleep`, `pg_read_file`, `dblink`, etc.
* No existe herramienta de shell, filesystem ni SQL libre.

### RAG

```bash
# Fallback local (sin dependencias extra)
make rag-index
# o:
python -m api.rag_indexer --rebuild

# Backend Chroma opcional
pip install -r api/requirements-llm.txt
python -m api.rag_indexer --rebuild
```

El Ã­ndice se guarda en `metadata/rag_index.json` (o en `metadata/rag_index.chroma/` con ChromaDB). La respuesta del chat cita las fuentes recuperadas.

### Levantar Ollama + Qwen2.5

```bash
make up-all
# El servicio ollama-init descarga OLLAMA_MODEL (qwen2.5:3b-instruct por defecto).
curl http://localhost:11434/api/tags
```

Si no hay GPU/RAM suficiente, la API sigue funcionando con el fallback de herramientas. El panel de chat muestra el estado de la respuesta y los badges de fuente, corte y disclaimer.

---

## 10. Dashboard y API (US-23 / US-24 / US-30)

El frontend estÃ¡ en `frontend/` y se sirve desde la misma FastAPI en `http://localhost:8000/`.

* Selector de activo (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`) y exchange.
* Timeframes `1m`, `5m`, `15m`, `1h`, `1d`.
* Velas OHLCV + volumen dibujadas en Canvas (sin librerÃ­as externas), crosshair y tooltip.
* KPIs: precio actual, retorno 24h, volumen 24h y VWAP.
* Polling cada 5 segundos y etiqueta `Ãšltima actualizaciÃ³n: HH:MM:SS UTC`.
* Panel colapsable de salud: Kafka, Spark, Airflow, MinIO y DLQ. Se marca alerta si la frescura/lag supera 60 segundos.
* Chat lateral conectado a **SSE** (`/api/chat/stream`) con fallback a `POST /api/chat`; renderiza fuentes, fecha de corte, SQL de trazabilidad y disclaimer.

### Endpoints

| MÃ©todo | Ruta | DescripciÃ³n |
| :--- | :--- | :--- |
| `GET` | `/api/health` | Salud de API, Postgres, RAG, Ollama. |
| `GET` | `/api/assets` | SÃ­mbolos y fuentes disponibles. |
| `GET` | `/api/ohlcv?symbol=&interval=&limit=` | Velas OHLCV validadas. |
| `GET` | `/api/metrics?symbol=&window=` | Retorno, volumen, VWAP y varianza. |
| `GET` | `/api/pipeline/status` | TelemetrÃ­a Kafka/Spark/Airflow/MinIO/DLQ. |
| `POST` | `/api/chat` | Consulta al asistente con guardrails y RAG. |
| `GET` | `/api/chat/stream` | Misma consulta en SSE. |
| `GET` | `/api/rag/status` | Estado del Ã­ndice RAG. |

### Ejecutar sin Docker

```bash
pip install -r api/requirements.txt
python -m api.rag_indexer --rebuild
uvicorn api.main:app --reload --port 8000
```

AbrÃ­ `http://localhost:8000`. Sin Postgres/MinIO/Ollama la API usa datos deterministas de demo, por lo que el dashboard y el chat son funcionales desde el primer arranque.

### Pruebas

```bash
make test
# o:
python -m pytest -q
```

La suite cubre guardrails SQL, tools, RAG y el fallback del servicio LLM/API sin requerir Ollama.

---

## 11. Licencia

Este proyecto estÃ¡ bajo la Licencia MIT. Consulta el archivo [LICENSE](LICENSE) para mÃ¡s informaciÃ³n.
