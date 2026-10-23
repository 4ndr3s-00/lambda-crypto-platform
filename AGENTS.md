# AGENTS.md — Contexto del proyecto para OpenCode

> **Este archivo se carga automáticamente al abrir OpenCode en este repositorio.**
> Está escrito para que una sesión nueva (en cualquier máquina, incluso sin
> contexto previo) sepa de inmediato: qué es este proyecto, qué se hizo, qué
> falta y por qué se tomaron ciertas decisiones.
>
> Si vienes de otra sesión o de otra máquina, **léelo entero antes de tocar nada**.

---

## 1. Qué es este proyecto

Plataforma de datos financieros y de criptomonedas implementando la
**arquitectura Lambda clásica** (Batch + Speed + Serving), sin capas Medallion.

- **Batch Layer**: Spark Batch recalcula el histórico completo (velas OHLCV, retornos).
- **Speed Layer**: Spark Structured Streaming calcula la ventana reciente (provisional).
- **Serving Layer**: FastAPI + reconciliación que une ambas rutas sin doble conteo.

El equipo de 5 personas tiene una historia por persona. **Andrés Blanco es el
responsable de la capa Spark**:

| Historia | Descripción | Responsable |
| :--- | :--- | :--- |
| US-10 | Spark Batch: lectura, normalización y deduplicación | Andrés Blanco |
| US-11 | Spark Batch: OHLCV idempotente y retornos | Andrés Blanco |
| US-12 | Spark Batch: backfill masivo y recálculo | Andrés Blanco |
| US-21 | Serving: reconciliación Speed vs Batch | Andrés Blanco |

Las historias se gestionan como issues de GitHub en
`4ndr3s-00/lambda-crypto-platform`. Cada issue tiene un bloque de **Criterios
de Aceptación en BDD** y una **lista de tareas técnicas** marcadas con `- [ ]`.
**Al terminar una historia, marca sus casillas y comenta el issue.**

---

## 2. Estado actual (actualizado 2026-10-23)

> Como siempre, verifica el estado real antes de actuar — esta sección puede
> volver a quedarse vieja:
>
> ```bash
> git branch --show-current
> git log --oneline origin/develop -3
> gh api repos/4ndr3s-00/lambda-crypto-platform/pulls/37 --jq '.state, .merged'
> ```

**El PR #37 ya se mergeó.** El trabajo de Spark Batch y reconciliación está en
`develop`; la rama `feature/spark-batch-ohlcv` es historia. Trabaja sobre
`develop`.

US-21 ya está cerrada. **US-10, US-11 y US-12 siguen abiertas** aunque su
código está mergeado — nadie las cerró después del merge. Si te toca una de
ellas, cierra la issue (o pide que la cierren) en vez de asumir que aún falta
código.

### Hecho y verificado

| Entregable | Archivo | Estado |
| :--- | :--- | :--- |
| US-10 Lectura S3A, normalización y dedup | `spark/batch_historical.py` | ✅ 24/24 tests pasan (`test_spark_batch.py`) |
| US-11 OHLCV idempotente y retornos | `spark/batch_historical.py` | ✅ Verificado end-to-end |
| US-12 Backfill parametrizado | `spark/batch_historical.py` | ✅ Verificado end-to-end |
| US-21 Reconciliación Speed/Batch | `spark/serving_reconcile.py` | ✅ 24/24 tests pasan (`test_spark_batch.py`) |
| Sesión Spark compartida (S3A/Hadoop-AWS) | `spark/spark_session.py` | ✅ |
| DAG de recálculo batch | `dags/batch_recompute.py` | ✅ `airflow dags test` → success |
| Fix de IAM para traversal S3A | `infra/minio/` | ✅ Verificado |
| Escalado del clúster a N workers | `infra/docker-compose.scale.yml` | ✅ 3 workers, 6 cores |
| Guía de continuidad | `documentacion/CONTINUIDAD.md` | ✅ |

### Pendiente (prioridad en este orden)

1. **Integrar `wip/infra-airflow-spark-client` en `develop`.** Ese cambio
   (PySpark 3.5.1 + jars Hadoop-AWS en la imagen de Airflow) **es requisito del
   DAG `batch_recompute`**: sin él, el DAG falla con `No module named 'py4j'`.
   Es trabajo de otra persona, preservado en esa rama para que no se perdiera.
   ⚠️ Ojo: `wip/infra-airflow-spark-client` contiene **cambios de
   `infra/docker-compose.yml`**, y `develop` también tocó ese archivo desde
   entonces (el override `docker-compose.scale.yml` es un archivo nuevo, pero
   el `Makefile` sí cambió). Al integrar puede haber conflictos menores.

2. **Implementar la ruta Speed** (US-18/19/20, de Diego). Es el único hueco
   real que queda: US-21 define y prueba la reconciliación, pero hoy la ruta
   Speed no existe, así que la reconciliación solo se ejercita con datasets
   sintéticos en las pruebas.

3. **Cerrar US-10, US-11 y US-12 y marcar sus casillas de tareas técnicas**
   (ya se dejó un comentario de avance en cada una, pero siguen abiertas y sin
   marcar aunque el código ya está en `develop`).

### Checklist de revisión para esta capa

Puntos donde más fácil es encontrar un fallo en código de Spark Batch/Serving.
En su mayoría ya están cubiertos por tests, pero conviene confirmarlos a mano
en cualquier PR que toque esta capa:

| # | Qué mirar | Dónde |
| :--- | :--- | :--- |
| 1 | ¿La deduplicación es la que pide el criterio? (`event_id` **y** clave compuesta) | `deduplicate_events()` |
| 2 | ¿La escritura sobrescribe **solo** la partición de fecha afectada? | `write_idempotent()` |
| 3 | ¿La reconciliación puede devolver la misma vela dos veces? | `reconcile()` / `reconcile_deduplicated()` |
| 4 | ¿El `cutoff` se resuelve bien cuando la ruta Batch está vacía? | `resolve_cutoff()` |
| 5 | ¿La CLI valida rangos y granularidades inválidos? | `parse_args()` |
| 6 | ¿El fix de IAM amplía permisos más de lo necesario? | `infra/minio/policies/` |
| 7 | ¿El override de escalado rompe el flujo por defecto (`make up`)? | `infra/docker-compose.scale.yml` |

Comando para correr los tests: ver §4.

---

## 3. Cómo levantar el entorno

> **Rama correcta:** `git clone` deja en `main`, que **no contiene este
> trabajo**. Después de clonar, haz siempre:
>
> ```bash
> git checkout develop && git pull origin develop
> ```

```bash
cp .env.example .env      # o: make env
make up                   # levanta MinIO, Kafka, Postgres, Spark, Airflow y API
make ps                   # verifica el estado
make scale-spark WORKERS=3   # escala el clúster Spark a 3 workers (opcional)
```

`make up` es idempotente: los contenedores de inicialización (`minio-init`,
`kafka-init`, `airflow-init`) recrean buckets, políticas IAM, topics y esquema
de base de datos. **No hay estado que sincronizar a mano entre máquinas**: el
código está en GitHub y todo lo demás se reconstruye.

### Puertos

| Servicio | Host | Credenciales por defecto |
| :--- | :--- | :--- |
| Serving API | 8000 | — |
| MinIO Console | 9001 | `minioadmin` / `miniopassword123` |
| MinIO S3 | 9000 | — |
| Airflow Web | 8085 | `admin` / `admin` |
| Spark Master UI | 8080 | — |
| Spark Worker UI | 8081, 8082, ... | — |
| Kafka | 29092 | — |
| Postgres | 5432 | `postgres` / `postgres_secure_pass` |

---

## 4. Cómo verificar que todo funciona

```bash
# 1. Pruebas (260 en total, no requieren el clúster en marcha)
docker exec lambda-spark-master bash -c '
  pip install --no-cache-dir -r /opt/bitnami/spark/repo/tests/requirements.txt &&
  cd /opt/bitnami/spark/repo &&
  PYTHONPATH=/opt/bitnami/spark/repo:/opt/bitnami/spark/python python -m pytest tests -v'

# 2. Ejecución real del job contra MinIO
docker exec lambda-spark-master bash -c '
  cd /opt/bitnami/spark/jobs && spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py --start-date 2026-09-01 --end-date 2026-09-30 --symbols BTCUSDT,ETHUSDT'
```

**Criterio de éxito del job:** imprime `raw_events`, `valid_events`,
`invalid_events`, `deduplicated_events` y `written`, y escribe en
`s3://market-lake/batch-results/metric=ohlcv/` y `metric=returns/`.

> **Nota:** el contenedor de Airflow monta `spark/` en solo lectura, así que
> para probar cambios en los jobs hay que copiarlos al contenedor de Spark
> (`docker cp`) o levantar el stack desde tu rama con los cambios ya commiteados.

---

## 5. Decisiones de diseño (no re-litigar sin una razón fuerte)

Estas decisiones se sostenieron porque **fallarían de forma confusa**
si alguien "simplifica" el código:

1. **Descubrimiento de particiones por listado S3, no con glob de Hadoop.**
   Un glob tipo `historical/source=*/date=*` lanza
   `IllegalArgumentException`, porque Hadoop no interpreta los directorios con
   formato Hive `clave=valor`. Por eso `batch_historical.py` lista el bucket y
   filtra las particiones con `_filter_partitions()`.

2. **`partitionOverwriteMode=dynamic` en el writer, no solo en la sesión.**
   La idempotencia de US-11 no debe depender de cómo se construyó la
   `SparkSession`, o el criterio "sobrescribir solo la partición de fecha
   afectada" falla silenciosamente.

3. **El corte de la reconciliación se resuelve con `crossJoin`.** Spark no
   admite `max()` dentro de un `WHERE`, así que `serving_reconcile.py` calcula
   el `batch_cutoff_timestamp` por separado y lo cruza con cada ruta.

4. **El traversal de directorios de S3A exige permisos explícitos.** Un `HEAD`
   sobre `market-lake/historical` no está cubierto por
   `market-lake/historical/*`, y MinIO responde **403 en lugar de 404**.
   Sin el fix de `infra/minio/`, Spark no puede leer el Data Lake.

5. **`unix_timestamp` no parsea cadenas ISO-8601.** Por eso `_bucket_start()`
   castea a `timestamp` antes de convertir a epoch; si no, todas las velas
   caen en el mismo bucket.

6. **El override de escalado es opt-in a propósito.** `make up` sigue
   levantando un único worker para no romper los flujos existentes;
   `make scale-spark` es el camino explícito.

---

## 6. Errores ya resueltos (no reintroducirlos)

| Síntoma | Causa | Solución |
| :--- | :--- | :--- |
| `403 Forbidden` al leer MinIO | IAM sin permisos de traversal | `fix(minio)` en `infra/minio/` |
| `PATH_NOT_FOUND` en backfill | Ruta de fecha sin partición | `_existing_partitions()` filtra antes de leer |
| `No module named 'py4j'` | El worker no encuentra py4j | `PYSPARK_PYTHON=/opt/bitnami/spark/venv/bin/python` |
| `max() in WHERE` inválido | Agregado dentro de un filtro | `crossJoin` con el corte |
| Todas las velas en un solo bucket | `unix_timestamp` sobre string | Castear a `timestamp` primero |
| No se pueden escalar workers | `container_name` fijo | `infra/docker-compose.scale.yml` |

---

## 7. Convenciones del repositorio

- **Ramas**: `feature/...` desde `develop`, PR hacia `develop` (ver §3 del README).
- **Commits**: Conventional Commits en español (`feat:`, `fix:`, `docs:`, `chore:`).
- **Contrato de datos**: todo evento de mercado valida contra
  `ingestion/contracts/market_event.schema.json` (`ingestion/contracts/validator.py`).
- **Storage**: organización **funcional** (`historical/`, `batch-results/`,
  `reference/`, `checkpoints/`, `quarantine/`), **prohibido** usar
  `bronze`/`silver`/`gold` (regla anti-Medallion, verificada por tests).
- **IAM**: mínimo privilegio. Cada servicio (`spark_service`, `airflow_service`,
  `api_service`) tiene su propio usuario y política; la API es **solo lectura**.
- **Comentarios y documentación**: en español, explicando el *porqué*, no el *qué*.

---

## 8. Comandos de OpenCode

- `/retomar` — lee el estado de git y los issues, y propone los siguientes pasos.
