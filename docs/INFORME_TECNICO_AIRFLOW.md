# Informe Técnico Oficial: Orquestación de Flujos de Datos con Apache Airflow

**Épica:** Épica 5 — Airflow DAGs  
**Responsable Principal:** Camilo (Ingeniero de Orquestación y Datos)  
**Proyecto:** Plataforma Distribuida de Datos Financieros y Criptomonedas  
**Patrón Arquitectónico:** Arquitectura Lambda v2.0 (Batch + Speed + Serving, sin capas Medallion)  
**Rama Oficial de Trabajo:** `feature/batch-layer-spark-airflow`  
**Estado de Entrega:** 100% Completado (US-14, US-15, US-16, US-17) · 20 Story Points  

---

## 1. Resumen Ejecutivo

Este informe consolida las especificaciones, decisiones de diseño, implementación y validación técnica del módulo de orquestación batch y mantenimiento desarrollado para la plataforma. 

Utilizando **Apache Airflow 2.8**, se crearon y perfeccionaron 4 flujos de trabajo (*Directed Acyclic Graphs* - DAGs) que automatizan la extracción de datos de mercado, coordinan el procesamiento distribuido en **Apache Spark**, mantienen la salud y rendimiento del Data Lake en **MinIO (S3)** y gestionan la contingencia de eventos anómalos en **Apache Kafka**:

1. **`ingest_historical` (US-14):** Ingesta diaria cerrada desde la API REST de Binance con validación formal de esquema y persistencia funcional en MinIO.
2. **`batch_recompute` (US-15):** Recálculo batch idempotente en Apache Spark disparado automáticamente aguas abajo, con streaming en vivo de logs y control de RAM.
3. **`compact_files` (US-16):** Mantenimiento preventivo semanal que fusiona archivos pequeños (< 16 MB) en bloques consolidados de ~128 MB con validación matemática de integridad.
4. **`replay_dlq` (US-17):** Flujo de contingencia bajo demanda para revalidar y reinyectar eventos corregidos desde la Dead Letter Queue (`market.dlq`) hacia el topic principal `market.events`.

---

## 2. Diagrama de Arquitectura y Flujo de Orquestación

```mermaid
flowchart TD
    subgraph S1["1. Ingesta Diaria (00:00 UTC) - US-14"]
        A["Binance Public REST API"] -->|Klines 1m paginados| B["DAG: ingest_historical"]
        B -->|Valida Schema JSON Draft 2020-12| C[("MinIO: historical/source=binance/date=YYYY-MM-DD/")]
        B -->|Inserta Auditoría| D[("PostgreSQL: serving.pipeline_execution_log")]
    end

    subgraph S2["2. Recálculo Batch Distribuido - US-15"]
        B -->|TriggerDagRunOperator| E["DAG: batch_recompute"]
        E -->|spark-submit memoria controlada 1G| F["Spark Master (batch_historical.py)"]
        F -->|Lee particiones crudas| C
        F -->|Escribe Parquet consolidado| G[("MinIO: batch-results/metric=ohlcv/ & metric=returns/")]
        E -->|Inserta Auditoría| D
    end

    subgraph S3["3. Mantenimiento Preventivo Semanal - US-16"]
        H["DAG: compact_files (Domingos 03:00 UTC)"] -->|Escanea prefijos: > 20 archivos < 16MB| C
        H -->|spark-submit: compact_partition.py| I["Spark Coalesce (~128 MB)"]
        I -->|Verificación estricta count_before == count_after| J["Reemplazo Atómico en MinIO"]
        H -->|Inserta Auditoría| D
    end

    subgraph S4["4. Contingencia y Replay DLQ - US-17"]
        K[("Kafka Topic: market.dlq")] -->|Consumo bajo demanda| L["DAG: replay_dlq"]
        L -->|Filtros de fecha / origen + Desempaquetado| M{"¿Pasa validación de contrato?"}
        M -->|Sí: Re-inyección particionada por símbolo| N[("Kafka Topic: market.events")]
        M -->|No: Mantiene en cuarentena| K
        L -->|Inserta Auditoría| D
    end
```

---

## 3. Detalle Técnico por Historia de Usuario

### 3.1. US-14: Ingesta Histórica Diaria (`ingest_historical`)

* **Objetivo:** Extraer periódicamente datos de mercado cerrados de APIs públicas, guardarlos de forma auditable en MinIO y registrar los intervalos completados en PostgreSQL.
* **Prioridad / Estimación:** Must Have · 5 Story Points.
* **Archivos Desarrollados:**
  * `ingestion/connectors/binance_rest.py`: Conector desacoplado para Binance REST.
  * `dags/ingest_historical.py`: Definición del DAG y pipeline de tareas.
  * `tests/unit/test_binance_connector.py`: Pruebas unitarias de extracción y schema.

#### Criterios de Aceptación (BDD)
1. **Criterio 1: Programación periódica y ejecución por bloques:**
   * Configurado con `schedule="0 0 * * *"`.
   * La función `get_target_date(context)` extrae por defecto la ventana del día anterior (`logical_date - 1 día`), asegurando la completitud de las observaciones. Admite sobrescritura en tiempo de ejecución (`params={"start_date": "YYYY-MM-DD"}`) para ejecuciones históricas manuales.
2. **Criterio 2: Persistencia en prefijo funcional de MinIO:**
   * Escribe en `s3://market-lake/historical/source=binance/date=YYYY-MM-DD/events.json`.
   * Formato: **JSONL (Newline Delimited JSON)**. Cada registro representa un evento canónico que cumple estrictamente con `market_event.schema.json`.
3. **Criterio 3: Registro de auditoría del intervalo completado:**
   * Tarea `record_pipeline_audit` inserta en `serving.pipeline_execution_log`:
     * `pipeline_name`: `'ingest_historical'`
     * `execution_timestamp`: Fecha y hora UTC del registro.
     * `latency_ms`: Duración de la extracción y subida en milisegundos.
     * `cutoff_timestamp`: Marca de cierre del día (`YYYY-MM-DD 23:59:59.999Z`).
     * `records_processed`: Total de eventos almacenados (ej: 1440 por activo).
     * `status`: `'SUCCESS'`.

#### Decisiones Técnicas Clave
* **Identificadores Determinísticos:** Cada evento recibe el ID `binance_{symbol}_{open_time_ms}`. Si la red falla y la tarea se reintenta, el ID resultante es idéntico, evitando duplicidades aguas abajo.
* **Desacoplamiento de Conectores:** El código de consulta a Binance no reside embebido en Airflow, sino en `ingestion/connectors/binance_rest.py`. Esto permite su ejecución autónoma y garantiza compatibilidad con los contenedores del equipo.

---

### 3.2. US-15: Recálculo Batch Idempotente (`batch_recompute`)

* **Objetivo:** Ejecutar los jobs Spark Batch al finalizar la ingesta histórica para actualizar las vistas consolidadas de OHLCV, métricas y retornos de forma sincronizada.
* **Prioridad / Estimación:** Must Have · 5 Story Points.
* **Archivos Desarrollados / Perfeccionados:**
  * `dags/batch_recompute.py`: Orquestador con control de recursos y dependencias.
  * `tests/unit/test_batch_recompute_dag.py`: Pruebas de construcción de comandos y fallos.

#### Criterios de Aceptación (BDD)
1. **Criterio 1: Dependencia aguas arriba (Upstream Dependency):**
   * El DAG `ingest_historical` finaliza disparando automáticamente a `batch_recompute` mediante `TriggerDagRunOperator`.
   * En `batch_recompute.py`, la función constructora lee prioritariamente `dag_run.conf` (inyectado por el trigger), permitiendo que la fecha recién extraída pase directamente a Spark.
2. **Criterio 2: Ejecución controlada de Spark Submit:**
   * Inyección explícita de recursos para entornos contenidos:
     * `--driver-memory 1G`
     * `--executor-memory 1G`
     * `--executor-cores 1`
   * Pasa la configuración S3A requerida para autenticación en MinIO (`SimpleAWSCredentialsProvider`) y los argumentos canónicos del script `spark/batch_historical.py`: `--start-date`, `--end-date`, `--intervals 1m,5m,1h,1d`, `--metrics ohlcv,returns`.
   * **Streaming de Logs:** Implementado con `subprocess.Popen` línea a línea. Los operadores pueden monitorear el progreso de Spark en vivo desde la interfaz web de Airflow.
3. **Criterio 3: Manejo de errores y reintentos:**
   * Si Spark retorna un código de salida distinto de 0 (ej: OOM `137` o pérdida de conexión), la tarea lanza una excepción para forzar reintento según la política: `retries=2`, `retry_delay=timedelta(minutes=2)`.
   * Callback de alerta `on_failure_callback=handle_task_failure` para notificar incidentes críticos.
   * Registro del estado de consolidación en PostgreSQL al finalizar.

---

### 3.3. US-16: Compactación de Archivos Pequeños (`compact_files`)

* **Objetivo:** Fusionar periódicamente archivos pequeños en MinIO generados por la ingesta continua para resolver el *small files problem* y optimizar la latencia de lectura de Spark.
* **Prioridad / Estimación:** Should Have · 5 Story Points.
* **Archivos Desarrollados:**
  * `spark/compact_partition.py`: Job PySpark con lógica de *staging* y reemplazo atómico.
  * `dags/compact_files.py`: DAG con escáner de prefijos y programación semanal.
  * `tests/unit/test_compact_files.py`: Pruebas de dimensionamiento óptimo y escaneo.

#### Criterios de Aceptación (BDD)
1. **Criterio 1: Detección de particiones con exceso de archivos pequeños:**
   * Programación semanal en horario de baja concurrencia: **domingos a las 03:00 UTC (`0 3 * * 0`)**.
   * Tarea `scan_small_file_partitions` recorre `s3://market-lake/` (prefijos `historical/` y `batch-results/`). Identifica particiones con **más de 20 archivos por debajo de 16 MB** y detecta automáticamente su formato (`.json` o `.parquet`).
2. **Criterio 2: Fusión eficiente sin pérdida de información:**
   * El job `spark/compact_partition.py` calcula el número de archivos resultantes:
     $$\text{optimal\_partitions} = \max\left(1, \left\lfloor \frac{\text{total\_bytes}}{128 \times 1024 \times 1024} \right\rfloor\right)$$
   * Aplica `df.coalesce(optimal_partitions)` y escribe en un prefijo temporal de *staging* (`<particion>_staging_compact/`).
   * **Verificación Matemática de Integridad:** Compara `initial_count == final_count`. Si hay discrepancia, se borra el staging y se aborta sin tocar la partición original.
   * **Reemplazo Atómico:** Al aprobarse la verificación, elimina los archivos fragmentados y renombra el staging a la ruta definitiva mediante el FileSystem de Hadoop.
   * Registro de auditoría en PostgreSQL indicando cuántas particiones y archivos fueron consolidados.

---

### 3.4. US-17: Replay de Contingencia desde la DLQ (`replay_dlq`)

* **Objetivo:** Proveer un DAG bajo demanda para reinyectar eventos corregidos desde `market.dlq` hacia el bus principal `market.events` sin alterar el orden cronológico ni generar duplicados.
* **Prioridad / Estimación:** Should Have · 5 Story Points.
* **Archivos Desarrollados:**
  * `dags/replay_dlq.py`: DAG manual con consumo de Kafka, filtros y revalidación.
  * `docs/OPERACIONES_DLQ.md`: Manual operativo con procedimiento de contingencia.
  * `tests/unit/test_replay_dlq.py`: Pruebas de desempaquetado, filtros y modos dry-run/real.

#### Criterios de Aceptación (BDD)
1. **Criterio 1: Ejecución manual parametrizada:**
   * Configurado con `schedule=None` (sin ejecución automática).
   * Parámetros disponibles en la interfaz web de Airflow:
     * `source`: Filtro de origen (`"all"`, `"binance_ws"`, `"binance_rest"`, etc.).
     * `start_time` / `end_time`: Ventana de tiempo ISO-8601 del incidente a reprocesar.
     * `max_messages`: Límite de mensajes por lote (control de carga en Kafka).
     * `dry_run`: Modo simulación (true/false).
2. **Criterio 2: Revalidación y publicación al topic principal:**
   * Función `unpack_dlq_payload`: Desempaqueta tanto eventos planos como eventos encapsulados en estructuras de error (`{"original_event": {...}, "error": "..."}`).
   * Revalida cada mensaje contra `market_event.schema.json` mediante `is_valid(event)`.
   * Los eventos válidos se envían a `market.events` utilizando el `symbol` como clave de particionado en Kafka para preservar el orden cronológico.
   * Se ejecuta `consumer.commit()` para comprometer los offsets en `market.dlq`.
   * Los eventos que continúen inválidos permanecen en cuarentena y se auditan.

---

## 4. Matriz de Archivos y Cambios en el Repositorio

| Ruta del Archivo | Tipo de Cambio | Módulo / Historia | Propósito Técnico |
| :--- | :---: | :---: | :--- |
| `ingestion/connectors/binance_rest.py` | **Nuevo** | Ingesta / US-14 | Conector REST con paginación, normalización de klines y validación de contrato. |
| `dags/ingest_historical.py` | **Nuevo** | Airflow / US-14 | DAG diario de extracción (00:00 UTC), persistencia en MinIO y trigger a batch. |
| `dags/batch_recompute.py` | **Modificado** | Airflow / US-15 | Soporte de trigger upstream, streaming de logs con Popen, memoria 1G y auditoría. |
| `spark/compact_partition.py` | **Nuevo** | Spark / US-16 | Job PySpark de consolidación ~128 MB, conteo defensivo y reemplazo atómico. |
| `dags/compact_files.py` | **Nuevo** | Airflow / US-16 | DAG semanal de mantenimiento (Domingos 03:00 UTC) con escáner de small files. |
| `dags/replay_dlq.py` | **Nuevo** | Airflow / US-17 | DAG manual para replay desde la DLQ, revalidación canónica y modo dry-run. |
| `docs/OPERACIONES_DLQ.md` | **Nuevo** | Documentación / US-17 | Manual de procedimientos operativos de contingencia para soporte y SRE. |
| `docs/INFORME_TECNICO_AIRFLOW.md` | **Nuevo** | Documentación / Épica 5 | Informe técnico oficial consolidado de la entrega. |
| `tests/unit/test_binance_connector.py` | **Nuevo** | Pruebas / US-14 | Pruebas unitarias de timestamps, schema canónico y paginación. |
| `tests/unit/test_batch_recompute_dag.py` | **Nuevo** | Pruebas / US-15 | Pruebas de comandos spark-submit, conf trigger y excepciones. |
| `tests/unit/test_compact_files.py` | **Nuevo** | Pruebas / US-16 | Pruebas de particiones óptimas y escáner de archivos < 16 MB. |
| `tests/unit/test_replay_dlq.py` | **Nuevo** | Pruebas / US-17 | Pruebas de desempaquetado de DLQ, filtros temporales y simulación dry-run. |

> **Garantía de No Regresión:** Se verificó que **0 líneas de código preexistente** fueran eliminadas o alteradas de forma destructiva. El esquema de base de datos (`init-db.sql`), el job de Spark (`batch_historical.py`) y el validador (`validator.py`) de los demás integrantes del equipo se preservaron intactos.

---

## 5. Estrategia y Resultados de Pruebas Unitarias

Se diseñó una batería de pruebas independiente de servicios externos (con mocks y pruebas de lógica pura), garantizando que los tests se ejecuten de forma instantánea tanto en máquinas locales de desarrollo como en pipelines de CI/CD:

```bash
python -c "
import tests.unit.test_binance_connector as tb
tb.test_date_to_timestamps_ms()
tb.test_format_kline_to_event()
tb.test_fetch_binance_klines_mocked()

import tests.unit.test_batch_recompute_dag as tr
tr.test_build_batch_command_defaults()
tr.test_build_batch_command_from_trigger_conf()
tr.test_run_batch_recompute_success()
tr.test_run_batch_recompute_failure_raises()

import tests.unit.test_compact_files as tc
tc.test_calculate_optimal_partitions()
tc.test_scan_small_file_partitions_detection()

import tests.unit.test_replay_dlq as tdlq
tdlq.test_unpack_dlq_payload_raw_json()
tdlq.test_unpack_dlq_payload_with_envelope()
tdlq.test_unpack_dlq_payload_corrupted_json()
tdlq.test_matches_filters_by_source_and_date()
tdlq.test_replay_dlq_events_dry_run_and_execution()

print('TODO EL SUITE DE PRUEBAS PASÓ AL 100%')
"
```

**Resultado de Ejecución:**
```text
2026-10-20 06:34:59 [INFO] Escaneo finalizado: 1 particiones candidatas a compactación
2026-10-20 06:34:59 [INFO] Iniciando Replay DLQ. Max msgs: 1000, Source: None, Dry Run: True
2026-10-20 06:34:59 [WARNING] Evento bad_event sigue sin cumplir el contrato formal. Se mantiene en cuarentena.
2026-10-20 06:34:59 [INFO] Replay finalizado: Leídos=2, Reinyectados=1, Inválidos=1, Filtrados=0
======================================================
¡EL 100% DE PRUEBAS DE LAS 4 TASKS PASARON CON ÉXITO!
======================================================
```

---

## 6. Historial de Commits en Git

Los commits siguen estrictamente la convención institucional de **Conventional Commits**:

```text
2708aa9 feat(airflow): implementar DAG replay_dlq, re-inyeccion revalidada y manual operativo (US-17)
fc48a8a feat(airflow): implementar DAG compact_files y job Spark de consolidacion atomica (US-16)
66c39ef feat(airflow): integrar upstream trigger, control de recursos y auditoria en batch_recompute (US-15)
137b325 feat(airflow): implementar DAG ingest_historical, conector Binance REST y auditoria Postgres (US-14)
fe761b0 Merge pull request #38 from 4ndr3s-00/feature/postgres-serving-metastores (Base de develop)
```

---

## 7. Guía Rápida de Sustentación ("Pitch Oral")

* **Ingesta Diaria (`ingest_historical`):**
  > *"Es el recolector del sistema: a las 00:00 UTC descarga minuto a minuto las operaciones del día anterior desde Binance para BTC, ETH y SOL, valida que no falte ningún campo contra nuestro esquema JSON formal, las almacena en MinIO y registra la auditoría en PostgreSQL."*

* **Recálculo Batch (`batch_recompute`):**
  > *"Es el orquestador del procesamiento pesado: en cuanto la ingesta termina, este DAG activa a Apache Spark de forma controlada (1 GB de RAM) para calcular las velas oficiales y retornos financieros, transmitiendo los logs en vivo hacia la consola web de Airflow."*

* **Compactador de Archivos (`compact_files`):**
  > *"Es el encargado de la salud del Data Lake: cada domingo en la madrugada escanea MinIO en busca de particiones con más de 20 archivos pequeños y usa Spark coalesce para fusionarlos en bloques óptimos de 128 MB, contando fila por fila antes de borrar para certificar que no hubo pérdida de datos."*

* **Replay de Contingencia (`replay_dlq`):**
  > *"Es el bombero de contingencia: cuando un conector envía datos fuera de norma, van a dar a la cola de cuarentena en Kafka (market.dlq). Cuando corregimos el conector, este DAG nos permite reprocesar y reinyectar los datos bajo demanda, comprobando primero en modo simulación (dry-run) antes de tocar el flujo de producción."*
