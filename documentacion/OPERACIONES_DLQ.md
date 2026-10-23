# Manual Operativo: Procedimiento de Contingencia y Replay de DLQ (US-17)

**Responsable:** Camilo  
**Componente:** Bus de Eventos Kafka (`market.dlq` ➔ `market.events`) / Apache Airflow  
**DAG Asociado:** `dags/replay_dlq.py`

---

## 1. Propósito y Alcance

Cuando un conector de ingesta o un productor emite eventos con anomalías de formato, tipos incompatibles o precios negativos, la plataforma los desvía hacia la **Dead Letter Queue (DLQ)** en el topic `market.dlq` para aislar el fallo y proteger los procesadores en streaming (Speed Layer) y batch.

Una vez que el error es identificado y corregido (ej. parche en el conector o actualización del schema canónico en `market_event.schema.json`), el operador debe ejecutar el DAG **`replay_dlq`** para reinyectar los eventos válidos al topic principal `market.events` sin duplicación ni pérdida de orden.

---

## 2. Parámetros de Ejecución del DAG

El DAG se ejecuta **bajo demanda** (sin programación automática) desde la interfaz web de Airflow:

| Parámetro | Tipo | Valor por Defecto | Descripción |
| :--- | :--- | :--- | :--- |
| `source` | `string` | `"all"` | Filtra por fuente de origen (`binance_ws`, `binance_rest`, `coingecko_rest`, etc.). Si se fija en `"all"`, evalúa todos los eventos. |
| `start_time` | `string` | `null` | Marca de tiempo ISO-8601 inicial (ej: `2026-10-20T00:00:00Z`). Solo se procesan eventos posteriores a esta marca. |
| `end_time` | `string` | `null` | Marca de tiempo ISO-8601 final (ej: `2026-10-20T04:00:00Z`). Solo se procesan eventos anteriores a esta marca. |
| `max_messages` | `integer` | `1000` | Límite máximo de mensajes a consumir por ejecución para control de carga en el broker. |
| `dry_run` | `boolean` | `false` | **Modo Simulación:** si es `true`, revalida los mensajes y genera estadísticas sin publicar a `market.events` ni comprometer offsets. |

---

## 3. Procedimiento Operativo Paso a Paso

### Paso 1: Verificación Preliminar (Modo Dry Run)
Antes de reinyectar datos reales a producción, ejecute una prueba simulada:
1. Ingrese a la consola web de Airflow: [http://localhost:8085](http://localhost:8085).
2. Localice el DAG `replay_dlq`.
3. Haga clic en **Trigger DAG w/ config**.
4. Configure el JSON de parámetros activando `dry_run`:
   ```json
   {
     "source": "binance_rest",
     "start_time": "2026-10-20T00:00:00Z",
     "end_time": "2026-10-20T06:00:00Z",
     "max_messages": 500,
     "dry_run": true
   }
   ```
5. Revise los logs de la tarea `execute_dlq_replay`. Compruebe cuántos mensajes son clasificados como `replayed_valid` y cuántos continúan como `still_invalid`.

### Paso 2: Ejecución Real del Replay
Una vez verificado que los mensajes superan la validación:
1. Dispare nuevamente el DAG con `"dry_run": false`.
2. El DAG leerá los mensajes, validará cada uno con `validate_event(event)`, publicará los conformes en `market.events` con clave particionada por `symbol`, y hará `commit` de los offsets en `market.dlq`.

### Paso 3: Auditoría y Verificación Posterior
1. La tarea `record_replay_audit` guardará en la tabla `serving.pipeline_execution_log` de PostgreSQL el conteo de eventos recuperados y la latencia del proceso.
2. Compruebe en la base de datos relacional:
   ```sql
   SELECT pipeline_name, records_processed, status, execution_timestamp 
   FROM serving.pipeline_execution_log 
   WHERE pipeline_name = 'replay_dlq' 
   ORDER BY execution_timestamp DESC 
   LIMIT 1;
   ```

---

## 4. Política ante Eventos Irrecuperables
Si un mensaje en `market.dlq` tiene el payload corrupto (ej. JSON malformado no parseable) o continúa violando el contrato canónico tras la corrección:
- **No se envía a `market.events`**: Permanece aislado para evitar fallos aguas abajo.
- **Registro en log**: Se emite una alerta en los logs de Airflow indicando el offset y el ID de evento descartado.

