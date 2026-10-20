# 🛡️ MANUAL DE OPERACIONES Y RECUPERACIÓN ANTE DESASTRES (DRP) (US-32)
**Plataforma de Datos Lambda (Finanzas & Cripto)**  
**Responsable:** Andrés Teherán (DevOps, SRE & Confiabilidad)  
**Versión:** 1.0 (Producción)

---

## 1. Objetivos del Plan de Recuperación
Establecer los procedimientos técnicos estandarizados para restaurar la operatividad de los servicios ante incidentes graves, caídas de nodos, particiones de red o corrupción en los componentes de la arquitectura Lambda.

### Objetivos de Nivel de Servicio (SLO/SLA)
- **RTO (Recovery Time Objective):** Menor a **5 minutos** para reanudación de ingesta y speed layer; menor a **15 minutos** para servicios analíticos batch.
- **RPO (Recovery Point Objective):** **0 segundos de pérdida de datos** para eventos confirmados en Kafka/MinIO; recuperación sin duplicación mediante semántica *Exactly-Once* basada en checkpoints.

---

## 2. Protocolos de Contingencia por Componente

### 2.1 Caída del Broker Kafka / Clúster de Mensajería
1. **Síntoma:** Los conectores de ingesta REST y WebSocket arrojan errores `KafkaTimeoutError` o `BrokerNotAvailableError`.
2. **Acción Automática:**
   - Los conectores activan el buffer local en memoria y reintentos exponenciales durante 60 segundos.
   - Si el broker no responde, derivan los payloads con marca temporal hacia el almacenamiento de emergencia local o MinIO (`s3://market-quarantine/`).
3. **Procedimiento de Restauración:**
   ```bash
   # Reiniciar el contenedor de Kafka
   docker compose restart kafka
   # Verificar que los topics conserven sus particiones y offsets
   bash infra/kafka/create-topics.sh
   ```

---

### 2.2 Caída del Worker de Spark Structured Streaming (Speed Layer)
1. **Síntoma:** Cesan las emisiones de métricas en el topic `market.metrics` y la tarjeta de frescura en el Frontend muestra lag creciente (> 60s).
2. **Acción de Recuperación:**
   - La arquitectura preserva los checkpoints duraderos en MinIO bajo `s3a://market-checkpoints/spark/query=market-stream/`.
   - Al reiniciar el proceso de Spark Streaming, el motor lee el directorio de offsets y commits de MinIO, reanudando el consumo desde el último offset confirmado de Kafka sin procesar eventos duplicados.
   ```bash
   # Reiniciar worker de Spark
   docker compose restart spark-worker
   ```

---

### 2.3 Indisponibilidad Temporal de MinIO S3
1. **Síntoma:** Jobs batch o streaming fallan con errores `S3Exception` o `ConnectionRefused`.
2. **Acción de Mitigación:**
   - Hadoop-AWS está afinado con reintentos automáticos (`fs.s3a.attempts.maximum=5`).
   - Tras restablecer el contenedor de MinIO (`docker compose restart minio`), los DAGs de Airflow se reintentan automáticamente gracias a la política `retries=3` y `retry_exponential_backoff=True` (US-13).

---

### 2.4 Eventos Corruptos o Malformados (Poison Pills)
1. **Síntoma:** El validador detecta payloads con tipos erróneos o precio negativo.
2. **Acción de Aislamiento:**
   - Ningún evento corrupto detiene el flujo principal; es derivado inmediatamente a `market.dlq` (US-09).
   - Una vez corregido el esquema o el bug en origen, se ejecuta el DAG `replay_dlq` de Airflow (US-17) para reinyectar los mensajes validados a `market.events`.
