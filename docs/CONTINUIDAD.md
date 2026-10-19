# Guía de continuidad

Documento de traspaso para retomar el trabajo en **otra máquina o en otro salón
de clases**. El objetivo es que no se pierda nada y que la reanudación sea una
operación de pocos minutos.

---

## Principio: qué se conserva y qué se reconstruye

La plataforma tiene dos tipos de estado. Solo uno necesita viajar:

| Estado | ¿Se conserva? | Cómo |
| :--- | :--- | :--- |
| **Código fuente** | ✅ Sí | Vive en GitHub. Se recupera con `git clone` + `git checkout`. |
| **Configuración** | ✅ Sí | `.env.example` está versionado. Se regenera con `make env`. |
| **Infraestructura** | ♻️ Se reconstruye | Los contenedores y volúmenes se recrean con `make up`. |
| **Datos en MinIO** | ♻️ Se reconstruye | `init-buckets.sh` recrea buckets, políticas IAM y prefijos. |
| **Toppings de Kafka** | ♻️ Se reconstruye | `init-topics.sh` recrea topics y particiones. |
| **Esquema de Postgres** | ♻️ Se reconstruye | `init.sql` recrea el esquema al inicializar el volumen. |

**Conclusión:** no hay nada que sincronizar a mano. Todo lo que se iba a perder
está en GitHub.

---

## Retomar en una máquina nueva

```bash
# 1. Clonar el repositorio
git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
cd lambda-crypto-platform

# 2. Situarte en la rama de tu trabajo
git checkout feature/spark-batch-ohlcv
# Si la rama ya existe en remoto y la tienes localmente:
#   git fetch origin && git checkout feature/spark-batch-ohlcv && git pull

# 3. Regenerar la configuración
make env            # equivalente a: cp .env.example .env

# 4. Levantar la infraestructura (recrea buckets, topics y esquema)
make up

# 5. Verificar
make ps
```

En el paso 4, `make up` ejecuta los contenedores de inicialización
(`minio-init`, `kafka-init`, `airflow-init`), que dejan el entorno en el mismo
estado que una máquina que lleva días en uso.

> **Nota sobre las credenciales:** `.env` está en `.gitignore` a propósito, así
> que nunca se sube al repositorio. Todos los valores por defecto ( MinIO,
> Postgres, Airflow) ya están en `.env.example` y coinciden con los que usan
> los scripts de inicialización. Si en la máquina original habías cambiado
> alguna credencial, hay que repetir ese cambio en el nuevo `.env`.

---

## Estado actual del trabajo de Spark (US-10, US-11, US-12, US-21)

Rama: `feature/spark-batch-ohlcv` → Pull Request **#37** hacia `develop`.

| Historia | Estado | Dónde |
| :--- | :--- | :--- |
| US-10 Lectura, normalización y dedup | ✅ Implementado y probado | `spark/batch_historical.py` |
| US-11 OHLCV idempotente y retornos | ✅ Implementado y probado | `spark/batch_historical.py` |
| US-12 Backfill parametrizado | ✅ Implementado y probado | `spark/batch_historical.py` + `dags/batch_recompute.py` |
| US-21 Reconciliación Speed/Batch | ✅ Implementado y probado | `spark/serving_reconcile.py` |

### Ejecutar el job de batch

```bash
docker exec lambda-spark-master bash -c '
  cd /opt/bitnami/spark/jobs && spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    batch_historical.py \
    --start-date 2026-09-01 --end-date 2026-09-30 \
    --symbols BTCUSDT,ETHUSDT'
```

Resultados en `s3://market-lake/batch-results/metric=ohlcv/` y
`metric=returns/`. La ejecución es **idempotente**: repetirla sobrescribe las
mismas particiones sin duplicar filas.

### Ejecutar las pruebas

```bash
docker exec lambda-spark-master bash -c '
  pip install --no-cache-dir -r /opt/bitnami/spark/jobs/tests/requirements.txt &&
  cd /opt/bitnami/spark/jobs && python -m pytest tests -v'
```

Las pruebas de Spark usan una SparkSession local, así que **no requieren el
clúster en marcha**.

### Escalar el clúster Spark

```bash
make scale-spark WORKERS=3   # 3 workers (6 cores, 3 GB)
make scale-down              # vuelve a 1 worker
```

Las Web UI de los workers quedan en `http://localhost:8081`, `:8082` y `:8083`,
y el master refleja el total del clúster en `http://localhost:8080`.

---

## Decisiones de diseño que conviene no re-litigar

1. **Descubrimiento de particiones por listado S3, no con glob de Hadoop.**
   El glob `historical/source=*/date=*` lanza `IllegalArgumentException`
   porque Hadoop no interpreta los directorios con formato Hive `clave=valor`.
   Por eso `batch_historical.py` lista el bucket y filtra las particiones.

2. **`partitionOverwriteMode=dynamic` en el writer, no solo en la sesión.**
   Así la idempotencia no depende de cómo se construyó la `SparkSession`.

3. **El corte de la reconciliación se resuelve con `crossJoin`.** Spark no
   admite `max()` dentro de un `WHERE`, por lo que `serving_reconcile.py`
   calcula el corte por separado y lo cruza con cada ruta.

4. **El traversal de directorios de S3A requiere permisos explícitos.** Un
   `HEAD` sobre `market-lake/historical` no está cubierto por
   `market-lake/historical/*`, y MinIO responde 403 en lugar de 404. Está
   corregido en `fix(minio)` dentro del mismo PR.

---

## Riesgos y pendientes

- **El DAG `batch_recompute` depende de la imagen de Airflow con PySpark.**
  Ese cambio (`infra/airflow/Dockerfile`: `pyspark==3.5.1` y los jars de
  Hadoop-AWS) está en la rama `wip/infra-airflow-spark-client`, pendiente de
  integrarse en `develop`. Si al retomar el DAG falla con
  `No module named 'py4j'`, es que falta ese cambio.
- **Falta la capa Speed.** US-21 define la reconciliación y sus pruebas, pero
  el job de Structured Streaming que produce la ruta Speed es de otra
  épica. Mientras tanto, `serving_reconcile.py` se puede probar con datasets
  sintéticos, como hacen las pruebas unitarias.
- **El `.env` de la máquina original tenía `API_PORT=8010`** en lugar de
  `8000`, por un conflicto de puertos con otro servicio local. Si en la
  máquina nueva el puerto 8000 está libre, `.env.example` funciona tal cual.
