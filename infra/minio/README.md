# MinIO Data Lake & Políticas de Acceso (US-03)

Este componente administra el almacenamiento de objetos S3-compatible sobre MinIO para la plataforma con **Arquitectura Lambda pura**, organizando los datos estrictamente por **función operativa** y aplicando el principio de **mínimo privilegio (Least Privilege IAM)**.

---

## 1. Buckets y Prefijos Operativos (Regla Anti-Medallion)

> **Principio de Diseño:** No se utilizan capas Medallion (*bronze/silver/gold*). La organización de los buckets responde al propósito operativo dentro de las capas Batch, Speed y Serving.

```text
s3://market-lake/
├── historical/source={exchange}/date=YYYY-MM-DD/     # Ingesta batch inmutable de eventos
├── batch-results/metric={metric}/date=YYYY-MM-DD/    # Métricas y agregaciones periódicas (Parquet)
└── reference/symbols/                                # Catálogo de activos y metadatos financieros

s3://market-checkpoints/
└── spark/query={query_name}/                         # Checkpoints y state-stores de Structured Streaming

s3://market-quarantine/
└── source={exchange}/date=YYYY-MM-DD/                # Dead-Letter Queue para eventos corruptos o no conformes
```

---

## 2. Matriz de Usuarios y Políticas de Acceso IAM

| Componente | Usuario de Servicio | Política Asignada | Permisos Principales |
| :--- | :--- | :--- | :--- |
| **Apache Spark** | `spark_service` | `policy-spark` | • **R/W/D:** `market-checkpoints/*`<br>• **Put:** `market-quarantine/*`<br>• **Put/Get:** `market-lake/batch-results/*`<br>• **Get:** `market-lake/historical/*`, `reference/*` |
| **Apache Airflow** | `airflow_service` | `policy-airflow` | • **R/W/D:** `market-lake/*`<br>• **Get:** `market-quarantine/*`<br>• **Deny:** `market-checkpoints` |
| **FastAPI (Serving)** | `api_service` | `policy-fastapi` | • **Get/List:** `market-lake/batch-results/*`, `reference/*`<br>• **Deny:** Todo acceso de escritura y a otros buckets |

Las definiciones formales en formato JSON se encuentran en:
- [`infra/minio/policies/spark-policy.json`](policies/spark-policy.json)
- [`infra/minio/policies/airflow-policy.json`](policies/airflow-policy.json)
- [`infra/minio/policies/api-policy.json`](policies/api-policy.json)

---

## 3. Ejecución y Pruebas

### Inicialización Automática
El contenedor `minio-init` ejecuta [`init-buckets.sh`](init-buckets.sh) automáticamente al iniciar el stack con:
```bash
make up
```

### Ejecución de Pruebas Automatizadas
La suite de pruebas valida la estructura canónica, la regla anti-medallion, el aislamiento de permisos y la compatibilidad con SDKs Python (`boto3` y `s3fs`):
```bash
pytest tests/ -v
```
