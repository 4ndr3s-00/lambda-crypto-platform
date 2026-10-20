# US-20: Prefijo de checkpoints duraderos en MinIO

## Qué implementa

La US-20 define el prefijo de checkpoints duraderos que Spark Structured Streaming utiliza para fault tolerance y recuperación ante fallos.

### Implementación en el código

| Archivo | Línea | Descripción |
|---------|-------|-------------|
| `spark/spark_session.py` | 120 | `def checkpoints_path(prefix: str = "") -> str:` |
| `spark/streaming_market.py` | 79 | `DEFAULT_CHECKPOINT_DIR = checkpoints_path("spark/query=market-stream")` |

### Función `checkpoints_path`

```python
def checkpoints_path(prefix: str = "") -> str:
    """Prefijo de checkpoints duraderos en MinIO (US-20)."""
    return lake_path(prefix, bucket=CHECKPOINTS_BUCKET)
```

Donde `lake_path` construye `s3a://market-checkpoints/{prefix}`.

### Bucket MinIO

- **Nombre**: `market-checkpoints`
- **Creado**: Durante `make up` (script init-buckets.sh)
- **Uso**: Checkpoints de DLQ y metrics en Spark Streaming

### ¿Por qué es importante?

1. **Fault tolerance**: Permite recuperar el estado del streaming job si falla el clúster
2. **Exactly-once semantics**: Garantiza que las métricas no se dupliquen al reconectar
3. **Persistencia**: Los checkpoints sobreviven reinicios del job (siempre que el bucket persista)

### Ubicación en el código

```python
# streaming_market.py línea 79
DEFAULT_CHECKPOINT_DIR = checkpoints_path("spark/query=market-stream")
```

Esto setea el checkpoint dir por defecto para todo el pipeline de Speed Layer.

### Uso en el job

```python
# build_speed_layer_pipeline recibe checkpoint_location
dlq_checkpoint = os.path.join(args.checkpoint_dir, "dlq")
metrics_checkpoint = os.path.join(args.checkpoint_dir, "metrics")
```

### Verificación

```bash
# Confirmar que el bucket existe
python3 -c "
from minio import Minio
mc = Minio('localhost:9000', access_key='minioadmin', secret_key='miniopassword123', secure=False)
buckets = [b.name for b in mc.list_buckets()]
print('market-checkpoints' in buckets)  # Debería imprimir: True
"
```

### Relación con otras US

| US | Relación |
|-----|----------|
| **US-18** | Usa los checkpoints para DLQ y metrics en Streaming |
| **US-19** | También usa el mismo DEFAULT_CHECKPOINT_DIR |
| **US-20** | Define el prefijo y bucket donde se guardan |
| **US-21** | Reconciliación usa datos batch, no checkpoints streaming |

### Comandos de verificación

```bash
# Verificar bucket en MinIO
python3 << 'EOF'
from minio import Minio
mc = Minio('localhost:9000', access_key='minioadmin', secret_key='miniopassword123', secure=False)
buckets = mc.list_buckets()
for b in buckets:
    print(f'Bucket: {b.name}')
    if b.name == 'market-checkpoints':
        print('  ✅ US-20: Bucket encontrado')
EOF

# Probar la función checkpoints_path
python3 << 'EOF'
import sys
sys.path.insert(0, '/home/cohorte5/Descargas/lambda-crypto-platform')
from spark_session import checkpoints_path

print(f'checkpoints_path():           {checkpoints_path()}')
print(f'checkpoints_path(\"test\"):     {checkpoints_path("test")}')
print(f'checkpoints_path default:      {checkpoints_path("spark/query=market-stream")}')
print('✅ Formato: s3a://market-checkpoints/...')
EOF
```

## Estado de finalización

- ✅ **Código implementado**: `checkpoints_path()` definida en `spark_session.py:120`
- ✅ **Bucket creado**: `market-checkpoints` en MinIO
- ✅ **Usado en streaming**: `DEFAULT_CHECKPOINT_DIR` en `streaming_market.py:79`
- ✅ **En develop**: Forma parte del merge del PR #37 y commits posteriores

## Para los compañeros

- **No requiere PR nuevo**: El código ya está en `develop` desde el PR #37 y commits subsiguientes
- **Verificar**: Usar los comandos de prueba arriba para confirmar que el bucket y la función existen
- **Usar en jobs**: Pasar `--checkpoint-dir` con el valor `s3a://market-checkpoints/spark/query=market-stream` o dejar vacío para usar el default