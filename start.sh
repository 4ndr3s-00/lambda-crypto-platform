#!/bin/bash
#============================================================================
# start.sh - Levanta todo el entorno y prueba el flujo Spark Batch
#============================================================================

set -e

echo "============================================================"
echo "  LAMBDA CRYPTO PLATFORM - INICIO DE ENTORNO SPARK"
echo "============================================================"

echo ""
echo "1. Verificando Docker y servicios..."
echo ""

# Verificar que Docker esté corriendo
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker no está corriendo. Ejecuta: make up"
    exit 1
fi

echo "✅ Docker está corriendo"

# Verificar que el master Spark esté accesible
echo "2. Verificando Master Spark (10.1.20.20:7077)..."
SPARK_MASTER="spark://10.1.20.20:7077"

# Hacer una petición simple para verificar que el master responde
if curl -sS --max-time 5 http://10.1.20.20:8080/json/ 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('Master UI:', d.get('url'))
workers = d.get('workers', [])
if workers:
    for w in workers:
        print('  Worker:', w.get('id'), 'state=', w.get('state'))
else:
    print('  No hay workers conectados todavía')
" 2>&1 | grep -q "ALIVE"; then
    echo "✅ Master Spark accesible y funcional"
else
    echo "⚠️  Master Spark no responde correctamente. Verifica que los containers estén corriendo."
    echo "   Intenta: docker-compose -f infra/docker-compose.yml ps"
fi

echo ""
echo "3. Verificando Worker (10.1.20.3)..."
# Verificar worker conectado
WORKER_CHECK=$(curl -sS --max-time 5 http://10.1.20.20:8080/json/ 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for w in d.get('workers', []):
    if '10.1.20.3' in w.get('id', ''):
        print('ALIVE')
        break
")
if [ "$WORKER_CHECK" = "ALIVE" ]; then
    echo "✅ Worker 10.1.20.3 conectado y funcional"
else
    echo "⚠️  Worker 10.1.20.3 no encontrado o desconectado."
    echo "   Intenta reiniciar: systemctl --user restart spark-worker.service"
fi

echo ""
echo "4. Ejecutando tests unitarios de Spark Batch..."
echo "   (estos tests usan local[2], no requieren Docker)"

# Ejecutar tests unitarios
cd /home/cohorte5/Descargas/lambda-crypto-platform
python3 -m pytest tests/unit/test_spark_batch.py -v --tb=short 2>&1 | tail -30

echo ""
echo "============================================================"
echo "  FIN DE LA COMPROBACIÓN INICIAL"
echo "  Revisa los resultados arriba"
echo "============================================================"
echo ""
echo "Siguientes pasos opcionales:"
echo "  - Probar job real: spark-submit --master spark://10.1.20.20:7077 ..."
echo "  - Probar UI: http://10.1.20.20:8080/"
echo "  - Verificar MinIO: http://10.1.20.20:9001"
echo ""
