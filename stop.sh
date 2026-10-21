#!/bin/bash
#============================================================================
# stop.sh - Detiene todo el entorno Spark y Docker de forma ordenada
#============================================================================

set -e

echo ""
echo "============================================================"
echo "  LAMBDA CRYPTO PLATFORM - DETENIENDO ENTORNO"
echo "============================================================"
echo ""

# 1. Detener Worker Spark
echo "1. Deteniendo Worker Spark..."
if systemctl --user status spark-worker.service >/dev/null 2>&1; then
    systemctl --user restart spark-worker.service 2>/dev/null && \
    echo "   ✅ Worker detenido via systemd" || \
    echo "   ⚠️  systemd no disponible, intentando matar proceso directamente"
fi

# Si systemd falló o no está, matar el proceso
pkill -f "org.apache.spark.deploy.worker.Worker" 2>/dev/null
sleep 1
if ps aux | grep "org.apache.spark.deploy.worker.Worker" | grep -v grep > /dev/null; then
    pkill -9 -f "org.apache.spark.deploy.worker.Worker" 2>/dev/null
    echo "   ⚠️  Worker forzadamente detenido"
else
    echo "   ✅ Worker detenido"
fi

echo ""

# 2. Detener Masters Spark (este PC - 10.1.20.20)
echo "2. Deteniendo Masters Spark (esta PC)..."
# Matar procesos master
for pid in $(ps aux | grep "org.apache.spark.deploy.master.Master" | grep -v grep | awk '{print $2}'); do
    kill -15 $pid 2>/dev/null || kill -9 $pid 2>/dev/null
done
sleep 2

# Verificar estado
MASTERS_REMAINING=$(ps aux | grep "org.apache.spark.deploy.master.Master" | grep -v grep | wc -l)
if [ "$MASTERS_REMAINING" -eq 0 ]; then
    echo "   ✅ Masters Spark detenidos"
else
    echo "   ⚠️  Forzando detención de masters..."
    for pid in $(ps aux | grep "org.apache.spark.deploy.master.Master" | grep -v grep | awk '{print $2}'); do
        kill -9 $pid 2>/dev/null
    done
    sleep 1
fi

# Verificar finalmente
MASTERS_REMAINING=$(ps aux | grep "org.apache.spark.deploy.master.Master" | grep -v grep | wc -l)
if [ "$MASTERS_REMAINING" -eq 0 ]; then
    echo "   ✅ Todos los masters detenidos"
else
    echo "   ⚠️  Algunos masters persisten - revisar manualmente"
fi

echo ""

# 3. Opcional: Detener containers Docker
read -p "¿Desea detener los containers Docker (MinIO, Kafka, Postgres, Spark, Airflow)? [y/N]: " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "   📡 Deteniendo containers Docker..."
    cd /home/cohorte5/Descargas/lambda-crypto-platform
    docker-compose -f infra/docker-compose.yml down 2>&1 | tail -10
    echo "   ✅ Containers Docker detenidos y redes limpiadas"
else
    echo "   ⏭️  Containers Docker dejados corriendo"
fi

echo ""

# 4. Limpiar procesos Java residuales
echo "3. Limpiando procesos Java residuales..."
pkill -9 -f "org.apache.spark" 2>/dev/null || echo "   (No hay procesos Java de Spark corrientes)"
pkill -9 -f "spark-class" 2>/dev/null || echo "   (No hay procesos spark-class corrientes)"

echo ""
echo "============================================================"
echo "  ENTORNO DETENIDO CORRECTAMENTE"
echo "============================================================"
echo ""
echo "Para reactivar luego:"
echo "  ./start.sh     # Levanta todo el entorno"
echo "  make up        # Alternativa con Makefile"
echo ""
