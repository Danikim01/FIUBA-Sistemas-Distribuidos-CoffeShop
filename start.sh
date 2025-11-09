#!/bin/bash

# Script para iniciar los contenedores y luego ejecutar el script de killing de workers
# Ejecuta docker compose up y espera a que todos los contenedores estén corriendo
# antes de ejecutar kill_tpv_workers.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "=========================================="
echo "Ejecutando run.sh para generar docker-compose.yml y levantar contenedores"
echo "=========================================="

# Ejecutar run.sh (genera docker-compose.yml y levanta contenedores)
# Si se pasa un parámetro numérico, se lo pasamos a run.sh para escalar los workers
if [ $# -gt 0 ]; then
    WORKER_COUNT=$1
    if ! [[ "$WORKER_COUNT" =~ ^[0-9]+$ ]] || [[ "$WORKER_COUNT" -le 0 ]]; then
        echo "Error: El parámetro debe ser un número entero positivo (recibido: '$WORKER_COUNT')." >&2
        echo "Uso: $0 [worker_count]" >&2
        exit 1
    fi
    echo "Escalando todos los workers a $WORKER_COUNT instancias..."
    bash "${SCRIPT_DIR}/run.sh" "$WORKER_COUNT"
else
    echo "Usando configuración de workers_config.json..."
    bash "${SCRIPT_DIR}/run.sh"
fi

echo ""
echo "Esperando a que todos los contenedores estén corriendo..."

# Esperar a que todos los contenedores estén corriendo
MAX_WAIT=120  # Máximo 2 minutos de espera
WAIT_INTERVAL=2  # Verificar cada 2 segundos
ELAPSED=0

# Esperar a que los contenedores estén corriendo
# Verificamos que haya contenedores corriendo y que no haya contenedores en estado "Restarting"
while [ $ELAPSED -lt $MAX_WAIT ]; do
    # Contar contenedores corriendo
    RUNNING_COUNT=$(docker compose ps --format json 2>/dev/null | grep -c '"State":"running"' || echo "0")
    RESTARTING_COUNT=$(docker compose ps --format json 2>/dev/null | grep -c '"State":"restarting"' || echo "0")
    TOTAL_COUNT=$(docker compose ps --format json 2>/dev/null | wc -l || echo "0")
    
    # Si hay contenedores restarting, esperar más
    if [ "$RESTARTING_COUNT" -gt 0 ]; then
        echo "  Esperando... (${ELAPSED}s/${MAX_WAIT}s) - ${RESTARTING_COUNT} contenedor(es) reiniciando..."
        sleep $WAIT_INTERVAL
        ELAPSED=$((ELAPSED + WAIT_INTERVAL))
        continue
    fi
    
    # Si todos los contenedores están corriendo, salir
    if [ "$RUNNING_COUNT" -gt 0 ] && [ "$RUNNING_COUNT" -eq "$TOTAL_COUNT" ]; then
        echo "✅ Todos los contenedores están corriendo (${RUNNING_COUNT}/${TOTAL_COUNT})"
        break
    fi
    
    echo "  Esperando... (${ELAPSED}s/${MAX_WAIT}s) - ${RUNNING_COUNT}/${TOTAL_COUNT} contenedores corriendo..."
    sleep $WAIT_INTERVAL
    ELAPSED=$((ELAPSED + WAIT_INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "⚠️  Timeout esperando a que todos los contenedores estén corriendo"
    echo "Continuando de todas formas..."
fi

# Esperar un poco más para que los servicios se estabilicen
echo ""
echo "Esperando 5 segundos adicionales para que los servicios se estabilicen..."
sleep 5

echo ""
echo "=========================================="
echo "Ejecutando script de killing de workers"
echo "=========================================="
echo ""

# Ejecutar el script de killing de workers
if [ -f "${SCRIPT_DIR}/kill_tpv_workers.sh" ]; then
    bash "${SCRIPT_DIR}/kill_tpv_workers.sh"
else
    echo "⚠️  Error: No se encontró el script kill_tpv_workers.sh"
    exit 1
fi

echo ""
echo "=========================================="
echo "Script start.sh completado"
echo "=========================================="

