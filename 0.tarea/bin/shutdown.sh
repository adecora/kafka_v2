#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
TAREA_DIR="$BASE_DIR/0.tarea"
DOCKER_DIR="$BASE_DIR/1.environment"

source "$SCRIPT_DIR/log.sh"

cd $DOCKER_DIR


# Detenemos las aplcaciones de Kafka Streams
warn "Deteniendo aplicaciones de Kafka Streams"
pkill -f com.farmia.streaming.SensorAlerterApp || true
pkill -f com.farmia.streaming.SalesSummaryApp || true
ok "Aplicaciones de Kafka Streams detenidas"

warn "Deteniendo entorno"
docker compose down --remove-orphans
ok "OK"
