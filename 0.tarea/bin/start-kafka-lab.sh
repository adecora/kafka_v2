#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
TAREA_DIR="$BASE_DIR/0.tarea"

source "$SCRIPT_DIR/log.sh"


# Setup del entorno del laboratorio de Kafka
bash "$SCRIPT_DIR/setup.sh"

# Iniciar los conectores de Kafka
bash "$SCRIPT_DIR/start_connectors.sh"

cd "$TAREA_DIR"

info "Iniciando aplicaciones de Kafka Streams..."
mvn exec:java -Dexec.mainClass=com.farmia.streaming.SensorAlerterApp >/dev/null 2>&1 &
export pid_sensor_alerter=$!
mvn exec:java -Dexec.mainClass=com.farmia.streaming.SalesSummaryApp >/dev/null 2>&1 &
export pid_sales_summary=$!
ok "Aplicaciones de Kafka Streams iniciadas en segundo plano con PID's: $pid_sensor_alerter, $pid_sales_summary"


info "Configurando el modo BACKWARD para los esquemas definidos en el Schema Registry..."
for schema in $(curl -s "http://localhost:8081/subjects" | jq -r '.[]'); do
  info "Configurando modo BACKWARD para el esquema: $schema"
  curl -s -X PUT -H "Content-Type: application/json" --data '{"compatibility": "backward"}' "http://localhost:8081/config/$schema" | jq
done
ok "Modo BACKWARD configurado para todos los esquemas en el Schema Registry"