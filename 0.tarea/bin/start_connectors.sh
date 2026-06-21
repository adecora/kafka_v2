#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
TAREA_DIR="$BASE_DIR/0.tarea"

source "$SCRIPT_DIR/log.sh"


info "Lanzando conectores"

for connector in "$TAREA_DIR/connectors"/*.json; do
  if [[ -s "$connector" ]]; then
    info "Lanzando conector: $connector"
    curl -s -d @"$connector" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
  fi
done

# curl -s -d @"$TAREA_DIR/connectors/source-datagen-_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq

# curl -s -d @"$TAREA_DIR/connectors/sink-mysql-_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq

# curl -s -d @"$TAREA_DIR/connectors/source-datagen-sensor-telemetry.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq

#curl -s -d @"$TAREA_DIR/connectors/source-mysql-sales_transactions.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq

#curl -s -d @"$TAREA_DIR/connectors/sink-mongodb-sensor_alerts.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq

ok "OK"

