#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
TAREA_DIR="$BASE_DIR/0.tarea"
DOCKER_DIR="$BASE_DIR/1.environment"

source "$SCRIPT_DIR/log.sh"

cd $DOCKER_DIR

info "Iniciando entorno"
docker compose up -d
sleep 30

info "Creando la tabla \`transactions\` en mysql..."
docker cp $TAREA_DIR/sql/ddl.sql mysql:/
docker exec mysql bash -c "mysql --user=root --password=password --database=db < /ddl.sql"
ok "Tabla \`transactions\` creada"

info "Instalando conectores..."
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
docker compose exec connect confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
docker compose exec connect confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest
docker compose exec connect confluent-hub install --no-prompt jcustenborder/kafka-connect-transform-common:latest
ok "Conectores instalados"

info "Copiando drivers MySQL..."
docker cp $DOCKER_DIR/mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar

info "Copiando schemas AVRO, para la generación de datos sintéticos con Datagen..."
docker cp $TAREA_DIR/src/main/avro/sensor-telemetry.avsc connect:/home/appuser/
docker cp $TAREA_DIR/datagen/_transactions.avsc connect:/home/appuser/
ok "Drivers y schemas copiados"

info "Reiniciando contenedor connect..."
docker compose restart connect

info "Esperando reinicio contenedor connect"
sleep 30

info "Creando topics de Kafka..."
for topic in _transactions sensor-telemetry sales_transactions sensor-alerts sales-summary; do
  docker compose exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --create --topic $topic --partitions 3 --replication-factor 3
done
ok "Topics de Kafka creados"

info "Registrando esquemas en el Schema Registry..."
registry_schema() {
  local schema_file="$1"
  local subject="$2"

  info "Registrando el esquema '${schema_file##*/}' en el subject '$subject'."

  jq '. | {schema: tojson}' "$schema_file" | \
    curl -s -X POST -d @- -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      http://localhost:8081/subjects/$subject/versions | jq
}

registry_schema $TAREA_DIR/datagen/_transactions.avsc _transactions-value
registry_schema $TAREA_DIR/src/main/avro/sensor-telemetry.avsc sensor-telemetry-value
registry_schema $TAREA_DIR/src/main/avro/sensor-alerts.avsc sensor-alerts-value
registry_schema $TAREA_DIR/src/main/avro/sales-summary.avsc sales-summary-value
ok "Esquemas registrados"

ok "Setup finalizado."
