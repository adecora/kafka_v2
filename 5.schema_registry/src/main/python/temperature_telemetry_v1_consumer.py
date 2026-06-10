import logging
from pathlib import Path

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BASE_DIR = Path(__file__).parent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema_registry_config = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_config)

schema_str = None
schema_path = (
    BASE_DIR.parent / "avro" / "com.ucmmaster.kafka.data.v1.TemperatureTelemetry.avsc"
)
with open(schema_path, "r") as f:
    schema_str = f.read()

value_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client, schema_str=schema_str
)

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "temperature-telemetry-group-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": value_deserializer,
}

consumer = DeserializingConsumer(conf)
topic = "temperature-telemetry"


# Consume los topics desde el principio cada vez que se inicia el consumidor
def on_assign(c, partitions):
    for p in partitions:
        p.offset = 1  # Offset 1 es el primer mensaje (offset 1 prueba que no cumple el esquema v1)
        # " Primer mensaje"
    c.assign(partitions)


consumer.subscribe([topic], on_assign=on_assign)
print(f"Consumiendo mensajes del topic '{topic}' con esquema v1...\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        error = msg.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                print("Llegamos al final de la partición")
            else:
                print(f"Error: {error}")
                break
        else:
            key = msg.key()
            value = msg.value()

            print("→ Mensaje recibido:")
            print(f"   id:         {value.get('id', 'N/A')}")
            print(f"   temperatura: {value.get('temperature', 'N/A')}")
            print("-" * 60)

except KeyboardInterrupt:
    print("\nDetenido por el usuario")
finally:
    consumer.close()
