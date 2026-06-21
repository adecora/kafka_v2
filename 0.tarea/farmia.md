# Arquitectura de datos de FarmIA

Tarea del **Máster en Big Data & Data Engineering 2025-2026**, construcción de un pipeline que procese datos en streaming y los transforme en información útil para la toma de decisiones y análisis.


## FarmIA

![Logo de FarmIA](./assets/farmia-logo.svg)

**FarmIA** está integrando sensores IoT en los campos agrícolas para monitorizar datos como temperatura, humedad y fertilidad del suelo. También recopila datos en tiempo real sobre transacciones de su plataforma de ventas en línea y quiere procesarlos para generar insights en tiempo real.

1. Procesar los datos de sensores agrícolas en tiempo real para detectar condiciones anormales (por ejemplo, temperatura muy alta o baja humedad).

2. Integrar los datos de transacciones de venta provenientes de una base de datos relacional (**MySQL**) usando **Kafka Connect**.

3. Transformar los datos mediante procesamiento streaming para generar insights clave:
  - Alertas de anomalías de los sensores.
  - Resumen de ventas por categoría de producto cada minuto.

## Notas

- El código completo del trabajo se encuentra alojado en el fork del repositorio original: [https://github.com/adecora/kafka_v2](https://github.com/adecora/kafka_v2).
- El proyecto se desarrolla en VS Code, la configuración del entorno se encuentra en la carpeta [.vscode](https://github.com/adecora/kafka_v2/tree/main/.vscode) en la ruta raíz del repositorio.[^1]

  ```
   .vscode
  ├──  extensions.json  # Extensiones recomendadas
  ├──  kafka-ntic.json  # Configuración para la conexión al cluster de kafka de la extensión Confluent for VS Code
  └──  settings.json    # Opciones para el formato de los ficheros .java
  ```
- Fuentes utilizadas:
  * El material de la asignatura **Kafka y procesamiento en tiempo real**.
  * [Documentación de Confluent](https://docs.confluent.io/#confluent-documentation).
  * Modelos de lenguaje consultados:
    * Gemini Pro Latest
    * Gemini Flash Latest
- El script `./bin/start-kafka-lab.sh`:
  * Inicia el entorno.
  * Crea la tabla `transactions` en **MySQL**.
  * Instala los conectores.
  * Copia los drivers **MySQL**.
  * Copia los esquemas **Avro**, para la generación de datos sintéticos con Datagen.
  * Crea los topics de **Kafka** con la configuración especificando el número de particiones y el factor de replicación.
  * Inicia los conectores de Kafka.
  * Inicia las aplicaciones de **Kafka Streams** para que se ejecuten en segundo plano.
  * Configura el modo de compatibilidad BACKWARD para los esquemas del **Schema Registry**.

[^1]: [Extensión recomendada por Confluent para VS Code.](https://docs.confluent.io/cloud/current/client-apps/vs-code-extension.html?session_ref=https%3A%2F%2Fdocs.confluent.io%2Fplatform%2Fcurrent%2Fconnect%2Fkafka_connectors.html%3Fsession_ref%3Dhttps%253A%252F%252Fdocs.confluent.io%252F%26url_ref%3Dhttps%253A%252F%252Fdocs.confluent.io%252F&url_ref=https%3A%2F%2Fdocs.confluent.io%2F#confluent-for-vs-code-for-ccloud)

### Tarea 1: Generación de Datos Sintéticos con Kafka Connect

Se define el esquema **Avro** [sensor-telemetry.avsc](./src/main/avro/sensor-telemetry.avsc) para generar datos aleatorios[^2], este esquema simula 20 sensores **[00..19]** y marcas de tiempo partiendo de **01/06/2026** cada **30 segundos**.

```python
>>> # Cálculo del timestamp inicial
>>> import datetime as dt
>>> # El método `timestamp` devuelve segundos pero el esquema avro utiliza milisegundos.
>>> dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc).timestamp() * 1000
1780272000000
```

```json
{
  "namespace": "com.farmia.iot",
  "name": "SensorTelemetry",
  "type": "record",
  "doc": "Esquema para los eventos de telemetría generados con DatagenConnector",
  "fields": [
    {
      "name": "sensor_id",
      "type": {
        "type": "string",
        "arg.properties": {
          "regex": "sensor_0[0-1][0-9]"
        }
      }
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis",
        "arg.properties": {
          "iteration": {
            "start": 1780272000000,
            "step": 30000
          }
        }
      }
    },
    {
      "name": "temperature",
      "type": {
        "type": "float",
        "arg.properties": {
          "range": {
            "min": -2.00,
            "max": 40.00
          }
        }
      }
    },
    {
      "name": "humidity",
      "type": {
        "type": "float",
        "arg.properties": {
          "range": {
            "min": 0.00,
            "max": 80.00
          }
        }
      }
    },
    {
      "name": "soil_fertility",
      "type": {
        "type": "float",
        "arg.properties": {
          "range": {
            "min": 30.00,
            "max": 90.00
          }
        }
      }
    }
  ]
}
```

Se define la configuración [source-datagen-sensor-telemetry.json](./connectors/source-datagen-sensor-telemetry.json)[^3] del conector **DatagenConnector** que consumirá este esquema para generar los datos sintéticos en el topic `sensor-telemetry`. La configuración es sencilla:

- El nombre del topic ` "kafka.topic": "sensor-telemetry"`.
- El nombre del campo que se usa como *key* de los mensajes `"schema.keyfield": "sensor_id"`.
- El intervalo máximo entre mensajes (ms) `"max.interval": 2000`.
- El número de mensajes que se generan `"iterations": 10000000`.
- El número de tareas que se ejecutan `"tasks.max": "1"`.


![Visualización del topic `sensor-telemetry`.](assets/sensor-telemetry.png)

[^2]: [Anotaciones soportadas en los esquemas **avro** para generar datos sintéticos.](https://github.com/confluentinc/avro-random-generator)
[^3]: [Configurar el Datagen Source Connector.](https://docs.confluent.io/kafka-connectors/datagen/current/overview.html#mock-data-avro-and-sr)

### Tarea 2: Integración de MySQL con Kafka Connect

Se define la configuración del conector **JdbcSourceConnector** para consumir datos de **MySQL**.

```json
"connection.url": "jdbc:mysql://mysql:3306/db?useSSL=false",
"connection.user": "user",
"connection.password": "password",
```

En este primer bloque de configuración se define los parámetros de conexión a la base de datos.

```json
"poll.interval.ms": 5000,
"table.whitelist": "sales_transactions",
"topic.prefix": "",
```

En este segundo bloque se define cada cuanto se consultan los datos de las tablas en **ms**, una lista separada por comas de las tablas que se van a consumir *(en este caso se trata de una única tabla, sales_transactions)* y el prefijo que se añade al nombre de las tablas para crear el topic en **Kafka**, se define vacío para que el nombre del topic sea equivalente al nombre de la tabla.

```json
"mode": "timestamp",
"timestamp.column.name": "timestamp",
```

Se utiliza el modo incremental por tiempo, observando la columna timestamp. En cada *poll*, el conector consulta la tabla y solo trae filas cuyo valor de `timestamp` sea posterior al último procesado.

> [!DANGER]
> Si durante un *poll* existen transacciones concurrentes en el mismo milisegundo, el modo `timestamp` provocaría perdida de datos.
>
> En producción es recomendable utilizar `timestamp+incrementing`, requiere un id autoincremental en la tabla y garantiza la captura sin pérdida de eventos concurrentes.

```json
"transforms": "createKey, extractKey, Cast",
"transforms.createKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
"transforms.createKey.fields": "transaction_id",
"transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
"transforms.extractKey.field": "transaction_id",
"transforms.Cast.type": "org.apache.kafka.connect.transforms.Cast$Value",
"transforms.Cast.spec": "price:float32",
```

Se definen tres **Single Message Transformations (SMT)**, las dos primeras **createKey, extractKey** se encargan de extraer el campo `transaction_id` de los mensajes y utilizarlo como *key* de los mensajes en el topic.

La transformación **Cast**[^4] se utiliza para castear `price` de **decimal** a **float**. La columna `price` en la tabla de **MySQL** es de tipo decimal:

```bash
$ docker exec -it mysql mysql -uuser -ppassword -D db -e "DESCRIBE sales_transactions;"
mysql: [Warning] Using a password on the command line interface can be insecure.
+----------------+---------------+------+-----+-------------------+-------------------+
| Field          | Type          | Null | Key | Default           | Extra             |
+----------------+---------------+------+-----+-------------------+-------------------+
| transaction_id | varchar(50)   | NO   | PRI | NULL              |                   |
| product_id     | varchar(50)   | NO   |     | NULL              |                   |
| category       | varchar(100)  | NO   |     | NULL              |                   |
| quantity       | int           | NO   |     | NULL              |                   |
| price          | decimal(10,2) | NO   |     | NULL              |                   |
| timestamp      | timestamp     | NO   | PRI | CURRENT_TIMESTAMP | DEFAULT_GENERATED |
+----------------+---------------+------+-----+-------------------+-------------------+
```

Para los tipos de **SQL** **numeric** y **decimal**, el conector **JdbcSourceConnector** utiliza el tipo lógico **decimal**[^5]. **Avro** serializa los tipos **decimal** como bytes que pueden ser más difíciles de consumir y pueden requerir conversiones adicionales a un tipo de datos apropiado[^6]. Aunque para trabajar con monedas o datos exactos es recomendable **decimal** ya que **float** introduce errores de redondeo[^7], para facilitar su visualización en el topic, se castea a **float**.

> [!DANGER]
> En un entorno de producción se mantendría `logicalType: decimal` para evitar pérdida de precisión.

```json
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081"
```

Por último se indica que el *value* de los mensajes se va a serializar con **Avro** y la **URL** donde se encuentra el **Schema Registry**.

```bash
$ curl -s "http://localhost:8081/subjects/sales_transactions-value/versions/1" | jq '.'
{
  "subject": "sales_transactions-value",
  "version": 1,
  "id": 3,
  "schema": "{\"type\":\"record\",\"name\":\"sales_transactions\",\"fields\":[{\"name\":\"transaction_id\",\"type\":\"string\"},{\"name\":\"product_id\",\"type\":\"string\"},{\"name\":\"category\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"float\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}}],\"connect.name\":\"sales_transactions\"}"
}
```

![Visualización del topic `sales_transactions`.](assets/sales-transactions.png)

[^4]: [Cast SMT.](https://docs.confluent.io/kafka-connectors/transforms/current/cast.html)
[^5]: [Tipo lógico **decimal** de **Avro**.](https://avro.apache.org/docs/++version++/specification/#decimal)
[^6]: [Propiedad de mapeo numérico del connector **JdbcSourceConnector**.](https://docs.confluent.io/kafka-connectors/jdbc/current/source-connector/overview.html#numeric-mapping-property)
[^7]: [Decimal vs float.](https://docs.python.org/3/library/decimal.html)

### Tarea 3: Procesamiento en Tiempo Real de sensores

Se define el esquema para los mensajes del topic de `sensor-alerts` en [**src/main/avro/sensor-alerts.avsc**](src/main/avro/sensor-alerts.avsc). Para definir las alertas se utiliza el tipo **enum**[^8].

```json
{
  "namespace": "com.farmia.iot",
  "name": "SensorAlerts",
  "type": "record",
  "doc": "Esquema para los eventos de alerta generados por el SensorAlerterApp",
  "fields": [
    {
      "name": "sensor_id",
      "type": "string"
    },
    {
      "name": "alert_type",
      "type": {
        "type": "enum",
        "name": "AlertType",
        "symbols": [
          "HIGH_TEMPERATURE",
          "LOW_HUMIDITY"
        ]
      }
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "details",
      "type": "string"
    }
  ]
}
```

Se procesan los datos de los sensores utilizando **Kafka Streams**[^9]. Para generar las clases Java definidas en los esquemas [**sensor-telemetry.avsc**](src/main/avro/sensor-telemetry.avsc) y [**sensor-alerts.avsc**](src/main/avro/sensor-alerts.avsc) es necesario ejecutar el plugin **avro-maven-plugin** con `mvn generate-sources`.

```java
public static final String INPUT_TOPIC = "sensor_telemetry";
public static final String OUTPUT_TOPIC = "sensor-alerts";

public Topology buildTopology(Properties allProps) {
  final String schemaRegistryUrl = allProps.getProperty("schema.registry.url", "http://localhost:8081");
  final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

  // Creamos un Serde de tipo Avro para el consumidor <String, SensorTelemetry>
  Serde<SensorTelemetry> sensorTelemetrySerde = new SpecificAvroSerde<>();
  sensorTelemetrySerde.configure(serdeConfig, false);

  // Creamos un Serde de tipo Avro para el productor <String, SensorAlerts>
  Serde<SensorAlerts> sensorAlertsSerde = new SpecificAvroSerde<>();
  sensorAlertsSerde.configure(serdeConfig, false);

  StreamsBuilder builder = new StreamsBuilder();
  builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, sensorTelemetrySerde))
          .peek((k, v) -> LOG.info("Evento recibido: {}", v))
          // Ver: https://docs.confluent.io/platform/current/streams/javadocs/javadoc/org/apache/kafka/streams/kstream/KStream.html#flatMapValues(org.apache.kafka.streams.kstream.ValueMapper)
          .flatMapValues(this::classifyAlert)
          .peek((k, v) -> LOG.info("Evento de alerta generado: {}", v))
          .to(OUTPUT_TOPIC, Produced.with(stringSerde, sensorAlertsSerde));

  return builder.build(allProps);
}
```

El método **flatMapValues** transforma cada valor capturado de la telemetría de los sensores y genera cero, una o dos alertas de salida, manteniendo la *key* del registro original.

```java
private List<SensorAlerts> classifyAlert(SensorTelemetry telemetry) {
  List<SensorAlerts> alerts = new ArrayList<>();

  if (telemetry.getTemperature() > 35) {
    SensorAlerts alert = new SensorAlerts();
    alert.setSensorId(telemetry.getSensorId());
    alert.setAlertType(AlertType.HIGH_TEMPERATURE);
    alert.setTimestamp(telemetry.getTimestamp());
    alert.setDetails("Temperature exceeded 35ºC");
    alerts.add(alert);
  }

  if (telemetry.getHumidity() < 30) {
    SensorAlerts alert = new SensorAlerts();
    alert.setSensorId(telemetry.getSensorId());
    alert.setAlertType(AlertType.LOW_HUMIDITY);
    alert.setTimestamp(telemetry.getTimestamp());
    alert.setDetails("Humidity dropped below 30%");
    alerts.add(alert);
  }

  return alerts;
}
```

```bash
$ # Limpia y compila el proyecto Java
$ mvn clean compile
$ # Limpia, compila y genera el fichero *.jar del proyecto Java
$ # mvn clean package
$ # Ejecuta la aplicación para generar las alertas
$ mvn exec:java -Dexec.mainClass=com.farmia.streaming.SensorAlerterApp
```

El *stream* se inicializa en [start-kafka-lab.sh](bin/start-kafka-lab.sh) y se ejecuta en segundo plano.

![Visualización del topic `sensor-alerts`.](assets/sensor-alerts.png)

[^8]: [Tipo enum **Avro**.](https://avro.apache.org/docs/1.11.1/specification/#enums)
[^9]: [Ejemplo de confluence de **Kafka Streams**.](https://github.com/confluentinc/tutorials/blob/master/creating-first-apache-kafka-streams-application/kstreams/src/main/java/io/confluent/developer/KafkaStreamsApplication.java)

### Tarea 4: Procesamiento en Tiempo Real de transacciones de ventas

Se define el esquema para los datos del topic `sales-summary`. El campo `total_revenue` (total de ganancias) exige precisión, por lo que lo más correcto es utilizar el tipo lógico **decimal**. Sin embargo, por simplicidad para visualizar los datos en el topic, se utiliza **float**.

```json
{
  "namespace": "com.farmia.sales",
  "name": "SalesSummary",
  "type": "record",
  "doc": "Esquema para los eventos de resumen de ventas generados por el SalesSummaryApp",
  "fields": [
    {
      "name": "category",
      "type": "string"
    },
    {
      "name": "total_quantity",
      "type": "int"
    },
    {
      "name": "total_revenue",
      "type": "float"
    },
    {
      "name": "window_start",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {
      "name": "window_end",
      "type": {
        "logicalType": "timestamp-millis",
        "type": "long"
      }
    }
  ]
}
```

El comando `mvn generate-sources` genera la clase definida por el esquema [**sales-summary.avsc**](src/main/avro/sales-summary.avsc), para el esquema de los datos del topic de entrada se utiliza **GenericAvroSerde**[^10]

```java
public static final String INPUT_TOPIC = "sales_transactions";
public static final String OUTPUT_TOPIC = "sales_summary";

public Topology buildTopology(Properties allProps) {
  final String schemaRegistryUrl = allProps.getProperty("schema.registry.url", "http://localhost:8081");
  final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaRegistryUrl);

  // Creamos un Serde de tipo Avro para el consumidor <String, GenericRecord>
  Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
  genericAvroSerde.configure(serdeConfig, false);

  // Creamos un Serde de tipo Avro para el productor <String, SalesSummary>
  Serde<SalesSummary> salesSummarySerde = new SpecificAvroSerde<>();
  salesSummarySerde.configure(serdeConfig, false);

  StreamsBuilder builder = new StreamsBuilder();
  builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, genericAvroSerde))
          .groupBy((k, v) -> v.get("category").toString(),
                  Grouped.with(stringSerde, genericAvroSerde)
          )
          .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
          .aggregate(
                  () -> SalesSummary.newBuilder()
                          .setCategory("")
                          .setTotalQuantity(0)
                          .setTotalRevenue(0.0f)
                          .setWindowStart(Instant.EPOCH)
                          .setWindowEnd(Instant.EPOCH)
                          .build(),
                  (category, transaction, summary) -> {
                    int quantity = (int) transaction.get("quantity");
                    float revenue = (float) transaction.get("price") * quantity;

                    return SalesSummary.newBuilder(summary)
                            .setCategory(category)
                            .setTotalQuantity(summary.getTotalQuantity() + quantity)
                            .setTotalRevenue(summary.getTotalRevenue() + revenue)
                            .build();
                  },
                  Materialized.with(stringSerde, salesSummarySerde)
          )
          .toStream()
          .map((wk, summary) -> {
            SalesSummary updatedSummary = SalesSummary.newBuilder(summary)
                    .setWindowStart(wk.window().startTime())
                    .setWindowEnd(wk.window().endTime())
                    .build();

            return KeyValue.pair(wk.key(), updatedSummary);
          })
          .peek((k, v) -> LOG.info("Evento de sales summary generado: {}", v))
          .to(OUTPUT_TOPIC, Produced.with(stringSerde, salesSummarySerde));

  return builder.build(allProps);
}
```

El *stream* realiza las siguientes transformaciones sobre el topic de entrada:

1. Se agrupan los mensajes utilizando la categoría como *key*, esta operación crea un topic intermedio **sales-summary-app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition** con la nueva repartición de los mensajes. Devuelve un **KGroupedStream**.
  ![Topic intermedio generado por la reparticion de los mensajes con groupBy](assets/topic-intermedio-groupby.png)
2. Se agrupan los mensajes que llegan en una ventana de tiempo de **1 minuto** de duración.
3. Se agrupan los datos de las ventanas **TimeWindowedKStream** generadas:
   1. Utiliza `SalesSummary.newBuilder()` para generar el registro inicial vacío.
   2. Se agregan los datos de cantidad y beneficio para cada categoría en la ventana de tiempo.
   3. Se define como se materializa el estado guardado.
4. El método `aggregate` genera una **KTable**, para volver a convertirla en stream se usa `toStream`, esta operación crea un topic intermedio.
  ![Topic intermedio generado por aggregate](assets/topic-intermedio-aggregate.png)
5. Se mapea para cada uno de los registros del cálculo agregado, el inicio y el fin de la ventana de agregación y se devuelve un registro cuya *key* es la *key* de cada ventana de agregación y el *value* el registro generado.


![Visualización del topic `sales-summary`.](assets/sales-summary.png)

```bash
$ # Limpia y compila el proyecto Java
$ mvn clean compile
$ # Limpia, compila y genera el fichero *.jar del proyecto Java
$ # mvn clean package
$ # Ejecuta la aplicación para generar las alertas
$ mvn exec:java -Dexec.mainClass=com.farmia.streaming.SalesSummaryApp
```

El *stream* se inicializa en [start-kafka-lab.sh](bin/start-kafka-lab.sh) y se ejecuta en segundo plano.


[^10]: [Serializador genérico de Avro proporcionado por Confluent.](https://docs.confluent.io/platform/current/streams/developer-guide/datatypes.html#avro)

### Tarea 5: Integración de MongoDB con Kafka Connect

> [!WARNING]
> La imagen de **MongoDB 8.0** falla al arrancar debido a una incompatibilidad conocida con kernels Linux 6.19 o superiores.
> ```bash
> $ docker logs mongodb  2>&1
> {"t":{"$date":"2026-06-21T11:23:27.294Z"},"s":"F",  "c":"CONTROL",  "id":12257600,"ctx":"main","msg":"MongoDB cannot start: Linux kernel versions 6.19 and newer has a known incompatibility with this version of MongoDB. See https://jira.mongodb.org/browse/SERVER-121912 for more information."}
>
> $ uname -a
> Linux lua 7.0.9-arch2-1 #1 SMP PREEMPT_DYNAMIC Fri, 22 May 2026 19:25:09 +0000 x86_64 GNU/Linux
> ```
> Se modifica la imagen para usar **MongoDB 7.0**.

Se define la configuración del conector para crear los datos de la colección `sensor_alerts` en **MongoDB**[^11].

```json
{
  "name": "sink-mongodb-sensor-alerts",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "connection.uri": "mongodb://admin:secret123@mongodb:27017/",
    "database": "course",
    "collection": "sensor_alerts",
    "topics": "sensor-alerts",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
}
```

Se valida que los datos se estén volcando en la colección de **MongoDB**.

```bash
$ docker exec -it mongodb mongosh mongodb://localhost:27017/course -u admin -p secret123 --authenticationDatabase admin --eval 'db.sensor_alerts.countDocuments()'
221

$ docker exec -it mongodb mongosh mongodb://localhost:27017/course -u admin -p secret123 --authenticationDatabase admin --eval 'db.sensor_alerts.find().limit(2).forEach(printjson)'
{
  _id: ObjectId('6a3736c0d6e9bcc0093e8564'),
  sensor_id: 'sensor_006',
  alert_type: 'LOW_HUMIDITY',
  timestamp: ISODate('2026-05-31T22:03:30.000Z'),
  details: 'Humidity dropped below 30%'
}
{
  _id: ObjectId('6a3736c0d6e9bcc0093e8565'),
  sensor_id: 'sensor_013',
  alert_type: 'LOW_HUMIDITY',
  timestamp: ISODate('2026-05-31T22:00:00.000Z'),
  details: 'Humidity dropped below 30%'
}
```

![Colección **sensor_alerts** en la base de datos **course** de MongoDB.](assets/coleccion-mongodb.png)

[^11]: [Configuración básica de un conector *sink* de **MongoDB**.](https://www.mongodb.com/docs/kafka-connector/current/tutorials/sink-connector/)

---
[@title]: #
[Source - https://stackoverflow.com/a/35760941]: #
[Posted by Harmon, modified by community. See post 'Timeline' for change history]: #
[Retrieved 2026-02-26, License - CC BY-SA 4.0]: #

<footer style="width:100%; display:flex; justify-content:center; margin:3rem 0;">
    <p align="center">
      <a href="https://alejandrodecora.es/til" style="width:100%; display:flex; justify-content:center; text-decoration: none;">
          Hecho con 💜 por
          <!-- prettier-ignore -->
          <svg viewBox="0 0 600 530" version="1.1" xmlns="http://www.w3.org/2000/svg" style="position: relative; top: 4px; height: 1.25em;">
          <path
            d="m135.72 44.03c66.496 49.921 138.02 151.14 164.28 205.46 26.262-54.316 97.782-155.54 164.28-205.46 47.98-36.021 125.72-63.892 125.72 24.795 0 17.712-10.155 148.79-16.111 170.07-20.703 73.984-96.144 92.854-163.25 81.433 117.3 19.964 147.14 86.092 82.697 152.22-122.39 125.59-175.91-31.511-189.63-71.766-2.514-7.3797-3.6904-10.832-3.7077-7.8964-0.0174-2.9357-1.1937 0.51669-3.7077 7.8964-13.714 40.255-67.233 197.36-189.63 71.766-64.444-66.128-34.605-132.26 82.697-152.22-67.108 11.421-142.55-7.4491-163.25-81.433-5.9562-21.282-16.111-152.36-16.111-170.07 0-88.687 77.742-60.816 125.72-24.795z"
            fill="#1185fe" />
          </svg>
          <code>@vichelocrego</code>
      </a>
    </p>
</footer>