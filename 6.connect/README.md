# Kafka Connect

En esta sección practicaremos con la administración de conectores **source** y **sink**.

El pipeline de datos streaming que crearemos es el siguiente:

![til](assets/connect-usecase.png)

## Get Kakfa Connect Status

Como primer paso vamos a comprobar el estado nuestro cluster de `Kafka Connect` para ello usaremos el [API de administración](https://docs.confluent.io/platform/current/connect/references/restapi.html).

Podemos acceder a esta [`API`](https://developer.confluent.io/courses/kafka-connect/rest-api/?session_ref=direct&url_ref=https%3A%2F%2Fdocs.confluent.io%2Fplatform%2Fcurrent%2Fconnect%2Freferences%2Frestapi.html) desde nuestra máquina.

Para comprobar el estado y versiones instaladas llamando:

```shell
curl -s http://localhost:8083 | jq
```

obtendremos una salida como esta:

```json
{
  "version": "7.8.0-ccs",
  "commit": "cc7168da1fddfcfde48b42031adde57bb5bcf529",
  "kafka_cluster_id": "Nk018hRAQFytWskYqtQduw"
}
```

En la respuesta podemos vemos la versión y commit del servidor y el id kafka cluster que hace de backend de el.

Si obtenemos esta clase de respuesta es que nuestro cluster de connect está preparado para trabajar.

## Get Connector Plugins

Para obtener los plugins instalados:

```shell
curl -s http://localhost:8083/connector-plugins | jq
```

Deberíamos obtener una salida parecida a esta:

```json
[
  {
    "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  }
]
```

como vemos los únicos plugins instalados son los correspondientes a `MirrorMaker`

MirrorMaker es el conector que permite hacer integraciones entre dos **clusters** Kafka.

## Get Connectors

Para obtener las instancias de los conectores:

```bash
curl http://localhost:8083/connectors | jq
```

```json
[]
```

obtenemos una salida vacía (ya que no tenemos todavía ningún conector ejecutando)

> Nota: Todos los comandos que vayamos ejecutando los podréis ejecutar como sh desde la carpeta 6.connect
> Os recomiendo usar un entorno linux (Linux, MAC o WSL en el caso de usar windows)

## Install Connector Plugins

### Install Datagen Source Connector Plugin

Como primer paso vamos a instalar un plugin de conector tipo **source**, es decir uno que escribe datos en un topic Kafka.

El connector que usaremos es `Datagen Source Connector`, un conector que nos permitirá generar dato sintético en un topic Kafka.

[Documentación](https://github.com/confluentinc/kafka-connect-datagen/blob/master/README.md)

Para instalar el `plugin` correspondiente al conector dentro de nuestro contenedor connect.
Usaremos el comando  `confluent-hub` ya instalado en nuestro contenedor.

```shell
docker exec -it connect /bin/bash
```
El siguiente comando lo ejecutamos dentro del **contenedor**:

```shell
confluent-hub install confluentinc/kafka-connect-datagen:0.6.6
```

```sh
[appuser@connect ~]$ confluent-hub install confluentinc/kafka-connect-datagen:0.6.6
The component can be installed in any of the following Confluent Platform installations:
  1. / (installed rpm/deb package)
  2. / (where this tool is installed)
Choose one of these to continue the installation (1-2): 1
Do you want to install this into /usr/share/confluent-hub-components? (yN) y


Component's license:
Apache License 2.0
https://www.apache.org/licenses/LICENSE-2.0
I agree to the software license agreement (yN) y

Downloading component Kafka Connect Datagen 0.6.6, provided by Confluent, Inc. from Confluent Hub and installing into /usr/share/confluent-hub-components
Detected Worker's configs:
  1. Standard: /etc/kafka/connect-distributed.properties
  2. Standard: /etc/kafka/connect-standalone.properties
  3. Standard: /etc/schema-registry/connect-avro-distributed.properties
  4. Standard: /etc/schema-registry/connect-avro-standalone.properties
  5. Used by Connect process with PID : /etc/kafka-connect/kafka-connect.properties
Do you want to update all detected configs? (yN) y

Adding installation directory to plugin path in the following files:
  /etc/kafka/connect-distributed.properties
  /etc/kafka/connect-standalone.properties
  /etc/schema-registry/connect-avro-distributed.properties
  /etc/schema-registry/connect-avro-standalone.properties
  /etc/kafka-connect/kafka-connect.properties

Completed
```

### Install JDBC Connector Plugin

Ahora vamos a instalar un plugin de conector tipo **sink**, es decir uno que escribe datos a sistemas desde topics de Kafka.

El connector que usaremos es `JDBC Sink Connector`, un conector que nos permitirá escribir datos en base de datos a través del protocolo JDBC (**J**ava **D**ata **B**ase **C**onnectivity)

[Documentación](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html)

Procedemos igual que antes:

```bash
confluent-hub install confluentinc/kafka-connect-jdbc:10.8.0
```

```bash
[appuser@connect ~]$ confluent-hub install confluentinc/kafka-connect-jdbc:10.8.0
The component can be installed in any of the following Confluent Platform installations:
  1. / (installed rpm/deb package)
  2. / (where this tool is installed)
Choose one of these to continue the installation (1-2): 1
Do you want to install this into /usr/share/confluent-hub-components? (yN) y


Component's license:
Confluent Community License
https://www.confluent.io/confluent-community-license
I agree to the software license agreement (yN) y

Downloading component Kafka Connect JDBC 10.8.0, provided by Confluent, Inc. from Confluent Hub and installing into /usr/share/confluent-hub-components
Detected Worker's configs:
  1. Standard: /etc/kafka/connect-distributed.properties
  2. Standard: /etc/kafka/connect-standalone.properties
  3. Standard: /etc/schema-registry/connect-avro-distributed.properties
  4. Standard: /etc/schema-registry/connect-avro-standalone.properties
  5. Used by Connect process with PID : /etc/kafka-connect/kafka-connect.properties
Do you want to update all detected configs? (yN) y

Adding installation directory to plugin path in the following files:
  /etc/kafka/connect-distributed.properties
  /etc/kafka/connect-standalone.properties
  /etc/schema-registry/connect-avro-distributed.properties
  /etc/schema-registry/connect-avro-standalone.properties
  /etc/kafka-connect/kafka-connect.properties

Completed
```

El conector [JDBC necesita los drivers Java específicos de cada BD](https://docs.confluent.io/kafka-connectors/jdbc/current/jdbc-drivers.html), en nuestro caso los de MySQL.
En la carpeta `1.environment/mysql` podéis encontrar el jar del driver en cuestión.
Para copiarlo a nuestro contendor (ejecutar este comando en el directorio 6.connect):

```bash
docker cp ../1.environment/mysql/mysql-connector-java-5.1.45.jar connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar

Successfully copied 1MB to connect:/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/mysql-connector-java-5.1.45.jar
```

> ❗️ **NOTA**<br/>Si tuviéramos que conectarnos a otro tipo de base de datos como Oracle, Postgres, DB2 ... bastaría con hacer lo mismo con el correspondiente driver

## Get Connector Plugins

Comprobemos si ya aparecen instalados ambos conectores:

```bash
curl -s http://localhost:8083/connector-plugins | jq
```

Los conectores instalados siguen sin aparecer.

Esto es porque el servicio Kafka Connect ya está ejecutando, y cuando arrancó no existían.
Necesitamos reiniciar el contenedor para que pueda detectar los nuevos conectores instalados.

Lo haremos ejecutando:

```bash
docker compose restart connect
```

una vez reiniciado comprobamos de nuevo la lista de plugins:

```bash
curl http://localhost:8083/connector-plugins | jq
```

observando que ahora si los tenemos disponible:

```json
[
  {
    "class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "type": "sink",
    "version": "10.8.0"
  },
  {
    "class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "type": "source",
    "version": "10.8.0"
  },
  {
    "class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "type": "source",
    "version": "null"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  },
  {
    "class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "type": "source",
    "version": "7.8.0-ccs"
  }
]

```

> ❗️ **NOTA**<br/>El connector JDBC instala dos conectores: el tipo Source como el Sink

## Connector Config

```bash
curl http://localhost:8083/connector-plugins/io.confluent.kafka.connect.datagen.DatagenConnector/config | jq
```

# Ejercicio 1

## Create Datagen Source Connector Instance

Lo siguiente será crear una nueva instancia de nuestro conector

Para ello primero deberemos crear una configuración válida para él.

[Documentación](https://github.com/confluentinc/kafka-connect-datagen/tree/master)

Tras revisar la documentación:

```json
{
  "name": "source-datagen-users",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "users",
    "quickstart": "users",
    "max.interval": 1000,
    "iterations": 10000000,
    "tasks.max": "1",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "false"
  }
}
```

Además de configuración específica del conector, como que usamos el quickstart con el modelo `users` o las iteraciones y el intervalo de publicación de mensaje, vemos alguna configuración genérica interesante:

1. `connector.class`: Clase que implementa el connector.
2. `kafka.topic`: Topic en el que publicará los mensajes (ojo esto es una configuración común pero no todos los conectores la llaman igual)
3. `quickstart` : nombre del dataset preconfigurado (también podemos usar uno propio mediante otra configuración)
4. `max.interval` : cada segundo
5. `ìterations`: numero de eventos a producir
6. `task.max`: Número máximo de tareas que se distribuirán en el cluster de connect.
7. `value.converter`: Establecemos serializacion en avro
8. `value.converter.schema.registry.url` : url del schema registry
9. `value.converter.schemas.enable` : indica que no se incruste el esquema completo dentro de cada mensaje Kafka sino el ID del esquema registrado en el Schema Registry. Es el comportamiento por defecto pero lo hago explicito porque en versiones anteriores no lo era.

para validar esta configuración podemos usar el siguiente endpoint

```bash
jq '.config' ./connectors/source-datagen-users.json | curl -s -d @-  -H "Content-Type: application/json" -X PUT http://localhost:8083/connector-plugins/io.confluent.kafka.connect.datagen.DatagenConnector/config/validate | jq
```

💎 Este comando es muy util para detectar problemas en nuestra configuración antes de lanzar el conector.

para publicar esta configuración volveremos a usar el api de connect:

```bash
curl -d @"./connectors/source-datagen-users.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
```

Con este curl estamos pasando un fichero que contiene la configuración para del connector, el verbo POST de HTTP nos indica que estamos en "modo creación"

> Nota:
>
> Este comando debe ejecutarse desde la carpeta 6.connect
>
> En caso de estar ejecutando los curl desde el contenedor debeis copiar primero el fichero de configuración json a la carpeta desde donde ejecutéis el curl:
>
> docker cp ./connectors/source-datagen-users.json connect:/home/appuser
>
> Este comando lo podemos usar para subir schemas avro para usar con el Datagen Source Connector

Si ejecutamos ahora la consulta de conectores corriendo:

```
curl -s http://localhost:8083/connectors | jq
```

```json
[
  "source-datagen-users"
]
```

Ahora mismo ya podemos ver mensajes llegando a nuestro topic `users`, la manera más fácil de hacerlo es a través de `Control Center`:

[Control Center](http://localhost:9021/clusters/Nk018hRAQFytWskYqtQduw/management/topics)

O a través de CLI:

```bash
docker exec -it schema-registry kafka-avro-console-consumer --bootstrap-server broker-1:29092 --topic users --property schema.registry.url=http://schema-registry:8081 --property print.key=true --property print.partition=true --property print.offset=true  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```


![til](assets/source-datagen-users.jpg)

Otro dato importante de este conector es que por defecto está haciendo uso de un schema (que ha registrado por nosotros), esto nos será especialmente util para el siguiente ejercicio. Podemos ver este esquema en la pestaña schema del topic:

![til](assets/source-datagen-users-schema.jpg)

Haciendo uso, de nuevo del api de connect podemos recibir la información importante del conector, parar, reiniciar, borrar, comprobar status de nuestro conector, etc:

Consulta Connector información:

```bash
curl http://localhost:8083/connectors/source-datagen-users | jq
```

```json
{
  "name": "source-datagen-users",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "quickstart": "users",
    "tasks.max": "1",
    "value.converter.schemas.enable": "false",
    "name": "source-datagen-users",
    "kafka.topic": "users",
    "max.interval": "1000",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "iterations": "10000000"
  },
  "tasks": [
    {
      "connector": "source-datagen-users",
      "task": 0
    }
  ],
  "type": "source"
}
```

## Connector Status

Para comprobar el estado de un conector:

```bash
curl -s http://localhost:8083/connectors/source-datagen-users/status | jq

```

```json
{
  "name": "source-datagen-users",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "source"
}

```

Este `endpoint` es uno de los más importantes ya que no solo no dará información de las tareas y los nodos en que corren estas sino que en caso de que alguna estuviera en estado de fallo podríamos ver los logs asociados al mismo.

Ejemplo:

```json
{
    "name": "sink-hdfs-connector",
    "connector": {
        "state": "RUNNING",
        "worker_id": "example:8083"
    },
    "tasks":
    [
        {
            "id": 0,
            "state": "RUNNING",
            "worker_id": "example:8083"
        },
        {
            "id": 1,
            "state": "FAILED",
            "worker_id": "example:8083",
            "trace": "org.apache.kafka.common.errors.RecordTooLargeException\n"
        }
    ]
}
```

## Connector Stop

Para detener un conector de forma controlada:

```bash
curl -s -X PUT http://localhost:8083/connectors/source-datagen-users/stop
```

Volvemos a comprobar su status:

```bash
curl http://localhost:8083/connectors/source-datagen-users/status | jq
```

```json
{
  "name": "source-datagen-users",
  "connector": {
    "state": "STOPPED",
    "worker_id": "connect:8083"
  },
  "tasks": [],
  "type": "source"
}
```

## Connector Pause

Permite pausar el connector (la aplicación sigue corriendo pero los **productores** están parados):

```bash
 curl -X PUT http://localhost:8083/connectors/source-datagen-users/pause
```

```bash
 curl http://localhost:8083/connectors/source-datagen-users/status | jq
```

```json
{
  "name": "source-datagen-users",
  "connector": {
    "state": "PAUSED",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "PAUSED",
      "worker_id": "connect:8083"
    }
  ],
  "type": "source"
}
```
## Connector Resume

Permite continuar donde los dejamos sin reiniciar:

```bash
curl -X PUT http://localhost:8083/connectors/source-datagen-users/resume &&
curl http://localhost:8083/connectors/source-datagen-users/status | jq
```

```json
{
  "name": "source-datagen-users",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "source"
}

```

## Connector Restart:

Reinicia el conector:

```bash
curl -X POST http://localhost:8083/connectors/source-datagen-users/restart &&
curl http://localhost:8083/connectors/source-datagen-users/status | jq
```

```json
{
  "name": "source-datagen-users",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "source"
}
```

> Nota: Todas estas operaciones podrían hacerse a nivel de TASK añadiendo el task id al path:

`curl http://localhost:8083/connectors/source-datagen-users/tasks/0/status`

```json
{
  "id": 0,
  "state": "RUNNING",
  "worker_id": "connect:8083"
}
```
## Connector Delete

Elimina el connector:

```bash
curl -X DELETE http://localhost:8083/connectors/source-datagen-users
```

Si comprobamos los conectores de nuestro cluster veremos que ahora esta vacío.

```bash
curl http://localhost:8083/connectors/ | jq
```

```json
[]
```

## Create MySQL Sink Connector Instance

En este caso leeremos los datos que hemos creado con el conector previo en el topic `users` y volcaremos los datos en una tabla en una instancia de MySQL disponible en nuestro entorno docker-compose

Para ello usaremos el [JDBC Sink Connector](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc)

para crear este conector de ejemplo usaremos esta configuracion:

```json
{
  "name": "sink-mysql-users",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    "topics": "users",
    "auto.create": "true",
    "auto.evolve": "true",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "false"
  }
}
```

en la que proveemos los datos de conexion con nuestra instancia de mysql y dejamos al connector que haga el resto por nosotros.

Creamos el nuevo conector usando este fichero:

```bash
curl -s -d @"./connectors/sink-mysql-users.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
```

```json
{
  "name": "sink-mysql-users",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
    "topics": "users",
    "auto.create": "true",
    "auto.evolve": "true",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "false",
    "name": "sink-mysql-users"
  },
  "tasks": [],
  "type": "sink"
}
```
```bash
curl -s http://localhost:8083/connectors/sink-mysql-users/status | jq
```

```json
{
  "name": "sink-mysql-users",
  "connector": {
    "state": "RUNNING",
    "worker_id": "connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "connect:8083"
    }
  ],
  "type": "sink"
}

```

Usaremos comandos MySQL ejecutados dentro del contenedor para observar que es lo que esta pasando en la base de datos.

```bash
docker exec -it mysql /bin/bash
```

```bash
mysql --user=root --password=password --database=db
```

```
bash-4.4# mysql --user=root --password=password --database=db
mysql: [Warning] Using a password on the command line interface can be insecure.
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 11
Server version: 8.3.0 MySQL Community Server - GPL

Copyright (c) 2000, 2024, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show tables;
+--------------+
| Tables_in_db |
+--------------+
| users        |
+--------------+
1 row in set (0.00 sec)

mysql> describe users;
+--------------+--------+------+-----+---------+-------+
| Field        | Type   | Null | Key | Default | Extra |
+--------------+--------+------+-----+---------+-------+
| registertime | bigint | NO   |     | NULL    |       |
| userid       | text   | NO   |     | NULL    |       |
| regionid     | text   | NO   |     | NULL    |       |
| gender       | text   | NO   |     | NULL    |       |
+--------------+--------+------+-----+---------+-------+
4 rows in set (0.00 sec)
```

> La configuracion **auto.create** del conector JDBC permite crear una tabla (digamos que ejecuta un DDL) basándose en el schema del topic pero siempre podremos indicarle entre otras propiedades la tabla y schema donde queramos que escriba
>
> Para ver más datos sobre las opciones de este conector podéis echar un ojo a la [referencia](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/sink_config_options.html#sink-config-options)

Y si lanzamos una `SELECT` sobre ella veremos como los datos van escribiendo:

```
mysql> select * from users order by registertime desc limit 10;
+---------------+--------+----------+--------+
| registertime  | userid | regionid | gender |
+---------------+--------+----------+--------+
| 1519252877093 | User_6 | Region_3 | FEMALE |
| 1519248799700 | User_9 | Region_1 | FEMALE |
| 1519208652935 | User_7 | Region_9 | FEMALE |
| 1519180270654 | User_2 | Region_9 | FEMALE |
| 1519176112399 | User_1 | Region_1 | FEMALE |
| 1519161592130 | User_9 | Region_7 | FEMALE |
| 1519156913486 | User_5 | Region_1 | OTHER  |
| 1519153963348 | User_8 | Region_5 | OTHER  |
| 1519136639062 | User_7 | Region_3 | OTHER  |
| 1519135224002 | User_1 | Region_4 | FEMALE |
+---------------+--------+----------+--------+
10 rows in set (0.00 sec)
```

```
mysql> select *,from_unixtime(registertime/1000) from users order by registertime desc limit 10;
+---------------+--------+----------+--------+----------------------------------+
| registertime  | userid | regionid | gender | from_unixtime(registertime/1000) |
+---------------+--------+----------+--------+----------------------------------+
| 1519269649108 | User_5 | Region_4 | FEMALE | 2018-02-22 03:20:49.1080         |
| 1519261872179 | User_1 | Region_2 | MALE   | 2018-02-22 01:11:12.1790         |
| 1519180613818 | User_8 | Region_7 | OTHER  | 2018-02-21 02:36:53.8180         |
| 1519176407977 | User_7 | Region_1 | MALE   | 2018-02-21 01:26:47.9770         |
| 1519160704236 | User_6 | Region_5 | MALE   | 2018-02-20 21:05:04.2360         |
| 1519153073388 | User_7 | Region_2 | MALE   | 2018-02-20 18:57:53.3880         |
| 1519132370588 | User_9 | Region_3 | OTHER  | 2018-02-20 13:12:50.5880         |
| 1519117514940 | User_2 | Region_1 | FEMALE | 2018-02-20 09:05:14.9400         |
| 1519100101404 | User_8 | Region_6 | OTHER  | 2018-02-20 04:15:01.4040         |
| 1519099280461 | User_3 | Region_5 | OTHER  | 2018-02-20 04:01:20.4610         |
+---------------+--------+----------+--------+----------------------------------+
```

```
mysql> quit
Bye
bash-4.4# exit
```
## Script de Instalación de Plugins

Para poder tener como base para experimentar con KSQLDB, KAFKA STREAMS o FLINK con lo trabajado hasta ahora en la carpeta `1.environment` teneis disponible el script `install-connect-plugins.sh` que automatiza la instalación de varios plugins.

> Nota: Este script es una manera de ponernos en un punto avanzado desde 0, la recomendación para el correcto aprendizaje es realizar todos los ejercicios en el orden propuesto.

## Debug 🪲

Muchas veces al configurar los conectores obtendremos errores.

Para poder depurar correctamente los logs debemos usar este comando:

```bash
docker logs -f connect
```

# Ejercicio 2

Antes de continuar debes parar los dos conectores del ejercicio anterior.

En este segundo ejercicio vamos a practicar con el mismo dataset pero vamos a hacer uso de una SMT para cambiar el tipo del campo registertime a timestamp.

Creamos el nuevo conector datagen usando esta configuración:

```json
{
  "name": "source-datagen-users-v2",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "users",
    "quickstart": "users",
    "max.interval": 1000,
    "iterations": 10000000,
    "tasks.max": "1",
    "transforms": "tsconverter",
    "transforms.tsconverter.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
    "transforms.tsconverter.field": "registertime",
    "transforms.tsconverter.target.type": "Timestamp",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "false"
  }
}
```
Es la misma configuración que antes pero en este caso añadiendo la transformación.

```bash
curl -s -d @"./connectors/source-datagen-users-v2.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
```

¡Ojo! el cambio de tipo de datos en un campo de un schema nunca es un cambio válido en la evolución de un schema, pero en este caso, el tipo sigue siendo long, lo único que ha cambiado en el schema avro es el tipo lógico (**logical-type**), y por lo tanto si es compatible

[users-value](http://localhost:9021/clusters/Nk018hRAQFytWskYqtQduw/management/topics/users/schema/value)

Debería haber una nueva versión del schema en el subject!

Si verificamos la tabla de la base de datos, nada ha cambiado ya que la columna sigue siendo de tipo bigint.

```
mysql> describe users;
+--------------+--------+------+-----+---------+-------+
| Field        | Type   | Null | Key | Default | Extra |
+--------------+--------+------+-----+---------+-------+
| registertime | bigint | NO   |     | NULL    |       |
| userid       | text   | NO   |     | NULL    |       |
| regionid     | text   | NO   |     | NULL    |       |
| gender       | text   | NO   |     | NULL    |       |
+--------------+--------+------+-----+---------+-------+
4 rows in set (0.01 sec)
```

Si quisiéramos que la tabla en base de datos tuviera el campo con tipo timestamp, necesitamos crear una nueva tabla

```json
{
    "name": "sink-mysql-users-v2",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": "jdbc:mysql://mysql:3306/db?user=user&password=password&useSSL=false",
        "topics": "users",
        "table.name.format":"${topic}_v2",
        "auto.create": "true",
        "auto.evolve": "true",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter.schemas.enable": "false",
        "value.converter.use.latest.version" : "true"
    }
}
```
Lo importante en esta configuración es la linea
1. `table.name.format` : "${topic}_v2" para crear una nueva tabla con el sufijo _v2
2. `value.converter.use.latest.version` : para que el schema de la tabla sea acorde a la ultima version (define registertime como timestamp)

```bash
curl -s -d @"./connectors/sink-mysql-users-v2.json" -H "Content-Type: application/json" -X POST http://localhost:8083/connectors | jq
```

```bash
mysql> show tables;
+--------------+
| Tables_in_db |
+--------------+
| users        |
| users_v2     |
+--------------+
2 rows in set (0.01 sec)

mysql> describe users_v2;
+--------------+-------------+------+-----+---------+-------+
| Field        | Type        | Null | Key | Default | Extra |
+--------------+-------------+------+-----+---------+-------+
| registertime | datetime(3) | NO   |     | NULL    |       |
| userid       | text        | NO   |     | NULL    |       |
| regionid     | text        | NO   |     | NULL    |       |
| gender       | text        | NO   |     | NULL    |       |
+--------------+-------------+------+-----+---------+-------+
4 rows in set (0.00 sec)

```
