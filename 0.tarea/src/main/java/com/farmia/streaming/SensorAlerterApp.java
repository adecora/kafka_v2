package com.farmia.streaming;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.farmia.iot.AlertType;
import com.farmia.iot.SensorAlerts;
import com.farmia.iot.SensorTelemetry;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class SensorAlerterApp {

  private static final Logger LOG = LoggerFactory.getLogger(SensorAlerterApp.class);
  private final Serde<String> stringSerde = Serdes.String();
  public static final String APPLICATION_ID_CONFIG = "sensor-alerts-app";
  public static final String CONFIG_FILE = "/streams.properties";
  public static final String INPUT_TOPIC = "sensor-telemetry";
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

  public static void main(String[] args) throws IOException {
    final Properties props = ConfigLoader.loadConfig(CONFIG_FILE);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
    SensorAlerterApp sensorAlerterApp = new SensorAlerterApp();

    Topology topology = sensorAlerterApp.buildTopology(props);
    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, props)) {
      CountDownLatch countdownLatch = new CountDownLatch(1);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        LOG.info("Apagando la aplicación...");
        kafkaStreams.close(Duration.ofSeconds(5));
        countdownLatch.countDown();
      }));

      kafkaStreams.start();
      countdownLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Aplicación interrumpida", e);
      Thread.currentThread().interrupt();
    }
  }
}
