package com.farmia.streaming;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.farmia.sales.SalesSummary;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class SalesSummaryApp {

  private static final Logger LOG = LoggerFactory.getLogger(SalesSummaryApp.class);
  private final Serde<String> stringSerde = Serdes.String();
  public static final String APPLICATION_ID_CONFIG = "sales-summary-app";
  public static final String CONFIG_FILE = "/streams.properties";
  public static final String INPUT_TOPIC = "sales_transactions";
  public static final String OUTPUT_TOPIC = "sales-summary";

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

  public static void main(String[] args) throws IOException {
    final Properties props = ConfigLoader.loadConfig(CONFIG_FILE);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
    SalesSummaryApp salesSummaryApp = new SalesSummaryApp();

    Topology topology = salesSummaryApp.buildTopology(props);
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
