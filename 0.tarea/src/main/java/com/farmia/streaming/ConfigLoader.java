package com.farmia.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.checkerframework.checker.nullness.qual.NonNull;

public class ConfigLoader {

  private ConfigLoader() {
    // Constructor privado para evitar instanciación
  }

  public static @NonNull
  Properties loadConfig(final String configFile) throws IOException {
    final Properties cfg = new Properties();
    try (InputStream configInputStream = ConfigLoader.class.getResourceAsStream(configFile)) {

      if (configInputStream == null) {
        throw new IOException(configFile + " no se encontró en el classpath.");
      }

      cfg.load(configInputStream);
    }

    return cfg;
  }
}
