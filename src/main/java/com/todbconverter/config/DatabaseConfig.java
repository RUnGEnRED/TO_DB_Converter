package com.todbconverter.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DatabaseConfig {
    private final Properties properties;

    public DatabaseConfig(String configFile) throws IOException {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                throw new IOException("Unable to find " + configFile);
            }
            properties.load(input);
        }
    }

    public String getPostgresHost() {
        return properties.getProperty("postgres.host", "localhost");
    }

    public int getPostgresPort() {
        return Integer.parseInt(properties.getProperty("postgres.port", "5432"));
    }

    public String getPostgresDatabase() {
        return properties.getProperty("postgres.database");
    }

    public String getPostgresUsername() {
        return properties.getProperty("postgres.username");
    }

    public String getPostgresPassword() {
        return properties.getProperty("postgres.password");
    }

    public String getPostgresSchema() {
        return properties.getProperty("postgres.schema", "public");
    }

    public String getMongoHost() {
        return properties.getProperty("mongo.host", "localhost");
    }

    public int getMongoPort() {
        return Integer.parseInt(properties.getProperty("mongo.port", "27017"));
    }

    public String getMongoDatabase() {
        return properties.getProperty("mongo.database");
    }

    public String getMongoUsername() {
        return properties.getProperty("mongo.username");
    }

    public String getMongoPassword() {
        return properties.getProperty("mongo.password");
    }

    public String getMongoConnectionString() {
        return properties.getProperty("mongo.connectionString");
    }
}
