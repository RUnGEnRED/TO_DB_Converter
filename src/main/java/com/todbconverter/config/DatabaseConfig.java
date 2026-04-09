package com.todbconverter.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    private static final String VALID_SCHEMA_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";
    private static final String VALID_QUOTED_SCHEMA_PATTERN = "^\"[^\"]+\"$";
    
    private boolean isValidSchemaName(String schema) {
        return schema.matches(VALID_SCHEMA_PATTERN) || schema.matches(VALID_QUOTED_SCHEMA_PATTERN);
    }
    
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
        String portStr = properties.getProperty("postgres.port", "5432");
        try {
            int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid PostgreSQL port: " + portStr);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid PostgreSQL port: " + portStr);
        }
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
        String schema = properties.getProperty("postgres.schema", "public");
        if (!isValidSchemaName(schema)) {
            logger.warn("Invalid schema name '{}' - using 'public' instead", schema);
            return "public";
        }
        return schema;
    }

    public String getMongoHost() {
        return properties.getProperty("mongo.host", "localhost");
    }

    public int getMongoPort() {
        String portStr = properties.getProperty("mongo.port", "27017");
        try {
            int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Invalid MongoDB port: " + portStr);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid MongoDB port: " + portStr);
        }
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

    public enum ConversionDirection {
        POSTGRES_TO_MONGO,
        MONGO_TO_POSTGRES
    }

    public ConversionDirection getConversionDirection() {
        String directionStr = properties.getProperty("conversion.direction", "POSTGRES_TO_MONGO");
        try {
            return ConversionDirection.valueOf(directionStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ConversionDirection.POSTGRES_TO_MONGO;
        }
    }

    public boolean shouldDropExistingTables() {
        return Boolean.parseBoolean(properties.getProperty("postgres.dropExistingTables", "true"));
    }

    public ConversionDirection overrideDirection(String cliDirection) {
        if (cliDirection == null || cliDirection.isEmpty()) {
            return getConversionDirection();
        }
        try {
            ConversionDirection direction = ConversionDirection.valueOf(cliDirection.toUpperCase());
            logger.info("Using CLI direction: {}", direction);
            return direction;
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid CLI direction '{}' - using default from config", cliDirection);
            return getConversionDirection();
        }
    }
}
