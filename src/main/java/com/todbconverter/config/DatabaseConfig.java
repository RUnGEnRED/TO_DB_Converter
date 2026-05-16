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
    
    public DatabaseConfig() {
        this.properties = new Properties();
    }

    public DatabaseConfig(Properties properties) {
        this.properties = properties;
    }

    public DatabaseConfig(String configFile) throws IOException {
        this();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                throw new IOException("Unable to find " + configFile);
            }
            properties.load(input);
        }
    }
    
    public Properties getProperties() {
        return properties;
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

    public enum RelationshipStrategy {
        EMBED, REFERENCE
    }

    public enum ManyToManyMode {
        FULL, IDS
    }

    public boolean useReferencingStrategy() {
        return "referencing".equalsIgnoreCase(properties.getProperty("relationship.strategy", "embedding"));
    }

    public RelationshipStrategy getRelationshipStrategy(String tableName) {
        String value = properties.getProperty("relationship.strategy." + tableName);
        if (value == null) {
            value = properties.getProperty("relationship.strategy.default");
        }
        if (value == null) {
            value = properties.getProperty("relationship.strategy", "EMBED");
        }
        try {
            String upper = value.toUpperCase();
            if ("REFERENCING".equals(upper)) return RelationshipStrategy.REFERENCE;
            if ("EMBEDDING".equals(upper)) return RelationshipStrategy.EMBED;
            return RelationshipStrategy.valueOf(upper);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid relationship strategy '{}' for table '{}', using EMBED", value, tableName);
            return RelationshipStrategy.EMBED;
        }
    }

    public ManyToManyMode getManyToManyMode(String parentTable, String childTable) {
        String key = "relationship.mn_mode." + parentTable + "_" + childTable;
        String defaultValue = properties.getProperty("relationship.mn_mode.default", "FULL");
        String value = properties.getProperty(key, defaultValue);
        try {
            return ManyToManyMode.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid M:N mode '{}' for {}_{}, using FULL", value, parentTable, childTable);
            return ManyToManyMode.FULL;
        }
    }

    public int getWarnThreshold() {
        String val = properties.getProperty("relationship.warn_threshold", "1000");
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return 1000;
        }
    }
    
    public boolean useAttributePattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.attribute.enabled", "true"));
    }
    
    public int getAttributePatternThreshold() {
        try {
            return Integer.parseInt(properties.getProperty("pattern.attribute.threshold", "2"));
        } catch (NumberFormatException e) {
            return 2;
        }
    }
    
    public boolean useBucketPattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.bucket.enabled", "false"));
    }
    
    public int getBucketSize() {
        try {
            return Integer.parseInt(properties.getProperty("pattern.bucket.size", "10"));
        } catch (NumberFormatException e) {
            return 10;
        }
    }
    
    public boolean useSubsetPattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.subset.enabled", "false"));
    }
    
    public int getSubsetLimit() {
        try {
            return Integer.parseInt(properties.getProperty("pattern.subset.limit", "10"));
        } catch (NumberFormatException e) {
            return 10;
        }
    }
    
    public boolean useOutlierPattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.outlier.enabled", "false"));
    }
    
    public int getOutlierThreshold() {
        try {
            return Integer.parseInt(properties.getProperty("pattern.outlier.threshold", "50"));
        } catch (NumberFormatException e) {
            return 50;
        }
    }

    public String getBucketKey() {
        return properties.getProperty("pattern.bucket.key");
    }

    public boolean useComputedPattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.computed.enabled", "false"));
    }

    public String getComputedFields() {
        return properties.getProperty("pattern.computed.fields", "");
    }

    public boolean useApproximationPattern() {
        return Boolean.parseBoolean(properties.getProperty("pattern.approximation.enabled", "false"));
    }

    public int getApproximationGranularity() {
        try {
            return Integer.parseInt(properties.getProperty("pattern.approximation.granularity", "100"));
        } catch (NumberFormatException e) {
            return 100;
        }
    }

    public String getApproximationFields() {
        return properties.getProperty("pattern.approximation.fields", "");
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
