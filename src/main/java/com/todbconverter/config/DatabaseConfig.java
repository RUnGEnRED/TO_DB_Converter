package com.todbconverter.config;

import com.todbconverter.core.model.Strategy;
import com.todbconverter.exception.ConfigException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Configuration for database connections and conversion strategies.
 */
public class DatabaseConfig {

    // Source Database
    private String sourceJdbcUrl;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDriver;

    // Target MongoDB
    private String targetMongoUri;
    private String targetDatabase;

    // Relationship Strategies (edge-based)
    private final Map<String, Strategy> relationshipStrategies = new HashMap<>();
    private Strategy defaultStrategy = Strategy.EMBED;

    // Design Patterns (simplified - stored as string configs)
    private final Map<String, Map<String, String>> patterns = new HashMap<>();

    // Safeguards
    private int maxChildrenPerParent = 1000;

    public DatabaseConfig() {
    }

    // === Getters and Setters ===

    public String getSourceJdbcUrl() { return sourceJdbcUrl; }
    public void setSourceJdbcUrl(String sourceJdbcUrl) { this.sourceJdbcUrl = sourceJdbcUrl; }

    public String getSourceUsername() { return sourceUsername; }
    public void setSourceUsername(String sourceUsername) { this.sourceUsername = sourceUsername; }

    public String getSourcePassword() { return sourcePassword; }
    public void setSourcePassword(String sourcePassword) { this.sourcePassword = sourcePassword; }

    public String getSourceDriver() { return sourceDriver; }
    public void setSourceDriver(String sourceDriver) { this.sourceDriver = sourceDriver; }

    public String getTargetMongoUri() { return targetMongoUri; }
    public void setTargetMongoUri(String targetMongoUri) { this.targetMongoUri = targetMongoUri; }

    public String getTargetDatabase() { return targetDatabase; }
    public void setTargetDatabase(String targetDatabase) { this.targetDatabase = targetDatabase; }

    public Strategy getDefaultStrategy() { return defaultStrategy; }
    public void setDefaultStrategy(Strategy defaultStrategy) { this.defaultStrategy = defaultStrategy; }

    public int getMaxChildrenPerParent() { return maxChildrenPerParent; }
    public void setMaxChildrenPerParent(int maxChildrenPerParent) { this.maxChildrenPerParent = maxChildrenPerParent; }

    // === Strategy Methods ===

    /**
     * Get strategy for a specific edge (fkTableName = child, pkTableName = parent).
     */
    public Strategy getStrategy(String fkTableName, String pkTableName) {
        String key1 = fkTableName + "." + pkTableName;
        if (relationshipStrategies.containsKey(key1)) {
            return relationshipStrategies.get(key1);
        }
        String key2 = pkTableName + "." + fkTableName;
        return relationshipStrategies.getOrDefault(key2, defaultStrategy);
    }

    /**
     * Set strategy for a specific edge.
     */
    public void setStrategy(String fkTableName, String pkTableName, Strategy strategy) {
        String key = fkTableName + "." + pkTableName;
        relationshipStrategies.put(key, strategy);
    }

    /**
     * Get all configured strategies.
     */
    public Map<String, Strategy> getRelationshipStrategies() {
        return Collections.unmodifiableMap(relationshipStrategies);
    }

    // === Pattern Methods ===

    /**
     * Get pattern configuration for a table.
     */
    public Map<String, String> getPatternConfig(String tableName) {
        Map<String, String> m = patterns.get(tableName);
        return m == null ? Collections.emptyMap() : Collections.unmodifiableMap(m);
    }

    /**
     * Set pattern configuration for a table.
     */
    public void setPatternConfig(String tableName, String patternType, String config) {
        patterns.computeIfAbsent(tableName, k -> new HashMap<>()).put(patternType, config);
    }

    /**
     * Remove pattern configuration for a table.
     */
    public void removePatternConfig(String tableName, String patternType) {
        Map<String, String> m = patterns.get(tableName);
        if (m != null) { m.remove(patternType); if (m.isEmpty()) patterns.remove(tableName); }
    }

    // === I/O Methods ===

    /**
     * Load configuration from a properties file.
     */
    public static DatabaseConfig loadFromFile(String filePath) throws ConfigException {
        DatabaseConfig config = new DatabaseConfig();
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new ConfigException("Configuration file not found: " + filePath);
        }

        Properties props = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            props.load(input);
        } catch (IOException e) {
            throw new ConfigException("Failed to read configuration file: " + filePath, e);
        }

        // Source Database
        config.sourceJdbcUrl = props.getProperty("source.jdbc.url");
        config.sourceUsername = props.getProperty("source.jdbc.username");
        config.sourcePassword = props.getProperty("source.jdbc.password");
        config.sourceDriver = props.getProperty("source.jdbc.driver");

        // Target MongoDB
        config.targetMongoUri = props.getProperty("target.mongodb.uri");
        config.targetDatabase = props.getProperty("target.mongodb.database");

        // Default strategy
        String defaultStrategyStr = props.getProperty("relationship.strategy.default", "EMBED");
        config.defaultStrategy = Strategy.valueOf(defaultStrategyStr.toUpperCase());

        // Edge strategies
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("relationship.strategy.") && !key.equals("relationship.strategy.default")) {
                String edge = key.substring("relationship.strategy.".length());
                Strategy strategy = Strategy.valueOf(props.getProperty(key).toUpperCase());
                config.relationshipStrategies.put(edge, strategy);
            }
        }

        // Patterns
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("pattern.")) {
                String[] parts = key.substring("pattern.".length()).split("\\.", 2);
                if (parts.length == 2) {
                    String patternType = parts[0];
                    String tableName = parts[1];
                    String value = props.getProperty(key);
                    config.patterns.computeIfAbsent(tableName, k -> new HashMap<>())
                            .put(patternType, value);
                }
            }
        }

        // Safeguards
        String maxChildren = props.getProperty("safeguard.max_children_per_parent");
        if (maxChildren != null) {
            config.maxChildrenPerParent = Integer.parseInt(maxChildren);
        }

        return config;
    }

    /**
     * Save configuration to a properties file.
     */
    public void saveToFile(String filePath) throws ConfigException {
        Properties props = new Properties();

        // Source Database
        if (sourceJdbcUrl != null) props.setProperty("source.jdbc.url", sourceJdbcUrl);
        if (sourceUsername != null) props.setProperty("source.jdbc.username", sourceUsername);
        if (sourcePassword != null) props.setProperty("source.jdbc.password", sourcePassword);
        if (sourceDriver != null) props.setProperty("source.jdbc.driver", sourceDriver);

        // Target MongoDB
        if (targetMongoUri != null) props.setProperty("target.mongodb.uri", targetMongoUri);
        if (targetDatabase != null) props.setProperty("target.mongodb.database", targetDatabase);

        // Default strategy
        props.setProperty("relationship.strategy.default", defaultStrategy.name());

        // Edge strategies
        for (Map.Entry<String, Strategy> entry : relationshipStrategies.entrySet()) {
            props.setProperty("relationship.strategy." + entry.getKey(), entry.getValue().name());
        }

        // Patterns
        for (Map.Entry<String, Map<String, String>> tableEntry : patterns.entrySet()) {
            for (Map.Entry<String, String> patternEntry : tableEntry.getValue().entrySet()) {
                props.setProperty("pattern." + patternEntry.getKey() + "." + tableEntry.getKey(),
                        patternEntry.getValue());
            }
        }

        // Safeguards
        props.setProperty("safeguard.max_children_per_parent", String.valueOf(maxChildrenPerParent));

        try (OutputStream output = Files.newOutputStream(Paths.get(filePath))) {
            props.store(output, "TO_DB Converter Configuration");
        } catch (IOException e) {
            throw new ConfigException("Failed to write configuration file: " + filePath, e);
        }
    }

    /**
     * Create a default configuration.
     */
    public static DatabaseConfig defaults() {
        return new DatabaseConfig();
    }

    @Override
    public String toString() {
        return "DatabaseConfig{" +
                "sourceJdbcUrl='" + sourceJdbcUrl + '\'' +
                ", targetMongoUri='" + targetMongoUri + '\'' +
                ", targetDatabase='" + targetDatabase + '\'' +
                ", strategies=" + relationshipStrategies +
                ", patterns=" + patterns.keySet() +
                '}';
    }
}
