package com.todbconverter.config;

import com.todbconverter.core.model.Strategy;
import com.todbconverter.exception.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for DatabaseConfig.
 */
class DatabaseConfigTest {

    @Test
    void shouldLoadConfigFromFile(@TempDir Path tempDir) throws IOException, ConfigException {
        Path configFile = tempDir.resolve("test.properties");
        Files.writeString(configFile, """
                source.jdbc.url=jdbc:postgresql://localhost:5433/testdb
                source.jdbc.username=testuser
                source.jdbc.password=testpass
                source.jdbc.driver=org.postgresql.Driver
                target.mongodb.uri=mongodb://localhost:27017
                target.mongodb.database=testdb
                relationship.strategy.default=EMBED
                relationship.strategy.customers.addresses=EMBED
                relationship.strategy.cities.addresses=REFERENCE
                safeguard.max_children_per_parent=500
                """);

        DatabaseConfig config = DatabaseConfig.loadFromFile(configFile.toString());

        assertThat(config.getSourceJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5433/testdb");
        assertThat(config.getSourceUsername()).isEqualTo("testuser");
        assertThat(config.getSourcePassword()).isEqualTo("testpass");
        assertThat(config.getSourceDriver()).isEqualTo("org.postgresql.Driver");
        assertThat(config.getTargetMongoUri()).isEqualTo("mongodb://localhost:27017");
        assertThat(config.getTargetDatabase()).isEqualTo("testdb");
        assertThat(config.getDefaultStrategy()).isEqualTo(Strategy.EMBED);
        assertThat(config.getStrategy("customers", "addresses")).isEqualTo(Strategy.EMBED);
        assertThat(config.getStrategy("cities", "addresses")).isEqualTo(Strategy.REFERENCE);
        assertThat(config.getMaxChildrenPerParent()).isEqualTo(500);
    }

    @Test
    void shouldThrowWhenFileNotFound() {
        assertThatThrownBy(() -> DatabaseConfig.loadFromFile("nonexistent.properties"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void shouldSaveConfigToFile(@TempDir Path tempDir) throws ConfigException, IOException {
        Path configFile = tempDir.resolve("output.properties");

        DatabaseConfig config = new DatabaseConfig();
        config.setSourceJdbcUrl("jdbc:postgresql://localhost:5433/testdb");
        config.setSourceUsername("testuser");
        config.setSourcePassword("testpass");
        config.setTargetMongoUri("mongodb://localhost:27017");
        config.setTargetDatabase("testdb");
        config.setStrategy("customers", "addresses", Strategy.EMBED);

        config.saveToFile(configFile.toString());

        assertThat(Files.exists(configFile)).isTrue();

        // Reload and verify
        DatabaseConfig loaded = DatabaseConfig.loadFromFile(configFile.toString());
        assertThat(loaded.getSourceJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5433/testdb");
        assertThat(loaded.getStrategy("customers", "addresses")).isEqualTo(Strategy.EMBED);
    }

    @Test
    void shouldReturnDefaultStrategyForUnconfiguredEdge() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDefaultStrategy(Strategy.REFERENCE);

        assertThat(config.getStrategy("unknown", "table")).isEqualTo(Strategy.REFERENCE);
    }
}
