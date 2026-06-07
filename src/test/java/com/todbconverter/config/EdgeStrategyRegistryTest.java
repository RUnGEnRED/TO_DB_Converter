package com.todbconverter.config;

import com.todbconverter.core.model.Strategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for EdgeStrategyRegistry.
 */
class EdgeStrategyRegistryTest {

    @Test
    void shouldReturnDefaultStrategy() {
        EdgeStrategyRegistry registry = new EdgeStrategyRegistry(Strategy.REFERENCE);

        assertThat(registry.getStrategy("any", "table")).isEqualTo(Strategy.REFERENCE);
    }

    @Test
    void shouldSetAndGetStrategy() {
        EdgeStrategyRegistry registry = new EdgeStrategyRegistry();
        registry.setStrategy("customers", "addresses", Strategy.EMBED);

        assertThat(registry.getStrategy("customers", "addresses")).isEqualTo(Strategy.EMBED);
    }

    @Test
    void shouldOverrideDefaultWithSpecific() {
        EdgeStrategyRegistry registry = new EdgeStrategyRegistry(Strategy.REFERENCE);
        registry.setStrategy("customers", "addresses", Strategy.EMBED);

        assertThat(registry.getStrategy("customers", "addresses")).isEqualTo(Strategy.EMBED);
        assertThat(registry.getStrategy("other", "table")).isEqualTo(Strategy.REFERENCE);
    }

    @Test
    void shouldLoadFromConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDefaultStrategy(Strategy.REFERENCE);
        config.setStrategy("a", "b", Strategy.EMBED);

        EdgeStrategyRegistry registry = EdgeStrategyRegistry.fromConfig(config);

        assertThat(registry.getDefaultStrategy()).isEqualTo(Strategy.REFERENCE);
        assertThat(registry.getStrategy("a", "b")).isEqualTo(Strategy.EMBED);
    }
}
