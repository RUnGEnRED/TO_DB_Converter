package com.todbconverter.config;

import com.todbconverter.core.model.Strategy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for managing relationship strategies on a per-edge basis.
 */
public class EdgeStrategyRegistry {

    private final Map<String, Strategy> strategies;
    private Strategy defaultStrategy;

    public EdgeStrategyRegistry() {
        this.strategies = new HashMap<>();
        this.defaultStrategy = Strategy.EMBED;
    }

    public EdgeStrategyRegistry(Strategy defaultStrategy) {
        this.strategies = new HashMap<>();
        this.defaultStrategy = defaultStrategy;
    }

    /**
     * Get strategy for a specific edge (parent -> child).
     *
     * @param parentTable the parent table name
     * @param childTable  the child table name
     * @return the strategy (EMBED or REFERENCE)
     */
    public Strategy getStrategy(String parentTable, String childTable) {
        String key1 = parentTable + "." + childTable;
        if (strategies.containsKey(key1)) {
            return strategies.get(key1);
        }
        String key2 = childTable + "." + parentTable;
        return strategies.getOrDefault(key2, defaultStrategy);
    }

    /**
     * Set strategy for a specific edge.
     */
    public void setStrategy(String parentTable, String childTable, Strategy strategy) {
        String key = parentTable + "." + childTable;
        strategies.put(key, strategy);
    }

    /**
     * Set the default strategy for all edges.
     */
    public void setDefaultStrategy(Strategy defaultStrategy) {
        this.defaultStrategy = defaultStrategy;
    }

    /**
     * Get the default strategy.
     */
    public Strategy getDefaultStrategy() {
        return defaultStrategy;
    }

    /**
     * Get all configured strategies.
     */
    public Map<String, Strategy> getAllStrategies() {
        return Collections.unmodifiableMap(strategies);
    }

    /**
     * Load strategies from a DatabaseConfig.
     */
    public static EdgeStrategyRegistry fromConfig(DatabaseConfig config) {
        EdgeStrategyRegistry registry = new EdgeStrategyRegistry(config.getDefaultStrategy());
        registry.strategies.putAll(config.getRelationshipStrategies());
        return registry;
    }

    @Override
    public String toString() {
        return "EdgeStrategyRegistry{" +
                "strategies=" + strategies +
                ", defaultStrategy=" + defaultStrategy +
                '}';
    }
}
