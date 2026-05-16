package com.todbconverter.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConfigTest {

    private DatabaseConfig config;
    private Properties props;

    @BeforeEach
    void setUp() {
        props = new Properties();
        props.setProperty("postgres.host", "localhost");
        props.setProperty("postgres.port", "5432");
        props.setProperty("postgres.database", "testdb");
        props.setProperty("postgres.username", "user");
        props.setProperty("postgres.password", "pass");
        props.setProperty("mongo.host", "localhost");
        props.setProperty("mongo.port", "27017");
        props.setProperty("mongo.database", "testdb");
        config = new DatabaseConfig(props);
    }

    @Test
    void perTableStrategy_specificOverridesDefault() {
        props.setProperty("relationship.strategy.default", "EMBED");
        props.setProperty("relationship.strategy.logs", "REFERENCE");

        assertEquals(DatabaseConfig.RelationshipStrategy.EMBED, config.getRelationshipStrategy("orders"));
        assertEquals(DatabaseConfig.RelationshipStrategy.REFERENCE, config.getRelationshipStrategy("logs"));
    }

    @Test
    void perTableStrategy_noOverrideUsesDefault() {
        props.setProperty("relationship.strategy.default", "REFERENCE");

        assertEquals(DatabaseConfig.RelationshipStrategy.REFERENCE, config.getRelationshipStrategy("orders"));
        assertEquals(DatabaseConfig.RelationshipStrategy.REFERENCE, config.getRelationshipStrategy("logs"));
    }

    @Test
    void perTableStrategy_defaultIsEmbedWhenNotSet() {
        assertEquals(DatabaseConfig.RelationshipStrategy.EMBED, config.getRelationshipStrategy("any_table"));
    }

    @Test
    void perTableStrategy_invalidValueFallsBackToDefault() {
        props.setProperty("relationship.strategy.broken", "INVALID");

        assertEquals(DatabaseConfig.RelationshipStrategy.EMBED, config.getRelationshipStrategy("broken"));
    }

    @Test
    void manyToManyMode_fullByIds() {
        props.setProperty("relationship.mn_mode.default", "IDS");
        props.setProperty("relationship.mn_mode.students_courses", "FULL");

        assertEquals(DatabaseConfig.ManyToManyMode.FULL, config.getManyToManyMode("students", "courses"));
        assertEquals(DatabaseConfig.ManyToManyMode.IDS, config.getManyToManyMode("actors", "movies"));
    }

    @Test
    void manyToManyMode_defaultIsFull() {
        assertEquals(DatabaseConfig.ManyToManyMode.FULL, config.getManyToManyMode("a", "b"));
    }

    @Test
    void manyToManyMode_invalidValueFallsBackToDefault() {
        props.setProperty("relationship.mn_mode.x_y", "BAD");

        assertEquals(DatabaseConfig.ManyToManyMode.FULL, config.getManyToManyMode("x", "y"));
    }

    @Test
    void warnThreshold_defaultIs1000() {
        assertEquals(1000, config.getWarnThreshold());
    }

    @Test
    void warnThreshold_customValue() {
        props.setProperty("relationship.warn_threshold", "500");
        assertEquals(500, config.getWarnThreshold());
    }

    @Test
    void warnThreshold_invalidValueFallsBackToDefault() {
        props.setProperty("relationship.warn_threshold", "abc");
        assertEquals(1000, config.getWarnThreshold());
    }

    @Test
    void useReferencingStrategy_backwardCompatible() {
        props.setProperty("relationship.strategy", "referencing");
        assertTrue(config.useReferencingStrategy());

        props.setProperty("relationship.strategy", "embedding");
        assertFalse(config.useReferencingStrategy());
    }

    @Test
    void relationshipStrategyEnums_values() {
        assertEquals(2, DatabaseConfig.RelationshipStrategy.values().length);
        assertNotNull(DatabaseConfig.RelationshipStrategy.EMBED);
        assertNotNull(DatabaseConfig.RelationshipStrategy.REFERENCE);

        assertEquals(2, DatabaseConfig.ManyToManyMode.values().length);
        assertNotNull(DatabaseConfig.ManyToManyMode.FULL);
        assertNotNull(DatabaseConfig.ManyToManyMode.IDS);
    }
}
