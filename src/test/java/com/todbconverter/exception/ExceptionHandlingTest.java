package com.todbconverter.exception;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.JDBCConnection;
import com.todbconverter.connection.MongoDBConnection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for exception handling.
 */
class ExceptionHandlingTest {

    @Test
    void shouldThrowSourceConnectionException() {
        SourceConnectionException ex = new SourceConnectionException("Connection failed");

        assertThat(ex.getMessage()).isEqualTo("Connection failed");
        assertThat(ex).isInstanceOf(ConnectionException.class);
        assertThat(ex).isInstanceOf(ConverterException.class);
    }

    @Test
    void shouldThrowSourceConnectionExceptionWithCause() {
        Exception cause = new RuntimeException("Root cause");
        SourceConnectionException ex = new SourceConnectionException("Connection failed", cause);

        assertThat(ex.getCause()).isEqualTo(cause);
    }

    @Test
    void shouldThrowTargetConnectionException() {
        TargetConnectionException ex = new TargetConnectionException("MongoDB unreachable");

        assertThat(ex.getMessage()).isEqualTo("MongoDB unreachable");
        assertThat(ex).isInstanceOf(ConnectionException.class);
    }

    @Test
    void shouldThrowConfigException() {
        ConfigException ex = new ConfigException("Invalid config file");

        assertThat(ex.getMessage()).isEqualTo("Invalid config file");
        assertThat(ex).isInstanceOf(ConverterException.class);
    }

    @Test
    void shouldThrowTransformationException() {
        TransformationException ex = new TransformationException("Transform failed");

        assertThat(ex.getMessage()).isEqualTo("Transform failed");
        assertThat(ex).isInstanceOf(ConverterException.class);
    }

    @Test
    void shouldThrowCycleDetectedException() {
        CycleDetectedException ex = new CycleDetectedException(
                "Cycle detected", "A -> B -> C -> A");

        assertThat(ex.getMessage()).contains("Cycle detected");
        assertThat(ex.getCyclePath()).isEqualTo("A -> B -> C -> A");
        assertThat(ex).isInstanceOf(TransformationException.class);
    }

    @Test
    void shouldThrowUnboundedArrayException() {
        UnboundedArrayException ex = new UnboundedArrayException("employees", 5000, 1000);

        assertThat(ex.getMessage()).contains("5000");
        assertThat(ex.getMessage()).contains("1000");
        assertThat(ex.getTableName()).isEqualTo("employees");
        assertThat(ex.getChildCount()).isEqualTo(5000);
        assertThat(ex.getThreshold()).isEqualTo(1000);
    }

    @Test
    void shouldThrowSchemaException() {
        SchemaException ex = new SchemaException("Schema extraction failed");

        assertThat(ex.getMessage()).isEqualTo("Schema extraction failed");
        assertThat(ex).isInstanceOf(ConverterException.class);
    }

    @Test
    void shouldThrowPatternException() {
        PatternException ex = new PatternException("Invalid pattern config");

        assertThat(ex.getMessage()).isEqualTo("Invalid pattern config");
        assertThat(ex).isInstanceOf(ConverterException.class);
    }

    @Test
    void shouldThrowOnInvalidConfigFile() {
        assertThatThrownBy(() -> DatabaseConfig.loadFromFile("nonexistent.properties"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void shouldThrowOnInvalidJdbcUrl() {
        DatabaseConfig config = new DatabaseConfig();
        config.setSourceJdbcUrl("jdbc:invalid://localhost:99999");

        JDBCConnection conn = new JDBCConnection();

        assertThatThrownBy(() -> conn.connect(config))
                .isInstanceOf(SourceConnectionException.class);
    }

    @Test
    void shouldThrowOnInvalidMongoUri() {
        DatabaseConfig config = new DatabaseConfig();
        config.setTargetMongoUri("mongodb://invalid:99999");
        config.setTargetDatabase("test");

        MongoDBConnection conn = new MongoDBConnection();

        assertThatThrownBy(() -> conn.connect(config))
                .isInstanceOf(TargetConnectionException.class);
    }
}
