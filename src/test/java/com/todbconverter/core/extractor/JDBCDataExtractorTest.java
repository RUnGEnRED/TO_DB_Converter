package com.todbconverter.core.extractor;

import com.todbconverter.exception.SchemaException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JDBCDataExtractor using H2 in-memory database.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JDBCDataExtractorTest {

    private Connection connection;
    private JDBCDataExtractor extractor;

    @BeforeAll
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), age INT)");
            stmt.execute("INSERT INTO users VALUES (1, 'Jan', 30), (2, 'Anna', 25), (3, 'Piotr', 35)");
        }

        extractor = new JDBCDataExtractor();
    }

    @Test
    void shouldExtractTableData() throws SchemaException {
        List<Map<String, Object>> data = extractor.extractTableData(connection, "USERS");

        assertThat(data).hasSize(3);
    }

    @Test
    void shouldExtractCorrectColumnValues() throws SchemaException {
        List<Map<String, Object>> data = extractor.extractTableData(connection, "USERS");

        Map<String, Object> firstRow = data.get(0);
        assertThat(firstRow.get("ID")).isEqualTo(1);
        assertThat(firstRow.get("NAME")).isEqualTo("Jan");
        assertThat(firstRow.get("AGE")).isEqualTo(30);
    }

    @Test
    void shouldHandleEmptyTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE empty_table (id INT PRIMARY KEY)");
        }

        List<Map<String, Object>> data = extractor.extractTableData(connection, "EMPTY_TABLE");

        assertThat(data).isEmpty();
    }

    @Test
    void shouldExecuteCustomQuery() throws SchemaException {
        String sql = "SELECT id, name FROM users WHERE age > 28";
        List<Map<String, Object>> data = extractor.executeQuery(connection, sql);

        assertThat(data).hasSize(2);
        assertThat(data.get(0).get("NAME")).isEqualTo("Jan");
    }
}
