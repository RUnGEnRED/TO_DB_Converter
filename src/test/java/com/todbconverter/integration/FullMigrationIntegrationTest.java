package com.todbconverter.integration;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.core.model.Strategy;
import com.todbconverter.core.service.ETLPipeline;
import com.todbconverter.exception.ConverterException;
import com.todbconverter.ui.TerminalRenderer;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests.
 * Tests the full ETL pipeline with H2 -> MongoDB.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FullMigrationIntegrationTest {

    private static final String H2_URL = "jdbc:h2:mem:integration_test" + System.nanoTime();
    private static final String MONGO_URI = "mongodb://admin:password@localhost:27018";
    private static final String MONGO_DB = "integration_test_db";

    private static Connection h2Connection;
    private static com.mongodb.client.MongoClient mongoClient;

    @BeforeAll
    static void setUp() throws Exception {
        // Setup H2
        h2Connection = DriverManager.getConnection(H2_URL, "sa", "");
        try (Statement stmt = h2Connection.createStatement()) {
            stmt.execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(100), department_id INT)");
            stmt.execute("CREATE TABLE employee_details (id INT PRIMARY KEY, employee_id INT UNIQUE, pesel VARCHAR(11))");
            stmt.execute("ALTER TABLE employees ADD FOREIGN KEY (department_id) REFERENCES departments(id)");
            stmt.execute("ALTER TABLE employee_details ADD FOREIGN KEY (employee_id) REFERENCES employees(id)");

            stmt.execute("INSERT INTO departments VALUES (1, 'IT'), (2, 'HR')");
            stmt.execute("INSERT INTO employees VALUES (1, 'Jan', 1), (2, 'Anna', 1), (3, 'Piotr', 2)");
            stmt.execute("INSERT INTO employee_details VALUES (1, 1, '90010112345')");
        }

        // Setup MongoDB
        com.mongodb.ConnectionString connString = new com.mongodb.ConnectionString(MONGO_URI);
        com.mongodb.MongoClientSettings settings = com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(connString)
                .build();
        mongoClient = com.mongodb.client.MongoClients.create(settings);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (h2Connection != null) h2Connection.close();
        if (mongoClient != null) {
            mongoClient.getDatabase(MONGO_DB).drop();
            mongoClient.close();
        }
    }

    @Test
    @Order(1)
    void shouldMigrateFullSchema() throws ConverterException {
        DatabaseConfig config = new DatabaseConfig();
        config.setSourceJdbcUrl(H2_URL);
        config.setSourceUsername("sa");
        config.setSourcePassword("");
        config.setTargetMongoUri(MONGO_URI);
        config.setTargetDatabase(MONGO_DB);
        config.setDefaultStrategy(Strategy.EMBED);

        ETLPipeline pipeline = new ETLPipeline(new TerminalRenderer());

        // Should not throw exception
        pipeline.execute(config);

        // Verify data in MongoDB
        com.mongodb.client.MongoDatabase db = mongoClient.getDatabase(MONGO_DB);

        // Check that collections were created
        assertThat(db.listCollectionNames().into(new java.util.HashSet<>()))
                .isNotEmpty();
    }

    @Test
    @Order(2)
    void shouldHandleEmptyDatabase() throws Exception {
        // Create empty H2 database
        String emptyUrl = "jdbc:h2:mem:empty_test" + System.nanoTime();
        try (Connection conn = DriverManager.getConnection(emptyUrl, "sa", "");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_table (id INT PRIMARY KEY)");
        }

        DatabaseConfig config = new DatabaseConfig();
        config.setSourceJdbcUrl(emptyUrl);
        config.setSourceUsername("sa");
        config.setSourcePassword("");
        config.setTargetMongoUri(MONGO_URI);
        config.setTargetDatabase("empty_test_db");

        ETLPipeline pipeline = new ETLPipeline(new TerminalRenderer());

        // Should not throw exception
        pipeline.execute(config);

        // Verify empty collection
        com.mongodb.client.MongoDatabase db = mongoClient.getDatabase("empty_test_db");
        assertThat(db.listCollectionNames().into(new java.util.HashSet<>())).isEmpty();
    }

    @Test
    @Order(3)
    void shouldThrowOnInvalidConnection() {
        DatabaseConfig config = new DatabaseConfig();
        config.setSourceJdbcUrl("jdbc:postgresql://invalid:99999/nonexistent");
        config.setSourceUsername("invalid");
        config.setSourcePassword("invalid");
        config.setTargetMongoUri("mongodb://invalid:99999");
        config.setTargetDatabase("test");

        ETLPipeline pipeline = new ETLPipeline(new TerminalRenderer());

        assertThatThrownBy(() -> pipeline.execute(config))
                .isInstanceOf(ConverterException.class);
    }
}
