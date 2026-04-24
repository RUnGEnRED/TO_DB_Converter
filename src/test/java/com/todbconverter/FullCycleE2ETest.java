package com.todbconverter;

import com.todbconverter.config.DatabaseConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class FullCycleE2ETest {
    private static final Logger logger = LoggerFactory.getLogger(FullCycleE2ETest.class);
    
    private static final String PG_URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String PG_USER = "user";
    private static final String PG_PASS = "password";
    private static final String MONGO_CONN = "mongodb://root:rootpassword@localhost:27017/?authSource=admin";
    private static final String MONGO_DB = "testdb";

    @BeforeAll
    public static void setup() throws Exception {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("DROP TABLE IF EXISTS customers CASCADE");
            stmt.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, email TEXT, score DOUBLE PRECISION)");
            stmt.execute("INSERT INTO customers VALUES (1, 'Jan Kowalski', 'jan@test.pl', 95.5), (2, 'Anna Nowak', 'anna@test.pl', 88.0)");
            
            logger.info("Sample PostgreSQL data populated");
        }
    }

    @Test
    public void testFullConversionCycle() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        Properties props = new Properties();
        
        // PG to Mongo Config
        props.setProperty("postgres.host", "localhost");
        props.setProperty("postgres.port", "5432");
        props.setProperty("postgres.database", "testdb");
        props.setProperty("postgres.username", "user");
        props.setProperty("postgres.password", "password");
        props.setProperty("postgres.schema", "public");
        
        props.setProperty("mongo.connectionString", MONGO_CONN);
        props.setProperty("mongo.database", MONGO_DB);
        props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
        props.setProperty("strategy.referencing", "false"); // Use embedding
        
        config.getProperties().putAll(props);
        
        // 1. Run PG -> MONGO
        logger.info("Starting PG -> MONGO...");
        ConverterService converter = new ConverterService(config);
        converter.convert();
        converter.close();
        
        // 2. Run MONGO -> PG
        logger.info("Starting MONGO -> PG...");
        props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
        props.setProperty("postgres.dropExistingTables", "true");
        config.getProperties().putAll(props);
        
        converter = new ConverterService(config);
        converter.convert();
        converter.close();
        
        // 3. Verify final PG data
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             java.sql.ResultSet rs = stmt.executeQuery("SELECT count(*) FROM customers")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        
        logger.info("E2E Test passed!");
    }
}
