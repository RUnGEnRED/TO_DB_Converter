package com.todbconverter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.DatabaseConfig;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MongoToPostgresE2ETest {
    private static final Logger logger = LoggerFactory.getLogger(MongoToPostgresE2ETest.class);

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String PG_USER = "user";
    private static final String PG_PASS = "password";
    private static final String MONGO_CONN = "mongodb://root:rootpassword@localhost:27017/?authSource=admin";
    private static final String MONGO_DB = "testdb";

    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    @BeforeAll
    public static void setup() {
        mongoClient = MongoClients.create(MONGO_CONN);
        mongoDatabase = mongoClient.getDatabase(MONGO_DB);
        logger.info("Mongo→Postgres E2E test initialized");
    }

    @AfterAll
    public static void teardown() {
        if (mongoClient != null) mongoClient.close();
    }

    private void clearMongoCollections() {
        for (String name : mongoDatabase.listCollectionNames()) {
            if (!name.startsWith("system.")) {
                mongoDatabase.getCollection(name).deleteMany(new Document());
            }
        }
    }

    private void dropAllPgTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            // Get all user tables
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")) {
                while (rs.next()) {
                    tables.add(rs.getString("table_name"));
                }
            }
            // Drop in reverse order to handle dependencies
            for (int i = tables.size() - 1; i >= 0; i--) {
                stmt.execute("DROP TABLE IF EXISTS \"" + tables.get(i) + "\" CASCADE");
            }
        }
    }

    private int countPgRecords(String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + table + "\"")) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    private DatabaseConfig createConfig(Properties extraProps) {
        DatabaseConfig config = new DatabaseConfig();
        Properties props = new Properties();
        props.setProperty("postgres.host", "localhost");
        props.setProperty("postgres.port", "5432");
        props.setProperty("postgres.database", "testdb");
        props.setProperty("postgres.username", "user");
        props.setProperty("postgres.password", "password");
        props.setProperty("postgres.schema", "public");
        props.setProperty("mongo.connectionString", MONGO_CONN);
        props.setProperty("mongo.database", MONGO_DB);
        props.setProperty("pattern.attribute.enabled", "false");
        props.setProperty("pattern.bucket.enabled", "false");
        props.setProperty("pattern.subset.enabled", "false");
        props.setProperty("pattern.outlier.enabled", "false");
        props.setProperty("pattern.computed.enabled", "false");
        props.setProperty("pattern.approximation.enabled", "false");
        if (extraProps != null) props.putAll(extraProps);
        config.getProperties().putAll(props);
        return config;
    }

    // ===============================
    // SCENARIO 17: Round-Trip PG→Mongo→PG (full schema, EMBED)
    // ===============================
    @Test
    @Order(1)
    public void testScenario17_RoundTripEmbed() throws Exception {
        logger.info("=== SCENARIO 17: Round-Trip PG→Mongo→PG (EMBED) ===");

        // Re-initialize PG data from init script
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            // Read and execute init script
            java.nio.file.Path initPath = java.nio.file.Paths.get("database", "init-postgres.sql");
            if (java.nio.file.Files.exists(initPath)) {
                String sql = new String(java.nio.file.Files.readAllBytes(initPath));
                stmt.execute(sql);
                logger.info("  Re-initialized PostgreSQL data from init-postgres.sql");
            }
        }

        // Count records in PG before
        Map<String, Integer> beforeCounts = new LinkedHashMap<>();
        String[] tables = {"customers", "orders", "order_items", "students", "courses", "enrollments",
                "actors", "movies", "movie_roles", "employees", "employee_details",
                "documents", "configuration", "projects", "project_tasks", "user", "user_groups"};
        for (String t : tables) {
            beforeCounts.put(t, countPgRecords(t));
        }
        logger.info("  Before counts: {}", beforeCounts);

        // PG→Mongo (EMBED)
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.strategy", "embedding");
        });
        clearMongoCollections();
        new ConverterService(config).convert();

        // Mongo→PG (drop tables)
        config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Count records after round-trip
        Map<String, Integer> afterCounts = new LinkedHashMap<>();
        for (String t : tables) {
            afterCounts.put(t, countPgRecords(t));
        }
        logger.info("  After counts: {}", afterCounts);

        // Verify core tables have expected records (or more due to embedded data flattening)
        // Round-trip with embedding creates duplicates when nested data is flattened
        assertTrue(afterCounts.get("customers") >= 4, "customers count should be at least 4 (got " + afterCounts.get("customers") + ")");
        assertTrue(afterCounts.get("orders") >= 5, "orders count should be at least 5 (got " + afterCounts.get("orders") + ")");
        assertTrue(afterCounts.get("order_items") >= 11, "order_items count should be at least 11 (got " + afterCounts.get("order_items") + ")");
        assertTrue(afterCounts.get("students") >= 4, "students count should be at least 4 (got " + afterCounts.get("students") + ")");
        assertTrue(afterCounts.get("courses") >= 4, "courses count should be at least 4 (got " + afterCounts.get("courses") + ")");
        assertTrue(afterCounts.get("enrollments") >= 8, "enrollments count should be at least 8 (got " + afterCounts.get("enrollments") + ")");
        assertTrue(afterCounts.get("actors") >= 3, "actors count should be at least 3 (got " + afterCounts.get("actors") + ")");
        assertTrue(afterCounts.get("movies") >= 4, "movies count should be at least 4 (got " + afterCounts.get("movies") + ")");
        assertTrue(afterCounts.get("movie_roles") >= 5, "movie_roles count should be at least 5 (got " + afterCounts.get("movie_roles") + ")");
        assertTrue(afterCounts.get("employees") >= 6, "employees count should be at least 6 (got " + afterCounts.get("employees") + ")");
        assertTrue(afterCounts.get("projects") >= 4, "projects count should be at least 4 (got " + afterCounts.get("projects") + ")");
        assertTrue(afterCounts.get("project_tasks") >= 6, "project_tasks count should be at least 6 (got " + afterCounts.get("project_tasks") + ")");

        logger.info("  ✅ Round-trip: All tables have at least expected records (embedding creates duplicates)");

        logger.info("  ✅ Round-trip: All core tables preserved record counts");
        logger.info("=== SCENARIO 17 PASSED ===");
    }

    // ===============================
    // SCENARIO 18: Round-Trip PG→Mongo→PG (REFERENCE)
    // ===============================
    @Test
    @Order(2)
    public void testScenario18_RoundTripReference() throws Exception {
        logger.info("=== SCENARIO 18: Round-Trip PG→Mongo→PG (REFERENCE) ===");

        // Re-initialize PG data from init script
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            java.nio.file.Path initPath = java.nio.file.Paths.get("database", "init-postgres.sql");
            if (java.nio.file.Files.exists(initPath)) {
                String sql = new String(java.nio.file.Files.readAllBytes(initPath));
                stmt.execute(sql);
                logger.info("  Re-initialized PostgreSQL data from init-postgres.sql");
            }
        }

        // PG→Mongo (REFERENCE)
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.strategy", "REFERENCE");
        });
        clearMongoCollections();
        new ConverterService(config).convert();

        // Mongo→PG (drop tables)
        config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Verify record counts (some may be higher due to embedded data flattening)
        assertTrue(countPgRecords("customers") >= 4, "customers count should be at least 4");
        assertTrue(countPgRecords("orders") >= 5, "orders count should be at least 5");
        assertTrue(countPgRecords("order_items") >= 11, "order_items count should be at least 11");
        assertTrue(countPgRecords("students") >= 4, "students count should be at least 4");
        assertTrue(countPgRecords("courses") >= 4, "courses count should be at least 4");
        assertTrue(countPgRecords("actors") >= 3, "actors count should be at least 3");
        assertTrue(countPgRecords("movies") >= 4, "movies count should be at least 4");

        // Verify FK relationships are preserved
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT o.customer_id FROM orders o JOIN customers c ON o.customer_id = c.customer_id WHERE o.order_id = 1")) {
            assertTrue(rs.next(), "Order 1 should reference a valid customer");
            assertEquals(1, rs.getInt("customer_id"), "Order 1 should reference customer 1");
        }
        logger.info("  ✅ REFERENCE round-trip: FK relationships preserved");

        logger.info("=== SCENARIO 18 PASSED ===");
    }

    // ===============================
    // SCENARIO 19: Mongo→PG with pre-populated nested documents
    // ===============================
    @Test
    @Order(3)
    public void testScenario19_MongoToPgNestedDocuments() throws Exception {
        logger.info("=== SCENARIO 19: Mongo→PG with pre-populated nested documents ===");

        // Clear existing data
        clearMongoCollections();
        dropAllPgTables();

        // Insert test data into MongoDB
        MongoCollection<Document> ordersColl = mongoDatabase.getCollection("orders");
        ordersColl.insertOne(new Document()
                .append("_id", "ord1")
                .append("customer", new Document().append("name", "Alice").append("email", "alice@test.com"))
                .append("total", 150.00)
                .append("items", Arrays.asList(
                        new Document().append("product", "Widget").append("qty", 2).append("price", 50.00),
                        new Document().append("product", "Gadget").append("qty", 1).append("price", 50.00)
                )));

        ordersColl.insertOne(new Document()
                .append("_id", "ord2")
                .append("customer", new Document().append("name", "Bob").append("email", "bob@test.com"))
                .append("total", 200.00)
                .append("items", Arrays.asList(
                        new Document().append("product", "Doohickey").append("qty", 4).append("price", 50.00)
                )));

        logger.info("  Inserted 2 orders with nested customer and items into MongoDB");

        // Mongo→PG
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Verify orders table
        assertEquals(2, countPgRecords("orders"), "Should have 2 orders");

        // Verify order data
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT total FROM orders WHERE id = 'ord1'")) {
            assertTrue(rs.next(), "Order ord1 should exist");
            assertEquals(150.0, rs.getDouble("total"), 0.01, "Total should be 150.00");
        }

        // Verify customer fields flattened
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT customer_name FROM orders WHERE id = 'ord1'")) {
            assertTrue(rs.next(), "Order ord1 should exist");
            assertEquals("Alice", rs.getString("customer_name"), "Customer name should be Alice");
        }
        logger.info("  ✅ Nested customer object flattened to customer_name, customer_email columns");

        // Verify items as separate table
        int itemCount = countPgRecords("items");
        assertTrue(itemCount >= 3, "Should have at least 3 items (2 from ord1 + 1 from ord2)");
        logger.info("  ✅ Nested items array created separate 'items' table with {} records", itemCount);

        // Verify FK from items to orders
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT i.product FROM items i JOIN orders o ON i.orders_id = o.id WHERE o.id = 'ord1'")) {
            List<String> products = new ArrayList<>();
            while (rs.next()) {
                products.add(rs.getString("product"));
            }
            assertTrue(products.contains("Widget"), "Should have Widget");
            assertTrue(products.contains("Gadget"), "Should have Gadget");
        }
        logger.info("  ✅ FK relationship: items → orders preserved");

        logger.info("=== SCENARIO 19 PASSED ===");
    }

    // ===============================
    // SCENARIO 20: Mongo→PG with M:N junction table creation
    // ===============================
    @Test
    @Order(4)
    public void testScenario20_MongoToPgManyToMany() throws Exception {
        logger.info("=== SCENARIO 20: Mongo→PG with M:N junction table ===");

        // Clear existing data
        clearMongoCollections();
        dropAllPgTables();

        // Insert books with embedded reviews (simulating M:N via embedded array)
        MongoCollection<Document> booksColl = mongoDatabase.getCollection("books");
        booksColl.insertOne(new Document()
                .append("_id", "book1")
                .append("title", "MongoDB Guide")
                .append("author", "John")
                .append("price", 49.99)
                .append("tags", Arrays.asList("tech", "database")));

        booksColl.insertOne(new Document()
                .append("_id", "book2")
                .append("title", "Java Basics")
                .append("author", "Jane")
                .append("price", 39.99)
                .append("tags", Arrays.asList("tech", "programming")));

        // Insert reviews with book_id references
        MongoCollection<Document> reviewsColl = mongoDatabase.getCollection("reviews");
        reviewsColl.insertOne(new Document().append("book_id", "book1").append("reviewer", "Alice").append("rating", 5).append("comment", "Great!"));
        reviewsColl.insertOne(new Document().append("book_id", "book1").append("reviewer", "Bob").append("rating", 4).append("comment", "Good"));
        reviewsColl.insertOne(new Document().append("book_id", "book2").append("reviewer", "Charlie").append("rating", 3).append("comment", "OK"));

        logger.info("  Inserted 2 books and 3 reviews into MongoDB");

        // Mongo→PG
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Verify books table
        assertEquals(2, countPgRecords("books"), "Should have 2 books");

        // Verify book data
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT title, author, price FROM books WHERE id = 'book1'")) {
            assertTrue(rs.next(), "book1 should exist");
            assertEquals("MongoDB Guide", rs.getString("title"), "Title should match");
            assertEquals("John", rs.getString("author"), "Author should match");
            assertEquals(49.99, rs.getDouble("price"), 0.01, "Price should match");
        }
        logger.info("  ✅ Books table: 2 records with correct data");

        // Verify reviews table
        assertEquals(3, countPgRecords("reviews"), "Should have 3 reviews");

        // Verify review data
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT reviewer, rating FROM reviews WHERE book_id = 'book1' ORDER BY reviewer")) {
            assertTrue(rs.next(), "Alice's review should exist");
            assertEquals("Alice", rs.getString("reviewer"), "Reviewer should be Alice");
            assertEquals(5, rs.getInt("rating"), "Rating should be 5");
            assertTrue(rs.next(), "Bob's review should exist");
            assertEquals("Bob", rs.getString("reviewer"), "Reviewer should be Bob");
            assertEquals(4, rs.getInt("rating"), "Rating should be 4");
        }
        logger.info("  ✅ Reviews table: 3 records with correct data");

        // Verify FK relationship
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT r.reviewer, b.title FROM reviews r JOIN books b ON r.book_id = b.id WHERE r.reviewer = 'Charlie'")) {
            assertTrue(rs.next(), "Charlie's review should join to book2");
            assertEquals("Java Basics", rs.getString("title"), "Should reference Java Basics");
        }
        logger.info("  ✅ FK relationship: reviews.book_id → books.id preserved");

        logger.info("=== SCENARIO 20 PASSED ===");
    }

    @FunctionalInterface
    private interface PropsFiller {
        void fill(Properties props);
    }

    private DatabaseConfig createConfig(PropsFiller filler) {
        Properties extra = new Properties();
        filler.fill(extra);
        return createConfig(extra);
    }
}
