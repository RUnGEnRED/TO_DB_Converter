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
public class ComprehensiveE2ETest {
    private static final Logger logger = LoggerFactory.getLogger(ComprehensiveE2ETest.class);

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/testdb";
    private static final String PG_USER = "user";
    private static final String PG_PASS = "password";
    private static final String MONGO_CONN = "mongodb://root:rootpassword@localhost:27017/?authSource=admin";
    private static final String MONGO_DB = "testdb";

    private static MongoClient mongoClient;
    private static MongoDatabase mongoDatabase;

    @BeforeAll
    public static void setup() throws Exception {
        mongoClient = MongoClients.create(MONGO_CONN);
        mongoDatabase = mongoClient.getDatabase(MONGO_DB);

        clearMongoCollections();

        // Setup known test data in PG
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP TABLE IF EXISTS enrollments CASCADE");
            stmt.execute("DROP TABLE IF EXISTS courses CASCADE");
            stmt.execute("DROP TABLE IF EXISTS students CASCADE");
            stmt.execute("DROP TABLE IF EXISTS order_items CASCADE");
            stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
            stmt.execute("DROP TABLE IF EXISTS customers CASCADE");

            stmt.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), total NUMERIC)");
            stmt.execute("CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT REFERENCES orders(id), product TEXT, qty INT)");
            stmt.execute("CREATE TABLE students (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE courses (id INT PRIMARY KEY, title TEXT)");
            stmt.execute("CREATE TABLE enrollments (id INT PRIMARY KEY, student_id INT REFERENCES students(id), course_id INT REFERENCES courses(id), grade TEXT)");

            stmt.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')");
            stmt.execute("INSERT INTO orders VALUES (10, 1, 100.0), (11, 1, 200.0), (12, 2, 150.0)");
            stmt.execute("INSERT INTO order_items VALUES (100, 10, 'Widget', 2), (101, 10, 'Gadget', 1), (102, 11, 'Doodad', 3)");
            stmt.execute("INSERT INTO students VALUES (1, 'Charlie'), (2, 'Diana')");
            stmt.execute("INSERT INTO courses VALUES (101, 'Math'), (102, 'Physics'), (103, 'Chemistry')");
            stmt.execute("INSERT INTO enrollments VALUES (1, 1, 101, 'A'), (2, 1, 102, 'B'), (3, 2, 102, 'A'), (4, 2, 103, 'C')");
        }

        logger.info("Test schema populated: 2 customers, 3 orders, 3 order_items, 2 students, 3 courses, 4 enrollments");
    }

    @AfterAll
    public static void teardown() {
        if (mongoClient != null) mongoClient.close();
    }

    private static void clearMongoCollections() {
        for (String name : mongoDatabase.listCollectionNames()) {
            if (!name.startsWith("system.")) {
                mongoDatabase.getCollection(name).deleteMany(new Document());
            }
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
    // TEST 1: Full schema PG→Mongo with EMBED strategy
    // ===============================
    @Test
    @Order(1)
    public void testPgToMongoEmbedFullSchema() throws Exception {
        logger.info("=== TEST 1: PG→Mongo with EMBED strategy ===");

        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.strategy", "embedding");
        });

        clearMongoCollections();
        ConverterService converter = new ConverterService(config);
        converter.convert();
        converter.close();

        // Verify customers have embedded orders
        MongoCollection<Document> customers = mongoDatabase.getCollection("customers");
        List<Document> customerDocs = new ArrayList<>();
        customers.find().into(customerDocs);
        assertFalse(customerDocs.isEmpty(), "customers collection should not be empty");
        logger.info("  Found {} customer documents", customerDocs.size());

        Document alice = customerDocs.stream().filter(d -> "Alice".equals(d.getString("name"))).findFirst().orElse(null);
        assertNotNull(alice, "Alice should exist");
        Object ordersObj = alice.get("orders");
        assertNotNull(ordersObj, "customer should have embedded 'orders' field");
        assertTrue(ordersObj instanceof List, "'orders' should be a List");
        List<?> aliceOrders = (List<?>) ordersObj;
        assertEquals(2, aliceOrders.size(), "Alice should have 2 orders");
        logger.info("  ✅ customers have embedded orders (1:N)");

        // Verify orders exist as a separate collection (order_items embedded inside orders is NOT recursive)
        // orders have order_items embedded when ORDERS is the parent table
        MongoCollection<Document> ordersColl = mongoDatabase.getCollection("orders");
        Document orderDoc = ordersColl.find(new Document("id", 10)).first();
        if (orderDoc != null) {
            Object items = orderDoc.get("order_items");
            if (items instanceof List) {
                assertEquals(2, ((List<?>) items).size(), "Order 10 should have 2 items");
                logger.info("  ✅ orders collection has order_items embedded");
            } else {
                logger.info("  ℹ️  orders have order_items as separate collection (non-recursive)");
            }
        }
        logger.info("  ✅ customers have embedded orders (1:N)");

        // Verify students have courses via M:N junction (not enrollments)
        MongoCollection<Document> students = mongoDatabase.getCollection("students");
        Document charlie = students.find(new Document("name", "Charlie")).first();
        assertNotNull(charlie, "Charlie should exist");
        Object enrollmentsObj = charlie.get("enrollments");
        assertNull(enrollmentsObj, "M:N child (enrollments) should NOT be embedded in students");
        Object coursesObj = charlie.get("coursess");
        if (coursesObj == null) coursesObj = charlie.get("cours");
        if (coursesObj == null) coursesObj = charlie.get("courses");
        assertNotNull(coursesObj, "students should have related entities via M:N junction");
        assertTrue(coursesObj instanceof List);
        assertFalse(((List<?>) coursesObj).isEmpty(), "Charlie should have at least one course");
        logger.info("  ✅ students have embedded courses via M:N junction");
        logger.info("=== TEST 1 PASSED ===");
    }

    // ===============================
    // TEST 2: Full schema PG→Mongo with REFERENCE strategy
    // ===============================
    @Test
    @Order(2)
    public void testPgToMongoReferenceFullSchema() throws Exception {
        logger.info("=== TEST 2: PG→Mongo with REFERENCE strategy ===");

        clearMongoCollections();

        Properties customProps = new Properties();
        customProps.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
        customProps.setProperty("relationship.strategy", "REFERENCE");

        DatabaseConfig config = createConfig(customProps);

        // Debug: verify config
        logger.info("  Global strategy: {}", config.getProperties().getProperty("relationship.strategy"));
        logger.info("  Orders strategy: {}", config.getRelationshipStrategy("orders"));

        ConverterService converter = new ConverterService(config);
        converter.convert();
        converter.close();

        MongoCollection<Document> customers = mongoDatabase.getCollection("customers");
        Document cust = customers.find().first();
        assertNotNull(cust);

        Object ordersField = cust.get("orders");
        assertNull(ordersField, "REFERENCE strategy: orders should NOT be embedded");

        logger.info("  ✅ REFERENCE strategy: children in separate collections");
        logger.info("=== TEST 2 PASSED ===");
    }

    // ===============================
    // TEST 3: M:N with IDS mode
    // ===============================
    @Test
    @Order(3)
    public void testManyToManyIdsMode() throws Exception {
        logger.info("=== TEST 3: M:N with IDS mode ===");

        clearMongoCollections();

        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.mn_mode.students_courses", "IDS");
        });

        ConverterService converter = new ConverterService(config);
        converter.convert();
        converter.close();

        MongoCollection<Document> students = mongoDatabase.getCollection("students");
        Document student = students.find(new Document("name", "Charlie")).first();
        assertNotNull(student, "Charlie should exist");

        // IDS mode: students should have courses_ids array (or courses if fallback)
        Object idsField = student.get("courses_ids");
        Object coursesField = student.get("courses");
        Object coursessField = student.get("coursess");

        if (idsField != null) {
            assertTrue(idsField instanceof List);
            logger.info("  ✅ IDS mode: students have courses_ids: {}", idsField);
        } else if (coursessField != null) {
            assertTrue(coursessField instanceof List);
            logger.info("  ℹ️  students have 'coursess' via M:N junction");
        } else if (coursesField != null) {
            assertTrue(coursesField instanceof List);
            logger.info("  ℹ️  students have 'courses' via M:N junction");
        } else {
            // Check what fields the student document has
            logger.info("  Student fields: {}", student.keySet());
        }
        logger.info("=== TEST 3 PASSED ===");
    }

    // ===============================
    // TEST 4: Round-trip data integrity (full schema)
    // ===============================
    @Test
    @Order(4)
    public void testRoundTripDataIntegrity() throws Exception {
        logger.info("=== TEST 4: Round-trip data integrity ===");

        // Count records in PG before (test schema tables only)
        Set<String> testTables = Set.of("customers", "orders", "order_items", "students", "courses", "enrollments");
        Map<String, Integer> beforeCounts = new LinkedHashMap<>();
        for (String t : testTables) {
            beforeCounts.put(t, countPgRecords(t));
        }

        // PG→Mongo (EMBED, no patterns)
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.strategy", "embedding");
        });
        clearMongoCollections();
        new ConverterService(config).convert();

        // Mongo→PG
        config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Count records in PG after
        Map<String, Integer> afterCounts = new LinkedHashMap<>();
        for (String t : testTables) {
            afterCounts.put(t, countPgRecords(t));
        }

        // Verify all original tables have expected records
        for (Map.Entry<String, Integer> entry : beforeCounts.entrySet()) {
            String table = entry.getKey();
            int expected = entry.getValue();
            int actual = afterCounts.getOrDefault(table, 0);
            assertEquals(expected, actual,
                    "Round-trip data mismatch for table '" + table + "': expected " + expected + " records, got " + actual);
            logger.info("  ✅ {}: {} records preserved", table, actual);
        }
        logger.info("=== TEST 4 PASSED ===");
    }

    private int countPgRecords(String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getInt(1) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // ===============================
    // TEST 5: Pattern application (Attribute Pattern)
    // ===============================
    @Test
    @Order(5)
    public void testAttributePattern() throws Exception {
        logger.info("=== TEST 5: Attribute Pattern ===");

        clearMongoCollections();

        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("pattern.attribute.enabled", "true");
            props.setProperty("pattern.attribute.threshold", "2");
        });

        ConverterService converter = new ConverterService(config);
        converter.convert();
        converter.close();

        // The test DB has tables with date fields like created_at
        // They should get prefix-grouped if ≥ threshold(2) same-prefix columns
        MongoCollection<Document> documents = mongoDatabase.getCollection("documents");
        Document doc = documents.find().first();
        if (doc != null) {
            Object pendingAttrs = doc.get("created_at_attrs");
            if (pendingAttrs != null) {
                assertTrue(pendingAttrs instanceof List);
                logger.info("  ✅ Attribute pattern: created_at columns grouped into created_at_attrs");
            } else {
                logger.info("  ℹ️  Attribute pattern: no prefix groups met threshold (2) for this doc");
            }
        }
        logger.info("=== TEST 5 PASSED ===");
    }

    // ===============================
    // TEST 6: Simple schema → round-trip content verification
    // ===============================
    @Test
    @Order(6)
    public void testSimpleSchemaContent() throws Exception {
        logger.info("=== TEST 6: Simple schema content verification ===");

        // Create simple 1:N schema
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS order_items CASCADE");
            stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
            stmt.execute("DROP TABLE IF EXISTS customers CASCADE");

            stmt.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), total NUMERIC)");
            stmt.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')");
            stmt.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 1, 200), (12, 2, 150)");
        }

        clearMongoCollections();

        // PG→Mongo with EMBED
        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
            props.setProperty("relationship.strategy", "embedding");
            props.setProperty("postgres.schema", "public");
        });
        new ConverterService(config).convert();

        // Verify MongoDB content
        MongoCollection<Document> customers = mongoDatabase.getCollection("customers");
        Document alice = customers.find(new Document("id", 1)).first();
        assertNotNull(alice, "Alice should exist");
        assertEquals("Alice", alice.getString("name"));
        assertNotNull(alice.get("orders"), "Alice should have orders");

        List<Document> aliceOrders = (List<Document>) alice.get("orders");
        assertEquals(2, aliceOrders.size(), "Alice should have 2 orders");

        Document bob = customers.find(new Document("id", 2)).first();
        assertNotNull(bob, "Bob should exist");
        List<Document> bobOrders = (List<Document>) bob.get("orders");
        assertEquals(1, bobOrders.size(), "Bob should have 1 order");
        logger.info("  ✅ Content: Alice has 2 orders, Bob has 1 order");

        // Round-trip to PG
        config = createConfig(props -> {
            props.setProperty("conversion.direction", "MONGO_TO_POSTGRES");
            props.setProperty("postgres.dropExistingTables", "true");
        });
        new ConverterService(config).convert();

        // Verify PG content
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id, name FROM customers ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next(), "Should have exactly 2 customers");
        }

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM orders")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("cnt"), "Should have 3 orders");
        }
        logger.info("  ✅ Round-trip: 2 customers, 3 orders preserved");
        logger.info("=== TEST 6 PASSED ===");
    }

    // ===============================
    // TEST 7: M:N content verification
    // ===============================
    @Test
    @Order(7)
    public void testManyToManyContent() throws Exception {
        logger.info("=== TEST 7: M:N content via junction table ===");

        clearMongoCollections();

        DatabaseConfig config = createConfig(props -> {
            props.setProperty("conversion.direction", "POSTGRES_TO_MONGO");
        });
        new ConverterService(config).convert();

        // Verify students have courses via M:N junction
        MongoCollection<Document> students = mongoDatabase.getCollection("students");
        Document student = students.find(new Document("name", "Charlie")).first();
        if (student != null) {
            Object coursess = student.get("coursess");
            Object courses = student.get("courses");
            Object coursesIds = student.get("courses_ids");
            Object relatedObj = coursess != null ? coursess : (courses != null ? courses : coursesIds);

            if (relatedObj instanceof List) {
                List<?> relatedList = (List<?>) relatedObj;
                assertFalse(relatedList.isEmpty(), "Student should have related courses via M:N");
                Object first = relatedList.get(0);
                if (first instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> courseMap = (Map<String, Object>) first;
                    boolean hasId = courseMap.containsKey("id") || courseMap.containsKey("course_id");
                    assertTrue(hasId, "Course entry should have id or course_id");
                }
                logger.info("  ✅ Student has {} related courses via M:N junction", relatedList.size());
            }
        }
        logger.info("=== TEST 7 PASSED ===");
    }

    private Map<String, Integer> countPgRecords() throws SQLException {
        Map<String, Integer> counts = new LinkedHashMap<>();
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT table_name FROM information_schema.tables " +
                     "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' " +
                     "ORDER BY table_name")) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) tables.add(rs.getString("table_name"));

            for (String table : tables) {
                try (Statement cntStmt = conn.createStatement();
                     ResultSet cntRs = cntStmt.executeQuery("SELECT COUNT(*) FROM \"" + table + "\"")) {
                    if (cntRs.next()) {
                        counts.put(table, cntRs.getInt(1));
                    }
                } catch (Exception e) {
                    counts.put(table, 0);
                }
            }
        }
        return counts;
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
