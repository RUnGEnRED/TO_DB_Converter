package com.todbconverter.core.loader;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.EdgeStrategyRegistry;
import com.todbconverter.core.model.*;
import com.todbconverter.exception.TransformationException;
import org.bson.Document;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for MongoDBLoader using Testcontainers.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MongoDBLoaderTest {

    private static MongoClient client;
    private static MongoDatabase database;
    private MongoDBLoader loader;
    private EdgeStrategyRegistry registry;

    @BeforeAll
    static void setUpClass() {
        // Connect to MongoDB running in Docker
        String uri = "mongodb://admin:password@localhost:27018";
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .build();
        client = MongoClients.create(settings);
        database = client.getDatabase("loader_test_db");
    }

    @BeforeEach
    void setUp() {
        loader = new MongoDBLoader(database);
        registry = new EdgeStrategyRegistry(Strategy.EMBED);

        // Clean up collections
        for (String name : database.listCollectionNames()) {
            database.getCollection(name).drop();
        }
    }

    @AfterAll
    static void tearDownClass() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @Order(1)
    void shouldLoadDocumentsIntoCollection() throws TransformationException {
        // Setup
        SchemaGraph graph = new SchemaGraph();
        graph.addTable(TableMetadata.builder()
                .name("users")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .build());

        Map<String, List<Map<String, Object>>> data = new LinkedHashMap<>();
        data.put("users", List.of(
                Map.of("id", 1, "name", "Jan"),
                Map.of("id", 2, "name", "Anna")
        ));

        // Load
        loader.loadAll(data, graph, registry);

        // Verify
        MongoCollection<Document> collection = database.getCollection("user");
        assertThat(collection.countDocuments()).isEqualTo(2);
    }

    @Test
    @Order(2)
    void shouldConvertTableNamesToCollectionNames() throws TransformationException {
        SchemaGraph graph = new SchemaGraph();
        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .build());

        Map<String, List<Map<String, Object>>> data = new LinkedHashMap<>();
        data.put("employees", List.of(Map.of("id", 1, "name", "Jan")));

        loader.loadAll(data, graph, registry);

        // Should create "employee" collection (singularized)
        assertThat(database.listCollectionNames()).contains("employee");
    }

    @Test
    @Order(3)
    void shouldSkipEmbeddedCollections() throws TransformationException {
        // Setup: employees has outgoing EMBED edge to departments
        SchemaGraph graph = new SchemaGraph();
        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("department_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .build());
        graph.addTable(TableMetadata.builder()
                .name("departments")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employees", "department_id", "departments", "id",
                Cardinality.ONE_TO_MANY));

        Map<String, List<Map<String, Object>>> data = new LinkedHashMap<>();
        data.put("employees", List.of(Map.of("id", 1, "department_id", 1)));
        data.put("departments", List.of(Map.of("id", 1, "name", "IT")));

        loader.loadAll(data, graph, registry);

        // employees should be skipped (all edges are EMBED)
        assertThat(database.listCollectionNames()).doesNotContain("employee");
    }

    @Test
    @Order(4)
    void shouldPreserveNestedDocuments() throws TransformationException {
        SchemaGraph graph = new SchemaGraph();
        graph.addTable(TableMetadata.builder()
                .name("users")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .build());

        Map<String, List<Map<String, Object>>> data = new LinkedHashMap<>();
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("id", 1);
        user.put("address", Map.of("street", "Main St", "city", "Warsaw"));
        user.put("tags", List.of("admin", "user"));
        data.put("users", List.of(user));

        loader.loadAll(data, graph, registry);

        MongoCollection<Document> collection = database.getCollection("user");
        Document doc = collection.find().first();

        assertThat(doc.get("address")).isInstanceOf(Document.class);
        assertThat(((Document) doc.get("address")).getString("street")).isEqualTo("Main St");
        assertThat(doc.get("tags")).isInstanceOf(List.class);
    }
}
