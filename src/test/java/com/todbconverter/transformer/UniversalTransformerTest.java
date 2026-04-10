package com.todbconverter.transformer;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class UniversalTransformerTest {

    private UniversalTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new UniversalTransformer();
    }

    @Test
    void testFlattenToRelational() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Test Document");
        
        Map<String, Object> childDoc = new HashMap<>();
        childDoc.put("child_id", "A1");
        childDoc.put("value", 100);
        
        List<Map<String, Object>> children = Arrays.asList(childDoc);
        doc.put("children", children);
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("parent", docs, metaMap);
        
        assertTrue(relational.containsKey("parent"), "Expected parent table");
        assertTrue(relational.containsKey("children"), "Expected children table");
        
        List<Map<String, Object>> parentRows = relational.get("parent");
        assertEquals(1, parentRows.size());
        assertEquals(1, parentRows.get(0).get("id"));
        assertEquals("Test Document", parentRows.get(0).get("name"));
        
        List<Map<String, Object>> childRows = relational.get("children");
        assertEquals(1, childRows.size());
        assertEquals("A1", childRows.get(0).get("child_id"));
        assertEquals(100, childRows.get(0).get("value"));
        assertEquals(1, childRows.get(0).get("parent_id"));
    }

    @Test
    void testFlattenToRelationalWithEmbeddedObject() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Order");
        
        Map<String, Object> address = new HashMap<>();
        address.put("street", "Main St");
        address.put("city", "Warsaw");
        doc.put("address", address);
        
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("orders", docs, metaMap);
        
        List<Map<String, Object>> rows = relational.get("orders");
        assertEquals(1, rows.size());
        assertEquals("Main St", rows.get(0).get("address_street"));
        assertEquals("Warsaw", rows.get(0).get("address_city"));
    }

    @Test
    void testFlattenToRelationalMultipleDocuments() {
        List<Map<String, Object>> docs = new ArrayList<>();
        
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("_id", 1);
        doc1.put("name", "First");
        docs.add(doc1);
        
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("_id", 2);
        doc2.put("name", "Second");
        docs.add(doc2);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("items", docs, metaMap);
        
        List<Map<String, Object>> rows = relational.get("items");
        assertEquals(2, rows.size());
    }

    @Test
    void testFlattenToRelationalNestedChildren() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("title", "Blog Post");
        
        List<Map<String, Object>> comments = new ArrayList<>();
        Map<String, Object> comment1 = new HashMap<>();
        comment1.put("author", "Alice");
        comment1.put("text", "Great post!");
        comments.add(comment1);
        
        Map<String, Object> comment2 = new HashMap<>();
        comment2.put("author", "Bob");
        comment2.put("text", "Thanks!");
        comments.add(comment2);
        
        doc.put("comments", comments);
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("posts", docs, metaMap);
        
        assertEquals(2, relational.size());
        assertEquals(1, relational.get("posts").size());
        assertEquals(2, relational.get("comments").size());
        
        List<Map<String, Object>> commentRows = relational.get("comments");
        assertEquals(1, commentRows.get(0).get("posts_id"));
        assertEquals("Alice", commentRows.get(0).get("author"));
        assertEquals("Bob", commentRows.get(1).get("author"));
    }

    @Test
    void testFlattenToRelationalGeneratesForeignKeyMetadata() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Parent");
        doc.put("children", Collections.singletonList(Collections.singletonMap("name", "Child")));
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        transformer.flattenToRelational("parent", docs, metaMap);
        
        TableMetadata childMeta = metaMap.get("children");
        assertNotNull(childMeta);
        assertEquals(1, childMeta.getForeignKeys().size());
        assertEquals("parent_id", childMeta.getForeignKeys().get(0).getColumnName());
        assertEquals("parent", childMeta.getForeignKeys().get(0).getReferencedTable());
    }

    @Test
    void testTransformToDocumentsWithManyToMany() {
        TableMetadata orderTable = createOrderTable();
        TableMetadata customerTable = createCustomerTable();
        
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        tablesMetadata.put("zamowienie", orderTable);
        tablesMetadata.put("klient", customerTable);
        
        Map<String, List<Map<String, Object>>> relatedData = new HashMap<>();
        relatedData.put("klient", Arrays.asList(
                createMap("id", 1, "imie", "Jan", "nazwisko", "Kowalski"),
                createMap("id", 2, "imie", "Anna", "nazwisko", "Nowak")
        ));
        relatedData.put("zamowienie", Arrays.asList(
                createMap("id", 10, "klient_id", 1, "status", "nowe"),
                createMap("id", 11, "klient_id", 2, "status", "zrealizowane")
        ));
        
        List<Map<String, Object>> documents = transformer.transformToDocuments(
                orderTable, relatedData.get("zamowienie"), relatedData, tablesMetadata
        );
        
        assertEquals(2, documents.size());
        
        Map<String, Object> order1 = documents.get(0);
        assertEquals(10, order1.get("id"));
        assertNotNull(order1.get("klient_data"));
        assertEquals("Jan", ((Map<?, ?>) order1.get("klient_data")).get("imie"));
        
        Map<String, Object> order2 = documents.get(1);
        assertEquals(11, order2.get("id"));
        assertNotNull(order2.get("klient_data"));
        assertEquals("Anna", ((Map<?, ?>) order2.get("klient_data")).get("imie"));
    }

    @Test
    void testTransformToDocumentsWithOneToMany() {
        TableMetadata customerTable = createCustomerTable();
        TableMetadata orderTable = createOrderTable();
        
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        tablesMetadata.put("klient", customerTable);
        tablesMetadata.put("zamowienie", orderTable);
        
        Map<String, List<Map<String, Object>>> relatedData = new HashMap<>();
        relatedData.put("klient", Arrays.asList(
                createMap("id", 1, "imie", "Jan", "nazwisko", "Kowalski")
        ));
        relatedData.put("zamowienie", Arrays.asList(
                createMap("id", 10, "klient_id", 1, "status", "nowe"),
                createMap("id", 11, "klient_id", 1, "status", "zrealizowane")
        ));
        
        List<Map<String, Object>> documents = transformer.transformToDocuments(
                customerTable, relatedData.get("klient"), relatedData, tablesMetadata
        );
        
        assertEquals(1, documents.size());
        Map<String, Object> customerDoc = documents.get(0);
        assertEquals("Jan", customerDoc.get("imie"));
        
        List<?> orders = (List<?>) customerDoc.get("zamowienie");
        assertNotNull(orders);
        assertEquals(2, orders.size());
    }

    @Test
    void testTransformToDocumentsWithEmptyChildren() {
        TableMetadata customerTable = createCustomerTable();
        TableMetadata orderTable = createOrderTable();
        
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        tablesMetadata.put("klient", customerTable);
        tablesMetadata.put("zamowienie", orderTable);
        
        Map<String, List<Map<String, Object>>> relatedData = new HashMap<>();
        relatedData.put("klient", Arrays.asList(
                createMap("id", 1, "imie", "Jan", "nazwisko", "Kowalski")
        ));
        relatedData.put("zamowienie", new ArrayList<>());
        
        List<Map<String, Object>> documents = transformer.transformToDocuments(
                customerTable, relatedData.get("klient"), relatedData, tablesMetadata
        );
        
        assertEquals(1, documents.size());
        List<?> orders = (List<?>) documents.get(0).get("zamowienie");
        assertNotNull(orders);
        assertTrue(orders.isEmpty());
    }

    @Test
    void testAggregateOneToMany() {
        TableMetadata customerTable = createCustomerTable();
        TableMetadata orderTable = createOrderTable();
        
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        tablesMetadata.put("zamowienie", orderTable);
        
        Map<String, List<Map<String, Object>>> childData = new HashMap<>();
        childData.put("zamowienie", Arrays.asList(
                createMap("id", 10, "klient_id", 1, "status", "nowe"),
                createMap("id", 11, "klient_id", 1, "status", "zrealizowane"),
                createMap("id", 12, "klient_id", 2, "status", "anulowane")
        ));
        
        List<Map<String, Object>> parentRecords = Arrays.asList(
                createMap("id", 1, "imie", "Jan", "nazwisko", "Kowalski"),
                createMap("id", 2, "imie", "Anna", "nazwisko", "Nowak")
        );
        
        List<Map<String, Object>> documents = transformer.aggregateOneToMany(
                customerTable, parentRecords, childData, tablesMetadata
        );
        
        assertEquals(2, documents.size());
        
        List<?> janOrders = (List<?>) documents.get(0).get("zamowienie");
        assertEquals(2, janOrders.size());
        
        List<?> annaOrders = (List<?>) documents.get(1).get("zamowienie");
        assertEquals(1, annaOrders.size());
    }

    @Test
    void testInferSqlType() {
        UniversalTransformer t = new UniversalTransformer();
        
        assertEquals("INT", t.inferSqlTypeForTest(42));
        assertEquals("BIGINT", t.inferSqlTypeForTest(42L));
        assertEquals("DOUBLE PRECISION", t.inferSqlTypeForTest(3.14));
        assertEquals("DOUBLE PRECISION", t.inferSqlTypeForTest(3.14f));
        assertEquals("BOOLEAN", t.inferSqlTypeForTest(true));
        assertEquals("DECIMAL", t.inferSqlTypeForTest(new BigDecimal("99.99")));
        assertEquals("TIMESTAMP", t.inferSqlTypeForTest(LocalDateTime.now()));
        assertEquals("DATE", t.inferSqlTypeForTest(LocalDate.now()));
        assertEquals("TIMESTAMP", t.inferSqlTypeForTest(new java.util.Date()));
        assertEquals("VARCHAR", t.inferSqlTypeForTest("test"));
        assertEquals("VARCHAR", t.inferSqlTypeForTest(null));
    }

    @Test
    void testFlattenToRelationalWithNullId() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "No ID Document");
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("items", docs, metaMap);
        
        List<Map<String, Object>> rows = relational.get("items");
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).get("id"));
    }

    @Test
    void testFlattenToRelationalDeeplyNestedLists() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Company");
        
        List<Map<String, Object>> departments = new ArrayList<>();
        Map<String, Object> dept = new HashMap<>();
        dept.put("dept_name", "IT");
        dept.put("employees", Arrays.asList(
                createMap("emp_id", 1, "name", "Alice"),
                createMap("emp_id", 2, "name", "Bob")
        ));
        departments.add(dept);
        doc.put("departments", departments);
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("companies", docs, metaMap);
        
        assertTrue(relational.containsKey("companies"));
        assertTrue(relational.containsKey("departments"));
    }

    @Test
    void testFlattenToRelationalWithManyToManyIds() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Student1");
        doc.put("course_ids", Arrays.asList(101, 102, 103));
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("students", docs, metaMap);
        
        String junctionTableName = "students_course_junction";
        assertTrue(relational.containsKey(junctionTableName), "Expected junction table");
        
        List<Map<String, Object>> junctionRows = relational.get(junctionTableName);
        assertEquals(3, junctionRows.size());
    }

    @Test
    void testFlattenToRelationalManyToManyCreatesTwoColumns() {
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Student1");
        doc.put("course_ids", Arrays.asList(101, 102));
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        transformer.flattenToRelational("students", docs, metaMap);
        
        String junctionTableName = "students_course_junction";
        TableMetadata junctionMeta = metaMap.get(junctionTableName);
        assertNotNull(junctionMeta, "Expected junction table metadata");
        
        List<ForeignKeyMetadata> fks = junctionMeta.getForeignKeys();
        assertEquals(2, fks.size(), "Expected two foreign keys");
    }

    @Test
    void testTransformToDocumentsManyToMany() {
        TableMetadata studentTable = createStudentTableWithM2M();
        TableMetadata courseTable = createCourseTableWithM2M();
        
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        tablesMetadata.put("students", studentTable);
        tablesMetadata.put("courses", courseTable);
        
        Map<String, List<Map<String, Object>>> relatedData = new HashMap<>();
        relatedData.put("students", Arrays.asList(
                createMap("id", 1, "name", "Alice"),
                createMap("id", 2, "name", "Bob")
        ));
        relatedData.put("courses", Arrays.asList(
                createMap("id", 101, "title", "Math", "student_id", 1),
                createMap("id", 102, "title", "Physics", "student_id", 1),
                createMap("id", 103, "title", "Chemistry", "student_id", 2)
        ));
        
        List<Map<String, Object>> documents = transformer.transformToDocuments(
                studentTable, relatedData.get("students"), relatedData, tablesMetadata
        );
        
        assertEquals(2, documents.size());
        
        Map<String, Object> aliceDoc = documents.get(0);
        assertEquals("Alice", aliceDoc.get("name"));
        assertNotNull(aliceDoc.get("courses_ids"));
        List<?> aliceCourses = (List<?>) aliceDoc.get("courses_ids");
        assertTrue(aliceCourses.contains(101) || aliceCourses.contains("101"));
    }

    @Test
    void testFlattenToRelationalComplexManyToMany() {
        List<Map<String, Object>> docs = new ArrayList<>();
        
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("_id", 1);
        doc1.put("name", "Student1");
        doc1.put("course_ids", Arrays.asList(101, 102));
        docs.add(doc1);
        
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("_id", 2);
        doc2.put("name", "Student2");
        doc2.put("course_ids", Arrays.asList(102, 103));
        docs.add(doc2);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("students", docs, metaMap);
        
        String junctionTableName = "students_course_junction";
        assertTrue(relational.containsKey(junctionTableName));
        
        List<Map<String, Object>> junctionRows = relational.get(junctionTableName);
        assertEquals(4, junctionRows.size());
    }

    private TableMetadata createStudentTableWithM2M() {
        TableMetadata table = new TableMetadata("students", "public");
        table.setPrimaryKeyColumn("id");
        
        ColumnMetadata idCol = new ColumnMetadata();
        idCol.setColumnName("id");
        idCol.setDataType("INT");
        idCol.setPrimaryKey(true);
        table.addColumn(idCol);
        
        ColumnMetadata nameCol = new ColumnMetadata();
        nameCol.setColumnName("name");
        nameCol.setDataType("VARCHAR");
        table.addColumn(nameCol);
        
        ForeignKeyMetadata fk = new ForeignKeyMetadata();
        fk.setColumnName("courses");
        fk.setReferencedTable("courses");
        fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.MANY_TO_MANY);
        table.addForeignKey(fk);
        
        return table;
    }

    private TableMetadata createCourseTableWithM2M() {
        TableMetadata table = new TableMetadata("courses", "public");
        table.setPrimaryKeyColumn("id");
        
        ColumnMetadata idCol = new ColumnMetadata();
        idCol.setColumnName("id");
        idCol.setDataType("INT");
        idCol.setPrimaryKey(true);
        table.addColumn(idCol);
        
        ColumnMetadata titleCol = new ColumnMetadata();
        titleCol.setColumnName("title");
        titleCol.setDataType("VARCHAR");
        table.addColumn(titleCol);
        
        ForeignKeyMetadata fk = new ForeignKeyMetadata();
        fk.setColumnName("student_id");
        fk.setReferencedTable("students");
        fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.MANY_TO_MANY);
        table.addForeignKey(fk);
        
        return table;
    }

    private TableMetadata createCustomerTable() {
        TableMetadata table = new TableMetadata("klient", "public");
        table.setPrimaryKeyColumn("id");
        
        ColumnMetadata idCol = new ColumnMetadata();
        idCol.setColumnName("id");
        idCol.setDataType("INT");
        idCol.setPrimaryKey(true);
        table.addColumn(idCol);
        
        ColumnMetadata imieCol = new ColumnMetadata();
        imieCol.setColumnName("imie");
        imieCol.setDataType("VARCHAR");
        table.addColumn(imieCol);
        
        ColumnMetadata nazwiskoCol = new ColumnMetadata();
        nazwiskoCol.setColumnName("nazwisko");
        nazwiskoCol.setDataType("VARCHAR");
        table.addColumn(nazwiskoCol);
        
        return table;
    }

    private TableMetadata createOrderTable() {
        TableMetadata table = new TableMetadata("zamowienie", "public");
        table.setPrimaryKeyColumn("id");
        
        ColumnMetadata idCol = new ColumnMetadata();
        idCol.setColumnName("id");
        idCol.setDataType("INT");
        idCol.setPrimaryKey(true);
        table.addColumn(idCol);
        
        ColumnMetadata klientIdCol = new ColumnMetadata();
        klientIdCol.setColumnName("klient_id");
        klientIdCol.setDataType("INT");
        klientIdCol.setForeignKey(true);
        table.addColumn(klientIdCol);
        
        ColumnMetadata statusCol = new ColumnMetadata();
        statusCol.setColumnName("status");
        statusCol.setDataType("VARCHAR");
        table.addColumn(statusCol);
        
        ForeignKeyMetadata fk = new ForeignKeyMetadata();
        fk.setColumnName("klient_id");
        fk.setReferencedTable("klient");
        fk.setReferencedColumn("id");
        table.addForeignKey(fk);
        
        return table;
    }

    private Map<String, Object> createMap(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }
}
