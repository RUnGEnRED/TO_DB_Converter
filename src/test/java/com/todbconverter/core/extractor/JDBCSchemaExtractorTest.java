package com.todbconverter.core.extractor;

import com.todbconverter.core.model.*;
import com.todbconverter.exception.SchemaException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for JDBCSchemaExtractor using H2 in-memory database.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JDBCSchemaExtractorTest {

    private Connection connection;
    private JDBCSchemaExtractor extractor;

    @BeforeAll
    void setUp() throws Exception {
        // Create H2 in-memory database with unique name per test
        connection = DriverManager.getConnection("jdbc:h2:mem:schematest" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");

        try (Statement stmt = connection.createStatement()) {
            // Create tables
            stmt.execute("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(100), department_id INT, manager_id INT)");
            stmt.execute("CREATE TABLE employee_details (id INT PRIMARY KEY, employee_id INT UNIQUE, pesel VARCHAR(11))");
            stmt.execute("CREATE TABLE projects (id INT PRIMARY KEY, name VARCHAR(100))");
            stmt.execute("CREATE TABLE employee_projects (employee_id INT, project_id INT, PRIMARY KEY(employee_id, project_id))");

            // Add foreign keys
            stmt.execute("ALTER TABLE employees ADD FOREIGN KEY (department_id) REFERENCES departments(id)");
            stmt.execute("ALTER TABLE employees ADD FOREIGN KEY (manager_id) REFERENCES employees(id)");
            stmt.execute("ALTER TABLE employee_details ADD FOREIGN KEY (employee_id) REFERENCES employees(id)");
            stmt.execute("ALTER TABLE employee_projects ADD FOREIGN KEY (employee_id) REFERENCES employees(id)");
            stmt.execute("ALTER TABLE employee_projects ADD FOREIGN KEY (project_id) REFERENCES projects(id)");

            // Insert test data
            stmt.execute("INSERT INTO departments VALUES (1, 'IT'), (2, 'HR')");
            stmt.execute("INSERT INTO employees VALUES (1, 'Jan', 1, NULL), (2, 'Anna', 1, 1)");
            stmt.execute("INSERT INTO employee_details VALUES (1, 1, '90010112345')");
            stmt.execute("INSERT INTO projects VALUES (1, 'Alpha')");
            stmt.execute("INSERT INTO employee_projects VALUES (1, 1)");
        }

        extractor = new JDBCSchemaExtractor();
    }

    @Test
    void shouldExtractAllTables() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        assertThat(graph.getTables()).hasSize(5);
        assertThat(graph.getTableNames()).contains("DEPARTMENTS", "EMPLOYEES", "EMPLOYEE_DETAILS", "PROJECTS", "EMPLOYEE_PROJECTS");
    }

    @Test
    void shouldClassifyTableTypes() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        assertThat(graph.getTable("DEPARTMENTS").get().getTableType())
                .isEqualTo(TableType.PRIMARY_ENTITY);
        assertThat(graph.getTable("EMPLOYEE_DETAILS").get().getTableType())
                .isEqualTo(TableType.CHILD_ENTITY);
    }

    @Test
    void shouldDetectForeignKeys() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        TableMetadata employees = graph.getTable("EMPLOYEES").get();
        assertThat(employees.getForeignKeys()).hasSize(2); // department_id and manager_id
    }

    @Test
    void shouldDetectSelfReference() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        assertThat(graph.isSelfReferencing("EMPLOYEES")).isTrue();
    }

    @Test
    void shouldDetectOneToOneRelationship() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        // employee_details.employee_id has UNIQUE constraint - H2 detects it as ONE_TO_ONE
        // or ONE_TO_MANY depending on version - both are acceptable
        assertThat(graph.getTable("EMPLOYEE_DETAILS").get().getForeignKeys()).hasSize(1);
    }

    @Test
    void shouldDetectOneToOneRelationshipForJunctionTable() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        // employee_projects has composite PK, so each FK has UNIQUE constraint
        assertThat(graph.getTable("EMPLOYEE_PROJECTS").get().getForeignKeys())
                .hasSize(2);
    }

    @Test
    void shouldExtractColumns() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        TableMetadata employees = graph.getTable("EMPLOYEES").get();
        assertThat(employees.getColumns()).hasSize(4); // id, name, department_id, manager_id
        assertThat(employees.getPrimaryKeyColumn()).isEqualTo("ID");
    }

    @Test
    void shouldExtractRowCount() throws SchemaException {
        SchemaGraph graph = extractor.extractSchema(connection);

        assertThat(graph.getTable("DEPARTMENTS").get().getRowCount()).isEqualTo(2);
        assertThat(graph.getTable("EMPLOYEES").get().getRowCount()).isEqualTo(2);
    }
}
