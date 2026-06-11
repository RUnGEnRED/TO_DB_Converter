package com.todbconverter.core.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.core.model.*;
import com.todbconverter.exception.TransformationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for UniversalTransformer.
 */
class UniversalTransformerTest {

    private UniversalTransformer transformer;
    private SchemaGraph graph;
    private Map<String, List<Map<String, Object>>> rawData;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDefaultStrategy(Strategy.EMBED);
        transformer = new UniversalTransformer(config);

        graph = new SchemaGraph();
        rawData = new HashMap<>();
    }

    @Test
    void shouldEmbedOneToOneRelationship() throws TransformationException {
        // Setup: employees (1) -> employee_details (1)
        // employee_details has FK to employees
        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employee_details")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("employee_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("pesel", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employee_details", "employee_id", "employees", "id",
                Cardinality.ONE_TO_ONE));

        rawData.put("employees", List.of(
                Map.of("id", 1, "name", "Jan"),
                Map.of("id", 2, "name", "Anna")
        ));

        rawData.put("employee_details", List.of(
                Map.of("id", 10, "employee_id", 1, "pesel", "90010112345")
        ));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify: employees should have employee_details embedded
        List<Map<String, Object>> employees = result.get("employees");
        assertThat(employees).hasSize(2);

        Map<String, Object> jan = employees.get(0);
        assertThat(jan.get("name")).isEqualTo("Jan");
        assertThat(jan.get("employee_details")).isNotNull();

        @SuppressWarnings("unchecked")
        Map<String, Object> embeddedDetails = (Map<String, Object>) jan.get("employee_details");
        assertThat(embeddedDetails.get("pesel")).isEqualTo("90010112345");
    }

    @Test
    void shouldEmbedOneToManyAsArray() throws TransformationException {
        // Setup: departments (1:N) -> employees
        // employees has FK to departments
        graph.addTable(TableMetadata.builder()
                .name("departments")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("department_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employees", "department_id", "departments", "id",
                Cardinality.ONE_TO_MANY));

        rawData.put("departments", List.of(
                Map.of("id", 1, "name", "IT")
        ));

        rawData.put("employees", List.of(
                Map.of("id", 10, "department_id", 1, "name", "Jan"),
                Map.of("id", 11, "department_id", 1, "name", "Anna")
        ));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify
        List<Map<String, Object>> departments = result.get("departments");
        Map<String, Object> itDept = departments.get(0);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> employees = (List<Map<String, Object>>) itDept.get("employees");
        assertThat(employees).isNotNull();
        assertThat(employees).hasSize(2);
        assertThat(employees.get(0).get("name")).isEqualTo("Jan");
    }

    @Test
    void shouldKeepReferenceWhenStrategyIsReference() throws TransformationException {
        // Setup with REFERENCE strategy
        DatabaseConfig config = new DatabaseConfig();
        config.setStrategy("employees", "departments", Strategy.REFERENCE);
        transformer = new UniversalTransformer(config);

        graph.addTable(TableMetadata.builder()
                .name("departments")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("department_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employees", "department_id", "departments", "id",
                Cardinality.ONE_TO_MANY));

        rawData.put("departments", List.of(Map.of("id", 1, "name", "IT")));
        rawData.put("employees", List.of(Map.of("id", 10, "department_id", 1, "name", "Jan")));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, config);

        // Verify: employees should NOT have embedded department
        List<Map<String, Object>> employees = result.get("employees");
        Map<String, Object> jan = employees.get(0);
        assertThat(jan).doesNotContainKey("departments");
        assertThat(jan.get("department_id")).isEqualTo(1); // FK preserved
    }

    @Test
    void shouldHandleSelfReference() throws TransformationException {
        // Setup: employees.manager_id -> employees.id (self-reference)
        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("manager_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employees", "manager_id", "employees", "id",
                Cardinality.ONE_TO_MANY));

        // Use mutable maps to avoid null issues with Map.of
        Map<String, Object> boss = new java.util.HashMap<>();
        boss.put("id", 1);
        boss.put("manager_id", null);
        boss.put("name", "Boss");

        Map<String, Object> worker = new java.util.HashMap<>();
        worker.put("id", 2);
        worker.put("manager_id", 1);
        worker.put("name", "Worker");

        rawData.put("employees", List.of(boss, worker));

        // Transform - should not throw exception
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify
        assertThat(result).containsKey("employees");
        assertThat(result.get("employees")).hasSize(2);
    }

    @Test
    void shouldThrowWhenOutlierWithEmbedStrategy() {
        // Setup: Outlier requires REFERENCE — pre-flight should reject EMBED
        DatabaseConfig config = new DatabaseConfig();
        config.setDefaultStrategy(Strategy.EMBED);
        config.setStrategy("activity_logs", "employees", Strategy.EMBED);
        config.setPatternConfig("employees", "outlier", "activity_logs=2");
        transformer = new UniversalTransformer(config);

        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("activity_logs")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("employee_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("action", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("activity_logs", "employee_id", "employees", "id",
                Cardinality.ONE_TO_MANY));

        rawData.put("employees", List.of(Map.of("id", 1, "name", "Jan")));
        rawData.put("activity_logs", List.of(
                Map.of("id", 1, "employee_id", 1, "action", "login"),
                Map.of("id", 2, "employee_id", 1, "action", "logout")
        ));

        assertThatThrownBy(() -> transformer.transform(graph, rawData, config))
                .isInstanceOf(TransformationException.class)
                .hasMessageContaining("REFERENCE");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldEmbedMultiLevelRelationship() throws TransformationException {
        // Setup: departments (1:N) -> employees (1:1) -> employee_details
        graph.addTable(TableMetadata.builder()
                .name("departments")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("department_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employee_details")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("employee_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .addColumn(new ColumnMetadata("pesel", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("employees", "department_id", "departments", "id",
                Cardinality.ONE_TO_MANY));
        graph.addEdge(new ForeignKeyMetadata("employee_details", "employee_id", "employees", "id",
                Cardinality.ONE_TO_ONE));

        rawData.put("departments", List.of(Map.of("id", 1, "name", "IT")));
        rawData.put("employees", List.of(Map.of("id", 10, "department_id", 1, "name", "Jan")));
        rawData.put("employee_details", List.of(Map.of("id", 100, "employee_id", 10, "pesel", "90010112345")));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify: departments has employees, and those employees have employee_details
        List<Map<String, Object>> departments = result.get("departments");
        assertThat(departments).hasSize(1);

        Map<String, Object> dept = departments.get(0);
        List<Map<String, Object>> employees = (List<Map<String, Object>>) dept.get("employees");
        assertThat(employees).hasSize(1);

        Map<String, Object> employee = employees.get(0);
        assertThat(employee.get("name")).isEqualTo("Jan");

        Map<String, Object> details = (Map<String, Object>) employee.get("employee_details");
        assertThat(details).isNotNull();
        assertThat(details.get("pesel")).isEqualTo("90010112345");
    }
}
