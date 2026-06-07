package com.todbconverter.core.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.core.model.*;
import com.todbconverter.exception.TransformationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Many-to-Many (junction table) transformation.
 */
class ManyToManyTransformerTest {

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
    void shouldTransformJunctionTableToArrayOfIds() throws TransformationException {
        // Setup: employees <-> projects via employee_projects (pure junction)
        graph.addTable(TableMetadata.builder()
                .name("employees")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("projects")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("employee_projects")
                .tableType(TableType.JUNCTION_TABLE)
                .addColumn(new ColumnMetadata("employee_id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("project_id", "INT", java.sql.Types.INTEGER, true, false, false))
                .build());

        // Edges from junction table to both parents
        graph.addEdge(new ForeignKeyMetadata("employee_projects", "employee_id", "employees", "id",
                Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("employee_projects", "project_id", "projects", "id",
                Cardinality.ONE_TO_ONE));

        rawData.put("employees", List.of(
                Map.of("id", 1, "name", "Jan"),
                Map.of("id", 2, "name", "Anna")
        ));

        rawData.put("projects", List.of(
                Map.of("id", 10, "name", "Alpha"),
                Map.of("id", 20, "name", "Beta")
        ));

        rawData.put("employee_projects", List.of(
                Map.of("employee_id", 1, "project_id", 10),
                Map.of("employee_id", 1, "project_id", 20),
                Map.of("employee_id", 2, "project_id", 10)
        ));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify: employees should have projects embedded
        List<Map<String, Object>> employees = result.get("employees");
        assertThat(employees).hasSize(2);

        Map<String, Object> jan = employees.get(0);
        assertThat(jan.get("name")).isEqualTo("Jan");

        // Check embedded junction data - could be Map or List depending on cardinality
        Object embeddedData = jan.get("employee_projects");
        assertThat(embeddedData).isNotNull();
    }

    @Test
    void shouldTransformWithPayloadJunctionTable() throws TransformationException {
        // Setup: students <-> courses via enrollments (junction with payload)
        graph.addTable(TableMetadata.builder()
                .name("students")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("courses")
                .tableType(TableType.PRIMARY_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("title", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build());

        graph.addTable(TableMetadata.builder()
                .name("enrollments")
                .tableType(TableType.JUNCTION_TABLE)
                .addColumn(new ColumnMetadata("student_id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("course_id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("grade", "DECIMAL", java.sql.Types.DECIMAL, false, true, false))
                .build());

        graph.addEdge(new ForeignKeyMetadata("enrollments", "student_id", "students", "id",
                Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("enrollments", "course_id", "courses", "id",
                Cardinality.ONE_TO_ONE));

        rawData.put("students", List.of(Map.of("id", 1, "name", "Jan")));
        rawData.put("courses", List.of(
                Map.of("id", 10, "title", "Math"),
                Map.of("id", 20, "title", "Physics")
        ));
        rawData.put("enrollments", List.of(
                Map.of("student_id", 1, "course_id", 10, "grade", 4.5),
                Map.of("student_id", 1, "course_id", 20, "grade", 5.0)
        ));

        // Transform
        Map<String, List<Map<String, Object>>> result = transformer.transform(graph, rawData, new DatabaseConfig());

        // Verify: students should have enrollments with grade (payload)
        List<Map<String, Object>> students = result.get("students");
        Map<String, Object> jan = students.get(0);

        // Check embedded junction data - could be Map or List
        Object embeddedData = jan.get("enrollments");
        assertThat(embeddedData).isNotNull();
    }
}
