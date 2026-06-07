package com.todbconverter.core.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SchemaGraph.
 */
class SchemaGraphTest {

    @Test
    void shouldAddTableAndRetrieveIt() {
        SchemaGraph graph = new SchemaGraph();

        TableMetadata table = TableMetadata.builder()
                .name("users")
                .tableType(TableType.PRIMARY_ENTITY)
                .build();

        graph.addTable(table);

        assertThat(graph.getTable("users")).isPresent();
        assertThat(graph.getTable("users").get().getName()).isEqualTo("users");
    }

    @Test
    void shouldDetectCycle() {
        SchemaGraph graph = new SchemaGraph();

        graph.addTable(TableMetadata.builder().name("A").build());
        graph.addTable(TableMetadata.builder().name("B").build());
        graph.addTable(TableMetadata.builder().name("C").build());

        graph.addEdge(new ForeignKeyMetadata("A", "b_id", "B", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("B", "c_id", "C", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("C", "a_id", "A", "id", Cardinality.ONE_TO_ONE));

        assertThat(graph.hasCycle()).isTrue();
    }

    @Test
    void shouldNotDetectCycleWhenNoCycle() {
        SchemaGraph graph = new SchemaGraph();

        graph.addTable(TableMetadata.builder().name("A").build());
        graph.addTable(TableMetadata.builder().name("B").build());
        graph.addTable(TableMetadata.builder().name("C").build());

        graph.addEdge(new ForeignKeyMetadata("A", "b_id", "B", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("B", "c_id", "C", "id", Cardinality.ONE_TO_ONE));

        assertThat(graph.hasCycle()).isFalse();
    }

    @Test
    void shouldDetectSelfReference() {
        SchemaGraph graph = new SchemaGraph();

        graph.addTable(TableMetadata.builder().name("employees").build());
        graph.addEdge(new ForeignKeyMetadata("employees", "manager_id", "employees", "id",
                Cardinality.ONE_TO_MANY));

        assertThat(graph.isSelfReferencing("employees")).isTrue();
    }

    @Test
    void shouldReturnCorrectTopologicalOrder() {
        SchemaGraph graph = new SchemaGraph();

        // A depends on B, B depends on C
        graph.addTable(TableMetadata.builder().name("A").build());
        graph.addTable(TableMetadata.builder().name("B").build());
        graph.addTable(TableMetadata.builder().name("C").build());

        graph.addEdge(new ForeignKeyMetadata("A", "b_id", "B", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("B", "c_id", "C", "id", Cardinality.ONE_TO_ONE));

        List<String> order = graph.getTopologicalOrder();

        // A should come before B, B before C (children-first)
        assertThat(order.indexOf("A")).isLessThan(order.indexOf("B"));
        assertThat(order.indexOf("B")).isLessThan(order.indexOf("C"));
    }

    @Test
    void shouldGetLeafTables() {
        SchemaGraph graph = new SchemaGraph();

        graph.addTable(TableMetadata.builder().name("A").build());
        graph.addTable(TableMetadata.builder().name("B").build());
        graph.addTable(TableMetadata.builder().name("C").build());

        graph.addEdge(new ForeignKeyMetadata("A", "b_id", "B", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("B", "c_id", "C", "id", Cardinality.ONE_TO_ONE));

        assertThat(graph.getLeafTables()).containsExactly("C");
    }

    @Test
    void shouldGetRootTables() {
        SchemaGraph graph = new SchemaGraph();

        graph.addTable(TableMetadata.builder().name("A").build());
        graph.addTable(TableMetadata.builder().name("B").build());
        graph.addTable(TableMetadata.builder().name("C").build());

        graph.addEdge(new ForeignKeyMetadata("A", "b_id", "B", "id", Cardinality.ONE_TO_ONE));
        graph.addEdge(new ForeignKeyMetadata("B", "c_id", "C", "id", Cardinality.ONE_TO_ONE));

        assertThat(graph.getRootTables()).containsExactly("A");
    }
}
