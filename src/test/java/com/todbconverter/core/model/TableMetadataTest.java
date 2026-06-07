package com.todbconverter.core.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TableMetadata.
 */
class TableMetadataTest {

    @Test
    void shouldBuildTableMetadata() {
        TableMetadata table = TableMetadata.builder()
                .name("employees")
                .tableType(TableType.CHILD_ENTITY)
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .addColumn(new ColumnMetadata("department_id", "INT", java.sql.Types.INTEGER, false, true, false))
                .rowCount(100)
                .build();

        assertThat(table.getName()).isEqualTo("employees");
        assertThat(table.getTableType()).isEqualTo(TableType.CHILD_ENTITY);
        assertThat(table.getColumns()).hasSize(3);
        assertThat(table.getRowCount()).isEqualTo(100);
    }

    @Test
    void shouldGetPrimaryKeyColumn() {
        TableMetadata table = TableMetadata.builder()
                .name("employees")
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build();

        assertThat(table.getPrimaryKeyColumn()).isEqualTo("id");
    }

    @Test
    void shouldReturnNullWhenNoPrimaryKey() {
        TableMetadata table = TableMetadata.builder()
                .name("view_table")
                .addColumn(new ColumnMetadata("col1", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build();

        assertThat(table.getPrimaryKeyColumn()).isNull();
    }

    @Test
    void shouldGetNonPrimaryKeyColumns() {
        TableMetadata table = TableMetadata.builder()
                .name("employees")
                .addColumn(new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false))
                .addColumn(new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .addColumn(new ColumnMetadata("email", "VARCHAR", java.sql.Types.VARCHAR, false, true, false))
                .build();

        assertThat(table.getNonPrimaryKeyColumns()).hasSize(2);
        assertThat(table.getNonPrimaryKeyColumns()).extracting(ColumnMetadata::getName)
                .containsExactly("name", "email");
    }

    @Test
    void shouldThrowWhenNameIsNull() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () ->
                TableMetadata.builder().build());
    }
}
