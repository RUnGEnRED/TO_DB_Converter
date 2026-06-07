package com.todbconverter.core.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ColumnMetadata.
 */
class ColumnMetadataTest {

    @Test
    void shouldCreateColumnMetadata() {
        ColumnMetadata column = new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false);

        assertThat(column.getName()).isEqualTo("id");
        assertThat(column.getTypeName()).isEqualTo("INT");
        assertThat(column.getSqlType()).isEqualTo(java.sql.Types.INTEGER);
        assertThat(column.isPrimaryKey()).isTrue();
        assertThat(column.isNullable()).isFalse();
        assertThat(column.isAutoIncrement()).isFalse();
    }

    @Test
    void shouldIdentifyPrimaryKey() {
        ColumnMetadata pk = new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false);
        ColumnMetadata nonPk = new ColumnMetadata("name", "VARCHAR", java.sql.Types.VARCHAR, false, true, false);

        assertThat(pk.isPrimaryKey()).isTrue();
        assertThat(nonPk.isPrimaryKey()).isFalse();
    }

    @Test
    void shouldIdentifyNullable() {
        ColumnMetadata nullable = new ColumnMetadata("email", "VARCHAR", java.sql.Types.VARCHAR, false, true, false);
        ColumnMetadata notNullable = new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false);

        assertThat(nullable.isNullable()).isTrue();
        assertThat(notNullable.isNullable()).isFalse();
    }

    @Test
    void shouldHaveCorrectToString() {
        ColumnMetadata column = new ColumnMetadata("id", "INT", java.sql.Types.INTEGER, true, false, false);

        assertThat(column.toString()).contains("id");
        assertThat(column.toString()).contains("INT");
    }
}
