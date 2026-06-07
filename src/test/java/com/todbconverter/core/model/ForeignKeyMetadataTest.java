package com.todbconverter.core.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ForeignKeyMetadata.
 */
class ForeignKeyMetadataTest {

    @Test
    void shouldCreateForeignKeyMetadata() {
        ForeignKeyMetadata fk = new ForeignKeyMetadata(
                "employees", "department_id", "departments", "id", Cardinality.ONE_TO_MANY);

        assertThat(fk.getFkTableName()).isEqualTo("employees");
        assertThat(fk.getFkColumnName()).isEqualTo("department_id");
        assertThat(fk.getPkTableName()).isEqualTo("departments");
        assertThat(fk.getPkColumnName()).isEqualTo("id");
        assertThat(fk.getCardinality()).isEqualTo(Cardinality.ONE_TO_MANY);
    }

    @Test
    void shouldDetectSelfReference() {
        ForeignKeyMetadata fk = new ForeignKeyMetadata(
                "employees", "manager_id", "employees", "id", Cardinality.ONE_TO_MANY);

        assertThat(fk.isSelfReferencing()).isTrue();
    }

    @Test
    void shouldDetectNonSelfReference() {
        ForeignKeyMetadata fk = new ForeignKeyMetadata(
                "employees", "department_id", "departments", "id", Cardinality.ONE_TO_MANY);

        assertThat(fk.isSelfReferencing()).isFalse();
    }

    @Test
    void shouldHaveCorrectToString() {
        ForeignKeyMetadata fk = new ForeignKeyMetadata(
                "employees", "department_id", "departments", "id", Cardinality.ONE_TO_MANY);

        assertThat(fk.toString()).contains("employees");
        assertThat(fk.toString()).contains("departments");
        assertThat(fk.toString()).contains("ONE_TO_MANY");
    }
}
