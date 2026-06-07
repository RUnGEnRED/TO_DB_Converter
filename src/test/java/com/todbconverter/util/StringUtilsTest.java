package com.todbconverter.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for StringUtils.
 */
class StringUtilsTest {

    @Test
    void shouldConvertTableNameToCollectionName() {
        assertThat(StringUtils.toCollectionName("employees")).isEqualTo("employee");
        assertThat(StringUtils.toCollectionName("departments")).isEqualTo("department");
        assertThat(StringUtils.toCollectionName("addresses")).isEqualTo("address");
    }

    @Test
    void shouldHandleIrregularPlurals() {
        assertThat(StringUtils.toCollectionName("categories")).isEqualTo("category");
        assertThat(StringUtils.toCollectionName("addresses")).isEqualTo("address");
    }

    @Test
    void shouldConvertColumnNameToCamelCase() {
        assertThat(StringUtils.toCamelCase("department_id")).isEqualTo("departmentId");
        assertThat(StringUtils.toCamelCase("first_name")).isEqualTo("firstName");
        assertThat(StringUtils.toCamelCase("id")).isEqualTo("id");
    }

    @Test
    void shouldGenerateForeignKeyField() {
        assertThat(StringUtils.generateForeignKeyField("department")).isEqualTo("departmentId");
        assertThat(StringUtils.generateForeignKeyField("project")).isEqualTo("projectId");
    }

    @Test
    void shouldGenerateReferencesField() {
        assertThat(StringUtils.generateReferencesField("course")).isEqualTo("courseIds");
        assertThat(StringUtils.generateReferencesField("tag")).isEqualTo("tagIds");
    }

    @Test
    void shouldHandleNullInput() {
        assertThat(StringUtils.toCollectionName(null)).isNull();
        assertThat(StringUtils.toCamelCase(null)).isNull();
    }

    @Test
    void shouldHandleEmptyInput() {
        assertThat(StringUtils.toCollectionName("")).isEqualTo("");
        assertThat(StringUtils.toCamelCase("")).isEqualTo("");
    }
}
