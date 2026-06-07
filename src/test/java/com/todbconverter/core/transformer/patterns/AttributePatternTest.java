package com.todbconverter.core.transformer.patterns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for AttributePattern.
 */
class AttributePatternTest {

    private AttributePattern pattern;

    @BeforeEach
    void setUp() {
        pattern = new AttributePattern();
    }

    @Test
    void shouldHaveCorrectPatternType() {
        assertThat(pattern.getPatternType()).isEqualTo("attribute");
    }

    @Test
    void shouldGroupFieldsIntoKeyValueArray() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("title", "Star Wars");
        document.put("release_US", "1977-05-20");
        document.put("release_France", "1977-10-19");
        document.put("release_Italy", "1977-10-20");

        Map<String, Object> context = new HashMap<>();
        context.put("arrayName", "releases");
        context.put("mappings", "release_US:USA,release_France:France,release_Italy:Italy");

        pattern.apply(document, context);

        assertThat(document).containsKey("releases");
        assertThat(document).doesNotContainKey("release_US");
        assertThat(document).doesNotContainKey("release_France");
        assertThat(document).doesNotContainKey("release_Italy");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> releases = (List<Map<String, Object>>) document.get("releases");
        assertThat(releases).hasSize(3);
        assertThat(releases.get(0).get("key")).isEqualTo("USA");
        assertThat(releases.get(0).get("value")).isEqualTo("1977-05-20");
    }

    @Test
    void shouldHandleNullValues() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("title", "Star Wars");
        document.put("release_US", "1977-05-20");
        document.put("release_France", null);

        Map<String, Object> context = new HashMap<>();
        context.put("arrayName", "releases");
        context.put("mappings", "release_US:USA,release_France:France");

        pattern.apply(document, context);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> releases = (List<Map<String, Object>>) document.get("releases");
        assertThat(releases).hasSize(1); // Only non-null value
    }
}
