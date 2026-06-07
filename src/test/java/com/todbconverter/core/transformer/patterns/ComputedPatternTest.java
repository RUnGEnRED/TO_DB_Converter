package com.todbconverter.core.transformer.patterns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ComputedPattern.
 */
class ComputedPatternTest {

    private ComputedPattern pattern;

    @BeforeEach
    void setUp() {
        pattern = new ComputedPattern();
    }

    @Test
    void shouldHaveCorrectPatternType() {
        assertThat(pattern.getPatternType()).isEqualTo("computed");
    }

    @Test
    void shouldCalculateCount() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 1);
        document.put("name", "Jan");

        List<Map<String, Object>> children = List.of(
                Map.of("id", 10, "parent_id", 1),
                Map.of("id", 11, "parent_id", 1),
                Map.of("id", 12, "parent_id", 1)
        );

        Map<String, Object> context = new HashMap<>();
        context.put("fieldName", "order_count");
        context.put("function", "COUNT");
        context.put("children", children);
        context.put("childColumn", "id");

        pattern.apply(document, context);

        assertThat(document.get("order_count")).isEqualTo(3);
    }

    @Test
    void shouldCalculateSum() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 1);

        List<Map<String, Object>> children = List.of(
                Map.of("id", 10, "price", 100),
                Map.of("id", 11, "price", 200),
                Map.of("id", 12, "price", 150)
        );

        Map<String, Object> context = new HashMap<>();
        context.put("fieldName", "total_spent");
        context.put("function", "SUM");
        context.put("children", children);
        context.put("childColumn", "price");

        pattern.apply(document, context);

        assertThat(document.get("total_spent")).isEqualTo(450);
    }

    @Test
    void shouldHandleEmptyChildren() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 1);

        List<Map<String, Object>> children = Collections.emptyList();

        Map<String, Object> context = new HashMap<>();
        context.put("fieldName", "order_count");
        context.put("function", "COUNT");
        context.put("children", children);
        context.put("childColumn", "id");

        pattern.apply(document, context);

        assertThat(document.get("order_count")).isEqualTo(0);
    }
}
