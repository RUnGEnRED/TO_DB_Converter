package com.todbconverter.core.transformer.patterns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutlierPatternTest {

    private OutlierPattern pattern;

    @BeforeEach
    void setUp() {
        pattern = new OutlierPattern();
    }

    @Test
    void shouldHaveCorrectPatternType() {
        assertThat(pattern.getPatternType()).isEqualTo("outlier");
    }

    @Test
    void shouldEmbedAllWhenBelowThreshold() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        children.add(Map.of("id", 1, "action", "login", "employee_id", 1));
        children.add(Map.of("id", 2, "action", "logout", "employee_id", 1));

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", children);
        context.put("arrayName", "activity_logs");
        context.put("fkColumn", "employee_id");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(false);
        List<Map<String, Object>> result = (List<Map<String, Object>>) document.get("activity_logs");
        assertThat(result).hasSize(2);
        assertThat(result.get(0)).doesNotContainKey("employee_id");
    }

    @Test
    void shouldCapWhenAboveThreshold() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        children.add(Map.of("id", 1, "action", "login", "employee_id", 1));
        children.add(Map.of("id", 2, "action", "edit", "employee_id", 1));
        children.add(Map.of("id", 3, "action", "view", "employee_id", 1));
        children.add(Map.of("id", 4, "action", "logout", "employee_id", 1));
        children.add(Map.of("id", 5, "action", "click", "employee_id", 1));

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", children);
        context.put("arrayName", "activity_logs");
        context.put("fkColumn", "employee_id");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(true);
        List<Map<String, Object>> result = (List<Map<String, Object>>) document.get("activity_logs");
        assertThat(result).hasSize(3);
    }

    @Test
    void shouldEmbedAllWhenExactlyAtThreshold() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        children.add(Map.of("id", 1, "action", "login"));
        children.add(Map.of("id", 2, "action", "logout"));
        children.add(Map.of("id", 3, "action", "edit"));

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", children);
        context.put("arrayName", "logs");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(false);
        assertThat((List<?>) document.get("logs")).hasSize(3);
    }

    @Test
    void shouldHandleEmptyChildren() {
        Map<String, Object> document = new LinkedHashMap<>();

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", new ArrayList<>());
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(false);
        assertThat((List<?>) document.get("activity_logs")).isEmpty();
    }

    @Test
    void shouldHandleNullChildren() {
        Map<String, Object> document = new LinkedHashMap<>();

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(false);
        assertThat((List<?>) document.get("activity_logs")).isEmpty();
    }

    @Test
    void shouldHandleNullDocument() {
        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", new ArrayList<>());
        context.put("arrayName", "activity_logs");

        pattern.apply(null, context);
        // No exception expected
    }

    @Test
    void shouldHandleNullContext() {
        Map<String, Object> document = new LinkedHashMap<>();

        pattern.apply(document, null);
        // No exception expected
    }

    @Test
    void shouldHandleMissingThreshold() {
        Map<String, Object> document = new LinkedHashMap<>();

        Map<String, Object> context = new HashMap<>();
        context.put("children", new ArrayList<>());
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);
        // No crash, no fields added
        assertThat(document).doesNotContainKey("activity_logs");
        assertThat(document).doesNotContainKey("has_extras");
    }

    @Test
    void shouldHandleMissingArrayName() {
        Map<String, Object> document = new LinkedHashMap<>();

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 3);
        context.put("children", new ArrayList<>());

        pattern.apply(document, context);
        // No crash
    }

    @Test
    void shouldRemoveFkColumnFromEmbeddedCopies() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> child = new LinkedHashMap<>();
        child.put("id", 1);
        child.put("action", "login");
        child.put("employee_id", 99);
        children.add(child);

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 5);
        context.put("children", children);
        context.put("arrayName", "activity_logs");
        context.put("fkColumn", "employee_id");

        pattern.apply(document, context);

        List<Map<String, Object>> result = (List<Map<String, Object>>) document.get("activity_logs");
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).containsOnlyKeys("id", "action");
    }

    @Test
    void shouldHandleThresholdZero() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        children.add(Map.of("id", 1, "action", "login"));

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 0);
        context.put("children", children);
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);

        assertThat(document.get("has_extras")).isEqualTo(true);
        assertThat((List<?>) document.get("activity_logs")).isEmpty();
    }

    @Test
    void shouldNotRemoveFkColumnWhenFkColumnIsNull() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        Map<String, Object> child = new LinkedHashMap<>();
        child.put("id", 1);
        child.put("action", "login");
        child.put("employee_id", 99);
        children.add(child);

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 5);
        context.put("children", children);
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);

        List<Map<String, Object>> result = (List<Map<String, Object>>) document.get("activity_logs");
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).containsKey("employee_id");
    }

    @Test
    void shouldPreserveOrderOfChildren() {
        Map<String, Object> document = new LinkedHashMap<>();
        List<Map<String, Object>> children = new ArrayList<>();
        children.add(Map.of("id", 1, "action", "first"));
        children.add(Map.of("id", 2, "action", "second"));
        children.add(Map.of("id", 3, "action", "third"));
        children.add(Map.of("id", 4, "action", "fourth"));

        Map<String, Object> context = new HashMap<>();
        context.put("threshold", 2);
        context.put("children", children);
        context.put("arrayName", "activity_logs");

        pattern.apply(document, context);

        List<Map<String, Object>> result = (List<Map<String, Object>>) document.get("activity_logs");
        assertThat(result).hasSize(2);
        assertThat(result.get(0).get("action")).isEqualTo("first");
        assertThat(result.get(1).get("action")).isEqualTo("second");
    }
}
