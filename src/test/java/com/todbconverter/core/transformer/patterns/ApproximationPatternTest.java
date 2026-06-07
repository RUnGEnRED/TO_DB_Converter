package com.todbconverter.core.transformer.patterns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ApproximationPattern.
 */
class ApproximationPatternTest {

    private ApproximationPattern pattern;

    @BeforeEach
    void setUp() {
        pattern = new ApproximationPattern();
    }

    @Test
    void shouldHaveCorrectPatternType() {
        assertThat(pattern.getPatternType()).isEqualTo("approximation");
    }

    @Test
    void shouldRoundIntegerToNearestMultiple() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("city", "New Perth");
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40100);
    }

    @Test
    void shouldRoundUpAtHalfWithHalfUpMode() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40150);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100");

        pattern.apply(document, context);

        // HALF_UP: 40150 / 100 = 401.5 → 402 → 40200
        assertThat(document.get("population")).isEqualTo(40200);
    }

    @Test
    void shouldHandleNegativeValues() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("delta", -23);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "delta:10");

        pattern.apply(document, context);

        assertThat(document.get("delta")).isEqualTo(-20);
    }

    @Test
    void shouldSkipNonNumericValues() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("city", "New Perth");
        document.put("country", "Australia");

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "country:10");

        pattern.apply(document, context);

        assertThat(document.get("country")).isEqualTo("Australia");
    }

    @Test
    void shouldSkipMissingFields() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("city", "New Perth");

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100");

        // No exception, no key added
        pattern.apply(document, context);

        assertThat(document).doesNotContainKey("population");
    }

    @Test
    void shouldReturnNarrowIntegerTypeForSmallResult() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("visits", 53L);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "visits:10");

        pattern.apply(document, context);

        // 53 / 10 = 5.3 → 5 (HALF_UP) → 50
        Object result = document.get("visits");
        assertThat(result).isInstanceOf(Integer.class);
        assertThat(result).isEqualTo(50);
    }

    @Test
    void shouldReturnLongTypeForIntegerOverflow() {
        Map<String, Object> document = new LinkedHashMap<>();
        // 3_000_000_000 needs Long, not Integer
        document.put("big", 3_000_000_000L);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "big:1");

        pattern.apply(document, context);

        Object result = document.get("big");
        assertThat(result).isInstanceOf(Long.class);
        assertThat(result).isEqualTo(3_000_000_000L);
    }

    @Test
    void shouldSkipZeroGranularity() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        // "0" is malformed and silently ignored — document unchanged
        context.put("fields", "population:0");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40123);
    }

    @Test
    void shouldBeIdempotentForGranularityOne() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("count", 7);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "count:1");

        pattern.apply(document, context);

        // 7 / 1 = 7 → 7
        assertThat(document.get("count")).isEqualTo(7);
    }

    @Test
    void shouldHandleMultipleFields() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);
        document.put("visits", 9847);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100,visits:1000");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40100);
        assertThat(document.get("visits")).isEqualTo(10000);
    }

    @Test
    void shouldHandleBigDecimalInput() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("price", new BigDecimal("99.99"));

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "price:10");

        pattern.apply(document, context);

        // 99.99 / 10 = 10.0 (HALF_UP from .999) → 10 → 100
        assertThat(document.get("price")).isEqualTo(100);
    }

    @Test
    void shouldHandleEmptyContextFields() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "");

        pattern.apply(document, context);

        // Empty config — no rounding applied
        assertThat(document.get("population")).isEqualTo(40123);
    }

    @Test
    void shouldHandleNullContextFields() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        // no "fields" key at all

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40123);
    }

    @Test
    void shouldHandleFloatInput() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("score", 87.5f);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "score:10");

        pattern.apply(document, context);

        // 87.5 / 10 = 8.75 → 9 (HALF_UP) → 90
        assertThat(document.get("score")).isEqualTo(90);
    }

    @Test
    void shouldHandleDoubleInput() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("temperature", 36.7);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "temperature:5");

        pattern.apply(document, context);

        // 36.7 / 5 = 7.34 → 7 (HALF_UP) → 35
        assertThat(document.get("temperature")).isEqualTo(35);
    }

    @Test
    void shouldHandleStringNumberInput() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("distance", "123.456");

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "distance:10");

        pattern.apply(document, context);

        // 123.456 / 10 = 12.3456 → 12 (HALF_UP) → 120
        assertThat(document.get("distance")).isEqualTo(120);
    }

    @Test
    void shouldSkipNullValueInDocument() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("city", "New Perth");
        document.put("population", null);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100");

        pattern.apply(document, context);

        assertThat(document.get("population")).isNull();
    }

    @Test
    void shouldSkipMalformedConfigEntries() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);
        document.put("visits", 9847);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100,malformed,visits:1000");

        pattern.apply(document, context);

        // "malformed" has no colon → split length != 2, skipped
        // population and visits should still be rounded
        assertThat(document.get("population")).isEqualTo(40100);
        assertThat(document.get("visits")).isEqualTo(10000);
    }

    @Test
    void shouldSkipNonNumericTypes() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("name", "test");
        document.put("data", Map.of("key", "value"));
        document.put("items", java.util.List.of(1, 2, 3));

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "name:10,data:5,items:3");

        pattern.apply(document, context);

        // All should remain unchanged (non-numeric)
        assertThat(document.get("name")).isEqualTo("test");
        assertThat(document.get("data")).isEqualTo(Map.of("key", "value"));
        assertThat(document.get("items")).isEqualTo(java.util.List.of(1, 2, 3));
    }

    @Test
    void shouldReturnBigDecimalForLongOverflow() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("big", Long.MAX_VALUE);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "big:10");

        pattern.apply(document, context);

        // Long.MAX_VALUE = 9223372036854775807
        // /10 = 922337203685477580.7 → 922337203685477581 (HALF_UP)
        // *10 = 9223372036854775810 → overflows Long → BigDecimal
        Object result = document.get("big");
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(result).isEqualTo(new BigDecimal("9223372036854775810"));
    }

    @Test
    void shouldSkipNegativeGranularity() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:-10");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40123);
    }

    @Test
    void shouldHandleWhitespaceInConfig() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "  population  :  100  ");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40100);
    }

    @Test
    void shouldHandleNullDocument() {
        Map<String, Object> context = new HashMap<>();
        context.put("fields", "population:100");

        // Should not throw NPE
        pattern.apply(null, context);
    }

    @Test
    void shouldHandleNullContext() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        // Should not throw NPE
        pattern.apply(document, null);
    }

    @Test
    void shouldHandleNonStringFieldsValue() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", Integer.valueOf(100)); // not a String

        pattern.apply(document, context);

        // Non-String fields value should be silently ignored
        assertThat(document.get("population")).isEqualTo(40123);
    }

    @Test
    void shouldRoundNegativeHalfValueUp() {
        // HALF_UP: discarded fraction ≥ 0.5 behaves as UP (away from zero)
        // -2.5 → -3, then *10 → -30
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("delta", -25);

        Map<String, Object> context = new HashMap<>();
        context.put("fields", "delta:10");

        pattern.apply(document, context);

        // -25 / 10 = -2.5 → HALF_UP (≥0.5 = UP, away from zero) → -3 → *10 → -30
        assertThat(document.get("delta")).isEqualTo(-30);
    }

    @Test
    void shouldSkipEmptyFieldNameInConfig() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("population", 40123);

        Map<String, Object> context = new HashMap<>();
        // Empty field name should be silently skipped
        context.put("fields", ":100");

        pattern.apply(document, context);

        assertThat(document.get("population")).isEqualTo(40123);
    }
}
