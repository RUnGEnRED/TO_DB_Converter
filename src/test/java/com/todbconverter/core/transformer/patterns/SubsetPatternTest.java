package com.todbconverter.core.transformer.patterns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SubsetPattern.
 */
class SubsetPatternTest {

    private SubsetPattern pattern;

    @BeforeEach
    void setUp() {
        pattern = new SubsetPattern();
    }

    @Test
    void shouldHaveCorrectPatternType() {
        assertThat(pattern.getPatternType()).isEqualTo("subset");
    }

    @Test
    void shouldEmbedOnlyRecentItems() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 1);
        document.put("name", "Product A");

        List<Map<String, Object>> allReviews = IntStream.range(0, 100)
                .mapToObj(i -> {
                    Map<String, Object> review = new LinkedHashMap<>();
                    review.put("id", i);
                    review.put("text", "Review " + i);
                    review.put("product_id", 1);
                    return review;
                })
                .toList();

        Map<String, Object> context = new HashMap<>();
        context.put("limit", 3);
        context.put("children", allReviews);
        context.put("arrayName", "recent_reviews");

        pattern.apply(document, context);

        assertThat(document).containsKey("recent_reviews");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recent = (List<Map<String, Object>>) document.get("recent_reviews");
        assertThat(recent).hasSize(3);
        // Should not contain product_id (FK removed)
        assertThat(recent.get(0)).doesNotContainKey("product_id");
    }

    @Test
    void shouldHandleFewerItemsThanLimit() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("id", 1);

        List<Map<String, Object>> reviews = List.of(
                Map.of("id", 1, "text", "Good"),
                Map.of("id", 2, "text", "OK")
        );

        Map<String, Object> context = new HashMap<>();
        context.put("limit", 5);
        context.put("children", reviews);
        context.put("arrayName", "recent_reviews");

        pattern.apply(document, context);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recent = (List<Map<String, Object>>) document.get("recent_reviews");
        assertThat(recent).hasSize(2); // Only 2 items available
    }
}
