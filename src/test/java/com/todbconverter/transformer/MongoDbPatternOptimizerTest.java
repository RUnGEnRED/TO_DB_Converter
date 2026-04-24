package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class MongoDbPatternOptimizerTest {

    @Mock
    private DatabaseConfig config;

    private MongoDbPatternOptimizer optimizer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        optimizer = new MongoDbPatternOptimizer(config);
        
        // Default mock settings
        when(config.useAttributePattern()).thenReturn(false);
        when(config.useBucketPattern()).thenReturn(false);
        when(config.useSubsetPattern()).thenReturn(false);
        when(config.useOutlierPattern()).thenReturn(false);
        when(config.useComputedPattern()).thenReturn(false);
        when(config.useApproximationPattern()).thenReturn(false);
    }

    @Test
    void testApplyComputedPattern() {
        when(config.useComputedPattern()).thenReturn(true);
        when(config.getComputedFields()).thenReturn("total_val:SUM(a+b),count_items:COUNT(items)");
        
        List<Map<String, Object>> documents = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("a", 10);
        doc.put("b", 20);
        doc.put("items", Arrays.asList(1, 2, 3));
        documents.add(doc);
        
        Map<String, List<Map<String, Object>>> result = optimizer.applyPatterns(documents, "test_table");
        
        Map<String, Object> optimizedDoc = result.get("test_table").get(0);
        assertEquals(30.0, optimizedDoc.get("total_val"));
        assertEquals(3, optimizedDoc.get("count_items"));
    }

    @Test
    void testApplyApproximationPattern() {
        when(config.useApproximationPattern()).thenReturn(true);
        when(config.getApproximationFields()).thenReturn("population");
        when(config.getApproximationGranularity()).thenReturn(100);
        
        List<Map<String, Object>> documents = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("population", 40123);
        documents.add(doc);
        
        Map<String, List<Map<String, Object>>> result = optimizer.applyPatterns(documents, "test_table");
        
        Map<String, Object> optimizedDoc = result.get("test_table").get(0);
        assertEquals(40100L, optimizedDoc.get("population"));
    }

    @Test
    void testApplyBucketPatternWithKey() {
        when(config.useBucketPattern()).thenReturn(true);
        when(config.getBucketKey()).thenReturn("customerId");
        when(config.getBucketSize()).thenReturn(2);
        
        List<Map<String, Object>> documents = new ArrayList<>();
        documents.add(createDoc("customerId", 1, "val", "A"));
        documents.add(createDoc("customerId", 1, "val", "B"));
        documents.add(createDoc("customerId", 1, "val", "C"));
        documents.add(createDoc("customerId", 2, "val", "D"));
        
        Map<String, List<Map<String, Object>>> result = optimizer.applyPatterns(documents, "trades");
        List<Map<String, Object>> bucketed = result.get("trades");
        
        assertEquals(3, bucketed.size()); // 2 buckets for customer 1, 1 bucket for customer 2
        
        // Check first bucket
        assertEquals(1, bucketed.get(0).get("customerId"));
        assertEquals(2, bucketed.get(0).get("count"));
        assertTrue(bucketed.get(0).containsKey("history"));
    }

    @Test
    void testApplySubsetPatternWithSeparateCollection() {
        when(config.useSubsetPattern()).thenReturn(true);
        when(config.getSubsetLimit()).thenReturn(1);
        
        List<Map<String, Object>> documents = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("id", 1);
        doc.put("reviews", Arrays.asList("Good", "Bad", "Excellent"));
        documents.add(doc);
        
        Map<String, List<Map<String, Object>>> result = optimizer.applyPatterns(documents, "products");
        
        assertTrue(result.containsKey("products"));
        assertTrue(result.containsKey("products_extras"));
        
        assertEquals(1, result.get("products").get(0).get("reviews") instanceof List ? ((List)result.get("products").get(0).get("reviews")).size() : -1);
        assertEquals(2, result.get("products_extras").size());
        assertEquals(1, result.get("products_extras").get(0).get("products_id"));
    }

    @Test
    void testApplyOutlierPatternWithSeparateCollection() {
        when(config.useOutlierPattern()).thenReturn(true);
        when(config.getOutlierThreshold()).thenReturn(2);
        
        List<Map<String, Object>> documents = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("id", 1);
        doc.put("sales", Arrays.asList("S1", "S2", "S3", "S4")); // Outlier
        documents.add(doc);
        
        Map<String, List<Map<String, Object>>> result = optimizer.applyPatterns(documents, "books");
        
        assertTrue(result.containsKey("books_outliers"));
        assertEquals(true, result.get("books").get(0).get("has_extras"));
        assertEquals(2, ((List)result.get("books").get(0).get("sales")).size());
        assertEquals(2, ((List)result.get("books_outliers").get(0).get("sales_extra")).size());
    }

    private Map<String, Object> createDoc(Object... kv) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i].toString(), kv[i+1]);
        }
        return map;
    }
}
