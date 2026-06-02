package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface SchemaPattern {

    String getName();

    boolean isEnabled(DatabaseConfig config);

    Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName);

    default Map<String, List<Map<String, Object>>> reverse(List<Map<String, Object>> documents, String tableName) {
        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        result.put(tableName, new ArrayList<>(documents));
        return result;
    }

    static Map<String, List<Map<String, Object>>> singleResult(String tableName, List<Map<String, Object>> documents) {
        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        result.put(tableName, documents);
        return result;
    }

    static void mergeResults(Map<String, List<Map<String, Object>>> target,
                             Map<String, List<Map<String, Object>>> source) {
        for (Map.Entry<String, List<Map<String, Object>>> entry : source.entrySet()) {
            target.put(entry.getKey(), entry.getValue());
        }
    }
}
