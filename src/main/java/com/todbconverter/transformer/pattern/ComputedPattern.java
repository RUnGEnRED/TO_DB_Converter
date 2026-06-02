package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ComputedPattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(ComputedPattern.class);
    private final DatabaseConfig config;

    public ComputedPattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Computed";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useComputedPattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        String fieldsConfig = config.getComputedFields();
        if (fieldsConfig == null || fieldsConfig.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        logger.info("Applying Computed Pattern on table '{}'", tableName);

        String[] rules = fieldsConfig.split(",(?![^()]*\\))");
        for (String rule : rules) {
            String[] parts = rule.split(":");
            if (parts.length < 2) continue;

            String targetField = parts[0].trim();
            String operation = parts[1].trim();
            String groupByKey = parts.length > 2 ? parts[2].trim() : null;

            if (groupByKey != null && groupByKey.startsWith("GROUP_BY(")) {
                groupByKey = groupByKey.substring(9, groupByKey.length() - 1);
            }

            if (groupByKey != null && !groupByKey.isEmpty()) {
                applyCrossDocumentAggregation(documents, targetField, operation, groupByKey);
            } else {
                applyIntraDocumentAggregation(documents, targetField, operation);
            }
        }

        return SchemaPattern.singleResult(tableName, documents);
    }

    private void applyCrossDocumentAggregation(List<Map<String, Object>> documents,
            String targetField, String operation, String groupByKey) {
        logger.info("Cross-document aggregation on '{}' grouped by '{}'", targetField, groupByKey);

        Map<Object, List<Map<String, Object>>> groupedDocs = new LinkedHashMap<>();
        for (Map<String, Object> doc : documents) {
            Object key = doc.get(groupByKey);
            if (key == null) key = "_NULL_";
            groupedDocs.computeIfAbsent(key, k -> new ArrayList<>()).add(doc);
        }

        Map<Object, Double> sumResults = new java.util.HashMap<>();
        Map<Object, Integer> countResults = new java.util.HashMap<>();

        for (Map.Entry<Object, List<Map<String, Object>>> entry : groupedDocs.entrySet()) {
            Object groupKey = entry.getKey();
            List<Map<String, Object>> groupDocs = entry.getValue();

            if (operation.startsWith("SUM(") && operation.endsWith(")")) {
                String colsStr = operation.substring(4, operation.length() - 1);
                String[] cols = colsStr.split(",");
                double sum = 0;
                for (Map<String, Object> doc : groupDocs) {
                    for (String col : cols) {
                        Object val = doc.get(col.trim());
                        if (val instanceof Number) {
                            sum += ((Number) val).doubleValue();
                        }
                    }
                }
                sumResults.put(groupKey, sum);
            } else if (operation.startsWith("COUNT(") && operation.endsWith(")")) {
                countResults.put(groupKey, groupDocs.size());
            }
        }

        for (Map<String, Object> doc : documents) {
            Object key = doc.get(groupByKey);
            if (key == null) key = "_NULL_";

            if (sumResults.containsKey(key)) {
                doc.put(targetField, sumResults.get(key));
            }
            if (countResults.containsKey(key)) {
                doc.put(targetField, countResults.get(key));
            }
        }
    }

    private void applyIntraDocumentAggregation(List<Map<String, Object>> documents,
            String targetField, String operation) {
        for (Map<String, Object> doc : documents) {
            if (operation.startsWith("SUM(") && operation.endsWith(")")) {
                String colsStr = operation.substring(4, operation.length() - 1);
                String[] cols = colsStr.split(",");
                double sum = 0;
                for (String col : cols) {
                    Object val = doc.get(col.trim());
                    if (val instanceof Number) {
                        sum += ((Number) val).doubleValue();
                    }
                }
                doc.put(targetField, sum);
            } else if (operation.startsWith("COUNT(") && operation.endsWith(")")) {
                String col = operation.substring(6, operation.length() - 1).trim();
                Object val = doc.get(col);
                if (val instanceof List) {
                    doc.put(targetField, ((List<?>) val).size());
                }
            } else if (operation.startsWith("AVG(") && operation.endsWith(")")) {
                String colsStr = operation.substring(4, operation.length() - 1);
                String[] cols = colsStr.split(",");
                double sum = 0;
                int count = 0;
                for (String col : cols) {
                    Object val = doc.get(col.trim());
                    if (val instanceof Number) {
                        sum += ((Number) val).doubleValue();
                        count++;
                    }
                }
                if (count > 0) {
                    doc.put(targetField, sum / count);
                }
            } else if (operation.startsWith("MAX(") && operation.endsWith(")")) {
                String col = operation.substring(4, operation.length() - 1).trim();
                Object val = doc.get(col);
                if (val instanceof Number) {
                    doc.put(targetField, ((Number) val).doubleValue());
                }
            } else if (operation.startsWith("MIN(") && operation.endsWith(")")) {
                String col = operation.substring(4, operation.length() - 1).trim();
                Object val = doc.get(col);
                if (val instanceof Number) {
                    doc.put(targetField, ((Number) val).doubleValue());
                }
            }
        }
    }
}
