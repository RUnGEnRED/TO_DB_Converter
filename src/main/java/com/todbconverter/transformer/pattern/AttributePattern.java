package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AttributePattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(AttributePattern.class);
    private static final Pattern PREFIX_PATTERN = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)_(.+)$");

    private final DatabaseConfig config;

    public AttributePattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Attribute";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useAttributePattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        if (documents.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        Map<String, Set<String>> prefixGroups = new HashMap<>();

        for (Map<String, Object> doc : documents) {
            for (String key : doc.keySet()) {
                Matcher matcher = PREFIX_PATTERN.matcher(key);
                if (matcher.matches()) {
                    String prefix = matcher.group(1);
                    prefixGroups.computeIfAbsent(prefix, k -> new HashSet<>()).add(key);
                }
            }
        }

        for (Map.Entry<String, Set<String>> entry : prefixGroups.entrySet()) {
            String prefix = entry.getKey();
            List<String> columns = new ArrayList<>(entry.getValue());

            if (columns.size() >= config.getAttributePatternThreshold()) {
                logger.info("Applying Attribute Pattern for prefix '{}' on table '{}': grouping {} columns",
                        prefix, tableName, columns.size());

                for (Map<String, Object> doc : documents) {
                    List<Map<String, Object>> grouped = new ArrayList<>();

                    for (String col : columns) {
                        if (doc.containsKey(col)) {
                            Object value = doc.get(col);
                            Map<String, Object> attr = new HashMap<>();
                            attr.put("k", col);
                            if (value != null) {
                                attr.put("v", value);
                                Matcher matcher = PREFIX_PATTERN.matcher(col);
                                if (matcher.matches()) {
                                    attr.put("u", matcher.group(2));
                                }
                            }
                            grouped.add(attr);
                            doc.remove(col);
                        }
                    }

                    if (!grouped.isEmpty()) {
                        doc.put(prefix + "_attrs", grouped);
                    }
                }
            }
        }

        return SchemaPattern.singleResult(tableName, documents);
    }

    public Map<String, List<Map<String, Object>>> reverse(List<Map<String, Object>> documents, String tableName) {
        Map<String, List<Map<String, Object>>> resultData = new HashMap<>();
        resultData.put(tableName, new ArrayList<>());

        if (documents == null || documents.isEmpty()) {
            return resultData;
        }

        for (Map<String, Object> doc : documents) {
            Map<String, Object> expandedDoc = new HashMap<>(doc);
            List<String> keysToRemove = new ArrayList<>();

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (key.endsWith("_attrs") && value instanceof List) {
                    String baseName = key.substring(0, key.length() - 6);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> attrs = (List<Map<String, Object>>) value;

                    for (Map<String, Object> attr : attrs) {
                        Object k = attr.get("k");
                        Object v = attr.get("v");
                        if (k != null && v != null) {
                            expandedDoc.put(k.toString(), v);
                        }
                    }
                    keysToRemove.add(key);
                }
            }

            for (String k : keysToRemove) {
                expandedDoc.remove(k);
            }

            resultData.get(tableName).add(expandedDoc);
        }

        return resultData;
    }
}
