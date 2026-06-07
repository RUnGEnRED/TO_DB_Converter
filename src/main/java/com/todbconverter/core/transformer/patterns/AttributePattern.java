package com.todbconverter.core.transformer.patterns;

import java.util.*;

/**
 * Attribute Pattern: Groups similar fields into an array of key-value pairs.
 * <p>
 * Configuration: pattern.attribute.[Table].[ArrayName]=[Col1]:[Key1],[Col2]:[Key2]
 * Example: pattern.attribute.movies.releases=release_US:USA,release_France:France
 * <p>
 * Result:
 * <pre>
 * {
 *   "title": "Star Wars",
 *   "releases": [
 *     { "location": "USA", "date": "1977-05-20" },
 *     { "location": "France", "date": "1977-10-19" }
 *   ]
 * }
 * </pre>
 */
public class AttributePattern implements PatternApplier {

    @Override
    public String getPatternType() {
        return "attribute";
    }

    @Override
    public void apply(Map<String, Object> document, Map<String, Object> context) {
        String arrayName = (String) context.get("arrayName");
        String mappingsStr = (String) context.get("mappings");

        if (arrayName == null || mappingsStr == null) {
            return;
        }

        // Parse mappings: "release_US:USA,release_France:France"
        List<Map<String, String>> mappings = parseMappings(mappingsStr);

        // Create the array
        List<Map<String, Object>> array = new ArrayList<>();

        for (Map<String, String> mapping : mappings) {
            String sourceColumn = mapping.get("column");
            String keyValue = mapping.get("key");

            // Always remove the source column to keep the document consistent,
            // even if the value is null. Otherwise the original column stays
            // in the document while no entry is added to the array.
            if (document.containsKey(sourceColumn)) {
                Object value = document.get(sourceColumn);
                if (value != null) {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("key", keyValue);
                    entry.put("value", value);
                    array.add(entry);
                }
                document.remove(sourceColumn);
            }
        }

        // Add the array to the document
        document.put(arrayName, array);
    }

    /**
     * Parse mapping string "release_US:USA,release_France:France" into list of maps.
     */
    private List<Map<String, String>> parseMappings(String mappingsStr) {
        List<Map<String, String>> mappings = new ArrayList<>();

        String[] pairs = mappingsStr.split(",");
        for (String pair : pairs) {
            String[] parts = pair.trim().split(":");
            if (parts.length == 2) {
                Map<String, String> mapping = new HashMap<>();
                mapping.put("column", parts[0].trim());
                mapping.put("key", parts[1].trim());
                mappings.add(mapping);
            }
        }

        return mappings;
    }
}
