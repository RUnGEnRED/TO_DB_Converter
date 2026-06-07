package com.todbconverter.core.transformer.patterns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Subset Pattern: Embeds only the most recent N child records into the parent document.
 * <p>
 * Configuration: pattern.subset.[ParentTable].[ChildTable]=[Limit]
 * Example: pattern.subset.products.reviews=3
 * <p>
 * The child table remains in a separate collection (REFERENCE strategy),
 * but the parent document gets an embedded subset of the most recent items.
 */
public class SubsetPattern implements PatternApplier {

    @Override
    public String getPatternType() {
        return "subset";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Map<String, Object> document, Map<String, Object> context) {
        Integer limit = (Integer) context.get("limit");
        List<Map<String, Object>> children = (List<Map<String, Object>>) context.get("children");
        String arrayName = (String) context.get("arrayName");
        String fkColumn = (String) context.get("fkColumn");

        if (limit == null || children == null || arrayName == null) {
            return;
        }

        // Extract subset (first N items, assuming children are already sorted)
        int subsetSize = Math.min(limit, children.size());
        List<Map<String, Object>> subset = new ArrayList<>(children.subList(0, subsetSize));

        // Clean up the FK column from embedded subset.
        // If the parser supplied the real FK column name, remove exactly that
        // column. Otherwise fall back to the historical "_id" suffix heuristic.
        subset = subset.stream()
                .map(row -> {
                    Map<String, Object> cleaned = new java.util.LinkedHashMap<>(row);
                    if (fkColumn != null) {
                        cleaned.remove(fkColumn);
                    } else {
                        cleaned.entrySet().removeIf(entry ->
                                entry.getKey().endsWith("_id") && entry.getValue() != null
                        );
                    }
                    return cleaned;
                })
                .toList();

        document.put(arrayName, subset);
    }
}
