package com.todbconverter.core.transformer.patterns;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OutlierPattern implements PatternApplier {

    @Override
    public String getPatternType() {
        return "outlier";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Map<String, Object> document, Map<String, Object> context) {
        if (document == null || context == null) {
            return;
        }

        Integer threshold = (Integer) context.get("threshold");
        List<Map<String, Object>> children = (List<Map<String, Object>>) context.get("children");
        String arrayName = (String) context.get("arrayName");
        String fkColumn = (String) context.get("fkColumn");

        if (threshold == null || arrayName == null) {
            return;
        }

        if (children == null || children.isEmpty()) {
            document.put(arrayName, new ArrayList<>());
            document.put("has_extras", false);
            return;
        }

        // Clean FK column from embedded copies (redundant in parent context)
        List<Map<String, Object>> cleaned;
        if (fkColumn != null) {
            cleaned = children.stream()
                    .map(row -> {
                        Map<String, Object> copy = new java.util.LinkedHashMap<>(row);
                        copy.remove(fkColumn);
                        return copy;
                    })
                    .toList();
        } else {
            cleaned = new ArrayList<>(children);
        }

        if (cleaned.size() <= threshold) {
            document.put(arrayName, cleaned);
            document.put("has_extras", false);
        } else {
            document.put(arrayName, cleaned.subList(0, threshold));
            document.put("has_extras", true);
        }
    }
}
