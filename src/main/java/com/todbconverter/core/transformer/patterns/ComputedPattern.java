package com.todbconverter.core.transformer.patterns;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Computed Pattern: Pre-calculates aggregate values during conversion.
 * <p>
 * Configuration: pattern.computed.[Table].[FieldName]=[FUNC]([ChildTable].[Column])
 * Supported functions: COUNT, SUM
 * <p>
 * Example: pattern.computed.customers.order_count=COUNT(orders.id)
 * Result: { "name": "Jan", "order_count": 5 }
 */
public class ComputedPattern implements PatternApplier {

    @Override
    public String getPatternType() {
        return "computed";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Map<String, Object> document, Map<String, Object> context) {
        String fieldName = (String) context.get("fieldName");
        String functionStr = (String) context.get("function");
        List<Map<String, Object>> children = (List<Map<String, Object>>) context.get("children");
        String childColumn = (String) context.get("childColumn");

        if (fieldName == null || functionStr == null || children == null) {
            return;
        }

        Object result;
        switch (functionStr.toUpperCase()) {
            case "COUNT":
                result = computeCount(children);
                break;
            case "SUM":
                result = computeSum(children, childColumn);
                break;
            default:
                throw new IllegalArgumentException("Unsupported computed function: " + functionStr);
        }

        document.put(fieldName, result);
    }

    /**
     * COUNT: Count the number of child records.
     */
    private int computeCount(List<Map<String, Object>> children) {
        return children.size();
    }

    /**
     * SUM: Sum numeric values in the specified column.
     */
    private Object computeSum(List<Map<String, Object>> children, String column) {
        if (column == null || children.isEmpty()) {
            return 0;
        }

        BigDecimal sum = BigDecimal.ZERO;
        for (Map<String, Object> child : children) {
            Object value = child.get(column);
            if (value instanceof Number number) {
                sum = sum.add(new BigDecimal(number.toString()));
            } else if (value instanceof String str) {
                try {
                    sum = sum.add(new BigDecimal(str));
                } catch (NumberFormatException e) {
                    // Skip non-numeric values
                }
            }
        }

        // Return the narrowest type that fits the value.
        // Integer/Long overflow would silently corrupt the result for large sums.
        if (sum.scale() == 0) {
            if (sum.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0
                    && sum.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
                return sum.intValue();
            }
            if (sum.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
                    && sum.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
                return sum.longValue();
            }
        }
        return sum;
    }
}
