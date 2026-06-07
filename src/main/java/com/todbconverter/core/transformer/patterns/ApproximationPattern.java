package com.todbconverter.core.transformer.patterns;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * Approximation Pattern: Stores coarse-grained numeric values to reduce the
 * frequency of updates and the application workload.
 * <p>
 * Configuration: pattern.approximation.[Table]=[Field1]:[Granularity1],[Field2]:[Granularity2]
 * <p>
 * Example: pattern.approximation.metrics=population:100,visits:1000
 * <p>
 * Result for population=40123 with granularity=100:
 * <pre>
 * { "city": "New Perth", "population": 40100 }
 * </pre>
 * <p>
 * Each field is rounded to the nearest multiple of its granularity using
 * HALF_UP rounding. The result is stored using the narrowest type that fits
 * (Integer → Long → BigDecimal) — same strategy as
 * {@link ComputedPattern#computeSum}.
 */
public class ApproximationPattern implements PatternApplier {

    @Override
    public String getPatternType() {
        return "approximation";
    }

    @Override
    public void apply(Map<String, Object> document, Map<String, Object> context) {
        if (document == null || context == null) {
            return;
        }
        Object fieldsObj = context.get("fields");
        if (!(fieldsObj instanceof String fieldsStr) || fieldsStr.isBlank()) {
            return;
        }

        Map<String, Integer> mappings = parseMappings(fieldsStr);
        for (Map.Entry<String, Integer> entry : mappings.entrySet()) {
            String fieldName = entry.getKey();
            int granularity = entry.getValue();

            if (!document.containsKey(fieldName)) {
                continue;
            }

            Object raw = document.get(fieldName);
            BigDecimal value = toBigDecimal(raw);
            if (value == null) {
                continue;
            }

            BigDecimal rounded = roundToGranularity(value, granularity);
            document.put(fieldName, narrowType(rounded));
        }
    }

    /**
     * Parse mapping string "population:100,visits:1000" into a field→granularity map.
     * Malformed pairs are silently skipped (consistent with AttributePattern).
     */
    private Map<String, Integer> parseMappings(String fieldsStr) {
        Map<String, Integer> result = new HashMap<>();
        String[] pairs = fieldsStr.split(",");
        for (String pair : pairs) {
            String[] parts = pair.trim().split(":");
            if (parts.length == 2) {
                String fieldName = parts[0].trim();
                if (fieldName.isEmpty()) {
                    continue;
                }
                try {
                    int gran = Integer.parseInt(parts[1].trim());
                    if (gran > 0) {
                        result.put(fieldName, gran);
                    }
                } catch (NumberFormatException ignored) {
                    // Skip non-integer granularities
                }
            }
        }
        return result;
    }

    /**
     * Round a BigDecimal to the nearest multiple of granularity using HALF_UP.
     * Math.round(value / granularity) * granularity avoids floating-point loss.
     */
    private BigDecimal roundToGranularity(BigDecimal value, int granularity) {
        return value.divide(BigDecimal.valueOf(granularity), 0, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(granularity));
    }

    /**
     * Convert any numeric value (Integer/Long/Double/Float/BigDecimal/parseable String)
     * to BigDecimal. Returns null for null, non-numeric strings, and unknown types.
     */
    private BigDecimal toBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof Integer i) {
            return BigDecimal.valueOf(i);
        }
        if (value instanceof Long l) {
            return BigDecimal.valueOf(l);
        }
        if (value instanceof Double d) {
            return BigDecimal.valueOf(d);
        }
        if (value instanceof Float f) {
            return BigDecimal.valueOf(f.doubleValue());
        }
        if (value instanceof Short s) {
            return BigDecimal.valueOf(s);
        }
        if (value instanceof Byte b) {
            return BigDecimal.valueOf(b);
        }
        if (value instanceof String str) {
            try {
                return new BigDecimal(str.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Return the narrowest numeric type that fits the rounded value.
     * Mirrors ComputedPattern.computeSum to keep type behaviour consistent
     * across patterns.
     */
    private Object narrowType(BigDecimal value) {
        if (value.scale() == 0) {
            if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0
                    && value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0) {
                return value.intValue();
            }
            if (value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0
                    && value.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
                return value.longValue();
            }
        }
        return value;
    }
}
