package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ApproximationPattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(ApproximationPattern.class);
    private final DatabaseConfig config;

    public ApproximationPattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Approximation";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useApproximationPattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        String fieldsConfig = config.getApproximationFields();
        if (fieldsConfig == null || fieldsConfig.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        int granularity = config.getApproximationGranularity();
        if (granularity <= 0) {
            logger.warn("Approximation Pattern: granularity must be > 0, got {}. Skipping.", granularity);
            return SchemaPattern.singleResult(tableName, documents);
        }

        logger.info("Applying Approximation Pattern on table '{}' with granularity {}", tableName, granularity);

        String[] fields = fieldsConfig.split(",");
        for (String field : fields) {
            String targetField = field.trim();
            for (Map<String, Object> doc : documents) {
                Object val = doc.get(targetField);
                if (val instanceof Number) {
                    long longVal = ((Number) val).longValue();
                    long approximated = Math.round((double) longVal / granularity) * granularity;
                    doc.put(targetField, approximated);
                }
            }
        }

        return SchemaPattern.singleResult(tableName, documents);
    }
}
