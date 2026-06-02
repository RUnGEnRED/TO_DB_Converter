package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.transformer.pattern.ApproximationPattern;
import com.todbconverter.transformer.pattern.AttributePattern;
import com.todbconverter.transformer.pattern.BucketPattern;
import com.todbconverter.transformer.pattern.ComputedPattern;
import com.todbconverter.transformer.pattern.OutlierPattern;
import com.todbconverter.transformer.pattern.SchemaPattern;
import com.todbconverter.transformer.pattern.SubsetPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDbPatternOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbPatternOptimizer.class);

    private final DatabaseConfig config;
    private final List<SchemaPattern> patterns;

    public MongoDbPatternOptimizer(DatabaseConfig config) {
        this.config = config;
        this.patterns = List.of(
                new ComputedPattern(config),
                new ApproximationPattern(config),
                new AttributePattern(config),
                new BucketPattern(config),
                new SubsetPattern(config),
                new OutlierPattern(config)
        );
    }

    public Map<String, List<Map<String, Object>>> applyPatterns(
            List<Map<String, Object>> documents, String tableName) {

        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        if (documents == null || documents.isEmpty()) {
            result.put(tableName, new ArrayList<>());
            return result;
        }
        result.put(tableName, new ArrayList<>(documents));

        for (SchemaPattern pattern : patterns) {
            if (!pattern.isEnabled(config)) continue;

            logger.info("Applying {} Pattern on table '{}'", pattern.getName(), tableName);
            List<Map<String, Object>> currentDocs = result.get(tableName);

            Map<String, List<Map<String, Object>>> patternResult = pattern.apply(currentDocs, tableName);

            for (Map.Entry<String, List<Map<String, Object>>> entry : patternResult.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }
}
