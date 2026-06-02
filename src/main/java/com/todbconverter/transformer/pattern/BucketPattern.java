package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BucketPattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(BucketPattern.class);
    private final DatabaseConfig config;

    public BucketPattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Bucket";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useBucketPattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        if (documents.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        String bucketKey = config.getBucketKey();
        int bucketSize = config.getBucketSize();

        List<Map<String, Object>> result;

        if (bucketKey != null && !bucketKey.isEmpty()) {
            logger.info("Applying Bucket Pattern on table '{}': grouping by key '{}' with size {}",
                    tableName, bucketKey, bucketSize);

            Map<Object, List<Map<String, Object>>> groupedByKey = new LinkedHashMap<>();
            for (Map<String, Object> doc : documents) {
                Object keyVal = doc.get(bucketKey);
                Object groupKey = keyVal != null ? keyVal : "_NULL_";
                groupedByKey.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(doc);
            }

            result = new ArrayList<>();
            for (Map.Entry<Object, List<Map<String, Object>>> entry : groupedByKey.entrySet()) {
                Object keyVal = "_NULL_".equals(entry.getKey()) ? null : entry.getKey();
                List<Map<String, Object>> groupDocs = entry.getValue();
                for (int i = 0; i < groupDocs.size(); i += bucketSize) {
                    Map<String, Object> bucket = new HashMap<>();
                    int end = Math.min(i + bucketSize, groupDocs.size());
                    List<Map<String, Object>> bucketData = new ArrayList<>(groupDocs.subList(i, end));

                    long epochSeconds = System.currentTimeMillis() / 1000;
                    if (keyVal != null) {
                        bucket.put(bucketKey, keyVal);
                    }
                    bucket.put("_id", (keyVal != null ? keyVal.toString() : "null") + "_" + epochSeconds + "_" + (i / bucketSize));
                    bucket.put("bucket_id", i / bucketSize);
                    bucket.put("count", bucketData.size());
                    bucket.put("data", bucketData);
                    result.add(bucket);
                }
            }
        } else {
            logger.info("Applying naive Bucket Pattern on table '{}': buckets of size {}",
                    tableName, bucketSize);

            result = new ArrayList<>();
            long epochSeconds = System.currentTimeMillis() / 1000;
            for (int i = 0; i < documents.size(); i += bucketSize) {
                Map<String, Object> bucket = new HashMap<>();
                int end = Math.min(i + bucketSize, documents.size());
                List<Map<String, Object>> bucketData = new ArrayList<>(documents.subList(i, end));

                bucket.put("_id", "batch_" + epochSeconds + "_" + (i / bucketSize));
                bucket.put("bucket_id", i / bucketSize);
                bucket.put("count", bucketData.size());
                bucket.put("data", bucketData);
                result.add(bucket);
            }
        }

        return SchemaPattern.singleResult(tableName, result);
    }

    public Map<String, List<Map<String, Object>>> reverse(List<Map<String, Object>> documents, String tableName) {
        Map<String, List<Map<String, Object>>> resultData = new HashMap<>();
        resultData.put(tableName, new ArrayList<>());

        if (documents == null || documents.isEmpty()) {
            return resultData;
        }

        boolean isBucketed = documents.stream()
                .allMatch(doc -> doc.containsKey("data") && doc.containsKey("bucket_id"));

        if (!isBucketed) {
            resultData.get(tableName).addAll(documents);
            return resultData;
        }

        logger.info("Reversing Bucket Pattern on table '{}'", tableName);

        String bucketKey = config.getBucketKey();

        for (Map<String, Object> bucket : documents) {
            Object data = bucket.get("data");
            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bucketData = (List<Map<String, Object>>) data;
                for (Map<String, Object> doc : bucketData) {
                    if (bucketKey != null && !bucketKey.isEmpty() && bucket.containsKey(bucketKey)) {
                        doc.putIfAbsent(bucketKey, bucket.get(bucketKey));
                    }
                }
                resultData.get(tableName).addAll(bucketData);
            }
        }

        return resultData;
    }
}
