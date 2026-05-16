package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MongoDbPatternOptimizer {
    private static final Logger logger = LoggerFactory.getLogger(MongoDbPatternOptimizer.class);
    
    private final DatabaseConfig config;
    
    private static final Pattern PREFIX_PATTERN = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)_(.+)$");
    
    public MongoDbPatternOptimizer(DatabaseConfig config) {
        this.config = config;
    }
    
    public Map<String, List<Map<String, Object>>> applyPatterns(List<Map<String, Object>> documents, String tableName) {
        Map<String, List<Map<String, Object>>> resultCollections = new HashMap<>();
        if (documents == null || documents.isEmpty()) {
            resultCollections.put(tableName, new ArrayList<>());
            return resultCollections;
        }
        resultCollections.put(tableName, new ArrayList<>(documents));
        
        if (config.useComputedPattern()) {
            applyComputedPattern(resultCollections.get(tableName), tableName);
        }
        
        if (config.useApproximationPattern()) {
            applyApproximationPattern(resultCollections.get(tableName), tableName);
        }
        
        if (config.useAttributePattern()) {
            applyAttributePattern(resultCollections.get(tableName), tableName);
        }
        
        if (config.useBucketPattern()) {
            List<Map<String, Object>> bucketed = applyBucketPattern(resultCollections.get(tableName), tableName);
            resultCollections.put(tableName, bucketed);
        }
        
        if (config.useSubsetPattern()) {
            applySubsetPattern(resultCollections, tableName);
        }
        
        if (config.useOutlierPattern()) {
            applyOutlierPattern(resultCollections, tableName);
        }
        
        return resultCollections;
    }
    
    private void applyComputedPattern(List<Map<String, Object>> documents, String tableName) {
        String fieldsConfig = config.getComputedFields();
        if (fieldsConfig == null || fieldsConfig.isEmpty()) return;
        
        logger.info("Applying Computed Pattern on table '{}'", tableName);
        
        // Expected format: field1:SUM(colA,colB),field2:COUNT(colC)
        // Note: Use commas to separate columns within SUM(), e.g., SUM(colA,colB)
        // Cross-document format: field1:SUM(colA,colB):GROUP_BY(groupKey)
        String[] rules = fieldsConfig.split(",(?![^()]*\\))"); // Split on commas not inside parentheses
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
    }
    
    private void applyCrossDocumentAggregation(List<Map<String, Object>> documents, 
            String targetField, String operation, String groupByKey) {
        logger.info("Cross-document aggregation on '{}' grouped by '{}'", targetField, groupByKey);
        
        // Group documents by the group key
        Map<Object, List<Map<String, Object>>> groupedDocs = new LinkedHashMap<>();
        for (Map<String, Object> doc : documents) {
            Object key = doc.get(groupByKey);
            if (key == null) key = "_NULL_";
            groupedDocs.computeIfAbsent(key, k -> new ArrayList<>()).add(doc);
        }
        
        // Compute aggregated values per group
        Map<Object, Double> sumResults = new HashMap<>();
        Map<Object, Integer> countResults = new HashMap<>();
        
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
        
        // Apply computed values to each document
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
    
    private void applyApproximationPattern(List<Map<String, Object>> documents, String tableName) {
        String fieldsConfig = config.getApproximationFields();
        if (fieldsConfig == null || fieldsConfig.isEmpty()) return;
        
        int granularity = config.getApproximationGranularity();
        if (granularity <= 0) {
            logger.warn("Approximation Pattern: granularity must be > 0, got {}. Skipping.", granularity);
            return;
        }
        logger.info("Applying Approximation Pattern on table '{}' with granularity {}", tableName, granularity);
        
        String[] fields = fieldsConfig.split(",");
        for (String field : fields) {
            String targetField = field.trim();
            for (Map<String, Object> doc : documents) {
                Object val = doc.get(targetField);
                if (val instanceof Number) {
                    long longVal = ((Number) val).longValue();
                    // Approximate to nearest multiple of granularity (round to nearest, not floor)
                    long approximated = Math.round((double) longVal / granularity) * granularity;
                    doc.put(targetField, approximated);
                }
            }
        }
    }
    
    private void applyAttributePattern(List<Map<String, Object>> documents, String tableName) {
        if (documents.isEmpty()) {
            return;
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
                                // Extract unit/type from suffix if present (e.g., release_US -> u: "US")
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
    }
    
    private List<Map<String, Object>> applyBucketPattern(List<Map<String, Object>> documents, String tableName) {
        if (documents.isEmpty()) {
            return documents;
        }
        
        String bucketKey = config.getBucketKey();
        int bucketSize = config.getBucketSize();
        
        if (bucketKey != null && !bucketKey.isEmpty()) {
            logger.info("Applying Bucket Pattern on table '{}': grouping by key '{}' with size {}", 
                    tableName, bucketKey, bucketSize);
            
            Map<Object, List<Map<String, Object>>> groupedByKey = new LinkedHashMap<>();
            for (Map<String, Object> doc : documents) {
                Object keyVal = doc.get(bucketKey);
                Object groupKey = keyVal != null ? keyVal : "_NULL_";
                groupedByKey.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(doc);
            }
            
            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<Object, List<Map<String, Object>>> entry : groupedByKey.entrySet()) {
                Object keyVal = "_NULL_".equals(entry.getKey()) ? null : entry.getKey();
                List<Map<String, Object>> groupDocs = entry.getValue();
                for (int i = 0; i < groupDocs.size(); i += bucketSize) {
                    Map<String, Object> bucket = new HashMap<>();
                    int end = Math.min(i + bucketSize, groupDocs.size());
                    List<Map<String, Object>> bucketData = new ArrayList<>(groupDocs.subList(i, end));
                    
                    // Use time-based _id format: keyVal_epochSeconds (per MongoDB official pattern)
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
            return result;
        } else {
            // Default index-based bucketing
            logger.info("Applying naive Bucket Pattern on table '{}': buckets of size {}", 
                    tableName, bucketSize);
            
            List<Map<String, Object>> bucketedDocuments = new ArrayList<>();
            long epochSeconds = System.currentTimeMillis() / 1000;
            for (int i = 0; i < documents.size(); i += bucketSize) {
                Map<String, Object> bucket = new HashMap<>();
                int end = Math.min(i + bucketSize, documents.size());
                List<Map<String, Object>> bucketData = new ArrayList<>(documents.subList(i, end));
                
                bucket.put("_id", "batch_" + epochSeconds + "_" + (i / bucketSize));
                bucket.put("bucket_id", i / bucketSize);
                bucket.put("count", bucketData.size());
                bucket.put("data", bucketData);
                bucketedDocuments.add(bucket);
            }
            return bucketedDocuments;
        }
    }
    
    private void applySubsetPattern(Map<String, List<Map<String, Object>>> resultCollections, String tableName) {
        List<Map<String, Object>> documents = resultCollections.get(tableName);
        if (documents == null || documents.isEmpty()) {
            return;
        }
        
        int limit = config.getSubsetLimit();
        String extrasTableName = tableName + "_extras";
        List<Map<String, Object>> mainResult = new ArrayList<>();
        List<Map<String, Object>> extrasResult = new ArrayList<>();
        
        Set<String> arrayFields = findArrayFields(documents);
        
        for (Map<String, Object> doc : documents) {
            Map<String, Object> mainDoc = new HashMap<>(doc);
            boolean hasSubsetted = false;
            
            for (String arrayField : arrayFields) {
                Object arrayValue = mainDoc.get(arrayField);
                if (arrayValue instanceof List) {
                    List<?> fullList = (List<?>) arrayValue;
                    
                    if (fullList.size() > limit) {
                        // Sort by date field if present (most recent first), otherwise keep original order
                        List<Object> sortedList = new ArrayList<>(fullList);
                        sortedList.sort((a, b) -> {
                            Object dateA = getDateValue(a);
                            Object dateB = getDateValue(b);
                            if (dateA != null && dateB != null) {
                                return compareDates(dateB, dateA); // descending (most recent first)
                            }
                            return 0;
                        });
                        
                        List<?> subset = sortedList.subList(0, limit);
                        List<?> extras = sortedList.subList(limit, sortedList.size());
                        
                        mainDoc.put(arrayField, new ArrayList<>(subset));
                        
                        // Create extra records linked to this main document
                        Object docId = mainDoc.get("_id");
                        if (docId == null) docId = mainDoc.get("id");
                        
                        for (Object extraItem : extras) {
                            Map<String, Object> extraRecord = new HashMap<>();
                            extraRecord.put(tableName + "_id", docId);
                            if (extraItem instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> nested = (Map<String, Object>) extraItem;
                                extraRecord.putAll(nested);
                            } else {
                                extraRecord.put("value", extraItem);
                            }
                            extrasResult.add(extraRecord);
                        }
                        
                        mainDoc.put(arrayField + "_has_more", true);
                        hasSubsetted = true;
                    }
                }
            }
            mainResult.add(mainDoc);
        }
        
        resultCollections.put(tableName, mainResult);
        if (!extrasResult.isEmpty()) {
            resultCollections.put(extrasTableName, extrasResult);
            logger.info("Subset Pattern: moved {} extra items to collection '{}'", extrasResult.size(), extrasTableName);
        }
    }
    
    private void applyOutlierPattern(Map<String, List<Map<String, Object>>> resultCollections, String tableName) {
        List<Map<String, Object>> documents = resultCollections.get(tableName);
        if (documents == null || documents.isEmpty()) {
            return;
        }
        
        int threshold = config.getOutlierThreshold();
        String extrasTableName = tableName + "_outliers";
        List<Map<String, Object>> mainDocs = new ArrayList<>();
        List<Map<String, Object>> outliers = new ArrayList<>();
        
        Set<String> arrayFields = findArrayFields(documents);
        
        for (Map<String, Object> doc : documents) {
            boolean isOutlier = false;
            Map<String, List<?>> oversizedArrays = new LinkedHashMap<>();
            
            for (String arrayField : arrayFields) {
                Object arrayValue = doc.get(arrayField);
                if (arrayValue instanceof List) {
                    List<?> list = (List<?>) arrayValue;
                    if (list.size() > threshold) {
                        oversizedArrays.put(arrayField, list);
                        isOutlier = true;
                    }
                }
            }
            
            if (isOutlier) {
                Map<String, Object> mainDoc = new HashMap<>(doc);
                
                for (Map.Entry<String, List<?>> oversized : oversizedArrays.entrySet()) {
                    String field = oversized.getKey();
                    List<?> fullList = oversized.getValue();
                    
                    List<?> head = fullList.subList(0, threshold);
                    List<?> tail = fullList.subList(threshold, fullList.size());
                    
                    mainDoc.put(field, new ArrayList<>(head));
                }
                
                mainDoc.put("has_extras", true);
                
                Object docId = mainDoc.get("_id");
                if (docId == null) docId = mainDoc.get("id");
                
                Map<String, Object> outlierExtra = new HashMap<>();
                outlierExtra.put(tableName + "_id", docId);
                for (Map.Entry<String, List<?>> oversized : oversizedArrays.entrySet()) {
                    String field = oversized.getKey();
                    List<?> fullList = oversized.getValue();
                    List<?> tail = fullList.subList(threshold, fullList.size());
                    outlierExtra.put(field + "_extra", new ArrayList<>(tail));
                }
                outliers.add(outlierExtra);
                
                mainDocs.add(mainDoc);
            } else {
                mainDocs.add(doc);
            }
        }
        
        resultCollections.put(tableName, mainDocs);
        if (!outliers.isEmpty()) {
            resultCollections.put(extrasTableName, outliers);
            logger.info("Outlier Pattern: moved {} large arrays to collection '{}'", outliers.size(), extrasTableName);
        }
    }
    
    private Set<String> findArrayFields(List<Map<String, Object>> documents) {
        Set<String> arrayFields = new HashSet<>();
        
        for (Map<String, Object> doc : documents) {
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                if (entry.getValue() instanceof List) {
                    arrayFields.add(entry.getKey());
                }
            }
        }
        
        return arrayFields;
    }
    
    private Object getDateValue(Object item) {
        if (!(item instanceof Map)) return null;
        Map<String, Object> map = (Map<String, Object>) item;
        // Common date field names
        String[] dateFields = {"date", "created_at", "updated_at", "published_date", "timestamp", "created"};
        for (String field : dateFields) {
            Object val = map.get(field);
            if (val instanceof java.util.Date || val instanceof java.time.LocalDateTime || 
                val instanceof java.time.LocalDate || val instanceof String) {
                return val;
            }
        }
        return null;
    }
    
    private int compareDates(Object dateA, Object dateB) {
        try {
            java.time.Instant instantA = toInstant(dateA);
            java.time.Instant instantB = toInstant(dateB);
            if (instantA != null && instantB != null) {
                return instantA.compareTo(instantB);
            }
        } catch (Exception e) {
            // Ignore comparison errors
        }
        return 0;
    }
    
    private java.time.Instant toInstant(Object dateObj) {
        if (dateObj instanceof java.util.Date) {
            return ((java.util.Date) dateObj).toInstant();
        }
        if (dateObj instanceof java.time.LocalDateTime) {
            return ((java.time.LocalDateTime) dateObj).atZone(java.time.ZoneId.systemDefault()).toInstant();
        }
        if (dateObj instanceof java.time.LocalDate) {
            return ((java.time.LocalDate) dateObj).atStartOfDay(java.time.ZoneId.systemDefault()).toInstant();
        }
        if (dateObj instanceof String) {
            try {
                return java.time.Instant.parse((String) dateObj);
            } catch (Exception e) {
                try {
                    return java.time.LocalDateTime.parse((String) dateObj).atZone(java.time.ZoneId.systemDefault()).toInstant();
                } catch (Exception e2) {
                    return null;
                }
            }
        }
        return null;
    }
    
    public Map<String, List<Map<String, Object>>> reverseAttributePattern(
            List<Map<String, Object>> documents, String tableName) {
        
        Map<String, List<Map<String, Object>>> resultData = new HashMap<>();
        resultData.put(tableName, new ArrayList<>());
        
        if (documents == null || documents.isEmpty()) {
            return resultData;
        }
        
        Map<String, String> attrReverseMap = new HashMap<>();
        
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
                            attrReverseMap.put(k.toString(), baseName);
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
    
    public Map<String, List<Map<String, Object>>> reverseBucketPattern(
            List<Map<String, Object>> documents, String tableName) {
        
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
    
    public Map<String, List<Map<String, Object>>> reverseOutlierPattern(
            List<Map<String, Object>> mainDocuments,
            List<Map<String, Object>> outlierDocuments,
            String tableName) {
        
        Map<String, List<Map<String, Object>>> resultData = new HashMap<>();
        resultData.put(tableName, new ArrayList<>());
        
        if (mainDocuments == null || mainDocuments.isEmpty()) {
            return resultData;
        }
        
        Map<Object, Map<String, Object>> outlierIndex = new HashMap<>();
        if (outlierDocuments != null) {
            for (Map<String, Object> outlier : outlierDocuments) {
                Object refId = outlier.get(tableName + "_id");
                if (refId != null) {
                    outlierIndex.put(refId.toString(), outlier);
                }
            }
        }
        
        for (Map<String, Object> doc : mainDocuments) {
            Map<String, Object> mergedDoc = new HashMap<>(doc);
            Object docId = mergedDoc.get("_id");
            if (docId == null) docId = mergedDoc.get("id");
            
            if (docId != null) {
                Map<String, Object> outlierData = outlierIndex.get(docId.toString());
                if (outlierData != null) {
                    for (Map.Entry<String, Object> entry : outlierData.entrySet()) {
                        String key = entry.getKey();
                        if (key.equals(tableName + "_id")) continue;
                        if (key.endsWith("_extra") && entry.getValue() instanceof List) {
                            String baseField = key.substring(0, key.length() - 6);
                            Object existing = mergedDoc.get(baseField);
                            if (existing instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> combined = new ArrayList<>((List<?>) existing);
                                combined.addAll((List<?>) entry.getValue());
                                mergedDoc.put(baseField, combined);
                            }
                        }
                    }
                    mergedDoc.remove("has_extras");
                }
            }
            
            resultData.get(tableName).add(mergedDoc);
        }
        
        return resultData;
    }

    public Map<String, List<Map<String, Object>>> reverseSubsetPattern(
            List<Map<String, Object>> documents, String tableName) {
        
        Map<String, List<Map<String, Object>>> resultData = new HashMap<>();
        resultData.put(tableName, new ArrayList<>());
        
        if (documents == null || documents.isEmpty()) {
            return resultData;
        }
        
        for (Map<String, Object> doc : documents) {
            Map<String, Object> mergedDoc = new HashMap<>(doc);
            List<String> extrasToRemove = new ArrayList<>();
            
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                if (key.endsWith("_has_more") && Boolean.TRUE.equals(entry.getValue())) {
                    String baseName = key.substring(0, key.length() - 9);
                    String extrasKey = baseName + "_extras";
                    Object extras = doc.get(extrasKey);
                    String dataKey = baseName;
                    Object data = doc.get(dataKey);
                    
                    if (data instanceof List && extras instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> fullList = new ArrayList<>((List<?>) data);
                        fullList.addAll((List<?>) extras);
                        mergedDoc.put(dataKey, fullList);
                        extrasToRemove.add(extrasKey);
                        extrasToRemove.add(key);
                    }
                }
            }
            
            for (String k : extrasToRemove) {
                mergedDoc.remove(k);
            }
            
            resultData.get(tableName).add(mergedDoc);
        }
        
        return resultData;
    }
}