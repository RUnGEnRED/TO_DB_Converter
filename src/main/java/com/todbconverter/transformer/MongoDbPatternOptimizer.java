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
        resultCollections.put(tableName, new ArrayList<>(documents));
        
        if (documents == null || documents.isEmpty()) {
            return resultCollections;
        }
        
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
        String[] rules = fieldsConfig.split(",");
        for (String rule : rules) {
            String[] parts = rule.split(":");
            if (parts.length != 2) continue;
            
            String targetField = parts[0].trim();
            String operation = parts[1].trim();
            
            for (Map<String, Object> doc : documents) {
                if (operation.startsWith("SUM(") && operation.endsWith(")")) {
                    String colsStr = operation.substring(4, operation.length() - 1);
                    String[] cols = colsStr.split("\\+"); // simple addition of fields
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
                }
            }
        }
    }
    
    private void applyApproximationPattern(List<Map<String, Object>> documents, String tableName) {
        String fieldsConfig = config.getApproximationFields();
        if (fieldsConfig == null || fieldsConfig.isEmpty()) return;
        
        int granularity = config.getApproximationGranularity();
        logger.info("Applying Approximation Pattern on table '{}' with granularity {}", tableName, granularity);
        
        String[] fields = fieldsConfig.split(",");
        for (String field : fields) {
            String targetField = field.trim();
            for (Map<String, Object> doc : documents) {
                Object val = doc.get(targetField);
                if (val instanceof Number) {
                    long longVal = ((Number) val).longValue();
                    // Approximate to the nearest granularity
                    long approximated = (longVal / granularity) * granularity;
                    doc.put(targetField, approximated);
                }
            }
        }
    }
    
    private List<Map<String, Object>> applyAttributePattern(List<Map<String, Object>> documents, String tableName) {
        if (documents.isEmpty()) {
            return documents;
        }
        
        Map<String, List<String>> prefixGroups = new HashMap<>();
        Set<String> processedColumns = new HashSet<>();
        
        Map<String, Object> firstDoc = documents.get(0);
        for (String key : firstDoc.keySet()) {
            Matcher matcher = PREFIX_PATTERN.matcher(key);
            if (matcher.matches()) {
                String prefix = matcher.group(1);
                String suffix = matcher.group(2);
                
                if (!processedColumns.contains(prefix)) {
                    prefixGroups.computeIfAbsent(prefix, k -> new ArrayList<>()).add(key);
                }
                processedColumns.add(prefix);
            }
        }
        
        for (Map.Entry<String, List<String>> entry : prefixGroups.entrySet()) {
            String prefix = entry.getKey();
            List<String> columns = entry.getValue();
            
            if (columns.size() >= config.getAttributePatternThreshold()) {
                logger.info("Applying Attribute Pattern for prefix '{}' on table '{}': grouping {} columns", 
                        prefix, tableName, columns.size());
                
                for (Map<String, Object> doc : documents) {
                    List<Map<String, Object>> grouped = new ArrayList<>();
                    
                    for (String col : columns) {
                        Object value = doc.get(col);
                        if (value != null) {
                            Map<String, Object> attr = new HashMap<>();
                            attr.put("k", col);
                            attr.put("v", value.toString());
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
        
        return documents;
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
                if (keyVal != null) {
                    groupedByKey.computeIfAbsent(keyVal, k -> new ArrayList<>()).add(doc);
                }
            }
            
            List<Map<String, Object>> result = new ArrayList<>();
            for (Map.Entry<Object, List<Map<String, Object>>> entry : groupedByKey.entrySet()) {
                List<Map<String, Object>> groupDocs = entry.getValue();
                for (int i = 0; i < groupDocs.size(); i += bucketSize) {
                    Map<String, Object> bucket = new HashMap<>();
                    int end = Math.min(i + bucketSize, groupDocs.size());
                    List<Map<String, Object>> bucketData = new ArrayList<>(groupDocs.subList(i, end));
                    
                    bucket.put(bucketKey, entry.getKey());
                    bucket.put("bucket_id", i / bucketSize);
                    bucket.put("count", bucketData.size());
                    bucket.put("history", bucketData); // Renamed from 'data' to 'history' per docs example
                    result.add(bucket);
                }
            }
            return result;
        } else {
            // Default index-based bucketing
            logger.info("Applying naive Bucket Pattern on table '{}': buckets of size {}", 
                    tableName, bucketSize);
            
            List<Map<String, Object>> bucketedDocuments = new ArrayList<>();
            for (int i = 0; i < documents.size(); i += bucketSize) {
                Map<String, Object> bucket = new HashMap<>();
                int end = Math.min(i + bucketSize, documents.size());
                List<Map<String, Object>> bucketData = new ArrayList<>(documents.subList(i, end));
                
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
                        List<?> subset = fullList.subList(0, limit);
                        List<?> extras = fullList.subList(limit, fullList.size());
                        
                        mainDoc.put(arrayField, new ArrayList<>(subset));
                        
                        // Create extra records linked to this main document
                        Object docId = mainDoc.get("_id");
                        if (docId == null) docId = mainDoc.get("id");
                        
                        for (Object extraItem : extras) {
                            Map<String, Object> extraRecord = new HashMap<>();
                            extraRecord.put(tableName + "_id", docId);
                            extraRecord.put("item", extraItem);
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
            String outlierField = null;
            
            for (String arrayField : arrayFields) {
                Object arrayValue = doc.get(arrayField);
                if (arrayValue instanceof List) {
                    List<?> list = (List<?>) arrayValue;
                    if (list.size() > threshold) {
                        isOutlier = true;
                        outlierField = arrayField;
                        break;
                    }
                }
            }
            
            if (isOutlier) {
                Map<String, Object> mainDoc = new HashMap<>(doc);
                List<?> fullList = (List<?>) mainDoc.get(outlierField);
                
                List<?> head = fullList.subList(0, threshold);
                List<?> tail = fullList.subList(threshold, fullList.size());
                
                mainDoc.put(outlierField, new ArrayList<>(head));
                mainDoc.put("has_extras", true);
                
                Object docId = mainDoc.get("_id");
                if (docId == null) docId = mainDoc.get("id");
                
                Map<String, Object> outlierExtra = new HashMap<>();
                outlierExtra.put(tableName + "_id", docId);
                outlierExtra.put(outlierField + "_extra", new ArrayList<>(tail));
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
        
        if (!documents.isEmpty()) {
            Map<String, Object> firstDoc = documents.get(0);
            for (Map.Entry<String, Object> entry : firstDoc.entrySet()) {
                if (entry.getValue() instanceof List) {
                    arrayFields.add(entry.getKey());
                }
            }
        }
        
        return arrayFields;
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
        
        for (Map<String, Object> bucket : documents) {
            Object data = bucket.get("data");
            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bucketData = (List<Map<String, Object>>) data;
                resultData.get(tableName).addAll(bucketData);
            }
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