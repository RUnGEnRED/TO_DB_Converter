package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubsetPattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(SubsetPattern.class);
    private final DatabaseConfig config;

    public SubsetPattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Subset";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useSubsetPattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        if (documents == null || documents.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        int limit = config.getSubsetLimit();
        String extrasTableName = tableName + "_extras";
        List<Map<String, Object>> mainResult = new ArrayList<>();
        List<Map<String, Object>> extrasResult = new ArrayList<>();

        Set<String> arrayFields = PatternUtils.findArrayFields(documents);

        for (Map<String, Object> doc : documents) {
            Map<String, Object> mainDoc = new HashMap<>(doc);
            boolean hasSubsetted = false;

            for (String arrayField : arrayFields) {
                Object arrayValue = mainDoc.get(arrayField);
                if (arrayValue instanceof List) {
                    List<?> fullList = (List<?>) arrayValue;

                    if (fullList.size() > limit) {
                        List<Object> sortedList = new ArrayList<>(fullList);
                        sortedList.sort((a, b) -> {
                            Object dateA = PatternUtils.getDateValue(a);
                            Object dateB = PatternUtils.getDateValue(b);
                            if (dateA != null && dateB != null) {
                                return PatternUtils.compareDates(dateB, dateA);
                            }
                            return 0;
                        });

                        List<?> subset = sortedList.subList(0, limit);
                        List<?> extras = sortedList.subList(limit, sortedList.size());

                        mainDoc.put(arrayField, new ArrayList<>(subset));

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

        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        result.put(tableName, mainResult);
        if (!extrasResult.isEmpty()) {
            result.put(extrasTableName, extrasResult);
            logger.info("Subset Pattern: moved {} extra items to collection '{}'", extrasResult.size(), extrasTableName);
        }
        return result;
    }

    public Map<String, List<Map<String, Object>>> reverse(List<Map<String, Object>> documents, String tableName) {
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
