package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OutlierPattern implements SchemaPattern {

    private static final Logger logger = LoggerFactory.getLogger(OutlierPattern.class);
    private final DatabaseConfig config;

    public OutlierPattern(DatabaseConfig config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return "Outlier";
    }

    @Override
    public boolean isEnabled(DatabaseConfig config) {
        return config.useOutlierPattern();
    }

    @Override
    public Map<String, List<Map<String, Object>>> apply(List<Map<String, Object>> documents, String tableName) {
        if (documents == null || documents.isEmpty()) {
            return SchemaPattern.singleResult(tableName, documents);
        }

        int threshold = config.getOutlierThreshold();
        String extrasTableName = tableName + "_outliers";
        List<Map<String, Object>> mainDocs = new ArrayList<>();
        List<Map<String, Object>> outliers = new ArrayList<>();

        Set<String> arrayFields = PatternUtils.findArrayFields(documents);

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

        Map<String, List<Map<String, Object>>> result = new HashMap<>();
        result.put(tableName, mainDocs);
        if (!outliers.isEmpty()) {
            result.put(extrasTableName, outliers);
            logger.info("Outlier Pattern: moved {} large arrays to collection '{}'", outliers.size(), extrasTableName);
        }
        return result;
    }

    public Map<String, List<Map<String, Object>>> reverse(List<Map<String, Object>> mainDocuments,
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
}
