package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DocumentToRelationalTransformer {

    private static final Logger logger = LoggerFactory.getLogger(DocumentToRelationalTransformer.class);
    private final DatabaseConfig config;
    private final Map<String, Set<Object>> processedIds = new HashMap<>();

    public DocumentToRelationalTransformer() {
        this.config = null;
    }

    public DocumentToRelationalTransformer(DatabaseConfig config) {
        this.config = config;
    }

    public void clearProcessedIds() {
        processedIds.clear();
    }

    public Map<String, List<Map<String, Object>>> flattenToRelational(
            String parentTableName,
            List<Map<String, Object>> documents,
            Map<String, TableMetadata> tablesMetadata) {

        Map<String, List<Map<String, Object>>> relationalData = new HashMap<>();
        relationalData.put(parentTableName, new ArrayList<>());

        if (!tablesMetadata.containsKey(parentTableName)) {
            tablesMetadata.put(parentTableName, new TableMetadata(parentTableName, "public"));
        }
        TableMetadata parentMeta = tablesMetadata.get(parentTableName);

        for (Map<String, Object> doc : documents) {
            Map<String, Object> flatParent = new HashMap<>();

            Object parentIdRaw = doc.get("id");
            if (parentIdRaw == null) parentIdRaw = doc.get("_id");
            String parentId = parentIdRaw != null ? parentIdRaw.toString() : UUID.randomUUID().toString();

            boolean alreadyProcessed = processedIds.computeIfAbsent(parentTableName, k -> new HashSet<>()).contains(parentId);

            if (!alreadyProcessed) {
                ColumnMetadata idCol = new ColumnMetadata();
                idCol.setColumnName("id");
                idCol.setDataType(inferSqlType(parentId));
                idCol.setPrimaryKey(true);
                addColIfNotExists(parentMeta, idCol);
                parentMeta.setPrimaryKeyColumn("id");
                flatParent.put("id", parentId);
            }

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (key.equals("_id") || key.equals("id")) continue;

                if (key.endsWith("_id") && !key.endsWith("_ids") && value != null) {
                    String refTableName = key.substring(0, key.length() - 3);
                    if (!alreadyProcessed) {
                        flatParent.put(key, value);

                        ColumnMetadata refCol = new ColumnMetadata();
                        refCol.setColumnName(key);
                        refCol.setDataType(inferSqlType(value));
                        addColIfNotExists(parentMeta, refCol);
                    }

                    if (!tablesMetadata.containsKey(refTableName)) {
                        tablesMetadata.put(refTableName, new TableMetadata(refTableName, "public"));
                    }
                    continue;
                }

                if (key.endsWith("_ids") && value instanceof List) {
                    String refTableName = key.substring(0, key.length() - 4);
                    handleManyToManyJunction(relationalData, tablesMetadata, parentTableName, parentMeta,
                            parentId, refTableName, (List<?>) value);
                } else if ((key.endsWith("s") || value instanceof List) && value instanceof List) {
                    String refTableName = key;

                    if (!tablesMetadata.containsKey(refTableName)) {
                        TableMetadata childMeta = new TableMetadata(refTableName, "public");
                        ForeignKeyMetadata fk = new ForeignKeyMetadata();
                        fk.setColumnName(parentTableName + "_id");
                        fk.setReferencedTable(parentTableName);
                        fk.setReferencedColumn("id");
                        childMeta.addForeignKey(fk);
                        tablesMetadata.put(refTableName, childMeta);
                    }

                    List<?> listValues = (List<?>) value;
                    relationalData.putIfAbsent(refTableName, new ArrayList<>());

                    String parentIdColumnName = parentTableName + "_id";

                    for (Object listItem : listValues) {
                        if (listItem instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> childDoc = (Map<String, Object>) listItem;
                            Map<String, Object> flatChild = flattenNestedDoc(childDoc);

                            Object childId = flatChild.get("id");
                            if (childId == null) childId = flatChild.get("_id");
                            if (childId == null) childId = UUID.randomUUID().toString();

                            if (processedIds.computeIfAbsent(refTableName, k -> new HashSet<>()).add(childId.toString())) {
                                flatChild.put(parentIdColumnName, parentId);
                                flatChild.put("id", childId);
                                relationalData.get(refTableName).add(flatChild);
                                inferColumns(tablesMetadata.get(refTableName), flatChild);
                            }
                        }
                    }
                } else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> embedded = (Map<String, Object>) value;

                    boolean isRelational = (key.endsWith("_data") || embedded.containsKey("id") || embedded.containsKey("_id"));

                    if (isRelational) {
                        String refTableName;
                        if (key.endsWith("_data")) {
                            refTableName = key.substring(0, key.length() - 5);
                        } else {
                            refTableName = key;
                        }

                        if (embedded.containsKey("id") || embedded.containsKey("_id")) {
                            Object refId = embedded.get("id");
                            if (refId == null) refId = embedded.get("_id");

                            if (refId != null) {
                                Object fkValue = refId;
                                if (refId instanceof Number && !(refId instanceof Float) && !(refId instanceof Double)) {
                                    fkValue = refId;
                                } else if (refId instanceof String) {
                                    try {
                                        fkValue = Integer.parseInt((String) refId);
                                    } catch (NumberFormatException e) {
                                    }
                                }

                                if (!alreadyProcessed) {
                                    flatParent.put(refTableName + "_id", fkValue);
                                    ColumnMetadata fkCol = new ColumnMetadata();
                                    fkCol.setColumnName(refTableName + "_id");
                                    fkCol.setDataType(inferSqlType(fkValue));
                                    addColIfNotExists(parentMeta, fkCol);
                                }

                                if (!tablesMetadata.containsKey(refTableName)) {
                                    tablesMetadata.put(refTableName, new TableMetadata(refTableName, "public"));
                                }
                            }
                        }
                    } else {
                        if (!alreadyProcessed) {
                            for (Map.Entry<String, Object> embEntry : embedded.entrySet()) {
                                String flatKey = key + "_" + embEntry.getKey();
                                flatParent.put(flatKey, embEntry.getValue());

                                ColumnMetadata col = new ColumnMetadata();
                                col.setColumnName(flatKey);
                                col.setDataType(inferSqlType(embEntry.getValue()));
                                addColIfNotExists(parentMeta, col);
                            }
                        }
                    }
                } else {
                    if (!alreadyProcessed) {
                        flatParent.put(key, value);
                        ColumnMetadata col = new ColumnMetadata();
                        col.setColumnName(key);
                        col.setDataType(inferSqlType(value));
                        addColIfNotExists(parentMeta, col);
                    }
                }
            }
            if (!alreadyProcessed) {
                relationalData.get(parentTableName).add(flatParent);
                processedIds.get(parentTableName).add(parentId);
            }
        }

        logger.info("Flattened {} documents into {} tables", documents.size(), relationalData.size());
        return relationalData;
    }

    private Map<String, Object> flattenNestedDoc(Map<String, Object> doc) {
        Map<String, Object> flat = new HashMap<>();

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> refData = (Map<String, Object>) value;
                boolean isRelational = (key.endsWith("_data") || refData.containsKey("id") || refData.containsKey("_id"));

                if (isRelational) {
                    String refTableName;
                    if (key.endsWith("_data")) {
                        refTableName = key.substring(0, key.length() - 5);
                    } else {
                        refTableName = key;
                    }

                    if (refData.containsKey("id") || refData.containsKey("_id")) {
                        Object refId = refData.get("id");
                        if (refId == null) refId = refData.get("_id");

                        if (refId != null) {
                            Object fkValue = refId;
                            if (refId instanceof Number && !(refId instanceof Float) && !(refId instanceof Double)) {
                                fkValue = refId;
                            } else if (refId instanceof String) {
                                try {
                                    fkValue = Integer.parseInt((String) refId);
                                } catch (NumberFormatException e) {
                                }
                            }
                            flat.put(refTableName + "_id", fkValue);
                        }
                    }
                } else {
                    for (Map.Entry<String, Object> nestedEntry : refData.entrySet()) {
                        flat.put(key + "_" + nestedEntry.getKey(), nestedEntry.getValue());
                    }
                }
            } else if (value instanceof List) {
            } else {
                flat.put(key, value);
            }
        }

        return flat;
    }

    private void handleManyToManyJunction(
            Map<String, List<Map<String, Object>>> relationalData,
            Map<String, TableMetadata> tablesMetadata,
            String parentTableName,
            TableMetadata parentMeta,
            Object parentId,
            String refTableName,
            List<?> relatedIds) {

        String junctionTableName = parentTableName + "_" + refTableName + "_junction";

        if (!tablesMetadata.containsKey(junctionTableName)) {
            TableMetadata junctionMeta = new TableMetadata(junctionTableName, "public");
            junctionMeta.setPrimaryKeyColumn("id");

            ForeignKeyMetadata fk1 = new ForeignKeyMetadata();
            fk1.setColumnName(parentTableName + "_id");
            fk1.setReferencedTable(parentTableName);
            fk1.setReferencedColumn("id");
            junctionMeta.addForeignKey(fk1);

            ForeignKeyMetadata fk2 = new ForeignKeyMetadata();
            fk2.setColumnName(refTableName + "_id");
            fk2.setReferencedTable(refTableName);
            fk2.setReferencedColumn("id");
            junctionMeta.addForeignKey(fk2);

            tablesMetadata.put(junctionTableName, junctionMeta);
        }

        relationalData.putIfAbsent(junctionTableName, new ArrayList<>());

        for (Object relatedId : relatedIds) {
            if (relatedId == null) continue;

            String junctionKey = parentId.toString() + "_" + relatedId.toString();
            if (processedIds.computeIfAbsent(junctionTableName, k -> new HashSet<>()).add(junctionKey)) {
                Map<String, Object> junctionRecord = new HashMap<>();
                junctionRecord.put("id", UUID.randomUUID().toString());
                junctionRecord.put(parentTableName + "_id", parentId);
                junctionRecord.put(refTableName + "_id", relatedId);

                relationalData.get(junctionTableName).add(junctionRecord);
                inferColumns(tablesMetadata.get(junctionTableName), junctionRecord);
            }
        }

        logger.info("Created junction table '{}' with {} records (after de-duplication)", junctionTableName, relatedIds.size());
    }

    private void inferColumns(TableMetadata tableMeta, Map<String, Object> flatRecord) {
        if (tableMeta == null || flatRecord == null) return;

        for (Map.Entry<String, Object> entry : flatRecord.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            if (value == null) continue;

            ColumnMetadata existingCol = tableMeta.getColumns().stream()
                    .filter(c -> c.getColumnName().equals(columnName))
                    .findFirst().orElse(null);

            if (existingCol == null) {
                ColumnMetadata col = new ColumnMetadata();
                col.setColumnName(columnName);
                col.setDataType(inferSqlType(value));
                if (columnName.equals("id") || columnName.equals("_id")) {
                    col.setPrimaryKey(true);
                }
                tableMeta.addColumn(col);
            }
        }
    }

    private void addColIfNotExists(TableMetadata meta, ColumnMetadata newCol) {
        boolean exists = meta.getColumns().stream()
                .anyMatch(c -> c.getColumnName().equals(newCol.getColumnName()));
        if (!exists) {
            meta.addColumn(newCol);
        }
    }

    public String inferSqlTypeForTest(Object value) {
        return inferSqlType(value);
    }

    private String inferSqlType(Object value) {
        return com.todbconverter.util.TypeMapper.inferSqlType(value);
    }
}
