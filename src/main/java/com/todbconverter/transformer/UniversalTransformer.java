package com.todbconverter.transformer;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

public class UniversalTransformer implements IDataTransformer {
    private static final Logger logger = LoggerFactory.getLogger(UniversalTransformer.class);

    @Override
    public List<Map<String, Object>> transformToDocuments(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {
        
        List<Map<String, Object>> documents = new ArrayList<>();

        Map<String, Map<Object, List<Map<String, Object>>>> childIndexes = buildChildIndexes(relatedData, tablesMetadata, parentTable.getTableName());

        Map<String, Map<String, Object>> referenceIndexes = buildReferenceIndexes(relatedData, tablesMetadata);

        for (Map<String, Object> parentRecord : parentRecords) {
            Map<String, Object> document = new HashMap<>(parentRecord);

            List<ForeignKeyMetadata> foreignKeys = parentTable.getForeignKeys();
            if (foreignKeys != null) {
                for (ForeignKeyMetadata fk : foreignKeys) {
                    String referencedTable = fk.getReferencedTable();
                    Map<String, Object> refIndex = referenceIndexes.get(referencedTable);
                    if (refIndex != null) {
                        Object fkValue = document.get(fk.getColumnName());
                        if (fkValue != null) {
                            Object referencedRecord = refIndex.get(fkValue.toString());
                            if (referencedRecord instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> refRecordMap = (Map<String, Object>) referencedRecord;
                                document.put(referencedTable + "_data", new HashMap<>(refRecordMap));
                            }
                        }
                    }
                }
            }

            String pkColumn = parentTable.getPrimaryKeyColumn();
            if (pkColumn != null) {
                for (Map.Entry<String, Map<Object, List<Map<String, Object>>>> childEntry : childIndexes.entrySet()) {
                    String childTable = childEntry.getKey();
                    Map<Object, List<Map<String, Object>>> childIndex = childEntry.getValue();
                    Object parentId = parentRecord.get(pkColumn);
                    if (parentId != null && childIndex.containsKey(parentId)) {
                        document.put(childTable, new ArrayList<>(childIndex.get(parentId)));
                    } else {
                        document.put(childTable, new ArrayList<>());
                    }
                }
            }

            documents.add(document);
        }

        logger.info("Transformed {} records from table {} to documents",
                    documents.size(), parentTable.getTableName());
        return documents;
    }

    private Map<String, Map<Object, List<Map<String, Object>>>> buildChildIndexes(
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata,
            String parentTableName) {
        
        Map<String, Map<Object, List<Map<String, Object>>>> childIndexes = new HashMap<>();

        for (Map.Entry<String, TableMetadata> entry : tablesMetadata.entrySet()) {
            String childTableName = entry.getKey();
            TableMetadata childTable = entry.getValue();

            if (childTableName.equals(parentTableName)) continue;

            List<ForeignKeyMetadata> childForeignKeys = childTable.getForeignKeys();
            if (childForeignKeys == null) continue;
            
            for (ForeignKeyMetadata fk : childForeignKeys) {
                if (fk.getReferencedTable().equals(parentTableName)) {
                    List<Map<String, Object>> childRecords = relatedData.get(childTableName);
                    if (childRecords != null) {
                        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
                        for (Map<String, Object> childRecord : childRecords) {
                            Object fkValue = childRecord.get(fk.getColumnName());
                            if (fkValue != null) {
                                index.computeIfAbsent(fkValue, k -> new ArrayList<>()).add(childRecord);
                            }
                        }
                        childIndexes.put(childTableName, index);
                    }
                    break;
                }
            }
        }

        return childIndexes;
    }

    private Map<String, Map<String, Object>> buildReferenceIndexes(
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {
        
        Map<String, Map<String, Object>> referenceIndexes = new HashMap<>();

        for (Map.Entry<String, TableMetadata> entry : tablesMetadata.entrySet()) {
            String tableName = entry.getKey();
            TableMetadata table = entry.getValue();
            List<Map<String, Object>> records = relatedData.get(tableName);

            if (records != null && table.getPrimaryKeyColumn() != null) {
                String pkColumn = table.getPrimaryKeyColumn();
                Map<String, Object> index = new HashMap<>();
                for (Map<String, Object> record : records) {
                    Object pkValue = record.get(pkColumn);
                    if (pkValue != null) {
                        index.put(pkValue.toString(), record);
                    }
                }
                referenceIndexes.put(tableName, index);
            }
        }

        return referenceIndexes;
    }

    @Override
    public List<Map<String, Object>> aggregateOneToMany(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> childData,
            Map<String, TableMetadata> tablesMetadata) {

        List<Map<String, Object>> documents = new ArrayList<>();

        for (Map<String, Object> parentRecord : parentRecords) {
            Map<String, Object> document = new HashMap<>(parentRecord);
            Object parentId = parentRecord.get(parentTable.getPrimaryKeyColumn());

            for (Map.Entry<String, List<Map<String, Object>>> entry : childData.entrySet()) {
                String childTableName = entry.getKey();
                List<Map<String, Object>> childRecords = entry.getValue();
                TableMetadata childTable = tablesMetadata.get(childTableName);

                if (childTable != null) {
                    List<Map<String, Object>> relatedChildren = findRelatedChildren(
                        parentId, parentTable.getTableName(), childRecords, childTable
                    );
                    document.put(childTableName, relatedChildren);
                }
            }

            documents.add(document);
        }

        logger.info("Aggregated {} documents with nested children", documents.size());
        return documents;
    }

    private List<Map<String, Object>> findRelatedChildren(
            Object parentId,
            String parentTableName,
            List<Map<String, Object>> childRecords,
            TableMetadata childTable) {

        List<Map<String, Object>> related = new ArrayList<>();
        
        if (parentId == null || childRecords == null || childTable == null) {
            return related;
        }

        List<ForeignKeyMetadata> foreignKeys = childTable.getForeignKeys();
        if (foreignKeys == null) {
            return related;
        }

        for (ForeignKeyMetadata fk : foreignKeys) {
            if (fk.getReferencedTable().equals(parentTableName)) {
                String fkColumn = fk.getColumnName();
                for (Map<String, Object> childRecord : childRecords) {
                    if (Objects.equals(childRecord.get(fkColumn), parentId)) {
                        related.add(childRecord);
                    }
                }
                break;
            }
        }

        return related;
    }

    @Override
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
            
            // Check "id" first (application-defined), then "_id" (MongoDB ObjectId), then fallback to UUID
            Object parentId = doc.get("id");
            if (parentId == null) {
                parentId = doc.get("_id");
            }
            if (parentId == null) {
                parentId = UUID.randomUUID().toString();
            }
            
            // Try to infer the type of ID from the actual value
            Object typedParentId = parentId;
            if (parentId instanceof Number && !(parentId instanceof Float) && !(parentId instanceof Double)) {
                typedParentId = parentId;
            } else if (parentId instanceof String) {
                try {
                    typedParentId = Integer.parseInt((String) parentId);
                } catch (NumberFormatException e) {
                    // keep as String
                }
            }
            
            ColumnMetadata idCol = new ColumnMetadata();
            idCol.setColumnName("id");
            idCol.setDataType(inferSqlType(typedParentId));
            idCol.setPrimaryKey(true);
            addColIfNotExists(parentMeta, idCol);
            parentMeta.setPrimaryKeyColumn("id");
            flatParent.put("id", typedParentId);

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (key.equals("_id") || key.equals("id")) continue;

                if (value instanceof List) {
                    String childTableName = key;
                    if (!tablesMetadata.containsKey(childTableName)) {
                        TableMetadata childMeta = new TableMetadata(childTableName, "public");
                        ForeignKeyMetadata fk = new ForeignKeyMetadata();
                        fk.setColumnName(parentTableName + "_id");
                        fk.setReferencedTable(parentTableName);
                        fk.setReferencedColumn("id");
                        childMeta.addForeignKey(fk);
                        tablesMetadata.put(childTableName, childMeta);
                    }

                    List<?> listValues = (List<?>) value;
                    relationalData.putIfAbsent(childTableName, new ArrayList<>());

                    TableMetadata existingChildMeta = tablesMetadata.get(childTableName);
                    String parentIdColumnName = parentTableName + "_id";
                    
                    for (Object listItem : listValues) {
                        if (listItem instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> childDoc = (Map<String, Object>) listItem;
                            Map<String, Object> flatChild = flattenNestedDoc(childDoc);
                            
                            Object parentIdValue = parentId;
                            
                            // Try to infer type from the parent ID value
                            if (parentId instanceof Number && !(parentId instanceof Float) && !(parentId instanceof Double)) {
                                parentIdValue = parentId;
                            } else if (parentId instanceof String) {
                                try {
                                    parentIdValue = Integer.parseInt((String) parentId);
                                } catch (NumberFormatException e) {
                                    // keep as String
                                }
                            }
                            
                            flatChild.put(parentIdColumnName, parentIdValue);
                            relationalData.get(childTableName).add(flatChild);

                            inferColumns(tablesMetadata.get(childTableName), flatChild);
                        }
                    }
                } else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> embedded = (Map<String, Object>) value;
                    
                    if (key.endsWith("_data")) {
                        String refTableName = key.substring(0, key.length() - 5);
                        
                        if (embedded.containsKey("id") || embedded.containsKey("_id")) {
                            Object refId = embedded.get("id");
                            if (refId == null) refId = embedded.get("_id");
                            
                            if (refId != null) {
                                Object fkValue = refId;
                                
                                // Try to infer type from the actual value in embedded doc
                                if (refId instanceof Number && !(refId instanceof Float) && !(refId instanceof Double)) {
                                    fkValue = refId;
                                } else if (refId instanceof String) {
                                    // Try to parse as integer if possible
                                    try {
                                        fkValue = Integer.parseInt((String) refId);
                                    } catch (NumberFormatException e) {
                                        // keep as String
                                    }
                                }
                                
                                flatParent.put(refTableName + "_id", fkValue);
                                
                                ColumnMetadata fkCol = new ColumnMetadata();
                                fkCol.setColumnName(refTableName + "_id");
                                fkCol.setDataType(inferSqlType(fkValue));
                                addColIfNotExists(parentMeta, fkCol);
                            }
                        }
                    } else {
                        for (Map.Entry<String, Object> embEntry : embedded.entrySet()) {
                            String flatKey = key + "_" + embEntry.getKey();
                            flatParent.put(flatKey, embEntry.getValue());
                            
                            ColumnMetadata col = new ColumnMetadata();
                            col.setColumnName(flatKey);
                            col.setDataType(inferSqlType(embEntry.getValue()));
                            addColIfNotExists(parentMeta, col);
                        }
                    }
                } else {
                    flatParent.put(key, value);
                    ColumnMetadata col = new ColumnMetadata();
                    col.setColumnName(key);
                    col.setDataType(inferSqlType(value));
                    addColIfNotExists(parentMeta, col);
                }
            }
            relationalData.get(parentTableName).add(flatParent);
        }

        logger.info("Flattened {} documents into {} tables", documents.size(), relationalData.size());
        return relationalData;
    }

    private Map<String, Object> flattenNestedDoc(Map<String, Object> doc) {
        Map<String, Object> flat = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (key.endsWith("_data") && value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> refData = (Map<String, Object>) value;
                String refTableName = key.substring(0, key.length() - 5);
                
                if (refData.containsKey("id") || refData.containsKey("_id")) {
                    // Check "id" first, then "_id"
                    Object refId = refData.get("id");
                    if (refId == null) refId = refData.get("_id");
                    
                    if (refId != null) {
                        Object fkValue = refId;
                        
                        // Try to infer type from the actual value
                        if (refId instanceof Number && !(refId instanceof Float) && !(refId instanceof Double)) {
                            fkValue = refId;
                        } else if (refId instanceof String) {
                            try {
                                fkValue = Integer.parseInt((String) refId);
                            } catch (NumberFormatException e) {
                                // keep as String
                            }
                        }
                        
                        flat.put(refTableName + "_id", fkValue);
                    }
                }
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) value;
                for (Map.Entry<String, Object> nestedEntry : nested.entrySet()) {
                    flat.put(key + "_" + nestedEntry.getKey(), nestedEntry.getValue());
                }
            } else {
                flat.put(key, value);
            }
        }
        
        return flat;
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
            
            // Only add new columns, don't change existing types
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

    String inferSqlTypeForTest(Object value) {
        return inferSqlType(value);
    }

    private String inferSqlType(Object value) {
        if (value == null) return "VARCHAR";
        if (value instanceof Integer) return "INT";
        if (value instanceof Long) return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE PRECISION";
        if (value instanceof BigDecimal) return "DECIMAL";
        if (value instanceof Boolean) return "BOOLEAN";
        if (value instanceof LocalDateTime) return "TIMESTAMP";
        if (value instanceof LocalDate) return "DATE";
        if (value instanceof java.util.Date) return "TIMESTAMP";
        return "VARCHAR";
    }
}
