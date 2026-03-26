package com.todbconverter.transformer;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UniversalTransformer implements IDataTransformer {
    private static final Logger logger = LoggerFactory.getLogger(UniversalTransformer.class);

    // Reuse existing logic for Relational -> Document
    @Override
    public List<Map<String, Object>> transformToDocuments(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {
        
        List<Map<String, Object>> documents = new ArrayList<>();
        for (Map<String, Object> parentRecord : parentRecords) {
            Map<String, Object> document = new HashMap<>(parentRecord);
            for (ForeignKeyMetadata fk : parentTable.getForeignKeys()) {
                String referencedTable = fk.getReferencedTable();
                if (relatedData.containsKey(referencedTable)) {
                    Object fkValue = document.get(fk.getColumnName());
                    for (Map<String, Object> referencedRecord : relatedData.get(referencedTable)) {
                        if (Objects.equals(referencedRecord.get(fk.getReferencedColumn()), fkValue)) {
                            document.put(fk.getReferencedTable() + "_data", referencedRecord);
                            break;
                        }
                    }
                }
            }
            documents.add(document);
        }
        return documents;
    }

    @Override
    public List<Map<String, Object>> aggregateOneToMany(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> childData,
            Map<String, TableMetadata> tablesMetadata) {
        // Omitting duplicate logic here to focus on simple mapping
        return new ArrayList<>();
    }

    // New logic for Document -> Relational
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
            Object parentId = doc.getOrDefault("_id", doc.getOrDefault("id", UUID.randomUUID().toString()));
            
            // Ensure parent id is recorded
            ColumnMetadata idCol = new ColumnMetadata();
            idCol.setColumnName("id");
            idCol.setDataType(parentId instanceof Integer ? "INT" : "VARCHAR");
            idCol.setPrimaryKey(true);
            addColIfNotExists(parentMeta, idCol);
            parentMeta.setPrimaryKeyColumn("id");
            flatParent.put("id", parentId);

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (key.equals("_id") || key.equals("id")) continue;

                if (value instanceof List) {
                    // Extract to child table
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

                    for (Object listItem : listValues) {
                        if (listItem instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> childDoc = (Map<String, Object>) listItem;
                            Map<String, Object> flatChild = new HashMap<>(childDoc);
                            flatChild.put(parentTableName + "_id", parentId);
                            relationalData.get(childTableName).add(flatChild);

                            // Infer columns for child
                            inferColumns(tablesMetadata.get(childTableName), flatChild);
                        }
                    }
                } else if (value instanceof Map) {
                    // Similar logic for single embedded objects, mapped as child or flattened
                    // Treat as 1:1, flatten by prefixing
                    @SuppressWarnings("unchecked")
                    Map<String, Object> embedded = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> embEntry : embedded.entrySet()) {
                        String flatKey = key + "_" + embEntry.getKey();
                        flatParent.put(flatKey, embEntry.getValue());
                        
                        ColumnMetadata col = new ColumnMetadata();
                        col.setColumnName(flatKey);
                        col.setDataType(inferSqlType(embEntry.getValue()));
                        addColIfNotExists(parentMeta, col);
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

    private void inferColumns(TableMetadata tableMeta, Map<String, Object> flatRecord) {
        for (Map.Entry<String, Object> entry : flatRecord.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            ColumnMetadata col = new ColumnMetadata();
            col.setColumnName(columnName);
            col.setDataType(inferSqlType(value));
            
            if (columnName.equals("id") || columnName.equals("_id")) {
                col.setPrimaryKey(true);
            }
            
            addColIfNotExists(tableMeta, col);
        }
    }

    private void addColIfNotExists(TableMetadata meta, ColumnMetadata newCol) {
        boolean exists = meta.getColumns().stream()
                             .anyMatch(c -> c.getColumnName().equals(newCol.getColumnName()));
        if (!exists) {
            meta.addColumn(newCol);
        }
    }

    private String inferSqlType(Object value) {
        if (value instanceof Integer) return "INT";
        if (value instanceof Long) return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE PRECISION";
        if (value instanceof Boolean) return "BOOLEAN";
        if (value instanceof java.util.Date) return "TIMESTAMP";
        return "VARCHAR";
    }
}
