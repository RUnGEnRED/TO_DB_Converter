package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RelationalToDocumentTransformer {

    private static final Logger logger = LoggerFactory.getLogger(RelationalToDocumentTransformer.class);
    private final DatabaseConfig config;

    private static final int MAX_EMBEDDED_ARRAY_SIZE = 1000;
    @SuppressWarnings("unused")
    private static final long MAX_DOCUMENT_SIZE_BYTES = 16 * 1024 * 1024;

    public RelationalToDocumentTransformer() {
        this.config = null;
    }

    public RelationalToDocumentTransformer(DatabaseConfig config) {
        this.config = config;
    }

    private boolean isEmbedStrategy(String tableName) {
        if (config == null) return true;
        return config.getRelationshipStrategy(tableName) == DatabaseConfig.RelationshipStrategy.EMBED;
    }

    public List<Map<String, Object>> transformToDocuments(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {

        List<Map<String, Object>> documents = new ArrayList<>();

        Map<String, Map<Object, List<Map<String, Object>>>> childIndexes =
                IndexBuilder.buildChildIndexes(relatedData, tablesMetadata, parentTable.getTableName());

        Map<String, Map<String, Object>> referenceIndexes =
                IndexBuilder.buildReferenceIndexes(relatedData, tablesMetadata);

        Map<String, Map<Object, Set<Object>>> manyToManyIndexes =
                IndexBuilder.buildManyToManyIndexes(relatedData, tablesMetadata, parentTable.getTableName());

        Map<String, Map<Object, List<Map<String, Object>>>> junctionMnIndexes =
                IndexBuilder.buildJunctionMnIndexes(
                        parentTable.getTableName(), childIndexes, relatedData, tablesMetadata, referenceIndexes);

        for (Map<String, Object> parentRecord : parentRecords) {
            Map<String, Object> document = new HashMap<>(parentRecord);

            List<ForeignKeyMetadata> foreignKeys = parentTable.getForeignKeys();
            if (foreignKeys != null) {
                for (ForeignKeyMetadata fk : foreignKeys) {
                    String referencedTable = fk.getReferencedTable();

                    if (fk.getRelationshipType() == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY) {
                        handleManyToManyReference(document, parentTable, parentRecord, referencedTable,
                                tablesMetadata, manyToManyIndexes, referenceIndexes);
                    } else if (fk.getRelationshipType() == ForeignKeyMetadata.RelationshipType.ONE_TO_ONE) {
                        if (!isEmbedStrategy(referencedTable)) {
                            Object fkValue = document.get(fk.getColumnName());
                            if (fkValue != null) {
                                document.put(referencedTable + "_id", fkValue);
                            }
                        } else {
                            Map<String, Object> refIndex = referenceIndexes.get(referencedTable);
                            if (refIndex != null) {
                                Object fkValue = document.get(fk.getColumnName());
                                if (fkValue != null) {
                                    Object referencedRecord = refIndex.get(fkValue.toString());
                                    if (referencedRecord instanceof Map) {
                                        @SuppressWarnings("unchecked")
                                        Map<String, Object> refRecordMap = (Map<String, Object>) referencedRecord;
                                        document.put(referencedTable, new HashMap<>(refRecordMap));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            String pkColumn = parentTable.getPrimaryKeyColumn();
            if (pkColumn != null) {
                for (Map.Entry<String, Map<Object, List<Map<String, Object>>>> childEntry : childIndexes.entrySet()) {
                    String childTable = childEntry.getKey();

                    TableMetadata childMeta = tablesMetadata.get(childTable);
                    if (childMeta != null && IndexBuilder.isManyToManyRelationship(childMeta, parentTable.getTableName())) {
                        continue;
                    }

                    String fieldName = childTable;
                    ForeignKeyMetadata.RelationshipType relType = null;
                    if (childMeta != null) {
                        relType = IndexBuilder.getRelationshipTypeTo(childMeta, parentTable.getTableName());
                        if (relType == ForeignKeyMetadata.RelationshipType.ONE_TO_MANY) {
                            if (!fieldName.endsWith("s")) {
                                fieldName += "s";
                            }
                        }
                    }

                    if (!isEmbedStrategy(childTable) && relType == ForeignKeyMetadata.RelationshipType.ONE_TO_MANY) {
                        continue;
                    }

                    Map<Object, List<Map<String, Object>>> childIndex = childEntry.getValue();
                    Object parentId = parentRecord.get(pkColumn);
                    if (parentId != null && childIndex.containsKey(parentId)) {
                        List<Map<String, Object>> children = childIndex.get(parentId);
                        if (children.size() > MAX_EMBEDDED_ARRAY_SIZE) {
                            logger.warn("Table '{}' has {} children for parent '{}'. Embedding first {} only. " +
                                            "Consider using REFERENCE strategy or enabling Bucket/Outlier patterns.",
                                    childTable, children.size(), parentId, MAX_EMBEDDED_ARRAY_SIZE);
                            document.put(fieldName, new ArrayList<>(children.subList(0, MAX_EMBEDDED_ARRAY_SIZE)));
                        } else {
                            document.put(fieldName, new ArrayList<>(children));
                        }
                    } else {
                        document.put(fieldName, new ArrayList<>());
                    }
                }
            }

            for (Map.Entry<String, Map<Object, List<Map<String, Object>>>> jmEntry : junctionMnIndexes.entrySet()) {
                String relatedTableName = jmEntry.getKey();
                Map<Object, List<Map<String, Object>>> jmIndex = jmEntry.getValue();
                Object parentId = parentRecord.get(pkColumn);
                if (parentId != null && jmIndex.containsKey(parentId)) {
                    boolean useIds = config != null && config.getManyToManyMode(
                            parentTable.getTableName(), relatedTableName) == DatabaseConfig.ManyToManyMode.IDS;
                    if (useIds) {
                        List<Object> ids = new ArrayList<>();
                        String relatedPkCol = "id";
                        TableMetadata relatedMeta = tablesMetadata.get(relatedTableName);
                        if (relatedMeta != null && relatedMeta.getPrimaryKeyColumn() != null) {
                            relatedPkCol = relatedMeta.getPrimaryKeyColumn();
                        }
                        for (Map<String, Object> entry : jmIndex.get(parentId)) {
                            Object idVal = entry.get(relatedPkCol);
                            if (idVal == null) idVal = entry.get("id");
                            if (idVal == null) idVal = entry.get("_id");
                            if (idVal != null) ids.add(idVal);
                        }
                        document.put(relatedTableName + "_ids", ids);
                    } else {
                        String fieldName = relatedTableName;
                        if (!fieldName.endsWith("s")) fieldName += "s";
                        List<Map<String, Object>> relatedDocs = jmIndex.get(parentId);
                        if (relatedDocs.size() > MAX_EMBEDDED_ARRAY_SIZE) {
                            logger.warn("M:N relationship to '{}' has {} related docs for parent '{}'. Embedding first {} only.",
                                    relatedTableName, relatedDocs.size(), parentId, MAX_EMBEDDED_ARRAY_SIZE);
                            document.put(fieldName, new ArrayList<>(relatedDocs.subList(0, MAX_EMBEDDED_ARRAY_SIZE)));
                        } else {
                            document.put(fieldName, new ArrayList<>(relatedDocs));
                        }
                    }
                }
            }

            documents.add(document);
        }

        logger.info("Transformed {} records from table {} to documents",
                documents.size(), parentTable.getTableName());
        return documents;
    }

    private void handleManyToManyReference(
            Map<String, Object> document,
            TableMetadata parentTable,
            Map<String, Object> parentRecord,
            String referencedTableName,
            Map<String, TableMetadata> tablesMetadata,
            Map<String, Map<Object, Set<Object>>> manyToManyIndexes,
            Map<String, Map<String, Object>> referenceIndexes) {

        String pkColumn = parentTable.getPrimaryKeyColumn();
        if (pkColumn == null) return;

        Object parentId = parentRecord.get(pkColumn);
        if (parentId == null) return;

        Map<Object, Set<Object>> m2mIndex = manyToManyIndexes.get(referencedTableName);
        if (m2mIndex == null) return;

        Set<Object> relatedIds = m2mIndex.get(parentId);

        String fieldName = referencedTableName;
        if (!fieldName.endsWith("s")) {
            fieldName += "s";
        }

        if (relatedIds == null || relatedIds.isEmpty()) {
            document.put(fieldName, new ArrayList<>());
            return;
        }

        boolean useIdsMode = config != null && config.getManyToManyMode(
                parentTable.getTableName(), referencedTableName) == DatabaseConfig.ManyToManyMode.IDS;

        if (useIdsMode) {
            List<Object> idsList = new ArrayList<>(relatedIds);
            document.put(referencedTableName + "_ids", idsList);
        } else {
            List<Map<String, Object>> embeddedData = new ArrayList<>();
            Map<String, Object> refIndex = referenceIndexes.get(referencedTableName);

            if (refIndex != null) {
                for (Object relatedId : relatedIds) {
                    Object referencedData = refIndex.get(relatedId.toString());
                    if (referencedData instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> refDataMap = (Map<String, Object>) referencedData;
                        embeddedData.add(new HashMap<>(refDataMap));
                    }
                }
            }
            if (embeddedData.size() > MAX_EMBEDDED_ARRAY_SIZE) {
                logger.warn("M:N relationship to '{}' has {} related docs. Embedding first {} only.",
                        referencedTableName, embeddedData.size(), MAX_EMBEDDED_ARRAY_SIZE);
                embeddedData = embeddedData.subList(0, MAX_EMBEDDED_ARRAY_SIZE);
            }
            document.put(fieldName, embeddedData);
        }
    }

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
}
