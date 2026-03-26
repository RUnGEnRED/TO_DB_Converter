package com.todbconverter.transformer;

import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DocumentTransformer implements IDataTransformer {
    private static final Logger logger = LoggerFactory.getLogger(DocumentTransformer.class);

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
                    embedReferencedData(document, fk, relatedData.get(referencedTable));
                }
            }

            documents.add(document);
        }

        logger.info("Transformed {} records from table {} to documents", 
                    documents.size(), parentTable.getTableName());
        return documents;
    }

    private void embedReferencedData(Map<String, Object> document, 
                                     ForeignKeyMetadata fk, 
                                     List<Map<String, Object>> referencedRecords) {

        Object fkValue = document.get(fk.getColumnName());

        for (Map<String, Object> referencedRecord : referencedRecords) {
            if (Objects.equals(referencedRecord.get(fk.getReferencedColumn()), fkValue)) {
                document.put(fk.getReferencedTable() + "_data", referencedRecord);
                break;
            }
        }
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

                    // Always include child array, even if empty (REQ-ERR-3)
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

        for (ForeignKeyMetadata fk : childTable.getForeignKeys()) {
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
        throw new UnsupportedOperationException("Flattening to relational is not supported in DocumentTransformer. Use UniversalTransformer instead.");
    }
}
