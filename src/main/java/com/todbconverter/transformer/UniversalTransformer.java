package com.todbconverter.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.model.TableMetadata;

import java.util.List;
import java.util.Map;

@Deprecated
public class UniversalTransformer implements IDataTransformer {

    private final RelationalToDocumentTransformer relationalToDocument;
    private final DocumentToRelationalTransformer documentToRelational;

    public UniversalTransformer() {
        this.relationalToDocument = new RelationalToDocumentTransformer();
        this.documentToRelational = new DocumentToRelationalTransformer();
    }

    public UniversalTransformer(DatabaseConfig config) {
        this.relationalToDocument = new RelationalToDocumentTransformer(config);
        this.documentToRelational = new DocumentToRelationalTransformer(config);
    }

    @Override
    public List<Map<String, Object>> transformToDocuments(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {
        return relationalToDocument.transformToDocuments(parentTable, parentRecords, relatedData, tablesMetadata);
    }

    @Override
    public List<Map<String, Object>> aggregateOneToMany(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> childData,
            Map<String, TableMetadata> tablesMetadata) {
        return relationalToDocument.aggregateOneToMany(parentTable, parentRecords, childData, tablesMetadata);
    }

    @Override
    public Map<String, List<Map<String, Object>>> flattenToRelational(
            String parentTableName,
            List<Map<String, Object>> documents,
            Map<String, TableMetadata> tablesMetadata) {
        return documentToRelational.flattenToRelational(parentTableName, documents, tablesMetadata);
    }

    public void clearProcessedIds() {
        documentToRelational.clearProcessedIds();
    }

    public String inferSqlTypeForTest(Object value) {
        return documentToRelational.inferSqlTypeForTest(value);
    }
}
