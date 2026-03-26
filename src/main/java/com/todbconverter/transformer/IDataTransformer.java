package com.todbconverter.transformer;

import com.todbconverter.model.TableMetadata;
import java.util.List;
import java.util.Map;

/**
 * Interface for data transformation from relational to document format.
 * Provides methods to transform flat relational data into nested document structures.
 */
public interface IDataTransformer {
    
    /**
     * Transforms parent records into documents with embedded referenced data.
     * 
     * @param parentTable metadata of the parent table
     * @param parentRecords list of parent records
     * @param relatedData map of related table data
     * @param tablesMetadata map of table metadata
     * @return list of transformed documents
     */
    List<Map<String, Object>> transformToDocuments(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata
    );
    
    /**
     * Aggregates child records into parent documents for One-to-Many relationships.
     * 
     * @param parentTable metadata of the parent table
     * @param parentRecords list of parent records
     * @param childData map of child table data
     * @param tablesMetadata map of table metadata
     * @return list of documents with nested children
     */
    List<Map<String, Object>> aggregateOneToMany(
            TableMetadata parentTable,
            List<Map<String, Object>> parentRecords,
            Map<String, List<Map<String, Object>>> childData,
            Map<String, TableMetadata> tablesMetadata
    );

    /**
     * Flattens complex documents into multiple relational datasets based on inferred or provided metadata.
     * 
     * @param parentTableName the root table/collection name
     * @param documents list of documents
     * @param tablesMetadata map to store generated or provided schemas
     * @return map of table names to their respective list of flat records
     */
    Map<String, List<Map<String, Object>>> flattenToRelational(
            String parentTableName,
            List<Map<String, Object>> documents,
            Map<String, TableMetadata> tablesMetadata
    );
}
