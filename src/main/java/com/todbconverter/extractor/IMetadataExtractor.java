package com.todbconverter.extractor;

import com.todbconverter.model.TableMetadata;
import java.util.List;

/**
 * Interface for metadata extraction from database schema.
 * Provides methods to extract table and relationship information.
 */
public interface IMetadataExtractor {
    
    /**
     * Extracts metadata for all tables in the specified schema.
     * 
     * @param schema the database schema name
     * @return list of table metadata
     * @throws Exception if extraction fails
     */
    List<TableMetadata> extractAllTables(String schema) throws Exception;
    
    /**
     * Extracts metadata for a specific table.
     * 
     * @param tableName the name of the table
     * @param schema the database schema name
     * @return table metadata
     * @throws Exception if extraction fails
     */
    TableMetadata extractTableMetadata(String tableName, String schema) throws Exception;
}
