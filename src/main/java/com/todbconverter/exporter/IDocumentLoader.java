package com.todbconverter.exporter;

import java.util.List;
import java.util.Map;

/**
 * Interface for document loading and export operations.
 * Provides methods to load documents into database and export to files.
 */
public interface IDocumentLoader {
    
    /**
     * Loads documents into the specified collection.
     * 
     * @param collectionName the name of the target collection
     * @param documents the list of documents to load
     */
    void loadDocuments(String collectionName, List<Map<String, Object>> documents);
    
    /**
     * Exports documents to a local JSON file.
     * 
     * @param filePath the path to the output file
     * @param documents the list of documents to export
     * @throws Exception if export fails
     */
    void exportToJsonFile(String filePath, List<Map<String, Object>> documents) throws Exception;
    
    /**
     * Clears all documents from the specified collection.
     * 
     * @param collectionName the name of the collection to clear
     */
    void clearCollection(String collectionName);
}
