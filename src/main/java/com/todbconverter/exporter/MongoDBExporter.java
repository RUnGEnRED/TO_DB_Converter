package com.todbconverter.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBExporter implements IDocumentLoader {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBExporter.class);
    private static final String VALID_COLLECTION_NAME_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_.-]*$";
    private static final long MAX_BSON_DOCUMENT_SIZE = 16 * 1024 * 1024; // 16MB
    private static final long WARNING_SIZE_THRESHOLD = 14 * 1024 * 1024; // 14MB warning threshold
    private final MongoDatabase database;
    private final ObjectMapper objectMapper;

    public MongoDBExporter(MongoDatabase database) {
        this.database = database;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void loadDocuments(String collectionName, List<Map<String, Object>> documents) {
        loadDocuments(collectionName, documents, 1000);
    }

    public void loadDocuments(String collectionName, List<Map<String, Object>> documents, int batchSize) {
        if (!isValidCollectionName(collectionName)) {
            throw new IllegalArgumentException("Invalid collection name: " + collectionName);
        }
        if (documents.isEmpty()) {
            logger.warn("No documents to export to collection: {}", collectionName);
            return;
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        for (int i = 0; i < documents.size(); i += batchSize) {
            int end = Math.min(i + batchSize, documents.size());
            List<Map<String, Object>> batch = documents.subList(i, end);
            List<Document> bsonDocuments = new ArrayList<>();

            for (Map<String, Object> doc : batch) {
                try {
                    Document bsonDoc = new Document(doc);
                    long estimatedSize = estimateBsonSize(bsonDoc);
                    if (estimatedSize > MAX_BSON_DOCUMENT_SIZE) {
                        logger.error("Document exceeds MongoDB 16MB limit ({} bytes). Skipping.", estimatedSize);
                        continue;
                    }
                    if (estimatedSize > WARNING_SIZE_THRESHOLD) {
                        logger.warn("Document is approaching 16MB limit ({} bytes). Consider using Bucket/Outlier patterns.",
                                estimatedSize);
                    }
                    bsonDocuments.add(bsonDoc);
                } catch (Exception e) {
                    logger.error("Failed to convert document to BSON: {}", e.getMessage());
                }
            }

            if (!bsonDocuments.isEmpty()) {
                collection.insertMany(bsonDocuments);
            }
        }
        
        logger.info("Exported {} documents to collection: {}", documents.size(), collectionName);
    }

    public void createIndexes(String collectionName, List<String> indexFields) {
        if (indexFields == null || indexFields.isEmpty()) return;
        
        MongoCollection<Document> collection = database.getCollection(collectionName);
        for (String field : indexFields) {
            logger.info("Creating index on {}.{}", collectionName, field);
            collection.createIndex(new Document(field, 1));
        }
    }
    
    public void createCompoundIndex(String collectionName, List<String> indexFields) {
        if (indexFields == null || indexFields.size() < 2) return;
        
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document indexDoc = new Document();
        for (String field : indexFields) {
            indexDoc.put(field, 1);
        }
        logger.info("Creating compound index on {}.{}", collectionName, indexDoc.toJson());
        collection.createIndex(indexDoc);
    }
    
    public void createPatternSpecificIndexes(String collectionName, boolean hasAttributePattern,
            boolean hasSubsetPattern, boolean hasOutlierPattern, String tableName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Index for Attribute Pattern array
        if (hasAttributePattern) {
            logger.info("Creating index on {}._attrs for Attribute Pattern", collectionName);
            collection.createIndex(new Document(collectionName + "_attrs", 1));
        }
        
        // Index for Subset/Outlier Pattern extras collection
        if (hasSubsetPattern || hasOutlierPattern) {
            String extrasCollName = tableName + "_extras";
            try {
                MongoCollection<Document> extrasColl = database.getCollection(extrasCollName);
                logger.info("Creating index on {}.{}_id for Subset/Outlier Pattern", extrasCollName, tableName);
                extrasColl.createIndex(new Document(tableName + "_id", 1));
            } catch (Exception e) {
                logger.warn("Could not create index on extras collection: {}", e.getMessage());
            }
        }
    }

    private boolean isValidCollectionName(String name) {
        return name != null && !name.isEmpty() && name.matches(VALID_COLLECTION_NAME_PATTERN);
    }
    
    private long estimateBsonSize(Document doc) {
        try {
            String json = doc.toJson();
            return json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        } catch (Exception e) {
            logger.warn("Failed to estimate BSON size, using fallback: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public void exportToJsonFile(String filePath, List<Map<String, Object>> documents) throws Exception {
        if (documents.isEmpty()) {
            logger.warn("No documents to export to file: {}", filePath);
            return;
        }
        
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.writeValue(new File(filePath), documents);
        objectMapper.disable(SerializationFeature.INDENT_OUTPUT);
        logger.info("Exported {} documents to file: {}", documents.size(), filePath);
    }

    @Override
    public void clearCollection(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(new Document());
        logger.info("Cleared collection: {}", collectionName);
    }
}
