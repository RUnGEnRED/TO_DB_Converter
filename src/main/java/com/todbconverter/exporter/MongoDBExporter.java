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
    private static final String VALID_COLLECTION_NAME_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";
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
        if (!isValidCollectionName(collectionName)) {
            throw new IllegalArgumentException("Invalid collection name: " + collectionName);
        }
        if (documents.isEmpty()) {
            logger.warn("No documents to export to collection: {}", collectionName);
            return;
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<Document> bsonDocuments = new ArrayList<>();

        for (Map<String, Object> doc : documents) {
            try {
                Document bsonDoc = new Document(doc);
                bsonDocuments.add(bsonDoc);
            } catch (Exception e) {
                logger.error("Failed to convert document to BSON: {}", e.getMessage());
                throw new IllegalArgumentException("Document contains non-BSON-compatible types", e);
            }
        }

        collection.insertMany(bsonDocuments);
        logger.info("Exported {} documents to collection: {}", bsonDocuments.size(), collectionName);
    }

    private boolean isValidCollectionName(String name) {
        return name != null && !name.isEmpty() && name.matches(VALID_COLLECTION_NAME_PATTERN);
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
