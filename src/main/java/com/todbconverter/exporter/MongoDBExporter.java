package com.todbconverter.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBExporter {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBExporter.class);
    private final MongoDatabase database;
    private final ObjectMapper objectMapper;

    public MongoDBExporter(MongoDatabase database) {
        this.database = database;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public void exportToCollection(String collectionName, List<Map<String, Object>> documents) {
        if (documents.isEmpty()) {
            logger.warn("No documents to export to collection: {}", collectionName);
            return;
        }

        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<Document> bsonDocuments = new ArrayList<>();

        for (Map<String, Object> doc : documents) {
            Document bsonDoc = new Document(doc);
            bsonDocuments.add(bsonDoc);
        }

        collection.insertMany(bsonDocuments);
        logger.info("Exported {} documents to collection: {}", bsonDocuments.size(), collectionName);
    }

    public void clearCollection(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(new Document());
        logger.info("Cleared collection: {}", collectionName);
    }
}
