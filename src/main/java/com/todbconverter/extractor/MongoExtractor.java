package com.todbconverter.extractor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(MongoExtractor.class);
    private final MongoDatabase database;

    public MongoExtractor(MongoDatabase database) {
        this.database = database;
    }

    public List<String> listCollections() {
        List<String> collections = new ArrayList<>();
        for (String name : database.listCollectionNames()) {
            collections.add(name);
        }
        return collections;
    }

    public List<Map<String, Object>> extractDocuments(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<Map<String, Object>> data = new ArrayList<>();
        for (Document doc : collection.find()) {
            Map<String, Object> record = new HashMap<>();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                // Convert ObjectId to String
                if (entry.getKey().equals("_id")) {
                    record.put(entry.getKey(), entry.getValue().toString());
                } else {
                    record.put(entry.getKey(), entry.getValue());
                }
            }
            data.add(record);
        }
        logger.info("Extracted {} documents from collection {}", data.size(), collectionName);
        return data;
    }
}
