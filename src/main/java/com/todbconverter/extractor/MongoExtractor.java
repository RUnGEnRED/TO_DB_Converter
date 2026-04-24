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
            if (!name.startsWith("system.")) {
                collections.add(name);
            }
        }
        return collections;
    }

    public List<Map<String, Object>> extractDocuments(String collectionName) {
        List<Map<String, Object>> allDocs = new ArrayList<>();
        extractDocumentsInBatches(collectionName, 1000, allDocs::addAll);
        return allDocs;
    }

    public void extractDocumentsInBatches(String collectionName, int batchSize, java.util.function.Consumer<List<Map<String, Object>>> batchConsumer) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        List<Map<String, Object>> batch = new ArrayList<>();
        
        for (Document doc : collection.find().batchSize(batchSize)) {
            Map<String, Object> record = new HashMap<>();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                Object value = entry.getValue();
                if (entry.getKey().equals("_id") && value != null) {
                    record.put(entry.getKey(), value.toString());
                } else {
                    record.put(entry.getKey(), value);
                }
            }
            batch.add(record);
            
            if (batch.size() >= batchSize) {
                batchConsumer.accept(batch);
                batch.clear();
            }
        }
        
        if (!batch.isEmpty()) {
            batchConsumer.accept(batch);
        }
    }
}
