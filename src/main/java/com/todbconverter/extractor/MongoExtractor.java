package com.todbconverter.extractor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.types.ObjectId;
import org.bson.BsonDateTime;
import org.bson.BsonBinary;
import org.bson.types.Decimal128;
import java.util.Date;
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
                record.put(entry.getKey(), convertBsonValue(entry.getValue()));
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
    
    private Object convertBsonValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ObjectId) {
            return value.toString();
        }
        if (value instanceof Document) {
            Document nestedDoc = (Document) value;
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, Object> entry : nestedDoc.entrySet()) {
                result.put(entry.getKey(), convertBsonValue(entry.getValue()));
            }
            return result;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            List<Object> result = new ArrayList<>();
            for (Object item : list) {
                result.add(convertBsonValue(item));
            }
            return result;
        }
        if (value instanceof BsonDateTime) {
            return new Date(((BsonDateTime) value).getValue());
        }
        if (value instanceof BsonBinary) {
            return ((BsonBinary) value).getData();
        }
        if (value instanceof Decimal128) {
            return ((Decimal128) value).bigDecimalValue();
        }
        if (value instanceof Date) {
            return value;
        }
        if (value instanceof Number || value instanceof String || value instanceof Boolean) {
            return value;
        }
        logger.warn("Unknown BSON type: {}, passing through as-is", value.getClass().getName());
        return value;
    }
}
