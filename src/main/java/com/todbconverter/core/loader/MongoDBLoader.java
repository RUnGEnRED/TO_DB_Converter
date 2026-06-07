package com.todbconverter.core.loader;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.EdgeStrategyRegistry;
import com.todbconverter.core.model.ForeignKeyMetadata;
import com.todbconverter.core.model.SchemaGraph;
import com.todbconverter.core.model.Strategy;
import com.todbconverter.core.model.TableMetadata;
import com.todbconverter.exception.TransformationException;
import com.todbconverter.util.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Loads transformed data into MongoDB.
 */
public class MongoDBLoader {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBLoader.class);

    private final MongoDatabase database;

    public MongoDBLoader(MongoDatabase database) {
        this.database = database;
    }

    /**
     * Load all transformed data into MongoDB.
     *
     * @param transformedData transformed document data
     * @param graph           schema graph
     * @param strategyRegistry strategy registry
     * @throws TransformationException if loading fails
     */
    public void loadAll(
            Map<String, List<Map<String, Object>>> transformedData,
            SchemaGraph graph,
            EdgeStrategyRegistry strategyRegistry) throws TransformationException {

        int totalDocuments = 0;

        for (Map.Entry<String, List<Map<String, Object>>> entry : transformedData.entrySet()) {
            String tableName = entry.getKey();
            List<Map<String, Object>> documents = entry.getValue();

            // Check if this table should be created as a standalone collection
            // Skip if all edges to this table are EMBED (data is already embedded in parents)
            if (shouldSkipCollection(tableName, graph, strategyRegistry)) {
                logger.info("Skipping collection '{}' - data embedded in parent collections", tableName);
                continue;
            }

            // Convert table name to collection name
            String collectionName = StringUtils.toCollectionName(tableName);

            // Load documents
            loadCollection(collectionName, documents);
            totalDocuments += documents.size();

            logger.info("Loaded {} documents into collection '{}'", documents.size(), collectionName);
        }

        logger.info("Total documents loaded: {}", totalDocuments);
    }

    /**
     * Check if a collection should be skipped (all data embedded elsewhere).
     * A table should be skipped if all its OUTGOING edges use EMBED strategy.
     * This means the table's data is embedded into parent collections.
     */
    private boolean shouldSkipCollection(
            String tableName,
            SchemaGraph graph,
            EdgeStrategyRegistry strategyRegistry) {

        // Get edges FROM this table to other tables
        List<ForeignKeyMetadata> outgoingEdges = graph.getEdges(tableName);

        // If no outgoing edges, this is a root table - always create collection
        if (outgoingEdges.isEmpty()) {
            return false;
        }

        // Check if all outgoing edges use EMBED strategy
        for (ForeignKeyMetadata fk : outgoingEdges) {
            Strategy strategy = strategyRegistry.getStrategy(tableName, fk.getPkTableName());
            if (strategy == Strategy.REFERENCE) {
                return false; // At least one edge uses REFERENCE, so keep the collection
            }
        }

        // All edges are EMBED - this table's data is embedded elsewhere
        return true;
    }

    /**
     * Load documents into a MongoDB collection.
     */
    private void loadCollection(String collectionName, List<Map<String, Object>> documents) {
        MongoCollection<Document> collection = database.getCollection(collectionName);

        // Drop existing collection if it exists
        collection.drop();

        if (documents.isEmpty()) {
            return;
        }

        // Convert maps to BSON Documents
        List<Document> bsonDocuments = documents.stream()
                .map(this::toBsonDocument)
                .toList();

        // Bulk insert
        collection.insertMany(bsonDocuments);
    }

    /**
     * Convert a Map<String, Object> to a MongoDB Document.
     */
    @SuppressWarnings("unchecked")
    private Document toBsonDocument(Map<String, Object> map) {
        Document doc = new Document();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value == null) {
                doc.append(key, null);
            } else if (value instanceof Map) {
                doc.append(key, toBsonDocument((Map<String, Object>) value));
            } else if (value instanceof List) {
                doc.append(key, toBsonArray((List<Object>) value));
            } else if (value instanceof BigDecimal bigDecimal) {
                // Convert BigDecimal to double for MongoDB
                doc.append(key, bigDecimal.doubleValue());
            } else if (value instanceof java.util.Date || value instanceof java.time.temporal.TemporalAccessor) {
                // Dates are already converted by DateUtils
                doc.append(key, value);
            } else {
                // Standard types (String, Integer, Long, Double, Boolean, etc.)
                doc.append(key, value);
            }
        }

        return doc;
    }

    /**
     * Convert a List to a BSON-compatible list.
     */
    @SuppressWarnings("unchecked")
    private List<Object> toBsonArray(List<Object> list) {
        List<Object> bsonList = new ArrayList<>();

        for (Object item : list) {
            if (item instanceof Map) {
                bsonList.add(toBsonDocument((Map<String, Object>) item));
            } else if (item instanceof List) {
                bsonList.add(toBsonArray((List<Object>) item));
            } else if (item instanceof BigDecimal bigDecimal) {
                bsonList.add(bigDecimal.doubleValue());
            } else {
                bsonList.add(item);
            }
        }

        return bsonList;
    }
}
