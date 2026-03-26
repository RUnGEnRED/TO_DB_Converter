package com.todbconverter;

import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.IDatabaseConnector;
import com.todbconverter.connection.MongoDBConnection;
import com.todbconverter.connection.PostgreSQLConnection;
import com.todbconverter.extractor.DataExtractor;
import com.todbconverter.extractor.IMetadataExtractor;
import com.todbconverter.extractor.MetadataExtractor;
import com.todbconverter.extractor.MongoExtractor;
import com.todbconverter.exporter.IDocumentLoader;
import com.todbconverter.exporter.MongoDBExporter;
import com.todbconverter.exporter.PostgresLoader;
import com.todbconverter.model.TableMetadata;
import com.todbconverter.transformer.UniversalTransformer;
import com.todbconverter.transformer.IDataTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConverterService {
    private static final Logger logger = LoggerFactory.getLogger(ConverterService.class);

    private final IDatabaseConnector postgresConnection;
    private final IDatabaseConnector mongoConnection;
    private final DatabaseConfig config;

    public ConverterService(DatabaseConfig config) {
        this.config = config;
        
        this.postgresConnection = new PostgreSQLConnection(
                config.getPostgresHost(),
                config.getPostgresPort(),
                config.getPostgresDatabase(),
                config.getPostgresUsername(),
                config.getPostgresPassword()
        );

        String mongoConnStr = config.getMongoConnectionString();
        if (mongoConnStr != null && !mongoConnStr.isEmpty()) {
            this.mongoConnection = new MongoDBConnection(mongoConnStr, config.getMongoDatabase());
        } else {
            this.mongoConnection = new MongoDBConnection(
                    config.getMongoHost(),
                    config.getMongoPort(),
                    config.getMongoDatabase(),
                    config.getMongoUsername(),
                    config.getMongoPassword()
            );
        }
    }

    public void convert() throws Exception {
        logger.info("Starting conversion process...");
        postgresConnection.connect();
        mongoConnection.connect();

        if (config.getConversionDirection() == DatabaseConfig.ConversionDirection.POSTGRES_TO_MONGO) {
            convertPostgresToMongo();
        } else {
            convertMongoToPostgres();
        }

        logger.info("Conversion completed successfully");
    }

    private void convertPostgresToMongo() throws Exception {
        logger.info("Direction: POSTGRES -> MONGO");
        Connection sqlConnection = ((PostgreSQLConnection) postgresConnection).getConnection();
        IMetadataExtractor metadataExtractor = new MetadataExtractor(sqlConnection);
        DataExtractor dataExtractor = new DataExtractor(sqlConnection);

        List<TableMetadata> tables = metadataExtractor.extractAllTables(config.getPostgresSchema());
        logger.info("Found {} tables to convert", tables.size());

        Map<String, TableMetadata> tablesMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> allData = new HashMap<>();

        for (TableMetadata table : tables) {
            tablesMap.put(table.getTableName(), table);
            List<Map<String, Object>> data = dataExtractor.extractData(table);
            allData.put(table.getTableName(), data);
        }

        IDataTransformer transformer = new UniversalTransformer();
        IDocumentLoader exporter = new MongoDBExporter(((MongoDBConnection) mongoConnection).getDatabase());

        for (TableMetadata table : tables) {
            String tableName = table.getTableName();
            List<Map<String, Object>> tableData = allData.get(tableName);

            List<Map<String, Object>> documents = transformer.transformToDocuments(
                    table, tableData, allData, tablesMap
            );

            exporter.loadDocuments(tableName, documents);
        }
    }

    private void convertMongoToPostgres() throws Exception {
        logger.info("Direction: MONGO -> POSTGRES");
        MongoDatabase mongoDb = ((MongoDBConnection) mongoConnection).getDatabase();
        MongoExtractor mongoExtractor = new MongoExtractor(mongoDb);

        Connection sqlConnection = ((PostgreSQLConnection) postgresConnection).getConnection();
        PostgresLoader sqlLoader = new PostgresLoader(sqlConnection);
        IDataTransformer transformer = new UniversalTransformer();

        List<String> collections = mongoExtractor.listCollections();
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        Map<String, List<Map<String, Object>>> allRelationalData = new HashMap<>();

        // 1. Extract and Flatten
        for (String collectionName : collections) {
            List<Map<String, Object>> docs = mongoExtractor.extractDocuments(collectionName);
            
            Map<String, List<Map<String, Object>>> relationalData = 
                transformer.flattenToRelational(collectionName, docs, tablesMetadata);
            
            for (Map.Entry<String, List<Map<String, Object>>> entry : relationalData.entrySet()) {
                allRelationalData.putIfAbsent(entry.getKey(), new java.util.ArrayList<>());
                allRelationalData.get(entry.getKey()).addAll(entry.getValue());
            }
        }

        // 2. Create Tables
        for (TableMetadata meta : tablesMetadata.values()) {
            sqlLoader.createTable(meta);
        }

        // 3. Load Data
        for (Map.Entry<String, List<Map<String, Object>>> entry : allRelationalData.entrySet()) {
            sqlLoader.loadData(tablesMetadata.get(entry.getKey()), entry.getValue());
        }

        // 4. Add Foreign Keys (after data to avoid constraint issues during insert)
        for (TableMetadata meta : tablesMetadata.values()) {
            sqlLoader.addForeignKeys(meta);
        }
    }

    public void close() {
        postgresConnection.disconnect();
        mongoConnection.disconnect();
    }
}
