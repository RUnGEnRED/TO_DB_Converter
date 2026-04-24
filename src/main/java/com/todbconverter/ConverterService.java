package com.todbconverter;

import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.IDatabaseConnector;
import com.todbconverter.connection.IMongoDBConnector;
import com.todbconverter.connection.IPostgreSQLConnector;
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
import com.todbconverter.transformer.MongoDbPatternOptimizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConverterService {
    private static final Logger logger = LoggerFactory.getLogger(ConverterService.class);

    private final IPostgreSQLConnector postgresConnection;
    private final IMongoDBConnector mongoConnection;
    private final DatabaseConfig config;
    private final DatabaseConfig.ConversionDirection direction;
    private final MongoDbPatternOptimizer patternOptimizer;

    public ConverterService(DatabaseConfig config) {
        this(config, null);
    }

    public ConverterService(DatabaseConfig config, String cliDirection) {
        this.config = config;
        this.direction = config.overrideDirection(cliDirection);
        
        this.postgresConnection = new com.todbconverter.connection.PostgreSQLConnection(
                config.getPostgresHost(),
                config.getPostgresPort(),
                config.getPostgresDatabase(),
                config.getPostgresUsername(),
                config.getPostgresPassword()
        );

        String mongoConnStr = config.getMongoConnectionString();
        if (mongoConnStr != null && !mongoConnStr.isEmpty()) {
            this.mongoConnection = new com.todbconverter.connection.MongoDBConnection(mongoConnStr, config.getMongoDatabase());
        } else {
            this.mongoConnection = new com.todbconverter.connection.MongoDBConnection(
                    config.getMongoHost(),
                    config.getMongoPort(),
                    config.getMongoDatabase(),
                    config.getMongoUsername(),
                    config.getMongoPassword()
            );
        }
        
        this.patternOptimizer = new MongoDbPatternOptimizer(config);
    }

    public void convert() throws Exception {
        logger.info("Starting conversion process...");
        postgresConnection.connect();
        mongoConnection.connect();

        if (direction == DatabaseConfig.ConversionDirection.POSTGRES_TO_MONGO) {
            convertPostgresToMongo();
        } else {
            convertMongoToPostgres();
        }

        logger.info("Conversion completed successfully");
    }

    private void convertPostgresToMongo() throws Exception {
        logger.info("Direction: POSTGRES -> MONGO");
        
        Connection sqlConnection = postgresConnection.getConnection();
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

        IDataTransformer transformer = new UniversalTransformer(config);
        MongoDBExporter exporter = new MongoDBExporter(mongoConnection.getDatabase());

        for (TableMetadata table : tables) {
            String tableName = table.getTableName();
            
            // Create indexes on PK and FKs
            List<String> indexFields = new ArrayList<>();
            indexFields.add(table.getPrimaryKeyColumn());
            for (com.todbconverter.model.ForeignKeyMetadata fk : table.getForeignKeys()) {
                indexFields.add(fk.getColumnName());
            }
            exporter.createIndexes(tableName, indexFields);

            dataExtractor.extractDataInBatches(table, 1000, batch -> {
                List<Map<String, Object>> documents = transformer.transformToDocuments(
                        table, batch, allData, tablesMap
                );
                
                Map<String, List<Map<String, Object>>> optimizedCollections = patternOptimizer.applyPatterns(documents, tableName);

                for (Map.Entry<String, List<Map<String, Object>>> entry : optimizedCollections.entrySet()) {
                    exporter.loadDocuments(entry.getKey(), entry.getValue());
                }
            });
        }
    }

    private void convertMongoToPostgres() throws Exception {
        logger.info("Direction: MONGO -> POSTGRES");
        
        MongoDatabase mongoDb = mongoConnection.getDatabase();
        MongoExtractor mongoExtractor = new MongoExtractor(mongoDb);

        Connection sqlConnection = postgresConnection.getConnection();
        boolean dropTables = config.shouldDropExistingTables();
        PostgresLoader sqlLoader = new PostgresLoader(sqlConnection, dropTables);
        UniversalTransformer transformer = new UniversalTransformer(config);

        List<String> collections = mongoExtractor.listCollections();
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();

        // Phase 1: Metadata Discovery (use small sample from each collection)
        logger.info("Phase 1: Discovering schema from MongoDB collections...");
        for (String collectionName : collections) {
            mongoExtractor.extractDocumentsInBatches(collectionName, 10, batch -> {
                transformer.flattenToRelational(collectionName, batch, tablesMetadata);
            });
        }

        // Phase 2: Create Tables
        logger.info("Phase 2: Creating PostgreSQL tables...");
        for (TableMetadata table : tablesMetadata.values()) {
            sqlLoader.createTable(table);
        }

        // Phase 3: Data Loading in Batches
        logger.info("Phase 3: Loading data in batches...");
        for (String collectionName : collections) {
            mongoExtractor.extractDocumentsInBatches(collectionName, 1000, batch -> {
                try {
                    Map<String, List<Map<String, Object>>> relationalData = 
                        transformer.flattenToRelational(collectionName, batch, tablesMetadata);
                    
                    for (Map.Entry<String, List<Map<String, Object>>> entry : relationalData.entrySet()) {
                        sqlLoader.loadData(tablesMetadata.get(entry.getKey()), entry.getValue());
                    }
                } catch (java.sql.SQLException e) {
                    logger.error("Error loading batch from collection {}: {}", collectionName, e.getMessage());
                }
            });
        }

        // Phase 4: Adding Foreign Keys
        logger.info("Phase 4: Adding foreign keys...");
        for (TableMetadata table : tablesMetadata.values()) {
            try {
                sqlLoader.addForeignKeys(table);
            } catch (java.sql.SQLException e) {
                logger.warn("Failed to add some foreign keys for {}: {}. This is common in schema-less to relational conversion.", 
                        table.getTableName(), e.getMessage());
            }
        }
    }

    public void close() {
        if (postgresConnection != null) {
            postgresConnection.disconnect();
        }
        if (mongoConnection != null) {
            mongoConnection.disconnect();
        }
    }
}
