package com.todbconverter;

import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.IMongoDBConnector;
import com.todbconverter.connection.IPostgreSQLConnector;
import com.todbconverter.extractor.DataExtractor;
import com.todbconverter.extractor.IMetadataExtractor;
import com.todbconverter.extractor.MetadataExtractor;
import com.todbconverter.extractor.MongoExtractor;
import com.todbconverter.exporter.MongoDBExporter;
import com.todbconverter.exporter.PostgresLoader;
import com.todbconverter.model.TableMetadata;
import com.todbconverter.transformer.DocumentToRelationalTransformer;
import com.todbconverter.transformer.MongoDbPatternOptimizer;
import com.todbconverter.transformer.RelationalToDocumentTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        this(config, cliDirection,
                new com.todbconverter.connection.PostgreSQLConnection(
                        config.getPostgresHost(), config.getPostgresPort(),
                        config.getPostgresDatabase(), config.getPostgresUsername(),
                        config.getPostgresPassword()),
                createMongoConnection(config),
                new MongoDbPatternOptimizer(config));
    }

    public ConverterService(DatabaseConfig config, String cliDirection,
                            IPostgreSQLConnector postgresConnection,
                            IMongoDBConnector mongoConnection,
                            MongoDbPatternOptimizer patternOptimizer) {
        this.config = config;
        this.direction = config.overrideDirection(cliDirection);
        this.postgresConnection = postgresConnection;
        this.mongoConnection = mongoConnection;
        this.patternOptimizer = patternOptimizer;
    }

    private static IMongoDBConnector createMongoConnection(DatabaseConfig config) {
        String mongoConnStr = config.getMongoConnectionString();
        if (mongoConnStr != null && !mongoConnStr.isEmpty()) {
            return new com.todbconverter.connection.MongoDBConnection(mongoConnStr, config.getMongoDatabase());
        }
        return new com.todbconverter.connection.MongoDBConnection(
                config.getMongoHost(), config.getMongoPort(),
                config.getMongoDatabase(), config.getMongoUsername(),
                config.getMongoPassword());
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
        for (TableMetadata table : tables) {
            tablesMap.put(table.getTableName(), table);
        }

        RelationalToDocumentTransformer transformer = new RelationalToDocumentTransformer(config);
        MongoDBExporter exporter = new MongoDBExporter(mongoConnection.getDatabase());

        for (TableMetadata table : tables) {
            String tableName = table.getTableName();

            Set<String> relatedNames = findRelatedTableNames(table, tablesMap);
            Map<String, List<Map<String, Object>>> relatedData = new HashMap<>();
            for (String relName : relatedNames) {
                TableMetadata relTable = tablesMap.get(relName);
                if (relTable != null) {
                    logger.debug("Loading related data for '{}': {}", tableName, relName);
                    relatedData.put(relName, dataExtractor.extractData(relTable));
                }
            }

            exporter.clearCollection(tableName);

            List<String> indexFields = new ArrayList<>();
            String pkCol = table.getPrimaryKeyColumn();
            if (pkCol != null) indexFields.add(pkCol);
            List<com.todbconverter.model.ForeignKeyMetadata> fks = table.getForeignKeys();
            if (fks != null) {
                for (com.todbconverter.model.ForeignKeyMetadata fk : fks) {
                    indexFields.add(fk.getColumnName());
                }
            }
            exporter.createIndexes(tableName, indexFields);

            dataExtractor.extractDataInBatches(table, 1000, batch -> {
                List<Map<String, Object>> documents = transformer.transformToDocuments(
                        table, batch, relatedData, tablesMap
                );

                Map<String, List<Map<String, Object>>> optimizedCollections = patternOptimizer.applyPatterns(documents, tableName);

                for (Map.Entry<String, List<Map<String, Object>>> entry : optimizedCollections.entrySet()) {
                    exporter.loadDocuments(entry.getKey(), entry.getValue());
                }
            });

            relatedData.clear();
        }
    }

    private Set<String> findRelatedTableNames(TableMetadata parent, Map<String, TableMetadata> allTables) {
        Set<String> related = new HashSet<>();
        String parentName = parent.getTableName();

        for (TableMetadata t : allTables.values()) {
            if (t.getTableName().equals(parentName)) continue;

            List<com.todbconverter.model.ForeignKeyMetadata> fks = t.getForeignKeys();
            if (fks == null || fks.isEmpty()) continue;

            boolean referencesParent = false;
            for (com.todbconverter.model.ForeignKeyMetadata fk : fks) {
                if (fk.getReferencedTable().equals(parentName)) {
                    related.add(t.getTableName());
                    referencesParent = true;
                    break;
                }
            }

            if (referencesParent) {
                for (com.todbconverter.model.ForeignKeyMetadata fk : fks) {
                    if (!fk.getReferencedTable().equals(parentName)) {
                        related.add(fk.getReferencedTable());
                    }
                }
            }
        }

        List<com.todbconverter.model.ForeignKeyMetadata> parentFks = parent.getForeignKeys();
        if (parentFks != null) {
            for (com.todbconverter.model.ForeignKeyMetadata fk : parentFks) {
                related.add(fk.getReferencedTable());
            }
        }

        related.remove(parentName);
        return related;
    }

    private void convertMongoToPostgres() throws Exception {
        logger.info("Direction: MONGO -> POSTGRES");

        MongoDatabase mongoDb = mongoConnection.getDatabase();
        MongoExtractor mongoExtractor = new MongoExtractor(mongoDb);

        Connection sqlConnection = postgresConnection.getConnection();
        boolean dropTables = config.shouldDropExistingTables();
        PostgresLoader sqlLoader = new PostgresLoader(sqlConnection, dropTables);
        DocumentToRelationalTransformer transformer = new DocumentToRelationalTransformer(config);

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
        transformer.clearProcessedIds();
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
