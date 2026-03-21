package com.todbconverter;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.MongoDBConnection;
import com.todbconverter.connection.PostgreSQLConnection;
import com.todbconverter.extractor.DataExtractor;
import com.todbconverter.extractor.MetadataExtractor;
import com.todbconverter.exporter.MongoDBExporter;
import com.todbconverter.model.TableMetadata;
import com.todbconverter.transformer.DocumentTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConverterService {
    private static final Logger logger = LoggerFactory.getLogger(ConverterService.class);

    private final PostgreSQLConnection postgresConnection;
    private final MongoDBConnection mongoConnection;
    private final String schema;

    public ConverterService(DatabaseConfig config) {
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

        this.schema = config.getPostgresSchema();
    }

    public void convert() throws Exception {
        logger.info("Starting conversion process...");

        Connection sqlConnection = postgresConnection.getConnection();
        MetadataExtractor metadataExtractor = new MetadataExtractor(sqlConnection);
        DataExtractor dataExtractor = new DataExtractor(sqlConnection);

        List<TableMetadata> tables = metadataExtractor.extractAllTables(schema);
        logger.info("Found {} tables to convert", tables.size());

        Map<String, TableMetadata> tablesMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> allData = new HashMap<>();

        for (TableMetadata table : tables) {
            tablesMap.put(table.getTableName(), table);
            List<Map<String, Object>> data = dataExtractor.extractData(table);
            allData.put(table.getTableName(), data);
        }

        DocumentTransformer transformer = new DocumentTransformer();
        MongoDBExporter exporter = new MongoDBExporter(mongoConnection.getDatabase());

        for (TableMetadata table : tables) {
            String tableName = table.getTableName();
            List<Map<String, Object>> tableData = allData.get(tableName);

            List<Map<String, Object>> documents = transformer.transformToDocuments(
                    table, tableData, allData, tablesMap
            );

            exporter.exportToCollection(tableName, documents);
        }

        logger.info("Conversion completed successfully");
    }

    public void close() {
        postgresConnection.close();
        mongoConnection.close();
    }
}
