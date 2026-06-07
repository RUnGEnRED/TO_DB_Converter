package com.todbconverter.core.service;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.JDBCConnection;
import com.todbconverter.connection.MongoDBConnection;
import com.todbconverter.core.extractor.JDBCDataExtractor;
import com.todbconverter.core.extractor.JDBCSchemaExtractor;
import com.todbconverter.core.loader.MongoDBLoader;
import com.todbconverter.core.model.SchemaGraph;
import com.todbconverter.core.transformer.UniversalTransformer;
import com.todbconverter.exception.ConverterException;
import com.todbconverter.ui.TerminalRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Main ETL pipeline orchestrator.
 * Coordinates the Extract, Transform, and Load phases.
 */
public class ETLPipeline {

    private static final Logger logger = LoggerFactory.getLogger(ETLPipeline.class);

    private final TerminalRenderer renderer;

    public ETLPipeline(TerminalRenderer renderer) {
        this.renderer = renderer;
    }

    /**
     * Execute the full ETL pipeline.
     *
     * @param config database configuration
     * @throws ConverterException if any phase fails
     */
    public void execute(DatabaseConfig config) throws ConverterException {
        long startTime = System.currentTimeMillis();

        // Phase 1: Connect to databases
        renderer.printStep("Connecting to source database...");
        JDBCConnection source = new JDBCConnection();
        source.connect(config);
        renderer.printSuccess("Connected to source database");

        renderer.printStep("Connecting to MongoDB...");
        MongoDBConnection target = new MongoDBConnection();
        target.connect(config);
        renderer.printSuccess("Connected to MongoDB");

        try {
            // Phase 2: Extract schema
            renderer.printStep("Analyzing database schema...");
            JDBCSchemaExtractor schemaExtractor = new JDBCSchemaExtractor();
            SchemaGraph graph = schemaExtractor.extractSchema(source.getConnection());
            renderer.printSuccess("Schema analysis complete: " + graph.getTables().size() + " tables found");

            // Phase 3: Extract data
            renderer.printStep("Extracting data from source database...");
            JDBCDataExtractor dataExtractor = new JDBCDataExtractor();
            Map<String, java.util.List<Map<String, Object>>> rawData =
                    dataExtractor.extractAllData(source.getConnection(), graph);
            long totalRows = rawData.values().stream().mapToLong(java.util.List::size).sum();
            renderer.printSuccess("Data extraction complete: " + totalRows + " rows from " + rawData.size() + " tables");

            // Phase 4: Transform data
            renderer.printStep("Transforming data to document model...");
            UniversalTransformer transformer = new UniversalTransformer(config);
            Map<String, java.util.List<Map<String, Object>>> transformedData =
                    transformer.transform(graph, rawData, config);
            renderer.printSuccess("Transformation complete");

            // Phase 5: Load into MongoDB
            renderer.printStep("Loading data into MongoDB...");
            MongoDBLoader loader = new MongoDBLoader(target.getDatabase());
            loader.loadAll(transformedData, graph,
                    com.todbconverter.config.EdgeStrategyRegistry.fromConfig(config));
            renderer.printSuccess("Data loaded into MongoDB");

            // Summary
            long duration = System.currentTimeMillis() - startTime;
            renderer.printSummary(graph, transformedData, duration);

        } finally {
            // Close connections
            source.close();
            target.close();
        }
    }

    /**
     * Test connections to both databases.
     *
     * @param config database configuration
     * @throws ConverterException if connection test fails
     */
    public void validateConnections(DatabaseConfig config) throws ConverterException {
        renderer.printStep("Testing source database connection...");
        JDBCConnection source = new JDBCConnection();
        source.connect(config);
        if (!source.testConnection()) {
            throw new com.todbconverter.exception.SourceConnectionException(
                    "Source database connection test failed");
        }
        renderer.printSuccess("Source database connection: OK");
        source.close();

        renderer.printStep("Testing MongoDB connection...");
        MongoDBConnection target = new MongoDBConnection();
        target.connect(config);
        if (!target.testConnection()) {
            throw new com.todbconverter.exception.TargetConnectionException(
                    "MongoDB connection test failed");
        }
        renderer.printSuccess("MongoDB connection: OK");
        target.close();
    }
}
