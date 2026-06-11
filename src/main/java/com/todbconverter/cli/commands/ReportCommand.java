package com.todbconverter.cli.commands;

import com.mongodb.client.MongoCursor;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.JDBCConnection;
import com.todbconverter.connection.MongoDBConnection;
import com.todbconverter.core.extractor.JDBCDataExtractor;
import com.todbconverter.core.extractor.JDBCSchemaExtractor;
import com.todbconverter.core.model.SchemaGraph;
import com.todbconverter.exception.ConverterException;
import com.todbconverter.report.HtmlReportGenerator;
import com.todbconverter.ui.TerminalRenderer;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;

@Command(name = "report", aliases = {"--report", "-rp"},
        description = "Generate HTML report comparing source and target databases")
public class ReportCommand implements Callable<Integer> {

    @Option(names = "--config", description = "Path to configuration file (default: db-converter.properties)")
    private String configPath = "db-converter.properties";

    @Option(names = {"--output", "-o"}, description = "Output HTML file path (default: report-<timestamp>.html)")
    private String outputPath = null;

    @Option(names = {"--samples", "-s"}, description = "Number of sample rows/docs per table/collection (default: 5)")
    private int sampleLimit = 5;

    private final TerminalRenderer renderer = new TerminalRenderer();

    @Override
    public Integer call() {
        try {
            renderer.printStep("Loading configuration");
            DatabaseConfig config = DatabaseConfig.loadFromFile(configPath);

            renderer.printStep("Connecting to source database");
            JDBCConnection sourceConn = new JDBCConnection();
            sourceConn.connect(config);

            renderer.printStep("Extracting schema from source");
            JDBCSchemaExtractor schemaExtractor = new JDBCSchemaExtractor();
            SchemaGraph graph = schemaExtractor.extractSchema(sourceConn.getConnection());
            renderer.printSuccess("Found " + graph.getTables().size() + " tables");

            renderer.printStep("Extracting sample data from source");
            JDBCDataExtractor dataExtractor = new JDBCDataExtractor();
            Map<String, List<Map<String, Object>>> samples = extractSamples(dataExtractor, sourceConn, graph);

            renderer.printStep("Connecting to MongoDB");
            MongoDBConnection targetConn = new MongoDBConnection();
            targetConn.connect(config);

            renderer.printStep("Querying MongoDB collections");
            Map<String, HtmlReportGenerator.CollectionInfo> collections = queryCollections(targetConn);

            renderer.printStep("Generating HTML report");
            String effectivePath = outputPath != null ? outputPath : defaultReportName();
            Path out = Paths.get(effectivePath).toAbsolutePath();
            HtmlReportGenerator gen = new HtmlReportGenerator(graph, samples, collections, config, out);
            gen.generate();

            sourceConn.close();
            targetConn.close();

            renderer.printSuccess("Report saved to " + out);
            return 0;

        } catch (ConverterException e) {
            renderer.printError(e.getMessage());
            return 1;
        } catch (Exception e) {
            renderer.printError("Unexpected error: " + e.getMessage());
            return 1;
        }
    }

    private static String defaultReportName() {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss"));
        return "reports/report-" + ts + ".html";
    }

    private Map<String, List<Map<String, Object>>> extractSamples(
            JDBCDataExtractor extractor, JDBCConnection conn, SchemaGraph graph) {

        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (var table : graph.getTables()) {
            String name = table.getName();
            try {
                List<Map<String, Object>> allRows = extractor.extractTableData(
                        conn.getConnection(), name);
                List<Map<String, Object>> limited = allRows.size() > sampleLimit
                        ? allRows.subList(0, sampleLimit)
                        : allRows;
                result.put(name, limited);
            } catch (Exception e) {
                result.put(name, List.of());
            }
        }
        return result;
    }

    private Map<String, HtmlReportGenerator.CollectionInfo> queryCollections(
            MongoDBConnection conn) {

        Map<String, HtmlReportGenerator.CollectionInfo> result = new LinkedHashMap<>();
        var db = conn.getDatabase();

        for (String collName : db.listCollectionNames()) {
            long count = db.getCollection(collName).countDocuments();
            List<String> sampleJsons = new ArrayList<>();

            try (MongoCursor<Document> cursor = db.getCollection(collName)
                    .find().limit(sampleLimit).iterator()) {
                while (cursor.hasNext()) {
                    Document doc = cursor.next();
                    sampleJsons.add(doc.toJson(JsonWriterSettings.builder().indent(true).build()));
                }
            }

            result.put(collName, new HtmlReportGenerator.CollectionInfo(
                    collName, count, sampleJsons));
        }
        return result;
    }
}
