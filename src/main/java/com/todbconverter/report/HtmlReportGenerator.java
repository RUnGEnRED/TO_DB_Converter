package com.todbconverter.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.core.model.*;
import org.bson.Document;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class HtmlReportGenerator {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final SchemaGraph sourceGraph;
    private final Map<String, List<Map<String, Object>>> sourceSamples;
    private final Map<String, CollectionInfo> targetCollections;
    private final DatabaseConfig config;
    private final Path outputPath;

    public record CollectionInfo(String name, long docCount, List<String> sampleJsons) {}

    public HtmlReportGenerator(
            SchemaGraph sourceGraph,
            Map<String, List<Map<String, Object>>> sourceSamples,
            Map<String, CollectionInfo> targetCollections,
            DatabaseConfig config,
            Path outputPath) {
        this.sourceGraph = sourceGraph;
        this.sourceSamples = sourceSamples;
        this.targetCollections = targetCollections;
        this.config = config;
        this.outputPath = outputPath;
    }

    public void generate() throws IOException {
        String html = buildHtml();
        java.nio.file.Files.createDirectories(outputPath.getParent());
        try (FileWriter w = new FileWriter(outputPath.toFile())) {
            w.write(html);
        }
    }

    private String buildHtml() {
        return """
            <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>TO DB Converter — Migration Report</title>
            <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                   background: #f5f7fa; color: #1a1a2e; line-height: 1.6; padding: 20px; }
            .container { max-width: 1200px; margin: 0 auto; }
            h1 { font-size: 1.8rem; padding: 20px 0; }
            h2 { font-size: 1.3rem; margin: 28px 0 12px; padding-bottom: 6px;
                 border-bottom: 2px solid #3a7bd5; color: #1a1a2e; }
            h3 { font-size: 1.05rem; margin: 20px 0 8px; color: #2d3748; }
            .header { background: linear-gradient(135deg, #2c3e6b, #3a7bd5);
                      color: white; padding: 28px 32px; border-radius: 10px; margin-bottom: 24px; }
            .header h1 { padding: 0 0 4px; font-size: 1.6rem; }
            .header .sub { opacity: 0.85; font-size: 0.9rem; }
            .card { background: white; border-radius: 8px; padding: 20px 24px;
                    margin-bottom: 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
            table { width: 100%; border-collapse: collapse; font-size: 0.88rem; margin: 8px 0; }
            th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #e2e8f0; }
            th { background: #edf2f7; font-weight: 600; color: #2d3748;
                 position: sticky; top: 0; }
            tr:nth-child(even) td { background: #f7fafc; }
            .badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
                     font-size: 0.78rem; font-weight: 600; }
            .badge-embed { background: #c6f6d5; color: #22543d; }
            .badge-reference { background: #fefcbf; color: #744210; }
            .badge-pk { background: #bee3f8; color: #2a4365; }
            .badge-fk { background: #fed7d7; color: #9b2c2c; }
            .badge-nullable { background: #e2e8f0; color: #4a5568; }
            .badge-pattern { background: #e9d8fd; color: #553c7b; }
            pre { background: #1a202c; color: #e2e8f0; padding: 14px 16px; border-radius: 6px;
                  font-size: 0.8rem; overflow-x: auto; line-height: 1.5; margin: 8px 0; }
            code { font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace; }
            .empty { color: #a0aec0; font-style: italic; padding: 12px 0; }
            .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
            .stat { text-align: center; padding: 16px; background: #f7fafc; border-radius: 6px; }
            .stat .num { font-size: 1.8rem; font-weight: 700; color: #2c3e6b; }
            .stat .lbl { font-size: 0.8rem; color: #718096; }
            .section-toggle { cursor: pointer; user-select: none; }
            .section-toggle:hover { opacity: 0.7; }
            .json-key { color: #63b3ed; }
            .json-string { color: #9ae6b4; }
            .json-number { color: #f6ad55; }
            .json-bool { color: #fc8181; }
            .json-null { color: #a0aec0; }
            .warning { background: #fffff0; border-left: 4px solid #ecc94b; padding: 12px 16px;
                       border-radius: 4px; margin: 8px 0; font-size: 0.88rem; }
            .flow-arrow { color: #3a7bd5; font-weight: bold; margin: 0 8px; }
            @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }
            </style>
            </head>
            <body>
            <div class="container">
            """ + headerSection()
            + statsSection()
            + configSection()
            + sourceSchemaSection()
            + relationshipMappingSection()
            + patternSection()
            + targetCollectionsSection()
            + """
            </div>
            </body>
            </html>
            """;
    }

    private String headerSection() {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        return """
            <div class="header">
              <h1>TO DB Converter — Migration Report</h1>
              <div class="sub">Generated: %s &nbsp;|&nbsp; %s &nbsp;→&nbsp; %s</div>
            </div>
            """.formatted(ts, labelSourceDb(), labelTargetDb());
    }

    private String labelSourceDb() {
        String url = config.getSourceJdbcUrl();
        if (url == null) return "PostgreSQL";
        if (url.contains("h2")) return "H2 (test)";
        return "PostgreSQL";
    }

    private String labelTargetDb() {
        String db = config.getTargetDatabase();
        return db != null ? "MongoDB / " + db : "MongoDB";
    }

    private String statsSection() {
        long sourceTables = sourceGraph.getTables().size();
        long sourceRows = sourceSamples.values().stream().mapToLong(List::size).sum();
        long targetColls = targetCollections.size();
        long targetDocs = targetCollections.values().stream().mapToLong(CollectionInfo::docCount).sum();
        return """
            <div class="grid-2">
              <div class="stat"><div class="num">%d</div><div class="lbl">Source Tables</div></div>
              <div class="stat"><div class="num">%d</div><div class="lbl">Source Rows</div></div>
              <div class="stat"><div class="num">%d</div><div class="lbl">Target Collections</div></div>
              <div class="stat"><div class="num">%d</div><div class="lbl">Target Documents</div></div>
            </div>
            """.formatted(sourceTables, sourceRows, targetColls, targetDocs);
    }

    private String configSection() {
        String url = config.getSourceJdbcUrl();
        String mongo = config.getTargetMongoUri();
        String db = config.getTargetDatabase();
        String def = String.valueOf(config.getDefaultStrategy());
        int safe = config.getMaxChildrenPerParent();
        return """
            <h2>Configuration</h2>
            <div class="card">
              <table>
                <tr><th>Setting</th><th>Value</th></tr>
                <tr><td>Source JDBC URL</td><td><code>%s</code></td></tr>
                <tr><td>MongoDB URI</td><td><code>%s</code></td></tr>
                <tr><td>Target Database</td><td><code>%s</code></td></tr>
                <tr><td>Default Strategy</td><td><span class="badge %s">%s</span></td></tr>
                <tr><td>Max Children / Parent</td><td>%d</td></tr>
              </table>
            </div>
            """.formatted(
                safe(url), safe(mongo), safe(db),
                def.equals("EMBED") ? "badge-embed" : "badge-reference", def, safe);
    }

    private static String safe(String s) {
        return s != null ? s.replace("<", "&lt;").replace(">", "&gt;") : "";
    }

    private String sourceSchemaSection() {
        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Source Database Schema</h2>\n");

        // Overview table
        sb.append("<div class=\"card\">\n");
        sb.append("<table><tr><th>Table</th><th>Type</th><th>Rows</th><th>Columns</th><th>Foreign Keys</th></tr>\n");
        for (TableMetadata t : sourceGraph.getTables()) {
            String name = t.getName();
            long colCount = t.getColumns().size();
            long fkCount = t.getForeignKeys().size();
            long rowCount = t.getRowCount();
            String typeLabel = switch (t.getTableType()) {
                case PRIMARY_ENTITY -> "PRIMARY";
                case CHILD_ENTITY -> "CHILD";
                case JUNCTION_TABLE -> "JUNCTION";
            };
            sb.append("<tr><td><strong>").append(esc(name)).append("</strong></td>")
              .append("<td><span class=\"badge badge-pk\">").append(typeLabel).append("</span></td>")
              .append("<td>").append(rowCount).append("</td>")
              .append("<td>").append(colCount).append("</td>")
              .append("<td>").append(fkCount).append("</td></tr>\n");
        }
        sb.append("</table>\n</div>\n");

        // Per-table details
        for (TableMetadata t : sourceGraph.getTables()) {
            sb.append("<h3>").append(esc(t.getName())).append("</h3>\n");
            sb.append("<div class=\"card\">\n");

            // Columns
            sb.append("<table><tr><th>Column</th><th>Type</th><th>PK</th><th>Nullable</th></tr>\n");
            for (ColumnMetadata c : t.getColumns()) {
                sb.append("<tr><td><code>").append(esc(c.getName())).append("</code></td>")
                  .append("<td>").append(esc(c.getTypeName())).append("</td>")
                  .append("<td>").append(c.isPrimaryKey() ? "<span class=\"badge badge-pk\">PK</span>" : "").append("</td>")
                  .append("<td>").append(c.isNullable() ? "<span class=\"badge badge-nullable\">YES</span>" : "NO").append("</td></tr>\n");
            }
            sb.append("</table>\n");

            // Foreign keys
            if (!t.getForeignKeys().isEmpty()) {
                sb.append("<div style=\"margin-top: 8px; font-size: 0.85rem;\"><strong>Foreign Keys:</strong><br>\n");
                for (ForeignKeyMetadata fk : t.getForeignKeys()) {
                    sb.append("&nbsp;&nbsp;").append(esc(fk.getFkColumnName()))
                      .append(" → <strong>").append(esc(fk.getPkTableName())).append(".").append(esc(fk.getPkColumnName()))
                      .append("</strong> (").append(fk.getCardinality()).append(")")
                      .append("<br>\n");
                }
                sb.append("</div>\n");
            }

            // Sample rows
            List<Map<String, Object>> samples = sourceSamples.get(t.getName());
            if (samples != null && !samples.isEmpty()) {
                sb.append("<div style=\"margin-top: 12px;\"><strong>Sample Data (").append(samples.size()).append(" rows):</strong></div>\n");
                String json = toPrettyJson(samples);
                sb.append("<pre>").append(esc(json)).append("</pre>\n");
            } else {
                sb.append("<div class=\"empty\">No sample data available</div>\n");
            }

            sb.append("</div>\n");
        }

        return sb.toString();
    }

    private String relationshipMappingSection() {
        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Relationship Mapping</h2>\n");
        sb.append("<div class=\"card\">\n");

        Set<String> seen = new HashSet<>();
        sb.append("<table><tr><th>Child Table</th><th></th><th>Parent Table</th><th>Cardinality</th><th>Strategy</th></tr>\n");

        for (TableMetadata t : sourceGraph.getTables()) {
            for (ForeignKeyMetadata fk : t.getForeignKeys()) {
                String child = fk.getFkTableName();
                String parent = fk.getPkTableName();
                String key = child + "|" + parent;
                if (seen.contains(key)) continue;
                seen.add(key);

                Strategy s = config.getStrategy(child, parent);
                String badgeClass = s == Strategy.EMBED ? "badge-embed" : "badge-reference";

                sb.append("<tr>")
                  .append("<td><strong>").append(esc(child)).append("</strong></td>")
                  .append("<td class=\"flow-arrow\">→</td>")
                  .append("<td><strong>").append(esc(parent)).append("</strong></td>")
                  .append("<td>").append(fk.getCardinality()).append("</td>")
                  .append("<td><span class=\"badge ").append(badgeClass).append("\">").append(s).append("</span></td>")
                  .append("</tr>\n");
            }
        }

        sb.append("</table>\n");
        sb.append("<div style=\"margin-top: 8px; font-size: 0.82rem; color: #718096;\">\n");
        sb.append("<span class=\"badge badge-embed\">EMBED</span> — child data nested inside parent document &nbsp;&nbsp;\n");
        sb.append("<span class=\"badge badge-reference\">REFERENCE</span> — child data in separate collection\n");
        sb.append("</div>\n");
        sb.append("</div>\n");
        return sb.toString();
    }

    private String patternSection() {
        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Design Patterns</h2>\n");
        sb.append("<div class=\"card\">\n");

        boolean hasPatterns = false;
        sb.append("<table><tr><th>Table</th><th>Pattern</th><th>Configuration</th></tr>\n");

        for (TableMetadata t : sourceGraph.getTables()) {
            String name = t.getName();
            Map<String, String> patterns = config.getPatternConfig(name);
            if (patterns.isEmpty()) continue;
            hasPatterns = true;
            boolean first = true;
            for (Map.Entry<String, String> e : patterns.entrySet()) {
                sb.append("<tr>");
                if (first) {
                    sb.append("<td rowspan=\"").append(patterns.size()).append("\"><strong>").append(esc(name)).append("</strong></td>");
                    first = false;
                }
                sb.append("<td><span class=\"badge badge-pattern\">").append(esc(e.getKey())).append("</span></td>")
                  .append("<td><code>").append(esc(e.getValue())).append("</code></td>")
                  .append("</tr>\n");
            }
        }

        if (!hasPatterns) {
            sb.append("<tr><td colspan=\"3\" class=\"empty\">No design patterns configured</td></tr>\n");
        }

        sb.append("</table>\n</div>\n");
        return sb.toString();
    }

    private String targetCollectionsSection() {
        StringBuilder sb = new StringBuilder();
        sb.append("<h2>Target Database Collections</h2>\n");

        if (targetCollections.isEmpty()) {
            sb.append("<div class=\"card\"><div class=\"empty\">No collections found in target database.</div></div>\n");
            return sb.toString();
        }

        // Overview
        sb.append("<div class=\"card\">\n");
        sb.append("<table><tr><th>Collection</th><th>Documents</th></tr>\n");
        for (CollectionInfo ci : targetCollections.values()) {
            sb.append("<tr><td><strong>").append(esc(ci.name())).append("</strong></td>")
              .append("<td>").append(ci.docCount()).append("</td></tr>\n");
        }
        sb.append("</table>\n</div>\n");

        // Per-collection samples
        for (CollectionInfo ci : targetCollections.values()) {
            sb.append("<h3>").append(esc(ci.name())).append("</h3>\n");
            sb.append("<div class=\"card\">\n");
            sb.append("<div style=\"margin-bottom: 8px; font-size: 0.85rem; color: #718096;\">")
              .append(ci.docCount()).append(" document(s)")
              .append("</div>\n");

            if (ci.sampleJsons().isEmpty()) {
                sb.append("<div class=\"empty\">No documents</div>\n");
            } else {
                for (String json : ci.sampleJsons()) {
                    sb.append("<pre>").append(esc(json)).append("</pre>\n");
                }
            }
            sb.append("</div>\n");
        }

        return sb.toString();
    }

    private static String toPrettyJson(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            return String.valueOf(obj);
        }
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
