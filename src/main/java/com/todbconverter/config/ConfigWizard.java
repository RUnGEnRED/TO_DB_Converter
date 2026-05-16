package com.todbconverter.config;

import com.todbconverter.connection.PostgreSQLConnection;
import com.todbconverter.extractor.MetadataExtractor;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;

import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;
import org.jline.utils.InfoCmp.Capability;

import static org.jline.keymap.KeyMap.key;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class ConfigWizard {
    private final DatabaseConfig config;
    private Terminal terminal;
    private LineReader reader;
    private Properties props;
    private List<TableMetadata> schemaTables;
    private Map<String, Integer> childCounts;

    public ConfigWizard(DatabaseConfig config) {
        this.config = config;
        this.props = config.getProperties();
    }

    public void run() {
        try {
            terminal = TerminalBuilder.builder().system(true).build();
            reader = LineReaderBuilder.builder().terminal(terminal).build();

            header("TO DB Converter - Configuration Wizard");

            promptConnectionSettings();

            try {
                schemaTables = discoverSchema();
                if (schemaTables != null && !schemaTables.isEmpty()) {
                    promptRelationshipStrategies();
                }
            } catch (Exception e) {
                terminal.writer().println(ansi("@|yellow DB schema discovery failed: " + e.getMessage() + "|@"));
                terminal.flush();
            }

            promptSchemaDesignPatterns();
            promptConversionDirection();
            saveConfig();

            terminal.writer().println(ansi("@|green Configuration saved successfully!|@"));
            terminal.flush();

        } catch (Exception e) {
            System.err.println("Wizard error: " + e.getMessage());
        } finally {
            try { if (terminal != null) terminal.close(); } catch (Exception ignored) {}
        }
    }

    private void header(String title) {
        clearScreen();
        terminal.writer().println(ansi("@|bold,green " + title + "|@"));
        terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
        terminal.writer().println();
        terminal.flush();
    }

    private void screenHeader(String stepLabel, String title) {
        clearScreen();
        terminal.writer().println(ansi("@|bold,green " + title + "|@"));
        terminal.writer().println(ansi("@|faint " + stepLabel + "|@"));
        terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
        terminal.writer().println();
        terminal.flush();
    }

    private void clearScreen() {
        terminal.puts(InfoCmp.Capability.clear_screen);
        terminal.writer().flush();
    }

    private static final Pattern MARKUP = Pattern.compile("@\\|([a-zA-Z,]+)\\s+(.*?)\\|@");

    private String ansi(String text) {
        Matcher m = MARKUP.matcher(text);
        if (!m.find()) {
            return text;
        }
        m.reset();
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String styles = m.group(1);
            String content = m.group(2);
            m.appendReplacement(sb, Matcher.quoteReplacement(ansiStart(styles) + content + "\033[0m"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String ansiStart(String styles) {
        StringBuilder sb = new StringBuilder("\033[");
        String[] parts = styles.split(",");
        boolean first = true;
        for (String part : parts) {
            String p = part.trim();
            if (!first) sb.append(";");
            first = false;
            switch (p) {
                case "bold": sb.append("1"); break;
                case "faint": sb.append("2"); break;
                case "g": case "green": sb.append("32"); break;
                case "r": case "red": sb.append("31"); break;
                case "c": case "cyan": sb.append("36"); break;
                case "y": case "yellow": sb.append("33"); break;
                case "white": sb.append("37"); break;
                case "m": case "magenta": sb.append("35"); break;
            }
        }
        sb.append("m");
        return sb.toString();
    }

    private String readLine(String prompt, String defaultValue) {
        String fullPrompt = prompt + " [" + defaultValue + "]: ";
        String input = reader.readLine(fullPrompt).trim();
        return input.isBlank() ? defaultValue : input;
    }

    private boolean readBoolean(String prompt, boolean defaultValue) {
        String label = defaultValue ? "yes" : "no";
        String fullPrompt = prompt + "? [" + label + "] / " + (defaultValue ? "no" : "yes") + ": ";
        String input = reader.readLine(fullPrompt).trim().toLowerCase();
        if (input.isEmpty()) return defaultValue;
        return input.equals("yes") || input.equals("y") || input.equals("true") || input.equals("1");
    }

    private String readChoice(String prompt, String defaultValue, List<String> validChoices) {
        String fullPrompt = prompt + " [" + defaultValue + "]: ";
        String input = reader.readLine(fullPrompt).trim().toUpperCase();
        if (input.isEmpty()) return defaultValue;
        for (String v : validChoices) {
            if (v.equals(input)) return input;
        }
        terminal.writer().println(ansi("@|yellow Invalid choice. Using default: " + defaultValue + "|@"));
        return defaultValue;
    }

    private void promptConnectionSettings() {
        screenHeader("Step 1/4", "Database Connections");

        terminal.writer().println(ansi("@|bold PostgreSQL|@"));
        String host = readLine("  Host", props.getProperty("postgres.host", "localhost"));
        props.setProperty("postgres.host", host);
        String port = readLine("  Port", props.getProperty("postgres.port", "5432"));
        props.setProperty("postgres.port", port);
        String db = readLine("  Database", props.getProperty("postgres.database", "source_db"));
        props.setProperty("postgres.database", db);
        String user = readLine("  Username", props.getProperty("postgres.username", "postgres"));
        props.setProperty("postgres.username", user);

        String pass = readLine("  Password", props.getProperty("postgres.password", ""));
        props.setProperty("postgres.password", pass);

        String schema = readLine("  Schema", props.getProperty("postgres.schema", "public"));
        props.setProperty("postgres.schema", schema);

        terminal.writer().println();
        terminal.writer().println(ansi("@|bold MongoDB|@"));
        String mongoHost = readLine("  Host", props.getProperty("mongo.host", "localhost"));
        props.setProperty("mongo.host", mongoHost);
        String mongoPort = readLine("  Port", props.getProperty("mongo.port", "27017"));
        props.setProperty("mongo.port", mongoPort);
        String mongoDb = readLine("  Database", props.getProperty("mongo.database", "testdb"));
        props.setProperty("mongo.database", mongoDb);
        String mongoUser = readLine("  Username", props.getProperty("mongo.username", "root"));
        props.setProperty("mongo.username", mongoUser);

        String mongoPass = readLine("  Password", props.getProperty("mongo.password", ""));
        props.setProperty("mongo.password", mongoPass);
        terminal.writer().println();
    }

    private List<TableMetadata> discoverSchema() {
        terminal.writer().println(ansi("@|cyan Connecting to PostgreSQL to discover schema...|@"));
        terminal.flush();
        try {
            PostgreSQLConnection pgConn = new PostgreSQLConnection(
                    props.getProperty("postgres.host", "localhost"),
                    Integer.parseInt(props.getProperty("postgres.port", "5432")),
                    props.getProperty("postgres.database", "source_db"),
                    props.getProperty("postgres.username", "postgres"),
                    props.getProperty("postgres.password", "postgres")
            );
            pgConn.connect();
            Connection conn = pgConn.getConnection();

            MetadataExtractor extractor = new MetadataExtractor(conn);
            String schema = props.getProperty("postgres.schema", "public");
            List<TableMetadata> tables = extractor.extractAllTables(schema);

            childCounts = estimateCounts(conn, tables);
            pgConn.disconnect();

            terminal.writer().println(ansi("@|green Found " + tables.size() + " tables|@"));
            terminal.writer().println();
            return tables;
        } catch (Exception e) {
            terminal.writer().println(ansi("@|yellow Warning: " + e.getMessage() + "|@"));
            terminal.writer().println("@|yellow Relationship config will use defaults|@");
            terminal.writer().println();
            return Collections.emptyList();
        }
    }

    private Map<String, Integer> estimateCounts(Connection conn, List<TableMetadata> tables) {
        Map<String, Integer> counts = new HashMap<>();
        for (TableMetadata table : tables) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + table.getTableName() + "\"")) {
                if (rs.next()) counts.put(table.getTableName(), rs.getInt(1));
            } catch (Exception ignored) {}
        }
        return counts;
    }

    private void promptRelationshipStrategies() {
        screenHeader("Step 2/4", "Relationship Strategies");
        terminal.writer().println(ansi("@|bold,cyan EMBED|@ = child data inside parent  |  @|bold,yellow REFERENCE|@ = separate collection"));
        terminal.writer().println();

        List<TableRow> rows = buildRows();
        if (rows.isEmpty()) return;

        int selected = 0;
        boolean[] embed = new boolean[rows.size()];
        boolean[] fullMode = new boolean[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            TableRow r = rows.get(i);
            int count = r.rowCount;
            boolean suggestEmbed = count <= 1000 || count < 0;
            embed[i] = suggestEmbed;
            String key = "relationship.strategy." + r.originTable;
            if (props.containsKey(key)) {
                embed[i] = "EMBED".equalsIgnoreCase(props.getProperty(key));
            }
            if (r.hasM2m) {
                // For M:N, use parent_table->target_table as key (not junction table name)
                String parentTable = r.isJunction ? r.parentTable : r.originTable;
                String m2mKey = "relationship.mn_mode." + parentTable + "_" + r.m2mTarget;
                fullMode[i] = !props.containsKey(m2mKey) || "FULL".equalsIgnoreCase(props.getProperty(m2mKey));
            }
        }

        BindingReader br = bindingReader();
        KeyMap<String> km = arrowKeyMap();
        Attributes orig = terminal.enterRawMode();
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                terminal.puts(InfoCmp.Capability.cursor_home);
                terminal.writer().print("\033[J");
                terminal.writer().println(ansi("@|bold,green Relationship Strategies|@"));
                terminal.writer().println(ansi("@|faint Step 2/4|@"));
                terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
                terminal.writer().println(ansi("@|bold,cyan EMBED|@ = child data inside parent  |  @|bold,yellow REFERENCE|@ = separate collection"));
                terminal.writer().println();
                renderTable(rows, embed, fullMode, selected);
                String action = br.readBinding(km);
                if (action == null) continue;
                switch (action) {
                    case "UP" -> { if (selected > 0) selected--; }
                    case "DOWN" -> { if (selected < rows.size() - 1) selected++; }
                    case "LEFT", "RIGHT" -> embed[selected] = !embed[selected];
                    case "TAB" -> { if (rows.get(selected).hasM2m) fullMode[selected] = !fullMode[selected]; }
                    case "ENTER" -> { break; }
                }
                if ("ENTER".equals(action)) break;
            }
        } finally {
            terminal.puts(Capability.keypad_local);
            terminal.setAttributes(orig);
            terminal.flush();
        }

        for (int i = 0; i < rows.size(); i++) {
            TableRow r = rows.get(i);
            if (r.relType == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY || r.isJunction) {
                // For M:N, use parent_table->target_table as key (not junction table name)
                // because transformer uses parentTable.getTableName() + "_" + relatedTableName
                String parentTable = r.isJunction ? r.parentTable : r.originTable;
                String m2mKey = "relationship.mn_mode." + parentTable + "_" + r.m2mTarget;
                props.setProperty(m2mKey, fullMode[i] ? "FULL" : "IDS");
            } else {
                props.setProperty("relationship.strategy." + r.originTable, embed[i] ? "EMBED" : "REFERENCE");
            }
        }

        clearScreen();
        terminal.writer().println(ansi("@|green Strategies configured for " + rows.size() + " tables|@"));
        terminal.writer().println();
    }

    private record TableRow(String originTable, String parentTable, ForeignKeyMetadata.RelationshipType relType, String relLabel, int rowCount,
                            boolean isJunction, boolean hasM2m, String m2mTarget) {}

    private List<TableRow> buildRows() {
        List<TableRow> rows = new ArrayList<>();
        Set<String> added = new HashSet<>();

        for (TableMetadata table : schemaTables) {
            List<ForeignKeyMetadata> fks = table.getForeignKeys();
            if (fks == null || fks.isEmpty()) continue;

            for (ForeignKeyMetadata fk : fks) {
                String key = table.getTableName() + "->" + fk.getReferencedTable();
                if (added.contains(key)) continue;
                added.add(key);

                ForeignKeyMetadata.RelationshipType rel = fk.getRelationshipType();
                String label;
                boolean isJunction = false;
                boolean hasM2m = false;
                String m2mTarget = fk.getReferencedTable();

                if (rel == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY) {
                    label = "M:N";
                    hasM2m = true;
                    if (isJunctionTable(table)) {
                        isJunction = true;
                        for (ForeignKeyMetadata otherFk : fks) {
                            if (!otherFk.getReferencedTable().equalsIgnoreCase(fk.getReferencedTable())) {
                                m2mTarget = otherFk.getReferencedTable();
                                break;
                            }
                        }
                    }
                } else if (table.getTableName().equals(fk.getReferencedTable())) {
                    label = "self-ref";
                    isJunction = false;
                } else if (isJunctionTable(table)) {
                    label = "M:N (junction)";
                    isJunction = true;
                    hasM2m = true;
                    // For junction tables, find the OTHER FK to determine the M:N target
                    for (ForeignKeyMetadata otherFk : fks) {
                        if (!otherFk.getReferencedTable().equalsIgnoreCase(fk.getReferencedTable())) {
                            m2mTarget = otherFk.getReferencedTable();
                            break;
                        }
                    }
                } else if (isColumnUnique(table, fk.getColumnName())) {
                    label = "1:1";
                } else {
                    label = "1:N";
                }

                int cnt = childCounts.getOrDefault(table.getTableName(), -1);
                rows.add(new TableRow(table.getTableName(), fk.getReferencedTable(), rel, label, cnt,
                        isJunction, hasM2m, m2mTarget));
            }
        }
        return rows;
    }

    private boolean isJunctionTable(TableMetadata table) {
        List<ForeignKeyMetadata> fks = table.getForeignKeys();
        return fks != null && fks.size() == 2 && table.getColumns().size() <= 5;
    }

    private boolean isColumnUnique(TableMetadata table, String columnName) {
        String[] pkCols = table.getPrimaryKeyColumnArray();
        if (pkCols.length == 1 && pkCols[0].equalsIgnoreCase(columnName)) return true;
        return false;
    }

    private void renderTable(List<TableRow> rows, boolean[] embed, boolean[] fullMode, int selected) {
        int w = Math.min(terminal.getWidth(), 80);

        String header = "  #  Table                \u2502 Rows  Strategy    M:N Mode";
        terminal.writer().println(ansi("@|bold,cyan " + header + "|@"));
        terminal.writer().println("\u2500".repeat(w));

        for (int i = 0; i < rows.size(); i++) {
            TableRow r = rows.get(i);
            boolean isSel = i == selected;
            String cursor = isSel ? "\u25b6 " : "  ";
            String num = String.format("%-2d", i + 1);
            String name = String.format("%-20s", r.originTable);
            String cnt = r.rowCount >= 0 ? String.format("%5d", r.rowCount) : "  N/A";

            String strat;
            if (r.isJunction || r.relType == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY) {
                strat = isSel ? (embed[i] ? "@|bold,g [ EMBED ]|@" : "@|bold,y [REFERENCE]|@") : (embed[i] ? "  EMBED  " : " REFERENCE");
            } else if (r.relType == ForeignKeyMetadata.RelationshipType.ONE_TO_ONE) {
                strat = "  " + r.relLabel + "  ";
            } else {
                strat = isSel ? (embed[i] ? "@|bold,g [ EMBED ]|@" : "@|bold,y [REFERENCE]|@") : (embed[i] ? "  EMBED  " : " REFERENCE");
            }

            String m2m;
            if (r.hasM2m) {
                if (r.isJunction) {
                    m2m = isSel ? (fullMode[i] ? "@|bold,c [ FULL ]|@" : "@|bold,m [  IDS  ]|@") : (fullMode[i] ? "  FULL  " : "  IDS  ");
                } else {
                    m2m = isSel ? (fullMode[i] ? "@|bold,c [ FULL ]|@" : "@|bold,m [  IDS  ]|@") : (fullMode[i] ? "  FULL  " : "  IDS  ");
                }
            } else {
                m2m = "   -   ";
            }

            String line = cursor + num + " " + name + " \u2502 " + cnt + "  " + strat + "   " + m2m;
            if (isSel) {
                String parent = r.parentTable.equals(r.originTable) ? "(self)" : r.parentTable;
                String suffix = "  " + r.relLabel + " -> " + parent;
                String processedLine = ansi(line);
                terminal.writer().println("\033[1;37m" + processedLine + "\033[0m\033[2m" + suffix + "\033[0m");
            } else {
                terminal.writer().println(ansi(line));
            }
        }

        terminal.writer().println(ansi("@|faint \u2191\u2195 move  \u2190\u2192 toggle strategy  [Tab] toggle M:N  [Enter] confirm|@"));
        terminal.flush();
    }

    private BindingReader bindingReader() {
        return new BindingReader(terminal.reader());
    }

    private KeyMap<String> arrowKeyMap() {
        KeyMap<String> map = new KeyMap<>();
        map.bind("UP",    key(terminal, Capability.key_up));
        map.bind("DOWN",  key(terminal, Capability.key_down));
        map.bind("LEFT",  key(terminal, Capability.key_left));
        map.bind("RIGHT", key(terminal, Capability.key_right));
        map.bind("TAB",   "\t", "\033[Z");
        map.bind("ENTER", "\r", "\n");
        map.bind("SPACE", " ");
        return map;
    }

    private void promptSchemaDesignPatterns() {
        screenHeader("Step 3/4", "Schema Design Patterns");

        record PatternDef(String key, String label, String paramLabel, String paramKey, String defaultParam) {}

        PatternDef[] patterns = {
            new PatternDef("pattern.attribute", "Attribute Pattern: group similar columns", "Min columns to group", "pattern.attribute.threshold", "3"),
            new PatternDef("pattern.bucket", "Bucket Pattern: group data into bounded chunks", "Max items per bucket", "pattern.bucket.size", "10"),
            new PatternDef("pattern.subset", "Subset Pattern: separate hot/cold data", "Max items in main doc", "pattern.subset.limit", "10"),
            new PatternDef("pattern.outlier", "Outlier Pattern: isolate large arrays", "Max array size before outlier", "pattern.outlier.threshold", "50"),
            new PatternDef("pattern.computed", "Computed Pattern: pre-compute aggregations", "Rules (field:SUM(a+b))", "pattern.computed.fields", ""),
            new PatternDef("pattern.approximation", "Approximation Pattern: round values", "Granularity", "pattern.approximation.granularity", "100"),
        };

        boolean[] enabled = new boolean[patterns.length];
        String[] params = new String[patterns.length];
        int selected = 0;

        for (int i = 0; i < patterns.length; i++) {
            enabled[i] = Boolean.parseBoolean(props.getProperty(patterns[i].key + ".enabled", i == 0 ? "true" : "false"));
            params[i] = props.getProperty(patterns[i].paramKey, patterns[i].defaultParam);
        }

        BindingReader br = bindingReader();
        KeyMap<String> km = arrowKeyMap();
        Attributes orig = terminal.enterRawMode();
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                terminal.puts(InfoCmp.Capability.cursor_home);
                terminal.writer().print("\033[J");
                terminal.writer().println(ansi("@|bold,green Schema Design Patterns|@"));
                terminal.writer().println(ansi("@|faint Step 3/4|@"));
                terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
                terminal.writer().println();

                for (int i = 0; i < patterns.length; i++) {
                    String check = enabled[i] ? "@|bold,g [x]|@" : "@|faint [ ]|@";
                    String label = patterns[i].label;
                    String suffix = enabled[i] && !params[i].isEmpty()
                        ? "@|faint  (" + patterns[i].paramLabel + ": " + params[i] + ")|@"
                        : "";

                    if (i == selected) {
                        String processedCheck = ansi(check);
                        terminal.writer().println("\033[1;37m\u25b6 " + processedCheck + " " + label + "\033[0m " + ansi(suffix));
                    } else {
                        terminal.writer().println(ansi("  " + check + " " + label + " " + suffix));
                    }
                }

                terminal.writer().println();
                terminal.writer().println(ansi("@|faint \u2191\u2195 navigate  [Space] toggle  [Enter] confirm|@"));
                terminal.flush();

                String action = br.readBinding(km);
                if ("UP".equals(action) && selected > 0) selected--;
                else if ("DOWN".equals(action) && selected < patterns.length - 1) selected++;
                else if ("ENTER".equals(action)) break;
                else if ("SPACE".equals(action)) {
                    enabled[selected] = !enabled[selected];
                    if (enabled[selected] && !patterns[selected].defaultParam.isEmpty()) {
                        terminal.puts(Capability.keypad_local);
                        terminal.setAttributes(orig);
                        terminal.writer().println();
                        terminal.flush();
                        String val = reader.readLine(ansi("@|cyan  " + patterns[selected].paramLabel + "|@") + " [" + params[selected] + "]: ").trim();
                        if (!val.isBlank()) params[selected] = val;
                        orig = terminal.enterRawMode();
                        terminal.puts(Capability.keypad_xmit);
                        terminal.flush();
                        br = bindingReader();
                    }
                }
            }
        } finally {
            terminal.puts(Capability.keypad_local);
            terminal.setAttributes(orig);
            terminal.flush();
        }

        for (int i = 0; i < patterns.length; i++) {
            props.setProperty(patterns[i].key + ".enabled", String.valueOf(enabled[i]));
            if (enabled[i]) {
                props.setProperty(patterns[i].paramKey, params[i]);
            }
        }
    }

    private void promptConversionDirection() {
        int selected = "MONGO_TO_POSTGRES".equals(props.getProperty("conversion.direction")) ? 1 : 0;
        String[] options = {"PostgreSQL \u2192 MongoDB", "MongoDB \u2192 PostgreSQL"};

        BindingReader br = bindingReader();
        KeyMap<String> km = arrowKeyMap();
        Attributes orig = terminal.enterRawMode();
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                    terminal.puts(InfoCmp.Capability.cursor_home);
                    terminal.writer().print("\033[J");
                    terminal.writer().println(ansi("@|bold,green Conversion Direction|@"));
                    terminal.writer().println(ansi("@|faint Step 4/4|@"));
                    terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
                    terminal.writer().println(ansi("@|bold Choose conversion direction:|@"));
                    terminal.writer().println();

                    for (int i = 0; i < options.length; i++) {
                        String radio = i == selected ? "@|bold,g (\u25c9)|@" : "@|faint (\u25cb)|@";
                        if (i == selected) {
                            String processedRadio = ansi(radio);
                            terminal.writer().println("\033[1;37m\u25b6 " + processedRadio + " " + options[i] + "\033[0m");
                        } else {
                            terminal.writer().println(ansi("  " + radio + " " + options[i]));
                        }
                    }

                    terminal.writer().println();
                    terminal.writer().println(ansi("@|faint \u2191\u2195 navigate  [Enter] confirm|@"));
                    terminal.flush();

                    String action = br.readBinding(km);
                    if ("UP".equals(action) && selected > 0) selected--;
                    else if ("DOWN".equals(action) && selected < options.length - 1) selected++;
                    else if ("ENTER".equals(action)) break;
            }
        } finally {
            terminal.puts(Capability.keypad_local);
            terminal.setAttributes(orig);
            terminal.flush();
        }

        props.setProperty("conversion.direction", selected == 0 ? "POSTGRES_TO_MONGO" : "MONGO_TO_POSTGRES");
    }

    private void saveConfig() {
        String configPath = "application.properties";
        try (OutputStream out = new FileOutputStream(configPath)) {
            props.store(out, "TO DB Converter Configuration - Generated by ConfigWizard");
            terminal.writer().println(ansi("@|green Configuration saved to: " + new java.io.File(configPath).getAbsolutePath() + "|@"));
        } catch (IOException e) {
            terminal.writer().println(ansi("@|red Error saving config: " + e.getMessage() + "|@"));
        }
        terminal.flush();
    }
}
