package com.todbconverter.ui;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.connection.JDBCConnection;
import com.todbconverter.connection.MongoDBConnection;
import com.todbconverter.core.extractor.JDBCSchemaExtractor;
import com.todbconverter.core.model.*;
import com.todbconverter.exception.ConfigException;
import com.todbconverter.exception.ConnectionException;
import com.todbconverter.util.StringUtils;
import org.jline.keymap.BindingReader;
import org.jline.keymap.KeyMap;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;
import org.jline.utils.InfoCmp.Capability;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.jline.keymap.KeyMap.key;

public class ConsoleWizard {
    private static final String CONFIG_FILE = "db-converter.properties";
    private static final Pattern MARKUP = Pattern.compile("@\\|([a-zA-Z,]+)\\s+(.*?)\\|@");

    private Terminal terminal;
    private LineReader reader;
    private DatabaseConfig config;

    public void run() {
        try {
            terminal = TerminalBuilder.builder().system(true).build();
            reader = LineReaderBuilder.builder().terminal(terminal).build();
            config = loadExistingConfig();
            header("TO_DB Converter - Configuration Wizard");
            promptConnectionSettings();
            SchemaGraph graph = null;
            try (JDBCConnection conn = new JDBCConnection()) {
                conn.connect(config);
                JDBCSchemaExtractor extractor = new JDBCSchemaExtractor();
                graph = extractor.extractSchema(conn.getConnection());
                if (graph == null || graph.getTables().isEmpty()) {
                    terminal.writer().println(ansi("@|faint No tables discovered.|@"));
                    terminal.flush();
                } else {
                    promptRelationshipStrategies(graph);
                }
            } catch (Exception e) {
                String msg = sanitize(e.getMessage());
                terminal.writer().println(ansi("@|yellow Schema discovery failed: " + msg + "|@"));
                terminal.flush();
            }
            promptDesignPatterns(graph);
            promptSummary(graph);
        } catch (Exception e) {
            System.err.println("Wizard error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (terminal != null) {
                    terminal.setAttributes(terminal.getAttributes());
                    terminal.close();
                }
            } catch (Exception ignored) {}
        }
    }

    private DatabaseConfig loadExistingConfig() {
        Path path = Paths.get(CONFIG_FILE);
        if (Files.exists(path)) {
            try {
                DatabaseConfig loaded = DatabaseConfig.loadFromFile(CONFIG_FILE);
                terminal.writer().println(ansi("@|green Loaded existing config from " + CONFIG_FILE + "|@"));
                terminal.flush();
                return loaded;
            } catch (ConfigException e) {
                terminal.writer().println(ansi("@|yellow Starting fresh|@"));
                terminal.flush();
            }
        }
        return new DatabaseConfig();
    }

    private void header(String t) {
        clearScreen();
        terminal.writer().println(ansi("@|bold,green " + truncateTitle(t) + "|@"));
        terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
        terminal.writer().println();
        terminal.flush();
    }

    private void screenHeader(String step, String title) {
        clearScreen();
        terminal.writer().println(ansi("@|bold,green " + truncateTitle(title) + "|@"));
        terminal.writer().println(ansi("@|faint " + step + "|@"));
        terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
        terminal.writer().println();
        terminal.flush();
    }

    private String truncateTitle(String t) {
        int w = Math.min(60, terminal.getWidth());
        return t.length() > w ? t.substring(0, w - 3) + "..." : t;
    }

    private void clearScreen() {
        if (terminal.getStringCapability(InfoCmp.Capability.clear_screen) != null) {
            terminal.puts(InfoCmp.Capability.clear_screen);
        } else {
            terminal.writer().print("\033[2J\033[H");
        }
        terminal.writer().flush();
    }

    private String ansi(String text) {
        Matcher m = MARKUP.matcher(text);
        if (!m.find()) return text;
        m.reset();
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, Matcher.quoteReplacement(ansiStart(m.group(1)) + m.group(2) + ansiReset(m.group(1))));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String ansiStart(String styles) {
        StringBuilder sb = new StringBuilder("\033[");
        boolean first = true;
        for (String p : styles.split(",")) {
            if (!first) sb.append(";");
            first = false;
            switch (p.trim()) {
                case "bold": sb.append("1"); break;
                case "faint": sb.append("2"); break;
                case "g": case "green": sb.append("32"); break;
                case "r": case "red": sb.append("31"); break;
                case "c": case "cyan": sb.append("36"); break;
                case "y": case "yellow": sb.append("33"); break;
                case "m": case "magenta": sb.append("35"); break;
            }
        }
        return sb.append("m").toString();
    }

    private String ansiReset(String styles) {
        StringBuilder sb = new StringBuilder("\033[");
        boolean first = true;
        for (String p : styles.split(",")) {
            if (!first) sb.append(";");
            first = false;
            switch (p.trim()) {
                case "bold": case "faint": sb.append("22"); break;
                case "g": case "green": sb.append("39"); break;
                case "r": case "red": sb.append("39"); break;
                case "c": case "cyan": sb.append("39"); break;
                case "y": case "yellow": sb.append("39"); break;
                case "m": case "magenta": sb.append("39"); break;
            }
        }
        return sb.append("m").toString();
    }

    private String readLine(String prompt, String def) {
        String input = reader.readLine(prompt + " [" + def + "]: ").trim();
        return input.isEmpty() ? def : input;
    }

    private boolean askYesNo(String prompt, boolean def) {
        int sel = def ? 0 : 1;
        String[] opts = {"Yes", "No"};
        BindingReader br = new BindingReader(terminal.reader());
        KeyMap<String> km = arrowKeyMap();
        Attributes orig = terminal.enterRawMode();
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                terminal.puts(InfoCmp.Capability.save_cursor);
                terminal.writer().print(prompt + " ");
                for (int i = 0; i < opts.length; i++) {
                    terminal.writer().print(i == sel ? ansi("@|bold,g (" + opts[i] + ")|@ ") : ansi("@|faint (" + opts[i] + ")|@ "));
                }
                terminal.puts(InfoCmp.Capability.restore_cursor);
                terminal.flush();
                String a = br.readBinding(km);
                if ("LEFT".equals(a)) sel = 0;
                else if ("RIGHT".equals(a)) sel = 1;
                else if ("ENTER".equals(a)) break;
            }
        } finally {
            terminal.puts(Capability.keypad_local);
            terminal.setAttributes(orig);
            terminal.flush();
            reader = LineReaderBuilder.builder().terminal(terminal).build();
        }
        terminal.writer().println();
        return sel == 0;
    }

    private String sanitize(String msg) {
        return StringUtils.sanitizeError(msg, config != null ? config.getSourcePassword() : null);
    }

    private KeyMap<String> arrowKeyMap() {
        KeyMap<String> m = new KeyMap<>();
        m.bind("UP", key(terminal, Capability.key_up));
        m.bind("DOWN", key(terminal, Capability.key_down));
        m.bind("LEFT", key(terminal, Capability.key_left));
        m.bind("RIGHT", key(terminal, Capability.key_right));
        m.bind("TAB", "\t", "\033[Z");
        m.bind("ENTER", "\r", "\n");
        m.bind("SPACE", " ");
        return m;
    }

    // ==================== STEP 1 ====================

    private void promptConnectionSettings() {
        screenHeader("Step 1/4", "Database Connections");
        terminal.writer().println(ansi("@|bold Source Database (JDBC)|@"));
        String url = readLine("  JDBC URL", config.getSourceJdbcUrl() != null ? config.getSourceJdbcUrl() : "jdbc:postgresql://localhost:5432/source_db");
        config.setSourceJdbcUrl(url);
        String user = readLine("  Username", config.getSourceUsername() != null ? config.getSourceUsername() : "postgres");
        config.setSourceUsername(user);
        String pass;
        if (config.getSourcePassword() != null && !config.getSourcePassword().isBlank()) {
            pass = readLine("  Password", "<saved>");
            if ("<saved>".equals(pass)) pass = config.getSourcePassword();
        } else {
            pass = readLine("  Password", "");
        }
        config.setSourcePassword(pass);
        terminal.writer().println();
        terminal.writer().println(ansi("@|bold Target MongoDB|@"));
        String uri = readLine("  URI", config.getTargetMongoUri() != null ? config.getTargetMongoUri() : "mongodb://localhost:27017");
        config.setTargetMongoUri(uri);
        String db = readLine("  Database", config.getTargetDatabase() != null ? config.getTargetDatabase() : "mydb_converted");
        config.setTargetDatabase(db);
        terminal.writer().println();
        terminal.writer().println(ansi("@|cyan Testing connections...|@"));
        terminal.flush();
        try (JDBCConnection c = new JDBCConnection()) { c.connect(config); if (!c.testConnection()) throw new ConnectionException("Source connection test returned false"); terminal.writer().println(ansi("@|green ✓ Source OK|@")); } catch (Exception e) { terminal.writer().println(ansi("@|red ✗ Source: " + sanitize(e.getMessage()) + "|@")); }
        try (MongoDBConnection c = new MongoDBConnection()) { c.connect(config); if (!c.testConnection()) throw new ConnectionException("MongoDB connection test returned false"); terminal.writer().println(ansi("@|green ✓ MongoDB OK|@")); } catch (Exception e) { terminal.writer().println(ansi("@|red ✗ MongoDB: " + sanitize(e.getMessage()) + "|@")); }
        terminal.writer().println(ansi("@|faint Press Enter...|@"));
        reader.readLine();
    }

    // ==================== STEP 2 ====================

    private void promptRelationshipStrategies(SchemaGraph graph) {
        List<ForeignKeyMetadata> edges = new ArrayList<>();
        for (TableMetadata t : graph.getTables()) edges.addAll(t.getForeignKeys());
        if (edges.isEmpty()) {
            terminal.writer().println(ansi("@|faint No relationships detected.|@"));
            terminal.writer().println();
            return;
        }
        List<RelRow> rows = new ArrayList<>();
        Set<String> added = new HashSet<>();
        for (ForeignKeyMetadata fk : edges) {
            String k = fk.getFkTableName() + "." + fk.getFkColumnName() + "->" + fk.getPkTableName();
            if (added.contains(k)) continue;
            added.add(k);
            boolean selfRef = fk.isSelfReferencing();
            boolean manyToMany = fk.getCardinality() == Cardinality.MANY_TO_MANY;
            boolean forcedRefer = selfRef || manyToMany;
            String label = selfRef ? "self-ref" : manyToMany ? "M:N" : fk.getCardinality() == Cardinality.ONE_TO_ONE ? "1:1" : "1:N";
            boolean embed = !forcedRefer && config.getStrategy(fk.getFkTableName(), fk.getPkTableName()) == Strategy.EMBED;
            rows.add(new RelRow(fk.getFkTableName(), fk.getPkTableName(), fk.getFkColumnName(), label, embed, forcedRefer));
        }
        int sel = 0;
        BindingReader br = new BindingReader(terminal.reader());
        KeyMap<String> km = arrowKeyMap();
        Attributes terminalOrig = terminal.enterRawMode();
        Attributes orig = terminalOrig;
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                terminal.puts(InfoCmp.Capability.cursor_home);
                terminal.writer().print("\033[J");
                terminal.writer().println(ansi("@|bold,green Relationship Strategies|@"));
                terminal.writer().println(ansi("@|faint Step 2/4|@"));
                terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
                terminal.writer().println(ansi("@|bold,cyan EMBED|@ = nested  |  @|bold,yellow REFER|@ = separate collection"));
                terminal.writer().println();
                terminal.writer().println(ansi("@|bold,cyan " + String.format("  %-4s %-18s %-18s %-8s %-10s", "#", "Child", "Parent", "Type", "Strategy") + "|@"));
                terminal.writer().println("\u2500".repeat(Math.min(60, terminal.getWidth())));
                for (int i = 0; i < rows.size(); i++) {
                    RelRow r = rows.get(i);
                    String stratTag = r.forcedRefer ? "REFER*" : (r.embed ? "EMBED " : "REFER ");
                    String prefix = i == sel ? "\u25b6 " : "  ";
                    String body = String.format("%-2d %-18.18s %-18.18s %-8.8s ", i + 1, r.child, r.parent, r.type);
                    String strategyPlain = i == sel ? "[" + stratTag + "]" : stratTag;
                    String strategyPadded = String.format("%-10s", strategyPlain);
                    if (i == sel) {
                        String color = r.embed ? "32" : "33";
                        strategyPadded = "\033[1;" + color + "m" + strategyPadded + "\033[22;39m";
                    }
                    String line = prefix + body + strategyPadded;
                    terminal.writer().println(i == sel ? "\033[4m" + line + "\033[24m" : line);
                }
                terminal.writer().println();
                terminal.writer().println(ansi("@|faint \u2191\u2195 move  \u2190\u2192 toggle  [Enter] confirm|@"));
                terminal.flush();
                String a = br.readBinding(km);
                if (a == null) continue;
                if ("UP".equals(a) && sel > 0) sel--;
                else if ("DOWN".equals(a) && sel < rows.size() - 1) sel++;
                else if ("LEFT".equals(a) || "RIGHT".equals(a)) {
                    if (rows.get(sel).forcedRefer) {
                        terminal.writer().println();
                        terminal.writer().println(ansi("@|yellow  " + rows.get(sel).type + " — forced REFERENCE, cannot embed|@"));
                        terminal.flush();
                    } else {
                        rows.get(sel).embed = !rows.get(sel).embed;
                    }
                }
                else if ("ENTER".equals(a)) break;
            }
            } finally {
                terminal.puts(Capability.keypad_local);
                terminal.setAttributes(terminalOrig);
                terminal.flush();
            reader = LineReaderBuilder.builder().terminal(terminal).build();
        }
        for (RelRow r : rows) config.setStrategy(r.child, r.parent, r.embed ? Strategy.EMBED : Strategy.REFERENCE);
        clearScreen();
        terminal.writer().println(ansi("@|green Configured " + rows.size() + " relationships|@"));
        terminal.writer().println();
    }

    private static class RelRow {
        final String child, parent, fkCol, type;
        final boolean forcedRefer;
        boolean embed;
        RelRow(String c, String p, String f, String t, boolean e, boolean fr) { child=c; parent=p; fkCol=f; type=t; embed=e; forcedRefer=fr; }
    }

    // ==================== STEP 3 ====================

    private void promptDesignPatterns(SchemaGraph graph) {
        screenHeader("Step 3/4", "Design Patterns");
        if (!askYesNo("Configure design patterns?", true)) { terminal.writer().println(ansi("@|faint Skipping.|@")); terminal.writer().println(); return; }

        if (graph == null) { terminal.writer().println(ansi("@|yellow No schema available.|@")); return; }
        List<String> tables = graph.getTables().stream().map(TableMetadata::getName).toList();
        if (tables.isEmpty()) return;

        String[] keys = {"attribute", "computed", "subset"};
        String[] labels = {"Attr", "Comp", "Subs"};
        String[] descs = {"Group fields into key-value array", "Pre-calculate COUNT/SUM", "Embed recent N records"};
        boolean[][] enabled = new boolean[tables.size()][keys.length];
        for (int t = 0; t < tables.size(); t++) for (int p = 0; p < keys.length; p++) enabled[t][p] = config.getPatternConfig(tables.get(t)).containsKey(keys[p]);

        int sr = 0, sc = 0;
        BindingReader br = new BindingReader(terminal.reader());
        KeyMap<String> km = arrowKeyMap();
        Attributes terminalOrig = terminal.enterRawMode();
        Attributes orig = terminalOrig;
        terminal.puts(Capability.keypad_xmit);
        terminal.flush();
        try {
            while (true) {
                terminal.puts(InfoCmp.Capability.cursor_home);
                terminal.writer().print("\033[J");
                terminal.writer().println(ansi("@|bold,green Design Patterns — per table|@"));
                terminal.writer().println(ansi("@|faint Step 3/4|@"));
                terminal.writer().println("=".repeat(Math.min(60, terminal.getWidth())));
                terminal.writer().println();
                terminal.writer().println(ansi("@|bold,cyan Patterns:|@"));
                terminal.writer().println(ansi("  @|bold,g Attr|@ = " + descs[0]));
                terminal.writer().println(ansi("  @|bold,c Comp|@ = " + descs[1]));
                terminal.writer().println(ansi("  @|bold,m Subs|@ = " + descs[2]));
                terminal.writer().println();
                StringBuilder h = new StringBuilder("  ");
                h.append(String.format("%-20s", "Table"));
                for (String l : labels) h.append(String.format(" %5s", l));
                terminal.writer().println(ansi("@|bold,cyan " + h + "|@"));
                terminal.writer().println("\u2500".repeat(Math.min(60, terminal.getWidth())));
                for (int t = 0; t < tables.size(); t++) {
                    String line = (t == sr ? "\u25b6 " : "  ") + String.format("%-20.20s", tables.get(t));
                    for (int p = 0; p < keys.length; p++) {
                        boolean cs = t == sr && p == sc;
                        String cellPlain = enabled[t][p] ? (cs ? " [x] " : "  x  ") : (cs ? " [ ] " : "     ");
                        cellPlain = String.format("%-5s", cellPlain);
                        if (cs) {
                            String color = enabled[t][p] ? "1;32" : "2";
                            cellPlain = "\033[" + color + "m" + cellPlain + "\033[22;39m";
                        }
                        line += " " + cellPlain;
                    }
                    terminal.writer().println(t == sr ? "\033[4m" + line + "\033[24m" : line);
                }
                terminal.writer().println();
                terminal.writer().println(ansi("@|faint \u2191\u2195 row  \u2190\u2192 pattern  [Space] toggle  [Enter] confirm|@"));
                terminal.flush();
                String a = br.readBinding(km);
                if (a == null) continue;
                if ("UP".equals(a) && sr > 0) sr--;
                else if ("DOWN".equals(a) && sr < tables.size() - 1) sr++;
                else if ("LEFT".equals(a) && sc > 0) sc--;
                else if ("RIGHT".equals(a) && sc < keys.length - 1) sc++;
                else if ("SPACE".equals(a)) {
                    enabled[sr][sc] = !enabled[sr][sc];
                    String t = tables.get(sr), p = keys[sc];
                    if (enabled[sr][sc]) {
                        terminal.puts(Capability.keypad_local);
                        terminal.setAttributes(terminalOrig);
                        terminal.flush();
                        reader = LineReaderBuilder.builder().terminal(terminal).build();
                        terminal.writer().println();
                        String val = askPatternConfig(t, p);
                        if (val != null && !val.isBlank()) { config.setPatternConfig(t, p, val); }
                        else enabled[sr][sc] = false;
                        terminal.enterRawMode();
                        terminal.puts(Capability.keypad_xmit);
                        br = new BindingReader(terminal.reader());
                        terminal.flush();
                    } else { config.removePatternConfig(t, p); }
                }
                if ("ENTER".equals(a)) break;
            }
        } finally {
            terminal.puts(Capability.keypad_local);
            terminal.setAttributes(orig);
            terminal.flush();
            reader = LineReaderBuilder.builder().terminal(terminal).build();
        }
        clearScreen();
        terminal.writer().println(ansi("@|green Patterns configured|@"));
        terminal.writer().println();
    }

    private String askPatternConfig(String table, String pattern) {
        String val;
        switch (pattern) {
            case "attribute" -> {
                do {
                    terminal.writer().println(ansi("@|bold Attribute for " + table + "|@"));
                    terminal.writer().println(ansi("@|faint Format: arrayName=col:Key,col:Key|@"));
                    terminal.writer().println(ansi("@|faint Example: releases=release_US:USA,release_France:France|@"));
                    val = readLine("  Config", "");
                    if (val.isEmpty()) return val;
                    if (!val.contains("=")) terminal.writer().println(ansi("@|yellow Must use format: name=col:Key,...|@"));
                } while (!val.contains("="));
                return val;
            }
            case "computed" -> {
                do {
                    terminal.writer().println(ansi("@|bold Computed for " + table + "|@"));
                    terminal.writer().println(ansi("@|faint Format: fieldName=FUNC(childTable.column)|@"));
                    terminal.writer().println(ansi("@|faint Example: order_count=COUNT(orders.id)|@"));
                    val = readLine("  Config", "");
                    if (val.isEmpty()) return val;
                    if (!val.contains("(")) terminal.writer().println(ansi("@|yellow Must use format: name=FUNC(child.column)|@"));
                } while (!val.contains("("));
                return val;
            }
            case "subset" -> {
                do {
                    terminal.writer().println(ansi("@|bold Subset for " + table + "|@"));
                    terminal.writer().println(ansi("@|faint Format: childTable=limit|@"));
                    terminal.writer().println(ansi("@|faint Example: reviews=3|@"));
                    val = readLine("  Config", "");
                    if (val.isEmpty()) return val;
                    if (!val.contains("=")) terminal.writer().println(ansi("@|yellow Must use format: childTable=limit|@"));
                } while (!val.contains("="));
                return val;
            }
        }
        return null;
    }

    // ==================== STEP 4 ====================

    private void promptSummary(SchemaGraph graph) {
        screenHeader("Step 4/4", "Summary");
        terminal.writer().println(ansi("@|bold Source Database|@"));
        terminal.writer().println("  JDBC URL: " + config.getSourceJdbcUrl());
        terminal.writer().println("  Username: " + config.getSourceUsername());
        terminal.writer().println();
        terminal.writer().println(ansi("@|bold Target MongoDB|@"));
        terminal.writer().println("  URI:      " + config.getTargetMongoUri());
        terminal.writer().println("  Database: " + config.getTargetDatabase());
        terminal.writer().println();
        Map<String, Strategy> strats = config.getRelationshipStrategies();
        if (!strats.isEmpty()) {
            terminal.writer().println(ansi("@|bold Relationship Strategies|@"));
            for (Map.Entry<String, Strategy> e : strats.entrySet()) {
                String c = e.getValue() == Strategy.EMBED ? "green" : "yellow";
                terminal.writer().println("  " + e.getKey() + " = " + ansi("@|" + c + " " + e.getValue() + "|@"));
            }
            terminal.writer().println();
        }
        if (graph != null) {
            boolean hasPatterns = false;
            StringBuilder sb = new StringBuilder();
            for (TableMetadata t : graph.getTables()) {
                Map<String, String> cfgs = config.getPatternConfig(t.getName());
                if (!cfgs.isEmpty()) {
                    hasPatterns = true;
                    for (Map.Entry<String, String> e : cfgs.entrySet()) {
                        sb.append("  ").append(t.getName()).append(".").append(e.getKey())
                          .append(" = ").append(e.getValue()).append("\n");
                    }
                }
            }
            if (hasPatterns) {
                terminal.writer().println(ansi("@|bold Design Patterns|@"));
                terminal.writer().print(sb.toString());
                terminal.writer().println();
            }
        }
        terminal.writer().println(ansi("@|faint Press Enter to save...|@"));
        while (!"".equals(reader.readLine("")));
        if (askYesNo("Save configuration?", true)) {
            if (!saveConfig()) {
                terminal.writer().println(ansi("@|red Configuration was NOT saved!|@"));
                terminal.flush();
            } else {
                terminal.writer().println(ansi("@|bold,green Configuration saved!|@"));
                terminal.writer().println(ansi("@|faint Run: java -jar to-db-converter.jar run|@"));
                terminal.flush();
            }
        } else {
            terminal.writer().println(ansi("@|yellow Configuration not saved.|@"));
            terminal.flush();
        }
    }

    private boolean saveConfig() {
        try { config.saveToFile(CONFIG_FILE); terminal.writer().println(ansi("@|green Saved to " + CONFIG_FILE + "|@")); terminal.flush(); return true; }
        catch (ConfigException e) { terminal.writer().println(ansi("@|red Error: " + e.getMessage() + "|@")); terminal.flush(); return false; }
    }
}
