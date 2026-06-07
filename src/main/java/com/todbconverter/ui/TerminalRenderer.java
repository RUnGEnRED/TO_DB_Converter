package com.todbconverter.ui;

import com.todbconverter.core.model.SchemaGraph;
import com.todbconverter.core.model.TableMetadata;
import org.jline.terminal.Terminal;

import java.util.List;
import java.util.Map;

/**
 * Terminal renderer for displaying progress and results.
 */
public class TerminalRenderer {

    // ANSI color codes
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BOLD = "\u001B[1m";

    private final Terminal terminal;

    public TerminalRenderer(Terminal terminal) {
        this.terminal = terminal;
    }

    public TerminalRenderer() {
        this.terminal = null;
    }

    /**
     * Print a step message.
     */
    public void printStep(String message) {
        print(ANSI_CYAN + ">>> " + message + ANSI_RESET);
    }

    /**
     * Print a success message.
     */
    public void printSuccess(String message) {
        print(ANSI_GREEN + "  OK " + message + ANSI_RESET);
    }

    /**
     * Print an error message.
     */
    public void printError(String message) {
        print(ANSI_RED + "ERROR: " + message + ANSI_RESET);
    }

    /**
     * Print a warning message.
     */
    public void printWarning(String message) {
        print(ANSI_YELLOW + "WARN: " + message + ANSI_RESET);
    }

    /**
     * Print a message with formatting.
     */
    public void print(String message) {
        if (terminal != null) {
            terminal.writer().println(message);
            terminal.writer().flush();
        } else {
            System.out.println(message);
        }
    }

    /**
     * Print the migration summary.
     */
    public void printSummary(SchemaGraph graph, Map<String, List<Map<String, Object>>> data, long durationMs) {
        print("");
        print(ANSI_BOLD + "=== Migration Summary ===" + ANSI_RESET);
        print("Tables processed: " + graph.getTables().size());
        print("Total documents: " + data.values().stream().mapToLong(List::size).sum());
        print("Duration: " + (durationMs / 1000.0) + " seconds");
        print(ANSI_GREEN + "Migration completed successfully!" + ANSI_RESET);
    }
}
