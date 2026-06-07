package com.todbconverter;

import com.todbconverter.cli.commands.RunCommand;
import com.todbconverter.cli.commands.ValidateCommand;
import com.todbconverter.cli.commands.WizardCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Main entry point for TO_DB Converter.
 * Database-agnostic SQL-to-NoSQL ETL tool with interactive TUI.
 */
@Command(
        name = "to-db-converter",
        mixinStandardHelpOptions = true,
        version = "1.0.0",
        description = "Convert data from relational databases (JDBC) to MongoDB",
        subcommands = {
                RunCommand.class,
                WizardCommand.class,
                ValidateCommand.class
        }
)
public class Main implements Runnable {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        // No arguments provided - show brief help
        System.out.println();
        System.out.println("TO_DB Converter - SQL to MongoDB Migration Tool");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  to-db-converter --wizard          Launch interactive configuration wizard");
        System.out.println("  to-db-converter --run             Run conversion using config file");
        System.out.println("  to-db-converter --run --config X  Run conversion using specified config file");
        System.out.println("  to-db-converter --validate        Test database connections");
        System.out.println("  to-db-converter --help            Show this help message");
        System.out.println("  to-db-converter --version         Show version");
        System.out.println();
        System.out.println("Default config file: db-converter.properties");
        System.out.println();
    }
}
