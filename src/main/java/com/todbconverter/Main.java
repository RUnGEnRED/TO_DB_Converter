package com.todbconverter;

import com.todbconverter.cli.commands.ReportCommand;
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
                ValidateCommand.class,
                ReportCommand.class
        },
        footer = {
                "",
                "Options per command:",
                "  run / -r           Migration ETL pipeline",
                "                       --config <path>     Default: db-converter.properties",
                "  wizard / -w       Interactive config wizard (no options)",
                "  validate / -v     Test connections to both databases",
                "                       --config <path>     Default: db-converter.properties",
                "  report / -rp      HTML report comparing source vs target",
                "                       --config <path>     Default: db-converter.properties",
                "                       --output/-o <path>  Default: report-<ts>.html",
                "                       --samples/-s <n>    Default: 5",
                "",
                "Examples:",
                "  to-db-converter --run",
                "  to-db-converter --run --config my-config.properties",
                "  to-db-converter --wizard",
                "  to-db-converter --validate --config custom.properties",
                "  to-db-converter --report",
                "  to-db-converter --report --samples 10 --output comparison.html"
        }
)
public class Main implements Runnable {

    public static void main(String[] args) {
        CommandLine cmd = new CommandLine(new Main());
        cmd.setParameterExceptionHandler((ex, argv) -> {
            System.err.println("Error: " + ex.getMessage());
            System.err.println("Use 'to-db-converter --help' for usage information.");
            return 2;
        });
        int exitCode = cmd.execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        System.out.println("TO_DB Converter - SQL to MongoDB Migration Tool");
        System.out.println("Use 'to-db-converter --help' for usage information.");
    }
}
