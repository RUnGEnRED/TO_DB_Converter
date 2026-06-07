package com.todbconverter.cli.commands;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.core.service.ETLPipeline;
import com.todbconverter.exception.ConverterException;
import com.todbconverter.ui.TerminalRenderer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

/**
 * Command to run the ETL conversion.
 */
@Command(name = "run", aliases = {"--run", "-r"}, description = "Run the database conversion")
public class RunCommand implements Callable<Integer> {

    @Option(names = "--config", description = "Path to configuration file (default: db-converter.properties)")
    private String configPath = "db-converter.properties";

    @Override
    public Integer call() {
        TerminalRenderer renderer = new TerminalRenderer();
        ETLPipeline pipeline = new ETLPipeline(renderer);

        try {
            DatabaseConfig config = DatabaseConfig.loadFromFile(configPath);
            pipeline.execute(config);
            return 0;
        } catch (ConverterException e) {
            renderer.printError(e.getMessage());
            return 1;
        } catch (Exception e) {
            renderer.printError("Unexpected error: " + e.getMessage());
            return 1;
        }
    }
}
