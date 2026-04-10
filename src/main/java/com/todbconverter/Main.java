package com.todbconverter;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("=== TO DB Converter - Start ===");

        ConverterService converterService = null;
        try {
            DatabaseConfig config = new DatabaseConfig("application.properties");
            
            String direction = parseDirectionArg(args);
            if (direction != null) {
                logger.info("Overriding conversion direction from CLI: {}", direction);
            }
            
            converterService = new ConverterService(config, direction);
            converterService.convert();

            logger.info("=== TO DB Converter - Finished Successfully ===");
        } catch (Exception e) {
            logger.error("Conversion failed", e);
            System.exit(1);
        } finally {
            if (converterService != null) {
                converterService.close();
            }
        }
    }

    private static String parseDirectionArg(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--direction".equals(args[i]) || "-d".equals(args[i])) {
                return args[i + 1];
            }
        }
        if (args.length > 0 && !args[0].startsWith("-")) {
            return args[0];
        }
        return null;
    }
}
