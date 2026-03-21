package com.todbconverter;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("=== TO DB Converter - Start ===");

        try {
            DatabaseConfig config = new DatabaseConfig("application.properties");
            
            ConverterService converterService = new ConverterService(config);
            converterService.convert();
            converterService.close();

            logger.info("=== TO DB Converter - Finished Successfully ===");
        } catch (Exception e) {
            logger.error("Conversion failed", e);
            System.exit(1);
        }
    }
}
