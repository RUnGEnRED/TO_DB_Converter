package com.todbconverter;

import com.todbconverter.config.ConfigWizard;
import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (hasFlag(args, "--wizard")) {
            runWizard();
            return;
        }

        logger.info("=== TO DB Converter - Start ===");

        ConverterService converterService = null;
        try {
            DatabaseConfig config = loadConfig();

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

    private static DatabaseConfig loadConfig() throws Exception {
        // Try CWD first, then classpath
        java.io.File cwdFile = new java.io.File("application.properties");
        if (cwdFile.exists()) {
            Properties props = new Properties();
            try (InputStream is = new FileInputStream(cwdFile)) {
                props.load(is);
            }
            return new DatabaseConfig(props);
        }
        return new DatabaseConfig("application.properties");
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equals(arg)) return true;
        }
        return false;
    }

    private static void runWizard() {
        DatabaseConfig config = new DatabaseConfig();
        try {
            java.util.Properties props = new java.util.Properties();
            try (java.io.FileInputStream fis = new java.io.FileInputStream("application.properties")) {
                props.load(fis);
            } catch (java.io.IOException e) {
                // No existing config
            }
            config.getProperties().putAll(props);
        } catch (Exception e) {
            // Ignore
        }
        ConfigWizard wizard = new ConfigWizard(config);
        wizard.run();
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
