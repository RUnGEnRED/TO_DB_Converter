package com.todbconverter.exception;

/**
 * Exception thrown when configuration loading or validation fails.
 */
public class ConfigException extends ConverterException {

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
