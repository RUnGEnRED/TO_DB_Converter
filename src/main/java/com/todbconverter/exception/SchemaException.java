package com.todbconverter.exception;

/**
 * Exception thrown when schema extraction from source database fails.
 */
public class SchemaException extends ConverterException {

    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
