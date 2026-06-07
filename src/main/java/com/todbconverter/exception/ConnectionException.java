package com.todbconverter.exception;

/**
 * Exception thrown when database connection fails.
 */
public class ConnectionException extends ConverterException {

    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
