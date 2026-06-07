package com.todbconverter.exception;

/**
 * Exception thrown when connection to source JDBC database fails.
 */
public class SourceConnectionException extends ConnectionException {

    public SourceConnectionException(String message) {
        super(message);
    }

    public SourceConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
