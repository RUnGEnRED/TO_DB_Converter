package com.todbconverter.exception;

/**
 * Exception thrown when connection to target MongoDB fails.
 */
public class TargetConnectionException extends ConnectionException {

    public TargetConnectionException(String message) {
        super(message);
    }

    public TargetConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
