package com.todbconverter.exception;

/**
 * Exception thrown when database connection operations fail.
 * Provides specific error messages for connection failures.
 */
public class ConnectionException extends Exception {
    
    public ConnectionException(String message) {
        super(message);
    }
    
    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
