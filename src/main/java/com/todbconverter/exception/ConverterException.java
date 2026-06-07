package com.todbconverter.exception;

/**
 * Base exception for all converter-related errors.
 */
public abstract class ConverterException extends Exception {

    public ConverterException(String message) {
        super(message);
    }

    public ConverterException(String message, Throwable cause) {
        super(message, cause);
    }
}
