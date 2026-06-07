package com.todbconverter.exception;

/**
 * Exception thrown when a design pattern configuration is invalid.
 */
public class PatternException extends ConverterException {

    public PatternException(String message) {
        super(message);
    }

    public PatternException(String message, Throwable cause) {
        super(message, cause);
    }
}
