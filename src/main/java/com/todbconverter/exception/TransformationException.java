package com.todbconverter.exception;

/**
 * Exception thrown when data transformation fails.
 */
public class TransformationException extends ConverterException {

    public TransformationException(String message) {
        super(message);
    }

    public TransformationException(String message, Throwable cause) {
        super(message, cause);
    }
}
