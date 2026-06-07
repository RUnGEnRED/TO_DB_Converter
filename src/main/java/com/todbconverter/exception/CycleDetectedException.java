package com.todbconverter.exception;

/**
 * Exception thrown when an unresolvable circular dependency is detected.
 * <p>
 * Note: The system attempts to auto-fix cycles by converting edges to REFERENCE.
 * This exception is only thrown when the cycle cannot be automatically resolved.
 */
public class CycleDetectedException extends TransformationException {

    private final String cyclePath;

    public CycleDetectedException(String message, String cyclePath) {
        super(message);
        this.cyclePath = cyclePath;
    }

    public CycleDetectedException(String message, String cyclePath, Throwable cause) {
        super(message, cause);
        this.cyclePath = cyclePath;
    }

    /**
     * @return The detected cycle path (e.g., "A -> B -> C -> A")
     */
    public String getCyclePath() {
        return cyclePath;
    }
}
