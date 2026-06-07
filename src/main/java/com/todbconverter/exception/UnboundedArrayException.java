package com.todbconverter.exception;

/**
 * Exception thrown when an unbounded array is detected that would exceed MongoDB's 16MB limit.
 */
public class UnboundedArrayException extends TransformationException {

    private final String tableName;
    private final int childCount;
    private final int threshold;

    public UnboundedArrayException(String tableName, int childCount, int threshold) {
        super(String.format(
                "Table '%s' has a parent with %d children (threshold: %d). " +
                "Strategy auto-downgraded to REFERENCE to prevent 16MB document limit.",
                tableName, childCount, threshold));
        this.tableName = tableName;
        this.childCount = childCount;
        this.threshold = threshold;
    }

    public String getTableName() {
        return tableName;
    }

    public int getChildCount() {
        return childCount;
    }

    public int getThreshold() {
        return threshold;
    }
}
