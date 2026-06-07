package com.todbconverter.core.transformer.patterns;

import java.util.Map;

/**
 * Interface for applying NoSQL design patterns during transformation.
 */
public interface PatternApplier {

    /**
     * Get the pattern type name.
     */
    String getPatternType();

    /**
     * Apply the pattern to a document.
     *
     * @param document the parent document being transformed
     * @param context  pattern-specific configuration and data
     */
    void apply(Map<String, Object> document, Map<String, Object> context);
}
