package com.todbconverter.core.model;

/**
 * Strategy for mapping relational data to MongoDB documents.
 */
public enum Strategy {
    /**
     * Embed related data directly inside the parent document.
     * - ONE_TO_ONE: Child becomes a nested subdocument
     * - ONE_TO_MANY: Children become an array of subdocuments
     * - MANY_TO_MANY: Junction data becomes array of IDs or subdocuments
     *
     * Best for: Data accessed together, bounded arrays, "contains" relationships.
     */
    EMBED,

    /**
     * Keep related data in a separate collection.
     * Parent document contains only a reference (ID) to the child.
     * Child document retains its foreign key field.
     *
     * Best for: Independent data, unbounded arrays, shared data across parents.
     */
    REFERENCE
}
