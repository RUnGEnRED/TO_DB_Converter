package com.todbconverter.core.model;

/**
 * Cardinality of relationships between database tables.
 */
public enum Cardinality {
    /**
     * One-to-One: Each row in table A relates to exactly one row in table B.
     * Detected by UNIQUE constraint on FK column.
     * Example: employees -> employee_details (one employee has one detail record)
     */
    ONE_TO_ONE,

    /**
     * One-to-Many: One row in table A relates to many rows in table B.
     * No UNIQUE constraint on FK column.
     * Example: departments -> employees (one department has many employees)
     */
    ONE_TO_MANY,

    /**
     * Many-to-Many: Many rows in table A relate to many rows in table B.
     * Implemented via junction table with at least two FKs.
     * Example: employees <-> projects (via employee_projects junction table)
     */
    MANY_TO_MANY
}
