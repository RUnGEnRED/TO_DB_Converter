package com.todbconverter.core.model;

/**
 * Classification of database tables based on their role in relationships.
 */
public enum TableType {
    /**
     * Table with no foreign keys, or FKs only point to static lookup tables.
     * Example: departments, categories
     */
    PRIMARY_ENTITY,

    /**
     * Table holding at least one FK pointing to another table.
     * Example: employees (has department_id FK)
     */
    CHILD_ENTITY,

    /**
     * Table holding at least two FKs pointing to different tables.
     * Used for many-to-many relationships.
     * Example: employee_projects (has employee_id and project_id FKs)
     */
    JUNCTION_TABLE
}
