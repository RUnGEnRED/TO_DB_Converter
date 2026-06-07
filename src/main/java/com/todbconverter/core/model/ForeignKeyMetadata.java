package com.todbconverter.core.model;

/**
 * Metadata for a foreign key relationship between two tables.
 */
public class ForeignKeyMetadata {

    private final String fkTableName;
    private final String fkColumnName;
    private final String pkTableName;
    private final String pkColumnName;
    private final Cardinality cardinality;

    public ForeignKeyMetadata(String fkTableName, String fkColumnName,
                              String pkTableName, String pkColumnName,
                              Cardinality cardinality) {
        this.fkTableName = fkTableName;
        this.fkColumnName = fkColumnName;
        this.pkTableName = pkTableName;
        this.pkColumnName = pkColumnName;
        this.cardinality = cardinality;
    }

    /**
     * @return Table containing the foreign key (child side)
     */
    public String getFkTableName() {
        return fkTableName;
    }

    /**
     * @return Column in child table that holds the foreign key
     */
    public String getFkColumnName() {
        return fkColumnName;
    }

    /**
     * @return Table being referenced (parent side)
     */
    public String getPkTableName() {
        return pkTableName;
    }

    /**
     * @return Primary key column in parent table
     */
    public String getPkColumnName() {
        return pkColumnName;
    }

    /**
     * @return Cardinality of this relationship
     */
    public Cardinality getCardinality() {
        return cardinality;
    }

    /**
     * @return True if this is a self-referencing relationship (table references itself)
     */
    public boolean isSelfReferencing() {
        return fkTableName.equals(pkTableName);
    }

    @Override
    public String toString() {
        return fkTableName + "." + fkColumnName + " -> " +
               pkTableName + "." + pkColumnName +
               " (" + cardinality + ")";
    }
}
