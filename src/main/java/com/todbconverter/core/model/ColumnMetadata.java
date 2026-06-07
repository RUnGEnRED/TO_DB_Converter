package com.todbconverter.core.model;

/**
 * Metadata for a single database column.
 */
public class ColumnMetadata {

    private final String name;
    private final String typeName;
    private final int sqlType;
    private final boolean primaryKey;
    private final boolean nullable;
    private final boolean autoIncrement;

    public ColumnMetadata(String name, String typeName, int sqlType,
                          boolean primaryKey, boolean nullable, boolean autoIncrement) {
        this.name = name;
        this.typeName = typeName;
        this.sqlType = sqlType;
        this.primaryKey = primaryKey;
        this.nullable = nullable;
        this.autoIncrement = autoIncrement;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "name='" + name + '\'' +
                ", type='" + typeName + '\'' +
                ", pk=" + primaryKey +
                ", nullable=" + nullable +
                '}';
    }
}
