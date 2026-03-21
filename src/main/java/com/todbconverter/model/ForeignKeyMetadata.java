package com.todbconverter.model;

public class ForeignKeyMetadata {
    private String foreignKeyName;
    private String columnName;
    private String referencedTable;
    private String referencedColumn;

    public ForeignKeyMetadata() {
    }

    public String getForeignKeyName() {
        return foreignKeyName;
    }

    public void setForeignKeyName(String foreignKeyName) {
        this.foreignKeyName = foreignKeyName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    public String getReferencedColumn() {
        return referencedColumn;
    }

    public void setReferencedColumn(String referencedColumn) {
        this.referencedColumn = referencedColumn;
    }

    @Override
    public String toString() {
        return "ForeignKeyMetadata{" +
                "foreignKeyName='" + foreignKeyName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", referencedTable='" + referencedTable + '\'' +
                ", referencedColumn='" + referencedColumn + '\'' +
                '}';
    }
}
