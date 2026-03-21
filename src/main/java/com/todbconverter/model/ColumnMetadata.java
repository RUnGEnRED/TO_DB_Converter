package com.todbconverter.model;

public class ColumnMetadata {
    private String columnName;
    private String dataType;
    private int columnSize;
    private boolean nullable;
    private boolean isPrimaryKey;
    private boolean isForeignKey;

    public ColumnMetadata() {
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public boolean isForeignKey() {
        return isForeignKey;
    }

    public void setForeignKey(boolean foreignKey) {
        isForeignKey = foreignKey;
    }

    @Override
    public String toString() {
        return "ColumnMetadata{" +
                "columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isForeignKey=" + isForeignKey +
                '}';
    }
}
