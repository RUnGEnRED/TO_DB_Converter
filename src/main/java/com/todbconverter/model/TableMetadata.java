package com.todbconverter.model;

import java.util.ArrayList;
import java.util.List;

public class TableMetadata {
    private String tableName;
    private String schema;
    private List<ColumnMetadata> columns;
    private List<ForeignKeyMetadata> foreignKeys;
    private String primaryKeyColumn;
    private List<String> primaryKeyColumns;

    public TableMetadata() {
        this.columns = new ArrayList<>();
        this.foreignKeys = new ArrayList<>();
    }

    public TableMetadata(String tableName, String schema) {
        this();
        this.tableName = tableName;
        this.schema = schema;
    }

    public TableMetadata(String tableName) {
        this();
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetadata> columns) {
        this.columns = columns;
    }

    public void addColumn(ColumnMetadata column) {
        this.columns.add(column);
    }

    public List<ForeignKeyMetadata> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(List<ForeignKeyMetadata> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public void addForeignKey(ForeignKeyMetadata foreignKey) {
        this.foreignKeys.add(foreignKey);
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public void addPrimaryKeyColumn(String pkColumn) {
        if (this.primaryKeyColumns == null) {
            this.primaryKeyColumns = new ArrayList<>();
        }
        this.primaryKeyColumns.add(pkColumn);
    }

    public String[] getPrimaryKeyColumnArray() {
        if (primaryKeyColumns == null || primaryKeyColumns.isEmpty()) {
            return primaryKeyColumn != null ? new String[]{primaryKeyColumn} : new String[0];
        }
        return primaryKeyColumns.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "tableName='" + tableName + '\'' +
                ", schema='" + schema + '\'' +
                ", columns=" + columns.size() +
                ", foreignKeys=" + foreignKeys.size() +
                ", primaryKey='" + primaryKeyColumn + '\'' +
                '}';
    }
}
