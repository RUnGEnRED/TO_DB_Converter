package com.todbconverter.core.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Metadata for a database table including columns, foreign keys, and classification.
 */
public class TableMetadata {

    private final String name;
    private final TableType tableType;
    private final List<ColumnMetadata> columns;
    private final List<ForeignKeyMetadata> foreignKeys;
    private final long rowCount;

    private TableMetadata(Builder builder) {
        this.name = builder.name;
        this.tableType = builder.tableType;
        this.columns = Collections.unmodifiableList(builder.columns);
        this.foreignKeys = Collections.unmodifiableList(builder.foreignKeys);
        this.rowCount = builder.rowCount;
    }

    public String getName() {
        return name;
    }

    public TableType getTableType() {
        return tableType;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public List<ForeignKeyMetadata> getForeignKeys() {
        return foreignKeys;
    }

    public long getRowCount() {
        return rowCount;
    }

    /**
     * Get the primary key column name.
     * @return PK column name, or null if no PK found
     */
    public String getPrimaryKeyColumn() {
        return columns.stream()
                .filter(ColumnMetadata::isPrimaryKey)
                .map(ColumnMetadata::getName)
                .findFirst()
                .orElse(null);
    }

    /**
     * Get columns that are not primary keys.
     */
    public List<ColumnMetadata> getNonPrimaryKeyColumns() {
        return columns.stream()
                .filter(c -> !c.isPrimaryKey())
                .toList();
    }

    @Override
    public String toString() {
        return "TableMetadata{" +
                "name='" + name + '\'' +
                ", type=" + tableType +
                ", columns=" + columns.size() +
                ", foreignKeys=" + foreignKeys.size() +
                ", rowCount=" + rowCount +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private TableType tableType = TableType.PRIMARY_ENTITY;
        private final List<ColumnMetadata> columns = new ArrayList<>();
        private final List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
        private long rowCount = 0;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder tableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder addColumn(ColumnMetadata column) {
            this.columns.add(column);
            return this;
        }

        public Builder addAllColumns(List<ColumnMetadata> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder addForeignKey(ForeignKeyMetadata foreignKey) {
            this.foreignKeys.add(foreignKey);
            return this;
        }

        public Builder addAllForeignKeys(List<ForeignKeyMetadata> foreignKeys) {
            this.foreignKeys.addAll(foreignKeys);
            return this;
        }

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public TableMetadata build() {
            if (name == null || name.isBlank()) {
                throw new IllegalStateException("Table name must not be null or blank");
            }
            return new TableMetadata(this);
        }
    }
}
