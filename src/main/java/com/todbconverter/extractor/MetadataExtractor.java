package com.todbconverter.extractor;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetadataExtractor implements IMetadataExtractor {
    private static final Logger logger = LoggerFactory.getLogger(MetadataExtractor.class);
    private final Connection connection;

    public MetadataExtractor(Connection connection) {
        this.connection = connection;
    }

    @Override
    public List<TableMetadata> extractAllTables(String schema) throws SQLException {
        List<TableMetadata> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        logger.info("Extracting tables from schema: {}", schema);

        try (ResultSet rs = metaData.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                TableMetadata table = extractTableMetadata(tableName, schema);
                if (!table.getColumns().isEmpty()) {
                    tables.add(table);
                    logger.debug("Extracted metadata for table: {}", tableName);
                } else {
                    logger.warn("Skipping table {} because it has no columns", tableName);
                }
            }
        }

        determineRelationshipTypes(tables, schema);
        detectManyToManyRelationships(tables);

        logger.info("Extracted {} tables from schema {}", tables.size(), schema);
        return tables;
    }

    @Override
    public TableMetadata extractTableMetadata(String tableName, String schema) throws SQLException {
        TableMetadata table = new TableMetadata(tableName, schema);

        extractColumns(table, schema);
        extractPrimaryKey(table, schema);
        extractForeignKeys(table, schema);

        return table;
    }

    private void extractColumns(TableMetadata table, String schema) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getColumns(null, schema, table.getTableName(), "%")) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                if (colName == null || colName.isEmpty()) continue;

                ColumnMetadata column = new ColumnMetadata();
                column.setColumnName(colName);
                column.setDataType(rs.getString("TYPE_NAME"));
                column.setColumnSize(rs.getInt("COLUMN_SIZE"));
                column.setNullable(rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable);

                table.addColumn(column);
            }
        }
    }

    private void extractPrimaryKey(TableMetadata table, String schema) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getPrimaryKeys(null, schema, table.getTableName())) {
            while (rs.next()) {
                String pkColumn = rs.getString("COLUMN_NAME");
                if (pkColumn == null || pkColumn.isEmpty()) continue;

                if (table.getPrimaryKeyColumn() == null) {
                    table.setPrimaryKeyColumn(pkColumn);
                }
                table.addPrimaryKeyColumn(pkColumn);

                for (ColumnMetadata column : table.getColumns()) {
                    if (column.getColumnName() != null && column.getColumnName().equalsIgnoreCase(pkColumn)) {
                        column.setPrimaryKey(true);
                    }
                }
            }
        }

        if (table.getPrimaryKeyColumn() == null && !table.getColumns().isEmpty()) {
            String firstColumn = table.getColumns().get(0).getColumnName();
            table.setPrimaryKeyColumn(firstColumn);
            table.addPrimaryKeyColumn(firstColumn);
            table.getColumns().get(0).setPrimaryKey(true);
        }
    }

    private void extractForeignKeys(TableMetadata table, String schema) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getImportedKeys(null, schema, table.getTableName())) {
            while (rs.next()) {
                String fkColName = rs.getString("FKCOLUMN_NAME");
                if (fkColName == null || fkColName.isEmpty()) continue;

                ForeignKeyMetadata fk = new ForeignKeyMetadata();
                fk.setForeignKeyName(rs.getString("FK_NAME"));
                fk.setColumnName(fkColName);
                fk.setReferencedTable(rs.getString("PKTABLE_NAME"));
                fk.setReferencedColumn(rs.getString("PKCOLUMN_NAME"));
                // Default to ONE_TO_MANY, will be refined in determineRelationshipTypes
                fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.ONE_TO_MANY);

                table.addForeignKey(fk);

                for (ColumnMetadata column : table.getColumns()) {
                    if (column.getColumnName() != null && column.getColumnName().equalsIgnoreCase(fkColName)) {
                        column.setForeignKey(true);
                    }
                }
            }
        }
    }

    private void determineRelationshipTypes(List<TableMetadata> tables, String schema) throws SQLException {
        logger.info("Determining relationship types (1:1, 1:N, M:N)...");

        for (TableMetadata table : tables) {
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                // Skip if already set to MANY_TO_MANY (will be set later)
                if (fk.getRelationshipType() == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY) {
                    continue;
                }

                boolean isUnique = isColumnUnique(table, fk.getColumnName(), schema);
                if (isUnique) {
                    fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.ONE_TO_ONE);
                    logger.debug("Detected ONE_TO_ONE: {}.{} -> {}", 
                            table.getTableName(), fk.getColumnName(), fk.getReferencedTable());
                } else {
                    fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.ONE_TO_MANY);
                }
            }
        }
    }

    private boolean isColumnUnique(TableMetadata table, String columnName, String schema) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        // Check if column is part of primary key
        for (String pkCol : table.getPrimaryKeyColumnArray()) {
            if (pkCol.equalsIgnoreCase(columnName)) {
                return true;
            }
        }

        // Check for unique indexes on this column
        try (ResultSet indexRs = metaData.getIndexInfo(null, schema, table.getTableName(), true, true)) {
            while (indexRs.next()) {
                String indexColName = indexRs.getString("COLUMN_NAME");
                boolean nonUnique = indexRs.getBoolean("NON_UNIQUE");

                if (!nonUnique && columnName.equalsIgnoreCase(indexColName)) {
                    return true;
                }
            }
        }

        return false;
    }

    public void detectManyToManyRelationships(List<TableMetadata> tables) {
        logger.info("Detecting many-to-many relationships (including junction tables)");

        for (TableMetadata table : tables) {
            // Check for bidirectional FKs (existing logic)
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                String referencedTableName = fk.getReferencedTable();
                TableMetadata referencedTable = findTableByName(tables, referencedTableName);

                if (referencedTable != null && hasForeignKeyTo(referencedTable, table.getTableName())) {
                    fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.MANY_TO_MANY);
                    logger.info("Detected many-to-many relationship (bidirectional FK): {} <-> {}",
                            table.getTableName(), referencedTableName);
                }
            }

            // Check for Junction Tables (new logic)
            // A junction table usually has exactly 2 foreign keys and potentially no other data
            List<ForeignKeyMetadata> fks = table.getForeignKeys();
            if (fks.size() == 2 && table.getColumns().size() <= 4) { // ID + 2 FKs + maybe a timestamp
                ForeignKeyMetadata fk1 = fks.get(0);
                ForeignKeyMetadata fk2 = fks.get(1);
                
                logger.info("Table '{}' identified as a junction table between '{}' and '{}'",
                        table.getTableName(), fk1.getReferencedTable(), fk2.getReferencedTable());
                
                // Mark both sides as M:N
                TableMetadata t1 = findTableByName(tables, fk1.getReferencedTable());
                TableMetadata t2 = findTableByName(tables, fk2.getReferencedTable());
                
                if (t1 != null && t2 != null) {
                    // This is handled during transformation by looking at the junction table
                }
            }
        }
    }

    private TableMetadata findTableByName(List<TableMetadata> tables, String tableName) {
        for (TableMetadata table : tables) {
            if (table.getTableName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }

    private boolean hasForeignKeyTo(TableMetadata table, String targetTableName) {
        if (table.getForeignKeys() == null) {
            return false;
        }
        for (ForeignKeyMetadata fk : table.getForeignKeys()) {
            if (fk.getReferencedTable().equalsIgnoreCase(targetTableName)) {
                return true;
            }
        }
        return false;
    }
}
