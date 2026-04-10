package com.todbconverter.extractor;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
                tables.add(table);
                logger.debug("Extracted metadata for table: {}", tableName);
            }
        }

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

    public void detectManyToManyRelationships(List<TableMetadata> tables) {
        logger.info("Detecting many-to-many relationships between tables");

        for (TableMetadata table : tables) {
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                String referencedTableName = fk.getReferencedTable();
                TableMetadata referencedTable = findTableByName(tables, referencedTableName);

                if (referencedTable != null && hasForeignKeyTo(referencedTable, table.getTableName())) {
                    fk.setRelationshipType(ForeignKeyMetadata.RelationshipType.MANY_TO_MANY);
                    logger.info("Detected many-to-many relationship: {} <-> {}",
                            table.getTableName(), referencedTableName);
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
