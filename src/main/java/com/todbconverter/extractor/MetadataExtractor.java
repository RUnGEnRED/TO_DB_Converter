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
                ColumnMetadata column = new ColumnMetadata();
                column.setColumnName(rs.getString("COLUMN_NAME"));
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
            if (rs.next()) {
                String pkColumn = rs.getString("COLUMN_NAME");
                table.setPrimaryKeyColumn(pkColumn);

                for (ColumnMetadata column : table.getColumns()) {
                    if (column.getColumnName().equals(pkColumn)) {
                        column.setPrimaryKey(true);
                        break;
                    }
                }
            }
        }
    }

    private void extractForeignKeys(TableMetadata table, String schema) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getImportedKeys(null, schema, table.getTableName())) {
            while (rs.next()) {
                ForeignKeyMetadata fk = new ForeignKeyMetadata();
                fk.setForeignKeyName(rs.getString("FK_NAME"));
                fk.setColumnName(rs.getString("FKCOLUMN_NAME"));
                fk.setReferencedTable(rs.getString("PKTABLE_NAME"));
                fk.setReferencedColumn(rs.getString("PKCOLUMN_NAME"));

                table.addForeignKey(fk);

                for (ColumnMetadata column : table.getColumns()) {
                    if (column.getColumnName().equals(fk.getColumnName())) {
                        column.setForeignKey(true);
                        break;
                    }
                }
            }
        }
    }
}
