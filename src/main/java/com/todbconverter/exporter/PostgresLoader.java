package com.todbconverter.exporter;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class PostgresLoader {
    public static final int DEFAULT_VARCHAR_SIZE = 255;
    private static final Logger logger = LoggerFactory.getLogger(PostgresLoader.class);
    private final Connection connection;
    private final boolean dropExistingTables;

    public PostgresLoader(Connection connection) {
        this(connection, true);
    }

    public PostgresLoader(Connection connection, boolean dropExistingTables) {
        this.connection = connection;
        this.dropExistingTables = dropExistingTables;
    }

    private String escapeIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException("Identifier cannot be null or empty");
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    public void createTable(TableMetadata table) throws SQLException {
        String schema = table.getSchema();
        String tableName = table.getTableName();
        
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(escapeIdentifier(schema)).append(".").append(escapeIdentifier(tableName)).append(" (\n");

        List<ColumnMetadata> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            sql.append("    ").append(escapeIdentifier(col.getColumnName())).append(" ");

            String dataType = col.getDataType();
            int columnSize = col.getColumnSize() > 0 ? col.getColumnSize() : DEFAULT_VARCHAR_SIZE;
            if ("VARCHAR".equalsIgnoreCase(dataType)) {
                sql.append("VARCHAR(").append(columnSize).append(")");
            } else if ("DECIMAL".equalsIgnoreCase(dataType)) {
                sql.append("DECIMAL(10, 2)");
            } else if (dataType != null && dataType.startsWith("_")) {
                sql.append("TEXT");
            } else if (dataType != null && (dataType.equalsIgnoreCase("BYTEA") || dataType.equalsIgnoreCase("BLOB"))) {
                sql.append("BYTEA");
            } else if (dataType != null && (dataType.equalsIgnoreCase("JSON") || dataType.equalsIgnoreCase("JSONB"))) {
                sql.append("TEXT");
            } else if (dataType != null && dataType.toUpperCase().contains("INT")) {
                sql.append("INTEGER");
            } else {
                sql.append(dataType != null ? dataType : "VARCHAR(255)");
            }

            if (col.isPrimaryKey()) {
                sql.append(" PRIMARY KEY");
            }

            if (i < columns.size() - 1) {
                sql.append(",\n");
            }
        }
        sql.append("\n)");

        logger.info("Creating table {}: {}", tableName, sql);

        try (Statement stmt = connection.createStatement()) {
            if (dropExistingTables) {
                String dropSql = "DROP TABLE IF EXISTS " + escapeIdentifier(schema) + "." + escapeIdentifier(tableName) + " CASCADE";
                logger.info("Dropping table if exists: {}", dropSql);
                stmt.execute(dropSql);
            }
            stmt.execute(sql.toString());
        }
    }

    public void addForeignKeys(TableMetadata table) throws SQLException {
        String schema = table.getSchema();
        String tableName = table.getTableName();
        
        for (ForeignKeyMetadata fk : table.getForeignKeys()) {
            String sql = String.format("ALTER TABLE %s.%s ADD CONSTRAINT fk_%s_%s FOREIGN KEY (%s) REFERENCES %s.%s(%s);",
                    escapeIdentifier(schema), escapeIdentifier(tableName),
                    tableName, fk.getColumnName(),
                    escapeIdentifier(fk.getColumnName()),
                    escapeIdentifier(schema), escapeIdentifier(fk.getReferencedTable()), escapeIdentifier(fk.getReferencedColumn()));
            
            logger.info("Adding foreign key: {}", sql);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            } catch (SQLException e) {
                logger.error("Failed to add foreign key for column {} in table {}: {}", 
                    fk.getColumnName(), tableName, e.getMessage());
                throw new SQLException("Failed to add foreign key constraint", e);
            }
        }
    }

    public void loadData(TableMetadata table, List<Map<String, Object>> data) throws SQLException {
        if (data.isEmpty()) return;
        if (table.getColumns().isEmpty()) {
            logger.warn("Skipping data load for {}: No columns defined in metadata", table.getTableName());
            return;
        }

        List<ColumnMetadata> columns = table.getColumns();
        String schema = table.getSchema();
        String tableName = table.getTableName();
        
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(escapeIdentifier(schema)).append(".").append(escapeIdentifier(tableName)).append(" (");

        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sql.append(escapeIdentifier(columns.get(i).getColumnName()));
            placeholders.append("?");
            if (i < columns.size() - 1) {
                sql.append(", ");
                placeholders.append(", ");
            }
        }
        sql.append(") VALUES (").append(placeholders).append(")");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            for (Map<String, Object> row : data) {
                for (int i = 0; i < columns.size(); i++) {
                    Object value = row.get(columns.get(i).getColumnName());
                    if (value instanceof java.util.Date && !(value instanceof java.sql.Date)) {
                        stmt.setTimestamp(i + 1, new java.sql.Timestamp(((java.util.Date) value).getTime()));
                    } else {
                        stmt.setObject(i + 1, value);
                    }
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            logger.info("Inserted {} records into {}", data.size(), table.getTableName());
        }
    }

    public void loadAllTables(Map<String, TableMetadata> tablesMetadata,
                              Map<String, List<Map<String, Object>>> allRelationalData) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);

            for (TableMetadata meta : tablesMetadata.values()) {
                createTable(meta);
            }

            for (Map.Entry<String, List<Map<String, Object>>> entry : allRelationalData.entrySet()) {
                TableMetadata meta = tablesMetadata.get(entry.getKey());
                if (meta == null) {
                    logger.warn("Skipping data load for {}: No table metadata found", entry.getKey());
                    continue;
                }
                loadData(meta, entry.getValue());
            }

            for (TableMetadata meta : tablesMetadata.values()) {
                addForeignKeys(meta);
            }

            connection.commit();
            logger.info("Transaction committed successfully");
        } catch (SQLException e) {
            try {
                connection.rollback();
                logger.error("Transaction rolled back due to error: {}", e.getMessage());
            } catch (SQLException rollbackEx) {
                logger.error("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            throw e;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }
}
