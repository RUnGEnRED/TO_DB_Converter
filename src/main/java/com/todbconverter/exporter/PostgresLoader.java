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
    private static final Logger logger = LoggerFactory.getLogger(PostgresLoader.class);
    private final Connection connection;

    public PostgresLoader(Connection connection) {
        this.connection = connection;
    }

    public void createTable(TableMetadata table) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(table.getSchema()).append(".").append(table.getTableName()).append(" (\n");

        List<ColumnMetadata> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata col = columns.get(i);
            sql.append("    ").append(col.getColumnName()).append(" ");

            String dataType = col.getDataType();
            if ("VARCHAR".equalsIgnoreCase(dataType)) {
                sql.append("VARCHAR(").append(col.getColumnSize() > 0 ? col.getColumnSize() : 255).append(")");
            } else {
                sql.append(dataType);
            }

            if (col.isPrimaryKey()) {
                sql.append(" PRIMARY KEY");
            }

            if (i < columns.size() - 1) {
                sql.append(",\n");
            }
        }
        sql.append("\n);");

        logger.info("Creating table {}: {}", table.getTableName(), sql);

        try (Statement stmt = connection.createStatement()) {
            String dropSql = "DROP TABLE IF EXISTS " + table.getSchema() + "." + table.getTableName() + " CASCADE";
            logger.info("Dropping table if exists: {}", dropSql);
            stmt.execute(dropSql);
            stmt.execute(sql.toString());
        }
    }

    public void addForeignKeys(TableMetadata table) throws SQLException {
        for (ForeignKeyMetadata fk : table.getForeignKeys()) {
            String sql = String.format("ALTER TABLE %s.%s ADD CONSTRAINT fk_%s_%s FOREIGN KEY (%s) REFERENCES %s.%s(%s);",
                    table.getSchema(), table.getTableName(),
                    table.getTableName(), fk.getColumnName(),
                    fk.getColumnName(),
                    table.getSchema(), fk.getReferencedTable(), fk.getReferencedColumn());
            
            logger.info("Adding foreign key: {}", sql);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            } catch (SQLException e) {
                logger.warn("Could not add foreign key (may already exist): {}", e.getMessage());
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
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(table.getSchema()).append(".").append(table.getTableName()).append(" (");

        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sql.append(columns.get(i).getColumnName());
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
                    if (value instanceof java.util.Date) {
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
}
