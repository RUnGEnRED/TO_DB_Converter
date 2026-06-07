package com.todbconverter.core.extractor;

import com.todbconverter.core.model.*;
import com.todbconverter.exception.SchemaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Extracts database schema from any JDBC-compliant database.
 * Uses standard DatabaseMetaData for database engine agnosticism.
 */
public class JDBCSchemaExtractor {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSchemaExtractor.class);

    /**
     * Extract complete schema from the database and build a SchemaGraph.
     *
     * @param connection active JDBC connection
     * @return SchemaGraph representing all tables and relationships
     * @throws SchemaException if schema extraction fails
     */
    public SchemaGraph extractSchema(Connection connection) throws SchemaException {
        SchemaGraph graph = new SchemaGraph();

        try {
            // 1. Get all table names
            List<String> tableNames = getTableNames(connection);
            logger.info("Found {} tables in database", tableNames.size());

            // 2. For each table: extract columns, PKs, FKs
            Map<String, List<ForeignKeyMetadata>> allForeignKeys = new HashMap<>();

            for (String tableName : tableNames) {
                List<ColumnMetadata> columns = getColumns(connection, tableName);
                List<ForeignKeyMetadata> foreignKeys = getForeignKeys(connection, tableName);
                long rowCount = getRowCount(connection, tableName);

                allForeignKeys.put(tableName, foreignKeys);

                // Classify table type
                TableType tableType = classifyTable(foreignKeys);

                TableMetadata table = TableMetadata.builder()
                        .name(tableName)
                        .tableType(tableType)
                        .addAllColumns(columns)
                        .addAllForeignKeys(foreignKeys)
                        .rowCount(rowCount)
                        .build();

                graph.addTable(table);
                logger.debug("Table: {} ({}, {} columns, {} FKs, {} rows)",
                        tableName, tableType, columns.size(), foreignKeys.size(), rowCount);
            }

            // 3. Determine cardinality for each FK and add edges to graph
            for (Map.Entry<String, List<ForeignKeyMetadata>> entry : allForeignKeys.entrySet()) {
                for (ForeignKeyMetadata fk : entry.getValue()) {
                    Cardinality cardinality = determineCardinality(connection, fk);
                    ForeignKeyMetadata updatedFk = new ForeignKeyMetadata(
                            fk.getFkTableName(), fk.getFkColumnName(),
                            fk.getPkTableName(), fk.getPkColumnName(),
                            cardinality
                    );
                    graph.addEdge(updatedFk);
                    logger.debug("Edge: {} -> {} ({})", fk.getFkTableName(),
                            fk.getPkTableName(), cardinality);
                }
            }

            logger.info("Schema extraction complete: {} tables, {} edges",
                    graph.getTables().size(), countEdges(graph));

            return graph;

        } catch (SQLException e) {
            throw new SchemaException("Failed to extract database schema: " + e.getMessage(), e);
        }
    }

    /**
     * Get all user table names from the database.
     */
    private List<String> getTableNames(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData meta = connection.getMetaData();

        // Get tables (exclude system tables like pg_*, information_schema, etc.)
        try (ResultSet rs = meta.getTables(connection.getCatalog(), connection.getSchema(),
                "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                // Skip system tables
                if (!tableName.startsWith("pg_") && !tableName.startsWith("sql_")
                        && !tableName.equals("system_columns")) {
                    tables.add(tableName);
                }
            }
        }

        return tables;
    }

    /**
     * Get column metadata for a table.
     */
    private List<ColumnMetadata> getColumns(Connection connection, String tableName) throws SQLException {
        List<ColumnMetadata> columns = new ArrayList<>();
        DatabaseMetaData meta = connection.getMetaData();

        // Get columns
        Map<String, Boolean> nullableMap = new HashMap<>();
        Map<String, Boolean> autoIncrementMap = new HashMap<>();
        Map<String, String> typeMap = new HashMap<>();

        try (ResultSet rs = meta.getColumns(connection.getCatalog(), connection.getSchema(),
                tableName, "%")) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                nullableMap.put(colName, "YES".equals(rs.getString("IS_NULLABLE")));
                autoIncrementMap.put(colName, "YES".equals(rs.getString("IS_AUTOINCREMENT")));
                typeMap.put(colName, rs.getString("TYPE_NAME"));
            }
        }

        // Get primary keys
        Set<String> primaryKeys = new HashSet<>();
        try (ResultSet rs = meta.getPrimaryKeys(connection.getCatalog(), connection.getSchema(),
                tableName)) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        }

        // Build column list (in order)
        try (ResultSet rs = meta.getColumns(connection.getCatalog(), connection.getSchema(),
                tableName, "%")) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                String typeName = typeMap.getOrDefault(colName, "UNKNOWN");
                boolean isPk = primaryKeys.contains(colName);
                boolean isNullable = nullableMap.getOrDefault(colName, true);
                boolean isAutoIncrement = autoIncrementMap.getOrDefault(colName, false);

                columns.add(new ColumnMetadata(colName, typeName, sqlType, isPk, isNullable, isAutoIncrement));
            }
        }

        return columns;
    }

    /**
     * Get foreign keys for a table.
     */
    private List<ForeignKeyMetadata> getForeignKeys(Connection connection, String tableName) throws SQLException {
        List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
        DatabaseMetaData meta = connection.getMetaData();

        try (ResultSet rs = meta.getImportedKeys(connection.getCatalog(), connection.getSchema(),
                tableName)) {
            while (rs.next()) {
                String fkTable = rs.getString("FKTABLE_NAME");
                String fkColumn = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkColumn = rs.getString("PKCOLUMN_NAME");

                // Temporary cardinality - will be determined later
                foreignKeys.add(new ForeignKeyMetadata(fkTable, fkColumn, pkTable, pkColumn,
                        Cardinality.ONE_TO_MANY));
            }
        }

        return foreignKeys;
    }

    /**
     * Checks if the FK column has a UNIQUE constraint (1:1) or not (1:N).
     * IMPORTANT: getIndexInfo returns one row per column of a composite index.
     * A column being part of a composite unique index does NOT make it unique alone.
     * We must group by INDEX_NAME and check that the index is single-column.
     */
    private Cardinality determineCardinality(Connection connection, ForeignKeyMetadata fk) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();

        // Group unique index columns by INDEX_NAME to distinguish single-column
        // unique constraints from composite unique constraints (e.g. composite PK).
        java.util.Map<String, java.util.List<String>> uniqueIndexColumns = new java.util.HashMap<>();
        try (ResultSet rs = meta.getIndexInfo(connection.getCatalog(), connection.getSchema(),
                fk.getFkTableName(), true, false)) {
            while (rs.next()) {
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                if (nonUnique) continue;
                String indexName = rs.getString("INDEX_NAME");
                String indexColumnName = rs.getString("COLUMN_NAME");
                if (indexName == null || indexColumnName == null) continue;
                uniqueIndexColumns.computeIfAbsent(indexName, k -> new java.util.ArrayList<>())
                        .add(indexColumnName);
            }
        }

        // A single-column unique index on the FK column -> ONE_TO_ONE
        for (java.util.List<String> cols : uniqueIndexColumns.values()) {
            if (cols.size() == 1 && fk.getFkColumnName().equals(cols.get(0))) {
                return Cardinality.ONE_TO_ONE;
            }
        }

        // Also check if the FK column is the SOLE primary key column
        // (composite PK does not make individual columns unique)
        java.util.List<String> pkColumns = new java.util.ArrayList<>();
        try (ResultSet rs = meta.getPrimaryKeys(connection.getCatalog(), connection.getSchema(),
                fk.getFkTableName())) {
            while (rs.next()) {
                String pkColumn = rs.getString("COLUMN_NAME");
                if (pkColumn != null) pkColumns.add(pkColumn);
            }
        }
        if (pkColumns.size() == 1 && fk.getFkColumnName().equals(pkColumns.get(0))) {
            return Cardinality.ONE_TO_ONE;
        }

        // No UNIQUE constraint (single-column) found -> ONE_TO_MANY
        return Cardinality.ONE_TO_MANY;
    }

    /**
     * Get the approximate row count for a table.
     */
    private long getRowCount(Connection connection, String tableName) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            // Ignore errors - some views may not support COUNT(*)
            return 0;
        }
        return 0;
    }

    /**
     * Classify a table based on its foreign keys.
     */
    private TableType classifyTable(List<ForeignKeyMetadata> foreignKeys) {
        if (foreignKeys.isEmpty()) {
            return TableType.PRIMARY_ENTITY;
        }

        // Check if this is a junction table (has 2+ FKs to different tables)
        Set<String> referencedTables = new HashSet<>();
        for (ForeignKeyMetadata fk : foreignKeys) {
            referencedTables.add(fk.getPkTableName());
        }

        if (referencedTables.size() >= 2) {
            // Could be a junction table - check if it has payload columns
            // For now, classify as JUNCTION_TABLE if it has 2+ FKs
            return TableType.JUNCTION_TABLE;
        }

        return TableType.CHILD_ENTITY;
    }

    /**
     * Count total edges in the graph.
     */
    private int countEdges(SchemaGraph graph) {
        int count = 0;
        for (TableMetadata table : graph.getTables()) {
            count += table.getForeignKeys().size();
        }
        return count;
    }
}
