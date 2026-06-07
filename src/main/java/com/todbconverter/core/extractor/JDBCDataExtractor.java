package com.todbconverter.core.extractor;

import com.todbconverter.core.model.SchemaGraph;
import com.todbconverter.core.model.TableMetadata;
import com.todbconverter.exception.SchemaException;
import com.todbconverter.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * Extracts raw data from JDBC database tables.
 */
public class JDBCDataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(JDBCDataExtractor.class);

    /**
     * Extract data from all tables in the graph.
     *
     * @param connection active JDBC connection
     * @param graph      schema graph with table metadata
     * @return map of table name to list of row maps
     * @throws SchemaException if data extraction fails
     */
    public Map<String, List<Map<String, Object>>> extractAllData(
            Connection connection, SchemaGraph graph) throws SchemaException {

        Map<String, List<Map<String, Object>>> allData = new LinkedHashMap<>();

        for (TableMetadata table : graph.getTables()) {
            List<Map<String, Object>> tableData = extractTableData(connection, table.getName());
            allData.put(table.getName(), tableData);
            logger.debug("Extracted {} rows from table '{}'", tableData.size(), table.getName());
        }

        logger.info("Data extraction complete: {} tables, {} total rows",
                allData.size(), allData.values().stream().mapToLong(List::size).sum());

        return allData;
    }

    /**
     * Extract all data from a single table.
     *
     * @param connection active JDBC connection
     * @param tableName  name of the table to extract
     * @return list of row maps (column name -> value)
     * @throws SchemaException if extraction fails
     */
    public List<Map<String, Object>> extractTableData(Connection connection, String tableName)
            throws SchemaException {

        List<Map<String, Object>> rows = new ArrayList<>();

        String sql = "SELECT * FROM " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);

                    // Convert temporal types
                    value = DateUtils.convertTemporal(value);

                    row.put(columnName, value);
                }

                rows.add(row);
            }

        } catch (SQLException e) {
            throw new SchemaException("Failed to extract data from table '" + tableName + "': " + e.getMessage(), e);
        }

        return rows;
    }

    /**
     * Execute a custom SQL query and return results.
     *
     * @param connection active JDBC connection
     * @param sql        SQL query to execute
     * @return list of row maps
     * @throws SchemaException if query fails
     */
    public List<Map<String, Object>> executeQuery(Connection connection, String sql)
            throws SchemaException {

        List<Map<String, Object>> rows = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = rs.getObject(i);
                    value = DateUtils.convertTemporal(value);
                    row.put(columnName, value);
                }

                rows.add(row);
            }

        } catch (SQLException e) {
            throw new SchemaException("Failed to execute query: " + e.getMessage(), e);
        }

        return rows;
    }
}
