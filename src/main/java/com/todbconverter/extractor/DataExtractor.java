package com.todbconverter.extractor;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractor.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private final Connection connection;

    public DataExtractor(Connection connection) {
        this.connection = connection;
    }

    public List<Map<String, Object>> extractData(TableMetadata table) throws SQLException {
        List<Map<String, Object>> allRecords = new ArrayList<>();
        extractDataInBatches(table, 1000, allRecords::addAll);
        return allRecords;
    }

    public void extractDataInBatches(TableMetadata table, int batchSize, java.util.function.Consumer<List<Map<String, Object>>> batchConsumer) throws SQLException {
        String query = buildQuery(table);
        logger.debug("Executing batch query: {}", query);

        if (table.getColumns() == null || table.getColumns().isEmpty()) {
            throw new SQLException("No columns defined in table metadata: " + table.getTableName());
        }

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            // Set fetch size to prevent JDBC driver from loading everything at once
            stmt.setFetchSize(batchSize);
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<Map<String, Object>> batch = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> record = new HashMap<>();
                    for (ColumnMetadata column : table.getColumns()) {
                        Object value = rs.getObject(column.getColumnName());
                        if (value != null) {
                            String dataType = column.getDataType();
                            if (dataType != null && (dataType.equalsIgnoreCase("JSON") || dataType.equalsIgnoreCase("JSONB"))) {
                                // Parse JSON/JSONB to Map for native MongoDB storage
                                String jsonStr = value.toString();
                                try {
                                    value = JSON_MAPPER.readValue(jsonStr, Map.class);
                                } catch (Exception e) {
                                    logger.debug("Failed to parse JSON/JSONB column '{}', keeping as string: {}", 
                                            column.getColumnName(), e.getMessage());
                                    value = jsonStr;
                                }
                            } else if (value.getClass().getName().contains("PGobject")) {
                                // Handle PGobject (JSON/JSONB from PostgreSQL)
                                String pgStr = value.toString();
                                if (dataType != null && (dataType.equalsIgnoreCase("JSON") || dataType.equalsIgnoreCase("JSONB"))) {
                                    try {
                                        value = JSON_MAPPER.readValue(pgStr, Map.class);
                                    } catch (Exception e) {
                                        value = pgStr;
                                    }
                                } else {
                                    value = pgStr;
                                }
                            } else if (value instanceof Array) {
                                // Convert SQL Array to Java List
                                Array sqlArray = (Array) value;
                                Object arrayElements = sqlArray.getArray();
                                if (arrayElements instanceof Object[]) {
                                    value = List.of((Object[]) arrayElements);
                                } else if (arrayElements instanceof String[]) {
                                    value = List.of((String[]) arrayElements);
                                } else if (arrayElements != null) {
                                    value = arrayElements;
                                }
                            } else if (value.getClass().getName().contains("PgArray") || (dataType != null && dataType.startsWith("_"))) {
                                // Handle PostgreSQL array types (TEXT[], INTEGER[], etc.)
                                if (value instanceof Array) {
                                    Array sqlArray = (Array) value;
                                    Object arrayElements = sqlArray.getArray();
                                    if (arrayElements instanceof Object[]) {
                                        value = List.of((Object[]) arrayElements);
                                    } else if (arrayElements instanceof String[]) {
                                        value = List.of((String[]) arrayElements);
                                    } else if (arrayElements != null) {
                                        value = arrayElements;
                                    }
                                } else {
                                    // Fallback: try to parse PostgreSQL array string format {a,b,c}
                                    String arrayStr = value.toString();
                                    if (arrayStr.startsWith("{") && arrayStr.endsWith("}")) {
                                        String content = arrayStr.substring(1, arrayStr.length() - 1);
                                        if (!content.isEmpty()) {
                                            String[] elements = content.split(",");
                                            List<String> list = new ArrayList<>();
                                            for (String elem : elements) {
                                                list.add(elem.trim());
                                            }
                                            value = list;
                                        } else {
                                            value = new ArrayList<>();
                                        }
                                    }
                                }
                            }
                        }
                        record.put(column.getColumnName(), value);
                    }
                    batch.add(record);
                    
                    if (batch.size() >= batchSize) {
                        batchConsumer.accept(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    batchConsumer.accept(batch);
                }
            }
        }
    }

    private String buildQuery(TableMetadata table) {
        StringBuilder sb = new StringBuilder("SELECT ");
        List<ColumnMetadata> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            sb.append("\"").append(columns.get(i).getColumnName()).append("\"");
            if (i < columns.size() - 1) sb.append(", ");
        }
        String schemaPrefix = table.getSchema() != null ? "\"" + table.getSchema() + "\"." : "";
        sb.append(" FROM ").append(schemaPrefix).append("\"").append(table.getTableName()).append("\"");
        return sb.toString();
    }
}
