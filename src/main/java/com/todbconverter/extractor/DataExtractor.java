package com.todbconverter.extractor;

import com.todbconverter.model.ColumnMetadata;
import com.todbconverter.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractor.class);
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
                            if (dataType != null && (dataType.equals("JSON") || dataType.equals("JSONB"))) {
                                value = value.toString();
                            } else if (value.getClass().getName().contains("PGobject")) {
                                value = value.toString();
                            } else if (value.getClass().getName().contains("PgArray") || (dataType != null && dataType.startsWith("_"))) {
                                value = value.toString();
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
