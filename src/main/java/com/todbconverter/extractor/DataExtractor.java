package com.todbconverter.extractor;

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
        List<Map<String, Object>> records = new ArrayList<>();
        String query = buildQuery(table);

        logger.debug("Executing query: {}", query);

        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                table.getColumns().forEach(column -> {
                    try {
                        record.put(column.getColumnName(), rs.getObject(column.getColumnName()));
                    } catch (SQLException e) {
                        logger.error("Error extracting column: {}", column.getColumnName(), e);
                    }
                });
                records.add(record);
            }
        }

        logger.info("Extracted {} records from table {}", records.size(), table.getTableName());
        return records;
    }

    private String buildQuery(TableMetadata table) {
        String schemaPrefix = table.getSchema() != null ? table.getSchema() + "." : "";
        return "SELECT * FROM " + schemaPrefix + table.getTableName();
    }
}
