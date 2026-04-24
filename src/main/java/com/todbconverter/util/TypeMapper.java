package com.todbconverter.util;

import org.bson.types.Binary;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public class TypeMapper {

    public static String inferSqlType(Object value) {
        if (value == null) return "VARCHAR(255)";
        if (value instanceof Integer) return "INTEGER";
        if (value instanceof Long) return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE PRECISION";
        if (value instanceof java.math.BigDecimal) return "DECIMAL(19,4)";
        if (value instanceof Boolean) return "BOOLEAN";
        if (value instanceof java.util.Date || value instanceof java.sql.Timestamp || value instanceof java.time.LocalDateTime) return "TIMESTAMP";
        if (value instanceof java.time.LocalDate) return "DATE";
        if (value instanceof byte[] || value instanceof org.bson.types.Binary) return "BYTEA";
        return "TEXT";
    }

    public static Object convertToSqlValue(Object value) {
        if (value == null) return null;
        if (value instanceof Date && !(value instanceof java.sql.Date) && !(value instanceof Timestamp)) {
            return new Timestamp(((Date) value).getTime());
        }
        if (value instanceof Binary) {
            return ((Binary) value).getData();
        }
        return value;
    }

    public static Object convertToMongoValue(Object value, String sqlDataType) {
        if (value == null) return null;
        if (sqlDataType != null) {
            String type = sqlDataType.toUpperCase();
            if (type.equals("JSON") || type.equals("JSONB")) {
                return value.toString();
            }
        }
        // Add more specific conversions if needed
        return value;
    }
}
