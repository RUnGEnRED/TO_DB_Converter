package com.todbconverter.util;

import java.sql.Timestamp;
import java.time.*;
import java.util.Date;

/**
 * Utility class for date/time conversion between JDBC and MongoDB formats.
 */
public final class DateUtils {

    private DateUtils() {
        // Utility class - no instantiation
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Convert java.sql.Timestamp to java.util.Date for MongoDB.
     */
    public static Date toDate(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        return new Date(timestamp.getTime());
    }

    /**
     * Convert java.time.LocalDateTime to java.util.Date.
     */
    public static Date toDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Convert java.time.LocalDate to java.util.Date.
     */
    public static Date toDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return Date.from(localDate.atStartOfDay(ZoneOffset.UTC).toInstant());
    }

    /**
     * Convert java.time.Instant to java.util.Date.
     */
    public static Date toDate(Instant instant) {
        if (instant == null) {
            return null;
        }
        return Date.from(instant);
    }

    /**
     * Smart conversion: handles various types from JDBC.
     */
    public static Object convertTemporal(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Timestamp timestamp) {
            return toDate(timestamp);
        } else if (value instanceof LocalDateTime localDateTime) {
            return toDate(localDateTime);
        } else if (value instanceof LocalDate localDate) {
            return toDate(localDate);
        } else if (value instanceof Instant instant) {
            return toDate(instant);
        } else if (value instanceof java.sql.Date sqlDate) {
            // java.sql.Date extends java.util.Date but its millis represent
            // midnight in the JVM's default zone. Convert via LocalDate
            // to store at UTC midnight and avoid date shifts in MongoDB.
            return toDate(sqlDate.toLocalDate());
        } else if (value instanceof Date date) {
            return date;
        }

        // Handle PostgreSQL array types (PgArray)
        if (value.getClass().getName().contains("PgArray")) {
            try {
                java.lang.reflect.Method getArray = value.getClass().getMethod("getArray");
                Object array = getArray.invoke(value);
                if (array != null && array.getClass().isArray()) {
                    int length = java.lang.reflect.Array.getLength(array);
                    java.util.List<Object> list = new java.util.ArrayList<>(length);
                    for (int i = 0; i < length; i++) {
                        list.add(java.lang.reflect.Array.get(array, i));
                    }
                    return list;
                }
            } catch (Exception e) {
                return value.toString();
            }
        }

        // Handle PostgreSQL JSONB types (PGobject)
        if (value.getClass().getName().contains("PGobject")) {
            try {
                java.lang.reflect.Method getValue = value.getClass().getMethod("getValue");
                Object val = getValue.invoke(value);
                if (val instanceof String str) {
                    // Try to parse as JSON using Jackson
                    try {
                        return JSON_MAPPER.readValue(str, Object.class);
                    } catch (Exception e) {
                        return str;
                    }
                }
            } catch (Exception e) {
                return value.toString();
            }
        }

        // Handle byte arrays (BYTEA)
        if (value instanceof byte[] bytes) {
            return bytes;
        }

        // Return as-is if not a recognized type
        return value;
    }
}
