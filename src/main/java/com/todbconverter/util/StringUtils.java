package com.todbconverter.util;

/**
 * Utility class for string manipulation.
 */
public final class StringUtils {

    private StringUtils() {
        // Utility class - no instantiation
    }

    /**
     * Convert a table name to MongoDB collection name.
     * Default: lowercase, singular (remove trailing 's' if present).
     *
     * @param tableName the SQL table name
     * @return the MongoDB collection name
     */
    public static String toCollectionName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return tableName;
        }
        // Simple singularization: remove trailing 's' or 'es'
        String lower = tableName.toLowerCase();
        if (lower.endsWith("ies")) {
            return lower.substring(0, lower.length() - 3) + "y";
        } else if (lower.endsWith("ses") || lower.endsWith("xes") || lower.endsWith("zes")) {
            return lower.substring(0, lower.length() - 2);
        } else if (lower.endsWith("s") && !lower.endsWith("ss")) {
            return lower.substring(0, lower.length() - 1);
        }
        return lower;
    }

    /**
     * Convert a column name to camelCase for MongoDB field names.
     *
     * @param columnName the SQL column name
     * @return the camelCase field name
     */
    public static String toCamelCase(String columnName) {
        if (columnName == null || columnName.isBlank()) {
            return columnName;
        }

        String[] parts = columnName.toLowerCase().split("_");
        StringBuilder result = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            result.append(Character.toUpperCase(parts[i].charAt(0)))
                  .append(parts[i].substring(1));
        }
        return result.toString();
    }

    /**
     * Generate a foreign key field name for a relationship.
     *
     * @param parentTable the parent table name
     * @return the FK field name (e.g., "parentId" or "parent_id")
     */
    public static String generateForeignKeyField(String parentTable) {
        return toCamelCase(parentTable) + "Id";
    }

    /**
     * Generate a references array field name.
     *
     * @param referencedTable the referenced table name
     * @return the field name (e.g., "courseIds")
     */
    public static String generateReferencesField(String referencedTable) {
        return toCamelCase(referencedTable) + "Ids";
    }

    /**
     * Sanitize error message by removing potential password leaks.
     */
    public static String sanitizeError(String msg, String password) {
        if (msg == null) return "";
        if (password != null && !password.isBlank()) {
            return msg.replace(password, "****");
        }
        return msg;
    }
}
