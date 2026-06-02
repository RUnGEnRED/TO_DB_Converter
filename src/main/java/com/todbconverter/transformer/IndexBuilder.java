package com.todbconverter.transformer;

import com.todbconverter.model.ForeignKeyMetadata;
import com.todbconverter.model.TableMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IndexBuilder {

    private IndexBuilder() {}

    public static Map<String, Map<Object, List<Map<String, Object>>>> buildChildIndexes(
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata,
            String parentTableName) {

        Map<String, Map<Object, List<Map<String, Object>>>> childIndexes = new HashMap<>();

        for (Map.Entry<String, TableMetadata> entry : tablesMetadata.entrySet()) {
            String childTableName = entry.getKey();
            TableMetadata childTable = entry.getValue();

            if (childTableName.equals(parentTableName)) continue;

            List<ForeignKeyMetadata> childForeignKeys = childTable.getForeignKeys();
            if (childForeignKeys == null) continue;

            for (ForeignKeyMetadata fk : childForeignKeys) {
                if (fk.getReferencedTable().equals(parentTableName)) {
                    List<Map<String, Object>> childRecords = relatedData.get(childTableName);
                    if (childRecords != null) {
                        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
                        for (Map<String, Object> childRecord : childRecords) {
                            Object fkValue = childRecord.get(fk.getColumnName());
                            if (fkValue != null) {
                                index.computeIfAbsent(fkValue, k -> new ArrayList<>()).add(childRecord);
                            }
                        }
                        childIndexes.put(childTableName, index);
                    }
                    break;
                }
            }
        }

        return childIndexes;
    }

    public static Map<String, Map<String, Object>> buildReferenceIndexes(
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata) {

        Map<String, Map<String, Object>> referenceIndexes = new HashMap<>();

        for (Map.Entry<String, TableMetadata> entry : tablesMetadata.entrySet()) {
            String tableName = entry.getKey();
            TableMetadata table = entry.getValue();
            List<Map<String, Object>> records = relatedData.get(tableName);

            if (records != null && table.getPrimaryKeyColumn() != null) {
                String pkColumn = table.getPrimaryKeyColumn();
                Map<String, Object> index = new HashMap<>();
                for (Map<String, Object> record : records) {
                    Object pkValue = record.get(pkColumn);
                    if (pkValue != null) {
                        index.put(pkValue.toString(), record);
                    }
                }
                referenceIndexes.put(tableName, index);
            }
        }

        return referenceIndexes;
    }

    public static Map<String, Map<Object, Set<Object>>> buildManyToManyIndexes(
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata,
            String parentTableName) {

        Map<String, Map<Object, Set<Object>>> indexes = new HashMap<>();

        for (Map.Entry<String, TableMetadata> entry : tablesMetadata.entrySet()) {
            String tableName = entry.getKey();
            TableMetadata table = entry.getValue();

            if (tableName.equals(parentTableName)) continue;

            if (!isManyToManyRelationship(table, parentTableName)) continue;

            List<Map<String, Object>> records = relatedData.get(tableName);
            if (records == null || records.isEmpty()) continue;

            String fkColumn = findForeignKeyColumnTo(table, parentTableName);
            if (fkColumn == null) continue;

            String pkColumn = table.getPrimaryKeyColumn();
            if (pkColumn == null) continue;

            Map<Object, Set<Object>> index = new HashMap<>();
            for (Map<String, Object> record : records) {
                Object fkValue = record.get(fkColumn);
                Object pkValue = record.get(pkColumn);
                if (fkValue != null && pkValue != null) {
                    index.computeIfAbsent(fkValue, k -> new HashSet<>()).add(pkValue);
                }
            }
            indexes.put(tableName, index);
        }

        return indexes;
    }

    public static Map<String, Map<Object, List<Map<String, Object>>>> buildJunctionMnIndexes(
            String parentTableName,
            Map<String, Map<Object, List<Map<String, Object>>>> childIndexes,
            Map<String, List<Map<String, Object>>> relatedData,
            Map<String, TableMetadata> tablesMetadata,
            Map<String, Map<String, Object>> referenceIndexes) {

        Map<String, Map<Object, List<Map<String, Object>>>> junctionIndexes = new HashMap<>();

        for (Map.Entry<String, Map<Object, List<Map<String, Object>>>> childEntry : childIndexes.entrySet()) {
            String childTableName = childEntry.getKey();
            TableMetadata childMeta = tablesMetadata.get(childTableName);

            if (childMeta == null) continue;
            if (!isManyToManyRelationship(childMeta, parentTableName)) continue;

            List<ForeignKeyMetadata> fks = childMeta.getForeignKeys();
            if (fks == null) continue;

            String otherFkCol = null;
            String otherTableName = null;
            for (ForeignKeyMetadata fk : fks) {
                if (!fk.getReferencedTable().equalsIgnoreCase(parentTableName)) {
                    otherFkCol = fk.getColumnName();
                    otherTableName = fk.getReferencedTable();
                    break;
                }
            }

            if (otherFkCol == null || otherTableName == null) continue;

            Map<String, Object> otherRefIndex = referenceIndexes.get(otherTableName);
            if (otherRefIndex == null) continue;

            Map<Object, List<Map<String, Object>>> index = new HashMap<>();
            String parentFkCol = findForeignKeyColumnTo(childMeta, parentTableName);
            if (parentFkCol == null) continue;

            List<Map<String, Object>> junctionRecords = relatedData.get(childTableName);
            if (junctionRecords != null) {
                Set<String> seenIds = new HashSet<>();
                for (Map<String, Object> jr : junctionRecords) {
                    Object parentFkVal = jr.get(parentFkCol);
                    Object otherFkVal = jr.get(otherFkCol);
                    if (parentFkVal != null && otherFkVal != null) {
                        String dedupKey = parentFkVal.toString() + "_" + otherFkVal.toString();
                        if (!seenIds.add(dedupKey)) continue;
                        Object relatedEntity = otherRefIndex.get(otherFkVal.toString());
                        if (relatedEntity instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> entityMap = (Map<String, Object>) relatedEntity;
                            index.computeIfAbsent(parentFkVal, k -> new ArrayList<>())
                                 .add(new HashMap<>(entityMap));
                        }
                    }
                }
            }

            junctionIndexes.put(otherTableName, index);
        }

        return junctionIndexes;
    }

    public static boolean isManyToManyRelationship(TableMetadata table, String targetTableName) {
        List<ForeignKeyMetadata> fks = table.getForeignKeys();
        if (fks == null) return false;

        for (ForeignKeyMetadata fk : fks) {
            if (fk.getReferencedTable().equalsIgnoreCase(targetTableName)
                    && fk.getRelationshipType() == ForeignKeyMetadata.RelationshipType.MANY_TO_MANY) {
                return true;
            }
        }
        return false;
    }

    public static String findForeignKeyColumnTo(TableMetadata table, String targetTableName) {
        List<ForeignKeyMetadata> fks = table.getForeignKeys();
        if (fks == null) return null;

        for (ForeignKeyMetadata fk : fks) {
            if (fk.getReferencedTable().equalsIgnoreCase(targetTableName)) {
                return fk.getColumnName();
            }
        }
        return null;
    }

    public static ForeignKeyMetadata.RelationshipType getRelationshipTypeTo(TableMetadata table, String targetTableName) {
        if (table == null || targetTableName == null) return null;

        List<ForeignKeyMetadata> fks = table.getForeignKeys();
        if (fks == null) return null;

        for (ForeignKeyMetadata fk : fks) {
            if (fk.getReferencedTable().equalsIgnoreCase(targetTableName)) {
                return fk.getRelationshipType();
            }
        }
        return null;
    }
}
