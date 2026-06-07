package com.todbconverter.core.transformer;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.config.EdgeStrategyRegistry;
import com.todbconverter.core.model.*;
import com.todbconverter.core.transformer.patterns.*;
import com.todbconverter.exception.TransformationException;
import com.todbconverter.exception.UnboundedArrayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Core transformation engine that converts relational data to document model.
 * <p>
 * Uses topological sorting and O(1) in-memory indexing for efficient transformation.
 */
public class UniversalTransformer {

    private static final Logger logger = LoggerFactory.getLogger(UniversalTransformer.class);

    private final EdgeStrategyRegistry strategyRegistry;
    private final List<PatternApplier> patternAppliers;
    private final int maxChildrenPerParent;

    public UniversalTransformer(DatabaseConfig config) {
        this.strategyRegistry = EdgeStrategyRegistry.fromConfig(config);
        this.maxChildrenPerParent = config.getMaxChildrenPerParent();

        // Initialize pattern appliers
        this.patternAppliers = List.of(
                new AttributePattern(),
                new ComputedPattern(),
                new SubsetPattern()
        );
    }

    /**
     * Transform all data from relational model to document model.
     *
     * @param graph   schema graph
     * @param rawData raw relational data
     * @param config  database configuration
     * @return transformed document data (table name -> list of documents)
     * @throws TransformationException if transformation fails
     */
    public Map<String, List<Map<String, Object>>> transform(
            SchemaGraph graph,
            Map<String, List<Map<String, Object>>> rawData,
            DatabaseConfig config) throws TransformationException {

        // 1. Handle cycles and self-references
        detectAndHandleCycles(graph);

        // 2. Check for unbounded arrays
        checkUnboundedArrays(rawData, graph);

        // 3. Get topological order (children first, using strategyRegistry)
        List<String> topoOrder = graph.getTopologicalOrder(strategyRegistry);
        logger.info("Topological order: {}", topoOrder);

        // 4. Initialize relationship indexes for O(1) lookups
        Map<String, Map<Object, List<Map<String, Object>>>> relationshipIndex = new HashMap<>();

        // 5. Transform tables in topological order
        Map<String, List<Map<String, Object>>> transformedData = new LinkedHashMap<>();

        for (String tableName : topoOrder) {
            TableMetadata table = graph.getTable(tableName).orElse(null);
            if (table == null) continue;

            List<Map<String, Object>> rows = rawData.get(tableName);
            if (rows == null || rows.isEmpty()) {
                transformedData.put(tableName, new ArrayList<>());
                continue;
            }

            List<Map<String, Object>> transformedRows = new ArrayList<>();

            for (Map<String, Object> row : rows) {
                Map<String, Object> document = new LinkedHashMap<>(row);

                // Find all tables that reference THIS table (children)
                for (ForeignKeyMetadata fk : graph.getIncomingEdges(tableName)) {
                    // fk points FROM child TO this table
                    // fk.getFkTableName() = child table
                    // fk.getPkTableName() = this table (parent)

                    Strategy strategy = strategyRegistry.getStrategy(
                            fk.getFkTableName(), fk.getPkTableName());

                    // Look up children where child.fk = parent.pk
                    String childIndexKey = fk.getFkTableName() + "." + fk.getFkColumnName();
                    Object parentPkValue = document.get(fk.getPkColumnName());

                    List<Map<String, Object>> children =
                            relationshipIndex.getOrDefault(childIndexKey, Collections.emptyMap())
                                    .getOrDefault(parentPkValue, Collections.emptyList());

                    if (strategy == Strategy.EMBED) {
                        applyEmbedStrategy(document, children, fk, graph, config);
                    } else {
                        // REFERENCE: keep FK in child document, parent has no embedded data
                        // Nothing to do here - children will be in their own collection
                    }
                }

                // Apply design patterns
                applyPatterns(document, tableName, rawData, config, graph);

                transformedRows.add(document);
            }

            transformedData.put(tableName, transformedRows);

            // Index this table's transformed rows dynamically for subsequent parents to query
            TableMetadata tableMeta = graph.getTable(tableName).orElse(null);
            if (tableMeta != null) {
                for (var column : tableMeta.getColumns()) {
                    String columnName = column.getName();
                    Map<Object, List<Map<String, Object>>> columnIndex = new HashMap<>();

                    for (Map<String, Object> row : transformedRows) {
                        Object value = row.get(columnName);
                        if (value != null) {
                            columnIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(row);
                        }
                    }

                    if (!columnIndex.isEmpty()) {
                        String indexKey = tableName + "." + columnName;
                        relationshipIndex.put(indexKey, columnIndex);
                    }
                }
            }
        }

        return transformedData;
    }

    /**
     * Build O(1) relationship index for fast lookups.
     * Index: Map<ChildTable.FkColumn, Map<FkValue, List<Row>>>
     * This allows finding all children of a parent by FK value.
     */
    private Map<String, Map<Object, List<Map<String, Object>>>> buildRelationshipIndex(
            Map<String, List<Map<String, Object>>> rawData, SchemaGraph graph) {

        Map<String, Map<Object, List<Map<String, Object>>>> index = new HashMap<>();

        // Build index for each table based on ALL columns that could be FKs
        for (TableMetadata table : graph.getTables()) {
            String tableName = table.getName();
            List<Map<String, Object>> rows = rawData.get(tableName);
            if (rows == null) continue;

            // For each column in this table, build an index
            for (var column : table.getColumns()) {
                String columnName = column.getName();
                Map<Object, List<Map<String, Object>>> columnIndex = new HashMap<>();

                for (Map<String, Object> row : rows) {
                    Object value = row.get(columnName);
                    if (value != null) {
                        columnIndex.computeIfAbsent(value, k -> new ArrayList<>()).add(row);
                    }
                }

                if (!columnIndex.isEmpty()) {
                    String indexKey = tableName + "." + columnName;
                    index.put(indexKey, columnIndex);
                }
            }
        }

        return index;
    }

    /**
     * Get the primary key column name for a table.
     * Falls back to first non-FK column if no PK found.
     */
    private String getPrimaryKeyColumn(SchemaGraph graph, String tableName) {
        return graph.getTable(tableName)
                .map(TableMetadata::getPrimaryKeyColumn)
                .orElseGet(() -> {
                    // Fallback: try common PK column names
                    return graph.getTable(tableName)
                            .map(t -> t.getColumns().stream()
                                    .filter(c -> !c.isNullable())
                                    .map(ColumnMetadata::getName)
                                    .findFirst()
                                    .orElse("id"))
                            .orElse("id");
                });
    }

    /**
     * Apply EMBED strategy for a relationship.
     */
    private void applyEmbedStrategy(
            Map<String, Object> parent,
            List<Map<String, Object>> children,
            ForeignKeyMetadata fk,
            SchemaGraph graph,
            DatabaseConfig config) {

        TableMetadata childTable = graph.getTable(fk.getFkTableName()).orElse(null);
        if (childTable == null) return;

        // Disambiguate the embedded field name when the parent has more than
        // one FK to the same child table. Without this, the second relation
        // silently overwrites the first under the same field name.
        String childFieldName = buildChildFieldName(graph, fk);

        if (fk.getCardinality() == Cardinality.ONE_TO_ONE) {
            // ONE_TO_ONE: embed as single object
            applyOneToOne(parent, children, fk.getFkColumnName(), childFieldName);
        } else if (fk.getCardinality() == Cardinality.ONE_TO_MANY) {
            // ONE_TO_MANY: embed as array of objects
            applyOneToMany(parent, children, fk.getFkColumnName(), childFieldName);
        } else if (fk.getCardinality() == Cardinality.MANY_TO_MANY) {
            // MANY_TO_MANY: embed junction table data
            applyManyToMany(parent, children, fk, childTable);
        }
    }

    /**
     * Build the field name used to embed the child in the parent.
     * If the parent has multiple FKs pointing to the same child table,
     * the FK column name is appended to disambiguate.
     */
    private String buildChildFieldName(SchemaGraph graph, ForeignKeyMetadata fk) {
        String childTableName = fk.getFkTableName();
        long sameChildCount = 0;
        for (ForeignKeyMetadata incoming : graph.getIncomingEdges(fk.getPkTableName())) {
            if (incoming.getFkTableName().equals(childTableName)) {
                sameChildCount++;
            }
        }
        if (sameChildCount > 1) {
            return childTableName + "_by_" + fk.getFkColumnName();
        }
        return childTableName;
    }

    /**
     * ONE_TO_ONE embedding: child becomes a nested subdocument.
     */
    private void applyOneToOne(
            Map<String, Object> parent,
            List<Map<String, Object>> children,
            String fkColumn,
            String fieldName) {

        if (children != null && !children.isEmpty()) {
            Map<String, Object> child = new LinkedHashMap<>(children.get(0));
            child.remove(fkColumn); // Remove FK from embedded document
            parent.put(fieldName, child);
        } else {
            parent.put(fieldName, null);
        }
    }

    /**
     * ONE_TO_MANY embedding: children become an array of subdocuments.
     */
    private void applyOneToMany(
            Map<String, Object> parent,
            List<Map<String, Object>> children,
            String fkColumn,
            String fieldName) {

        List<Map<String, Object>> embeddedChildren = new ArrayList<>();

        if (children != null) {
            for (Map<String, Object> child : children) {
                Map<String, Object> cleanChild = new LinkedHashMap<>(child);
                cleanChild.remove(fkColumn); // Remove FK from embedded document
                embeddedChildren.add(cleanChild);
            }
        }

        parent.put(fieldName, embeddedChildren);
    }

    /**
     * MANY_TO_MANY transformation for junction tables.
     */
    private void applyManyToMany(
            Map<String, Object> parent,
            List<Map<String, Object>> junctionRows,
            ForeignKeyMetadata fk,
            TableMetadata childTable) {

        // For junction tables, we store an array of IDs
        // The junction table's payload columns are also included
        List<Map<String, Object>> junctionData = new ArrayList<>();

        if (junctionRows != null) {
            for (Map<String, Object> row : junctionRows) {
                Map<String, Object> junctionEntry = new LinkedHashMap<>(row);

                // Remove the parent's FK column
                junctionEntry.remove(fk.getFkColumnName());

                // Find and remove the other FK column (the one pointing to the target table)
                for (ForeignKeyMetadata childFk : childTable.getForeignKeys()) {
                    if (!childFk.getPkTableName().equals(fk.getFkTableName())) {
                        junctionEntry.remove(childFk.getFkColumnName());
                    }
                }

                junctionData.add(junctionEntry);
            }
        }

        // Store with table name as key
        parent.put(fk.getPkTableName(), junctionData);
    }

    /**
     * Apply design patterns to a document.
     */
    private void applyPatterns(
            Map<String, Object> document,
            String tableName,
            Map<String, List<Map<String, Object>>> rawData,
            DatabaseConfig config,
            SchemaGraph graph) throws TransformationException {

        Map<String, String> patternConfigs = config.getPatternConfig(tableName);

        for (PatternApplier applier : patternAppliers) {
            String patternType = applier.getPatternType();
            String patternConfig = patternConfigs.get(patternType);

            if (patternConfig != null) {
                Map<String, Object> context = parsePatternContext(patternType, patternConfig, document, rawData, config, tableName, graph);
                if (context != null) {
                    applier.apply(document, context);
                }
            }
        }
    }

    /**
     * Parse pattern configuration into context map.
     */
    private Map<String, Object> parsePatternContext(
            String patternType,
            String patternConfig,
            Map<String, Object> document,
            Map<String, List<Map<String, Object>>> rawData,
            DatabaseConfig config,
            String tableName,
            SchemaGraph graph) throws TransformationException {

        Map<String, Object> context = new HashMap<>();

        switch (patternType) {
            case "attribute" -> {
                // Format: [ArrayName]=[Col1]:[Key1],[Col2]:[Key2]
                String[] parts = patternConfig.split("=", 2);
                if (parts.length == 2) {
                    context.put("arrayName", parts[0]);
                    context.put("mappings", parts[1]);
                }
            }
            case "computed" -> {
                // Format: [FieldName]=[FUNC]([ChildTable].[Column])
                String[] parts = patternConfig.split("=", 2);
                if (parts.length == 2) {
                    context.put("fieldName", parts[0]);
                    String funcExpr = parts[1].trim();

                    // Parse function: COUNT(orders.id) or SUM(orders.price).
                    // Accept lowercase and mixed case — the applier normalizes to upper.
                    java.util.regex.Matcher matcher =
                            java.util.regex.Pattern.compile("([A-Za-z]+)\\((\\w+)\\.(\\w+)\\)")
                                    .matcher(funcExpr);

                    if (matcher.matches()) {
                        context.put("function", matcher.group(1));
                        String childTable = matcher.group(2);
                        context.put("childColumn", matcher.group(3));

                        // Get children data
                        List<Map<String, Object>> children = rawData.get(childTable);
                        if (children != null) {
                            // Find the FK column that links childTable to current table
                            String parentTable = tableName;
                            String childFkCol = findForeignKeyColumn(graph, childTable, parentTable);

                            // Filter children related to current document
                            String parentPkCol = getPrimaryKeyColumn(graph, parentTable);
                            Object parentPk = document.get(parentPkCol);
                            if (parentPk != null && childFkCol != null) {
                                String finalChildFkCol = childFkCol;
                                // Compare values as strings to avoid Integer/Long
                                // type mismatches from JDBC (where the same value
                                // can come back as Integer OR Long depending on size).
                                children = children.stream()
                                        .filter(c -> {
                                            Object childFk = c.get(finalChildFkCol);
                                            return parentPk.equals(childFk)
                                                    || (childFk != null
                                                        && String.valueOf(parentPk).equals(String.valueOf(childFk)));
                                        })
                                        .toList();
                            } else {
                                children = Collections.emptyList();
                            }
                            context.put("children", children);
                        }
                    }
                }
            }
            case "subset" -> {
                // Format: [ChildTable]=[Limit]
                String[] parts = patternConfig.split("=", 2);
                if (parts.length == 2) {
                    int limit;
                    try {
                        limit = Integer.parseInt(parts[1].trim());
                    } catch (NumberFormatException nfe) {
                        throw new TransformationException(
                                "Invalid subset limit '" + parts[1] + "' for table " + tableName
                                        + " — must be a non-negative integer");
                    }
                    if (limit < 0) {
                        throw new TransformationException(
                                "Invalid subset limit " + limit + " for table " + tableName
                                        + " — must be >= 0");
                    }
                    context.put("limit", limit);
                    context.put("arrayName", "recent_" + parts[0]);

                    // Get children data
                    String childTable = parts[0];
                    List<Map<String, Object>> children = rawData.get(childTable);
                    if (children != null) {
                        String parentTable = tableName;
                        String childFkCol = findForeignKeyColumn(graph, childTable, parentTable);
                        String parentPkCol = getPrimaryKeyColumn(graph, parentTable);
                        Object parentPk = document.get(parentPkCol);
                        if (parentPk != null && childFkCol != null) {
                            String finalChildFkCol = childFkCol;
                            children = children.stream()
                                    .filter(c -> parentPk.equals(c.get(finalChildFkCol)))
                                    .toList();
                        } else {
                            children = Collections.emptyList();
                        }

                        // Sort children by PK descending to get the most recent ones
                        String childPkCol = getPrimaryKeyColumn(graph, childTable);
                        List<Map<String, Object>> sortedChildren = new ArrayList<>(children);
                        sortedChildren.sort((c1, c2) -> {
                            Object v1 = c1.get(childPkCol);
                            Object v2 = c2.get(childPkCol);
                            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                                return ((Comparable) v2).compareTo(v1); // Descending
                            }
                            return 0;
                        });

                        context.put("children", sortedChildren);
                        // Pass the real FK column to the applier so it removes
                        // exactly the right field, not any column ending in "_id".
                        context.put("fkColumn", childFkCol);
                    }
                }
            }
        }

        return context;
    }

    /**
     * Detect and handle cycles in the graph.
     * Auto-fixes by converting one edge in the cycle to REFERENCE.
     */
    private void detectAndHandleCycles(SchemaGraph graph) throws TransformationException {
        // Safety net: zawsze wymuszaj REFERENCE dla samoreferencji
        for (TableMetadata table : graph.getTables()) {
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                if (fk.isSelfReferencing()) {
                    strategyRegistry.setStrategy(fk.getFkTableName(), fk.getPkTableName(), Strategy.REFERENCE);
                }
            }
        }

        while (graph.hasCycle(strategyRegistry)) {
            List<String> cyclePath = graph.getCyclePath(strategyRegistry);
            if (cyclePath.isEmpty() || cyclePath.size() < 2) {
                break;
            }
            String cycleStr = String.join(" -> ", cyclePath);

            logger.warn("Circular dependency detected: {}. Auto-fixing by setting REFERENCE strategy.", cycleStr);

            // Auto-fix: set the first edge in the cycle to REFERENCE
            String from = cyclePath.get(0);
            String to = cyclePath.get(1);
            strategyRegistry.setStrategy(from, to, Strategy.REFERENCE);
        }
    }

    /**
     * Check for unbounded arrays and auto-downgrade to REFERENCE if needed.
     */
    private void checkUnboundedArrays(
            Map<String, List<Map<String, Object>>> rawData,
            SchemaGraph graph) {

        for (TableMetadata table : graph.getTables()) {
            for (ForeignKeyMetadata fk : table.getForeignKeys()) {
                if (fk.getCardinality() == Cardinality.ONE_TO_MANY && !fk.isSelfReferencing()) {
                    // Count children per parent
                    Map<Object, Long> childCounts = rawData.getOrDefault(fk.getFkTableName(), Collections.emptyList())
                            .stream()
                            .filter(row -> row.get(fk.getFkColumnName()) != null)
                            .collect(Collectors.groupingBy(
                                    row -> row.get(fk.getFkColumnName()),
                                    Collectors.counting()));

                    long maxChildren = childCounts.values().stream()
                            .max(Long::compareTo)
                            .orElse(0L);

                    if (maxChildren > maxChildrenPerParent) {
                        logger.warn("Table '{}' has parents with {} children (threshold: {}). " +
                                        "Auto-downgrading to REFERENCE.",
                                fk.getFkTableName(), maxChildren, maxChildrenPerParent);
                        strategyRegistry.setStrategy(fk.getFkTableName(), fk.getPkTableName(),
                                Strategy.REFERENCE);
                    }
                }
            }
        }
    }

    /**
     * Find the FK column in childTable that references parentTable.
     */
    private String findForeignKeyColumn(SchemaGraph graph, String childTable, String parentTable) {
        for (ForeignKeyMetadata fk : graph.getIncomingEdges(parentTable)) {
            if (fk.getFkTableName().equals(childTable)) {
                return fk.getFkColumnName();
            }
        }
        // Fallback: look for common FK naming patterns
        return parentTable.toLowerCase() + "_id";
    }
}
