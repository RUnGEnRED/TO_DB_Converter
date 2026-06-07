package com.todbconverter.core.model;

import java.util.*;

/**
 * Directed Dependency Graph (DDG) representing table relationships.
 * <p>
 * Nodes = Tables (via TableMetadata)
 * Edges = Foreign Key relationships (via ForeignKeyMetadata)
 * <p>
 * Edge direction: Child Table (holding FK) -> Parent Table (referenced)
 */
public class SchemaGraph {

    private final Map<String, TableMetadata> tables = new LinkedHashMap<>();
    private final Map<String, List<ForeignKeyMetadata>> adjacencyList = new HashMap<>();

    /**
     * Add a table to the graph.
     */
    public void addTable(TableMetadata table) {
        tables.put(table.getName(), table);
        adjacencyList.putIfAbsent(table.getName(), new ArrayList<>());
    }

    /**
     * Add a foreign key edge to the graph.
     * Edge goes from child (FK holder) to parent (referenced table).
     */
    public void addEdge(ForeignKeyMetadata foreignKey) {
        String fromTable = foreignKey.getFkTableName();
        adjacencyList.computeIfAbsent(fromTable, k -> new ArrayList<>()).add(foreignKey);

        // Ensure both tables exist in our table map
        adjacencyList.putIfAbsent(foreignKey.getPkTableName(), new ArrayList<>());
    }

    /**
     * Get all tables in the graph.
     */
    public Collection<TableMetadata> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    /**
     * Get table metadata by name.
     */
    public Optional<TableMetadata> getTable(String name) {
        return Optional.ofNullable(tables.get(name));
    }

    /**
     * Get all outgoing edges (foreign keys) from a table.
     */
    public List<ForeignKeyMetadata> getEdges(String tableName) {
        return Collections.unmodifiableList(
                adjacencyList.getOrDefault(tableName, Collections.emptyList())
        );
    }

    /**
     * Get all incoming edges (foreign keys pointing TO this table).
     */
    public List<ForeignKeyMetadata> getIncomingEdges(String tableName) {
        List<ForeignKeyMetadata> incoming = new ArrayList<>();
        for (List<ForeignKeyMetadata> edges : adjacencyList.values()) {
            for (ForeignKeyMetadata fk : edges) {
                if (fk.getPkTableName().equals(tableName)) {
                    incoming.add(fk);
                }
            }
        }
        return Collections.unmodifiableList(incoming);
    }

    /**
     * Get all table names in the graph.
     */
    public Set<String> getTableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }

    /**
     * Detect if the graph contains a cycle using DFS.
     */
    public boolean hasCycle() {
        return hasCycle(null);
    }

    public boolean hasCycle(com.todbconverter.config.EdgeStrategyRegistry strategyRegistry) {
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (String table : tables.keySet()) {
            if (!visited.contains(table)) {
                if (detectCycleDFS(table, visited, recursionStack, strategyRegistry)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get the path of a detected cycle, or empty if no cycle.
     */
    public List<String> getCyclePath() {
        return getCyclePath(null);
    }

    public List<String> getCyclePath(com.todbconverter.config.EdgeStrategyRegistry strategyRegistry) {
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();
        List<String> path = new ArrayList<>();

        for (String table : tables.keySet()) {
            if (!visited.contains(table)) {
                if (findCycleDFS(table, visited, recursionStack, path, strategyRegistry)) {
                    // Clean up path to only contain the cycle itself
                    if (path.size() >= 2) {
                        String lastNode = path.get(path.size() - 1);
                        int firstIdx = path.indexOf(lastNode);
                        if (firstIdx != -1 && firstIdx < path.size() - 1) {
                            return new ArrayList<>(path.subList(firstIdx, path.size()));
                        }
                    }
                    return path;
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Check if a table has a self-referencing foreign key.
     */
    public boolean isSelfReferencing(String tableName) {
        return adjacencyList.getOrDefault(tableName, Collections.emptyList()).stream()
                .anyMatch(ForeignKeyMetadata::isSelfReferencing);
    }

    /**
     * Get topological order of tables (leaves first, roots last).
     * Uses Kahn's algorithm, excluding self-references.
     */
    public List<String> getTopologicalOrder() {
        return getTopologicalOrder(null);
    }

    public List<String> getTopologicalOrder(com.todbconverter.config.EdgeStrategyRegistry strategyRegistry) {
        // Calculate in-degree for each table (excluding self-references and REFERENCE edges)
        Map<String, Integer> inDegree = new HashMap<>();
        for (String table : tables.keySet()) {
            inDegree.put(table, 0);
        }
        for (List<ForeignKeyMetadata> edges : adjacencyList.values()) {
            for (ForeignKeyMetadata fk : edges) {
                // Skip self-references for topological sort
                if (!fk.isSelfReferencing()) {
                    if (strategyRegistry != null) {
                        Strategy strategy = strategyRegistry.getStrategy(fk.getFkTableName(), fk.getPkTableName());
                        if (strategy == Strategy.REFERENCE) {
                            continue; // Skip REFERENCE edges from topological sort
                        }
                    }
                    inDegree.merge(fk.getPkTableName(), 1, Integer::sum);
                }
            }
        }

        // Start with nodes that have no incoming edges (roots)
        Queue<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            result.add(current);

            for (ForeignKeyMetadata fk : adjacencyList.getOrDefault(current, Collections.emptyList())) {
                // Skip self-references
                if (!fk.isSelfReferencing()) {
                    if (strategyRegistry != null) {
                        Strategy strategy = strategyRegistry.getStrategy(fk.getFkTableName(), fk.getPkTableName());
                        if (strategy == Strategy.REFERENCE) {
                            continue; // Skip REFERENCE edges
                        }
                    }
                    String child = fk.getPkTableName();
                    int newDegree = inDegree.get(child) - 1;
                    inDegree.put(child, newDegree);
                    if (newDegree == 0) {
                        queue.add(child);
                    }
                }
            }
        }

        // Return as children-first order directly, NO REVERSING!
        return result;
    }

    /**
     * Get tables with no outgoing edges (leaf tables - pure children).
     */
    public Set<String> getLeafTables() {
        Set<String> leaves = new HashSet<>(tables.keySet());
        for (Map.Entry<String, List<ForeignKeyMetadata>> entry : adjacencyList.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                leaves.remove(entry.getKey());
            }
        }
        return leaves;
    }

    /**
     * Get tables with no incoming edges (root tables - primary entities).
     */
    public Set<String> getRootTables() {
        Set<String> allChildren = new HashSet<>();
        for (List<ForeignKeyMetadata> edges : adjacencyList.values()) {
            for (ForeignKeyMetadata fk : edges) {
                allChildren.add(fk.getPkTableName());
            }
        }
        Set<String> roots = new HashSet<>(tables.keySet());
        roots.removeAll(allChildren);
        return roots;
    }

    /**
     * Get all tables that are referenced by the given table.
     */
    public Set<String> getReferencedTables(String tableName) {
        return adjacencyList.getOrDefault(tableName, Collections.emptyList()).stream()
                .map(ForeignKeyMetadata::getPkTableName)
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Get all tables that reference the given table.
     */
    public Set<String> getReferencingTables(String tableName) {
        Set<String> referencing = new HashSet<>();
        for (Map.Entry<String, List<ForeignKeyMetadata>> entry : adjacencyList.entrySet()) {
            for (ForeignKeyMetadata fk : entry.getValue()) {
                if (fk.getPkTableName().equals(tableName)) {
                    referencing.add(entry.getKey());
                }
            }
        }
        return referencing;
    }

    // Private helper methods for cycle detection

    private boolean detectCycleDFS(String node, Set<String> visited, Set<String> recursionStack,
                                   com.todbconverter.config.EdgeStrategyRegistry strategyRegistry) {
        visited.add(node);
        recursionStack.add(node);

        for (ForeignKeyMetadata fk : adjacencyList.getOrDefault(node, Collections.emptyList())) {
            if (strategyRegistry != null) {
                Strategy strategy = strategyRegistry.getStrategy(fk.getFkTableName(), fk.getPkTableName());
                if (strategy == Strategy.REFERENCE) {
                    continue; // Ignore REFERENCE edges
                }
            }
            String neighbor = fk.getPkTableName();
            if (!visited.contains(neighbor)) {
                if (detectCycleDFS(neighbor, visited, recursionStack, strategyRegistry)) {
                    return true;
                }
            } else if (recursionStack.contains(neighbor)) {
                return true;
            }
        }

        recursionStack.remove(node);
        return false;
    }

    private boolean findCycleDFS(String node, Set<String> visited, Set<String> recursionStack,
                                  List<String> path, com.todbconverter.config.EdgeStrategyRegistry strategyRegistry) {
        visited.add(node);
        recursionStack.add(node);
        path.add(node);

        for (ForeignKeyMetadata fk : adjacencyList.getOrDefault(node, Collections.emptyList())) {
            if (strategyRegistry != null) {
                Strategy strategy = strategyRegistry.getStrategy(fk.getFkTableName(), fk.getPkTableName());
                if (strategy == Strategy.REFERENCE) {
                    continue; // Ignore REFERENCE edges
                }
            }
            String neighbor = fk.getPkTableName();
            if (!visited.contains(neighbor)) {
                if (findCycleDFS(neighbor, visited, recursionStack, path, strategyRegistry)) {
                    return true;
                }
            } else if (recursionStack.contains(neighbor)) {
                // Found cycle - add neighbor to complete the cycle path
                path.add(neighbor);
                return true;
            }
        }

        path.remove(path.size() - 1);
        recursionStack.remove(node);
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SchemaGraph{\n");
        for (TableMetadata table : tables.values()) {
            sb.append("  ").append(table.getName()).append(" (").append(table.getTableType()).append(")\n");
            for (ForeignKeyMetadata fk : adjacencyList.getOrDefault(table.getName(), Collections.emptyList())) {
                sb.append("    -> ").append(fk).append("\n");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
