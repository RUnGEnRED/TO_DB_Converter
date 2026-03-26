package com.todbconverter.transformer;

import com.todbconverter.model.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class UniversalTransformerTest {

    @Test
    void testFlattenToRelational() {
        UniversalTransformer transformer = new UniversalTransformer();
        
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_id", 1);
        doc.put("name", "Test Document");
        
        Map<String, Object> childDoc = new HashMap<>();
        childDoc.put("child_id", "A1");
        childDoc.put("value", 100);
        
        List<Map<String, Object>> children = Arrays.asList(childDoc);
        doc.put("children", children);
        docs.add(doc);
        
        Map<String, TableMetadata> metaMap = new HashMap<>();
        Map<String, List<Map<String, Object>>> relational = transformer.flattenToRelational("parent", docs, metaMap);
        
        // Ensure both tables are generated
        assertTrue(relational.containsKey("parent"), "Expected parent table");
        assertTrue(relational.containsKey("children"), "Expected children table");
        
        // Verify parent table data
        List<Map<String, Object>> parentRows = relational.get("parent");
        assertEquals(1, parentRows.size());
        assertEquals(1, parentRows.get(0).get("id"));
        assertEquals("Test Document", parentRows.get(0).get("name"));
        
        // Verify child table data and the automatically generated foreign key
        List<Map<String, Object>> childRows = relational.get("children");
        assertEquals(1, childRows.size());
        assertEquals("A1", childRows.get(0).get("child_id"));
        assertEquals(100, childRows.get(0).get("value"));
        assertEquals(1, childRows.get(0).get("parent_id")); // Ensure FK to parent is generated
    }
}
