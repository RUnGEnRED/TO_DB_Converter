package com.todbconverter.transformer;

import com.todbconverter.model.TableMetadata;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class MongoToSqlEdgeCaseTest {

    @Test
    public void testMissingIdsAndDuplicates() {
        UniversalTransformer transformer = new UniversalTransformer();
        Map<String, TableMetadata> tablesMetadata = new HashMap<>();
        
        List<Map<String, Object>> docs = new ArrayList<>();
        
        // 1. Document with ID
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("id", 100);
        doc1.put("name", "Item 1");
        docs.add(doc1);
        
        // 2. Duplicate Document with same ID
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("id", 100);
        doc2.put("name", "Item 1 Duplicate");
        docs.add(doc2);
        
        // 3. Document without any ID
        Map<String, Object> doc3 = new HashMap<>();
        doc3.put("name", "Item No ID");
        docs.add(doc3);
        
        Map<String, List<Map<String, Object>>> relational = 
            transformer.flattenToRelational("test_table", docs, tablesMetadata);
            
        List<Map<String, Object>> tableData = relational.get("test_table");
        
        // Should have only 2 records (one with ID 100, one with generated UUID)
        // because the second ID 100 was skipped by de-duplication
        assertEquals(2, tableData.size(), "Should have de-duplicated records");
        
        boolean found100 = false;
        boolean foundGenerated = false;
        
        for (Map<String, Object> row : tableData) {
            Object id = row.get("id");
            assertNotNull(id, "ID should never be null");
            if (id.equals(100)) found100 = true;
            if (id instanceof String && ((String)id).length() > 20) foundGenerated = true;
        }
        
        assertTrue(found100, "Should have record with ID 100");
        assertTrue(foundGenerated, "Should have record with generated UUID");
    }
}
