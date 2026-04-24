package com.todbconverter.util;

import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import static org.junit.jupiter.api.Assertions.*;

public class TypeMapperTest {

    @Test
    public void testInferSqlType() {
        assertEquals("INTEGER", TypeMapper.inferSqlType(10));
        assertEquals("BIGINT", TypeMapper.inferSqlType(10L));
        assertEquals("DOUBLE PRECISION", TypeMapper.inferSqlType(10.5));
        assertEquals("BOOLEAN", TypeMapper.inferSqlType(true));
        assertEquals("TIMESTAMP", TypeMapper.inferSqlType(new Date()));
        assertEquals("DECIMAL(19,4)", TypeMapper.inferSqlType(new BigDecimal("10.5")));
        assertEquals("TEXT", TypeMapper.inferSqlType("hello"));
        assertEquals("BYTEA", TypeMapper.inferSqlType(new byte[]{1, 2}));
    }

    @Test
    public void testConvertToSqlValue() {
        Date date = new Date();
        Object convertedDate = TypeMapper.convertToSqlValue(date);
        assertTrue(convertedDate instanceof Timestamp);
        
        Binary binary = new Binary(new byte[]{1, 2});
        Object convertedBinary = TypeMapper.convertToSqlValue(binary);
        assertArrayEquals(new byte[]{1, 2}, (byte[]) convertedBinary);
        
        assertEquals("test", TypeMapper.convertToSqlValue("test"));
    }
}
