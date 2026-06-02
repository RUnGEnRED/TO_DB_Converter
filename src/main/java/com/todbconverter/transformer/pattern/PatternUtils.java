package com.todbconverter.transformer.pattern;

import com.todbconverter.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class PatternUtils {

    private static final String[] DATE_FIELDS = {"date", "created_at", "updated_at", "published_date", "timestamp", "created"};

    private PatternUtils() {}

    public static Set<String> findArrayFields(List<Map<String, Object>> documents) {
        Set<String> arrayFields = new HashSet<>();
        for (Map<String, Object> doc : documents) {
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                if (entry.getValue() instanceof List) {
                    arrayFields.add(entry.getKey());
                }
            }
        }
        return arrayFields;
    }

    public static Object getDateValue(Object item) {
        if (!(item instanceof Map)) return null;
        Map<String, Object> map = (Map<String, Object>) item;
        for (String field : DATE_FIELDS) {
            Object val = map.get(field);
            if (val instanceof Date || val instanceof LocalDateTime ||
                val instanceof LocalDate || val instanceof String) {
                return val;
            }
        }
        return null;
    }

    public static int compareDates(Object dateA, Object dateB) {
        try {
            Instant instantA = toInstant(dateA);
            Instant instantB = toInstant(dateB);
            if (instantA != null && instantB != null) {
                return instantA.compareTo(instantB);
            }
        } catch (Exception e) {
        }
        return 0;
    }

    public static Instant toInstant(Object dateObj) {
        if (dateObj instanceof Date) {
            return ((Date) dateObj).toInstant();
        }
        if (dateObj instanceof LocalDateTime) {
            return ((LocalDateTime) dateObj).atZone(ZoneId.systemDefault()).toInstant();
        }
        if (dateObj instanceof LocalDate) {
            return ((LocalDate) dateObj).atStartOfDay(ZoneId.systemDefault()).toInstant();
        }
        if (dateObj instanceof String) {
            try {
                return Instant.parse((String) dateObj);
            } catch (Exception e) {
                try {
                    return LocalDateTime.parse((String) dateObj).atZone(ZoneId.systemDefault()).toInstant();
                } catch (Exception e2) {
                    return null;
                }
            }
        }
        return null;
    }
}
