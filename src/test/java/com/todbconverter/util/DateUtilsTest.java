package com.todbconverter.util;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DateUtils.
 */
class DateUtilsTest {

    @Test
    void shouldConvertTimestampToDate() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:00");
        Date date = DateUtils.toDate(timestamp);

        assertThat(date).isNotNull();
        assertThat(date.getTime()).isEqualTo(timestamp.getTime());
    }

    @Test
    void shouldConvertLocalDateTimeToDate() {
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 15, 10, 30);
        Date date = DateUtils.toDate(ldt);

        assertThat(date).isNotNull();
    }

    @Test
    void shouldConvertLocalDateToDate() {
        LocalDate ld = LocalDate.of(2024, 1, 15);
        Date date = DateUtils.toDate(ld);

        assertThat(date).isNotNull();
    }

    @Test
    void shouldConvertLocalDateUsingUtcMidnight() {
        // Regression test: LocalDate must be stored as UTC midnight so the
        // calendar date is preserved regardless of the host's time zone.
        // Previously used ZoneId.systemDefault() which caused date shifts
        // (e.g. 1990-01-01 in CEST became 1989-12-31 in MongoDB UTC).
        LocalDate ld = LocalDate.of(1990, 1, 1);
        Date date = DateUtils.toDate(ld);

        assertThat(date).isEqualTo(Date.from(Instant.parse("1990-01-01T00:00:00Z")));
    }

    @Test
    void shouldConvertInstantToDate() {
        Instant instant = Instant.now();
        Date date = DateUtils.toDate(instant);

        assertThat(date).isNotNull();
        assertThat(date.getTime()).isEqualTo(instant.toEpochMilli());
    }

    @Test
    void shouldReturnNullForNullTimestamp() {
        assertThat(DateUtils.toDate((Timestamp) null)).isNull();
    }

    @Test
    void shouldReturnNullForNullLocalDateTime() {
        assertThat(DateUtils.toDate((LocalDateTime) null)).isNull();
    }

    @Test
    void shouldReturnNullForNullLocalDate() {
        assertThat(DateUtils.toDate((LocalDate) null)).isNull();
    }

    @Test
    void shouldConvertTimestampViaSmartConverter() {
        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:00");
        Object result = DateUtils.convertTemporal(timestamp);

        assertThat(result).isInstanceOf(Date.class);
    }

    @Test
    void shouldReturnNullForNullSmartConverter() {
        assertThat(DateUtils.convertTemporal(null)).isNull();
    }

    @Test
    void shouldConvertSqlDateViaUtcMidnight() {
        // Regression test: the PostgreSQL JDBC driver returns java.sql.Date
        // for DATE columns. Its epoch millis represent midnight in the JVM's
        // default time zone, so storing it as-is shifts the calendar date
        // when read back in UTC. The smart converter must normalise to UTC
        // midnight via LocalDate.
        java.sql.Date sqlDate = java.sql.Date.valueOf(LocalDate.of(1990, 1, 1));
        Object result = DateUtils.convertTemporal(sqlDate);

        assertThat(result).isInstanceOf(Date.class);
        assertThat((Date) result).isEqualTo(Date.from(Instant.parse("1990-01-01T00:00:00Z")));
    }

    @Test
    void shouldReturnNonTemporalAsIs() {
        String text = "not a date";
        assertThat(DateUtils.convertTemporal(text)).isEqualTo("not a date");
    }
}
