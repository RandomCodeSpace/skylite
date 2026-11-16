package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EndpointTest {
    @ParameterizedTest
    @CsvSource({
        "https://db.example/library, https://db.example, library",
        "HTTPS://DB.Example:443/library, https://db.example, library",
        "https://db.example:8443/base/library, https://db.example:8443/base, library",
        "https://db.example/base%2fx/%7Eme/library, https://db.example/base%2Fx/~me, library",
        "https://db.example/a%41b/library, https://db.example/aAb, library",
        "https://db.example//library, https://db.example, library",
        "https://[::1]:9/l1-b, https://[::1]:9, l1-b",
    })
    void canonicalisesTheBaseUrl(String url, String base, String db) throws SQLException {
        Endpoint e = Endpoint.parse(Endpoint.PREFIX + url, false);
        assertEquals(base, e.base());
        assertEquals(db, e.database());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "https://user:pass@db.example/library", // user information
        "https://db.example/library?token=x", // a query
        "https://db.example/library?", // an empty query
        "https://db.example/library#f", // a fragment
        "https://db.example/library#", // an empty fragment
        "https:///library", // no host
        "https://db.example/./library", // a . segment
        "https://db.example/base/%2e%2E/library", // an encoded .. segment
        "https://db.example/", // no database
        "https://db.example/Library", // an upper-case name
        "https://db.example/-lib", // a name starting with -
        "https://db.example/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // 64 characters
        "https://db.example/lib_x", // _ is not in the name rule
        "ftp://db.example/library", // another scheme
        "http://db.example/library", // plaintext without opt-in
        "https://db.example/a%zzb/library", // a broken percent-encoding
        "db.example/library", // not absolute
    })
    void refusesUrlsTheGoDriverRefuses(String url) {
        SQLException e = assertThrows(SQLException.class, () -> Endpoint.parse(Endpoint.PREFIX + url, false));
        assertEquals("08001", e.getSQLState());
        assertFalse(e.getMessage().contains("db.example"), "the error does not echo the URL: " + e.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
        "http://db.example:80/library, http://db.example",
        "http://db.example/library, http://db.example",
        "http://db.example:443/library, http://db.example:443",
        "https://db.example:80/library, https://db.example:80",
    })
    void plaintextNeedsTheOptInAndOnlyTheSchemesDefaultPortIsDropped(String url, String base) throws SQLException {
        assertEquals(base, Endpoint.parse(Endpoint.PREFIX + url, true).base());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "2026-11-16|2026-11-16T00:00|",
        "2026-11-16 10:20|2026-11-16T10:20|",
        "2026-11-16T10:20:30|2026-11-16T10:20:30|",
        "2026-11-16 10:20:30.5|2026-11-16T10:20:30.500|",
        "2026-11-16 10:20:30.123456789|2026-11-16T10:20:30.123456789|",
        "2026-11-16 10:20:30Z|2026-11-16T10:20:30|0",
        "2026-11-16 10:20:30.1+05:30|2026-11-16T10:20:30.100|19800",
        "0001-01-01 00:00|0001-01-01T00:00|",
        "2026-11-16 10:20-23:59|2026-11-16T10:20|-86340",
    })
    void readsTheDateAndTimestampForms(String text, String local, Integer offset) {
        Temporal.Stamp s = Temporal.stamp(text);
        assertEquals(LocalDateTime.parse(local), s.local());
        assertEquals(offset, s.offsetSeconds());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2026-11-16 10:20:30.1234567890", "0000-01-01", "2026-02-30", "2026-11-16 24:00", "2026-11-16Z",
        "26-11-16", "2026-11-16 10", "2026-11-16 10:20:30.", "2026-11-16 10:20+24:00", "2026-11-16 10:20+05",
        " 2026-11-16", "2026-11-16 ", "2026-1-16", "２０２６-11-16",
    })
    void refusesOtherText(String text) {
        assertNull(Temporal.stamp(text));
    }

    @ParameterizedTest
    @CsvSource({"10:20, 10:20", "10:20:30, 10:20:30", "10:20:30.000000001, 10:20:30.000000001"})
    void readsTimesOfDay(String text, String want) {
        assertEquals(LocalTime.parse(want), Temporal.timeOfDay(text));
    }

    @ParameterizedTest
    @CsvSource({
        "2026-11-16 10:20:30+01:00, 2026-11-16T09:20:30Z",
        "2026-11-16 10:20:30, 2026-11-16T10:20:30Z",
    })
    void readsInstantsByTheirOffsetAndUtcWithout(String text, String want) {
        assertEquals(Instant.parse(want), Temporal.stamp(text).instant());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0001-01-01 00:30+01:00", "9999-12-31 23:30-01:00"})
    void refusesInstantsOutsideTheYearRangeInUtc(String text) {
        assertNull(Temporal.stamp(text).instant());
        assertTrue(Temporal.stamp(text).local().getYear() >= 1);
    }

    @ParameterizedTest
    @CsvSource({
        "2026-11-16T10:20:30.5Z, 2026-11-16 10:20:30.500000000",
        "0001-01-01T00:00:00Z, 0001-01-01 00:00:00.000000000",
    })
    void writesInstantsInUtcWithNineFractionDigits(String instant, String want) {
        assertEquals(want, Temporal.format(Instant.parse(instant)));
    }
}
