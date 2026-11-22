package io.github.randomcodespace.skylite.jdbc;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Temporal values of ADR 0005 decision 8: TEXT in four canonical forms when written, and these forms only when read:
 * {@code YYYY-MM-DD}, then optionally a space or {@code T} and {@code HH:MM}, {@code HH:MM:SS} or {@code HH:MM:SS}
 * with 1 to 9 fraction digits, then, after a time, optionally {@code Z} or {@code +HH:MM} / {@code -HH:MM}; a time of
 * day from {@code HH:MM[:SS[.f{1,9}]]}. Years run from 0001 to 9999.
 */
final class Temporal {
    private Temporal() {}

    private static final Pattern DATE_TIME = Pattern.compile(
            "(\\d{4})-(\\d{2})-(\\d{2})(?:[ T](\\d{2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?(Z|[+-]\\d{2}:\\d{2})?)?");
    private static final Pattern TIME = Pattern.compile("(\\d{2}):(\\d{2})(?::(\\d{2})(?:\\.(\\d{1,9}))?)?");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter TIME_OF_DAY = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter LOCAL = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS");

    /** Text read as a date or timestamp: its fields, and its offset in seconds, or null without one. */
    record Stamp(LocalDateTime local, Integer offsetSeconds) {
        /** The instant it names: text without an offset is UTC. Null outside the years 0001 to 9999 in UTC. */
        Instant instant() {
            Instant i = local.toInstant(ZoneOffset.UTC).minusSeconds(offsetSeconds == null ? 0 : offsetSeconds);
            int y = LocalDateTime.ofInstant(i, ZoneOffset.UTC).getYear();
            return y >= 1 && y <= 9999 ? i : null;
        }
    }

    /** Reads a date or timestamp form, or returns null. */
    static Stamp stamp(String s) {
        Matcher m = DATE_TIME.matcher(s);
        if (!m.matches()) {
            return null;
        }
        try {
            LocalDate d = LocalDate.of(num(m, 1), num(m, 2), num(m, 3));
            if (d.getYear() < 1) {
                return null;
            }
            LocalTime t = m.group(4) == null ? LocalTime.MIDNIGHT : time(m, 4);
            Integer off = null;
            String z = m.group(8);
            if (z != null && !z.equals("Z")) {
                int oh = Integer.parseInt(z, 1, 3, 10), om = Integer.parseInt(z, 4, 6, 10);
                if (oh > 23 || om > 59) {
                    return null;
                }
                off = (z.charAt(0) == '-' ? -1 : 1) * (oh * 3600 + om * 60);
            } else if (z != null) {
                off = 0;
            }
            return new Stamp(LocalDateTime.of(d, t), off);
        } catch (DateTimeException e) {
            return null; // 31 April, hour 24 and the like
        }
    }

    /** Reads a time-of-day form, or returns null. */
    static LocalTime timeOfDay(String s) {
        Matcher m = TIME.matcher(s);
        if (!m.matches()) {
            return null;
        }
        try {
            return time(m, 1);
        } catch (DateTimeException e) {
            return null;
        }
    }

    private static LocalTime time(Matcher m, int g) {
        String frac = m.group(g + 3);
        int nanos = 0;
        if (frac != null) {
            nanos = Integer.parseInt(frac);
            for (int i = frac.length(); i < 9; i++) {
                nanos *= 10;
            }
        }
        return LocalTime.of(num(m, g), num(m, g + 1), m.group(g + 2) == null ? 0 : num(m, g + 2), nanos);
    }

    private static int num(Matcher m, int g) {
        return Integer.parseInt(m.group(g));
    }

    static boolean validYear(int y) {
        return y >= 1 && y <= 9999;
    }

    static String format(LocalDate d) {
        return DATE.format(d);
    }

    static String format(LocalTime t) {
        return TIME_OF_DAY.format(t);
    }

    static String format(LocalDateTime t) {
        return LOCAL.format(t);
    }

    /** An instant's canonical form: converted to UTC, no zone suffix. */
    static String format(Instant i) {
        return LOCAL.format(LocalDateTime.ofInstant(i, ZoneOffset.UTC));
    }
}
