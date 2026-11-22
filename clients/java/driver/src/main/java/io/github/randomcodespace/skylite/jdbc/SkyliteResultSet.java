package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Column;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Row;
import io.github.randomcodespace.skylite.jdbc.Values.TextBytes;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.rowset.serial.SerialBlob;

/**
 * A forward-only, read-only result set over a streaming response: each row is decoded as its frame arrives, and
 * {@link #close} before the end releases the response. A failed or incomplete response after rows were read is an
 * error, never a short result. Values keep their storage class (ADR 0005 decision 8); conversions are listed on
 * docs/content/docs/jdbc-driver.md.
 */
final class SkyliteResultSet implements ResultSet {
    private final SkyliteStatement stmt;
    private final FrameReader.Frames ex;
    private final List<Column> columns;
    private final long maxRows;
    private Object[] row;
    private long rowNumber;
    private boolean done, wasNull;
    private volatile boolean closed;
    private int fetchSize;

    SkyliteResultSet(SkyliteStatement stmt, FrameReader.Frames ex, List<Column> columns, long maxRows, int fetchSize) {
        this.stmt = stmt;
        this.ex = ex;
        this.columns = columns;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
    }

    private void checkOpen() throws SQLException {
        if (closed) {
            throw Errors.closed("result set");
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkOpen();
        row = null;
        if (done) {
            return false;
        }
        if (maxRows > 0 && rowNumber >= maxRows) {
            finish();
            return false;
        }
        Frame f;
        try {
            f = ex.next();
        } catch (SQLException e) {
            done = true;
            throw e;
        }
        switch (f) {
            case Row r -> {
                row = r.values();
                rowNumber++;
                return true;
            }
            case Done d -> {
                stmt.conn.session().raise(d.txn());
                finish();
                return false;
            }
            case Failure e -> {
                finish();
                throw Errors.map(e);
            }
            case null, default -> {
                finish();
                throw Errors.transport("malformed response: an unexpected frame among rows", null);
            }
        }
    }

    private void finish() {
        done = true;
        ex.close();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            row = null;
            if (!done) {
                finish();
            }
            stmt.resultClosed(this);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private Object value(int i) throws SQLException {
        checkOpen();
        if (row == null) {
            throw new SQLNonTransientException("skylite: the result set is not on a row", "24000");
        }
        if (i < 1 || i > columns.size()) {
            throw new SQLNonTransientException("skylite: column index " + i + " is outside 1 to " + columns.size(), "07009");
        }
        Object v = row[i - 1];
        wasNull = v == null;
        return v;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkOpen();
        return wasNull;
    }

    @Override
    public int findColumn(String label) throws SQLException {
        checkOpen();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(label)) {
                return i + 1; // the first match, as JDBC requires
            }
        }
        throw new SQLNonTransientException("skylite: no column labelled " + label, "42S22");
    }

    // ---- conversions ----

    private static SQLException cannot(Object v, String to) {
        return Errors.data("cannot read " + storageClass(v) + " as " + to, "22018");
    }

    static String storageClass(Object v) {
        return switch (v) {
            case null -> "NULL";
            case Long l -> "INTEGER";
            case Double d -> "REAL";
            case String s -> "TEXT";
            case TextBytes t -> "TEXT";
            default -> "BLOB";
        };
    }

    @Override
    public String getString(int i) throws SQLException {
        Object v = value(i);
        return switch (v) {
            case null -> null;
            case String s -> s;
            case Long l -> l.toString();
            case Double d -> d.toString();
            case TextBytes t -> throw Errors.data("TEXT that is not valid UTF-8; read it with getBytes", "22021");
            default -> throw cannot(v, "a string");
        };
    }

    @Override
    public long getLong(int i) throws SQLException {
        Object v = value(i);
        return switch (v) {
            case null -> 0;
            case Long l -> l;
            case Double d -> {
                if (d != Math.rint(d) || d < -0x1p63 || d >= 0x1p63) {
                    throw Errors.data("REAL " + d + " is not an integer in the 64-bit range", "22003");
                }
                yield d.longValue();
            }
            case String s -> {
                try {
                    yield Long.parseLong(s);
                } catch (NumberFormatException e) {
                    throw cannot(v, "an integer");
                }
            }
            default -> throw cannot(v, "an integer");
        };
    }

    private long ranged(int i, long lo, long hi, String type) throws SQLException {
        long v = getLong(i);
        if (v < lo || v > hi) {
            throw Errors.data("INTEGER " + v + " is outside the range of " + type, "22003");
        }
        return v;
    }

    @Override
    public int getInt(int i) throws SQLException {
        return (int) ranged(i, Integer.MIN_VALUE, Integer.MAX_VALUE, "int");
    }

    @Override
    public short getShort(int i) throws SQLException {
        return (short) ranged(i, Short.MIN_VALUE, Short.MAX_VALUE, "short");
    }

    @Override
    public byte getByte(int i) throws SQLException {
        return (byte) ranged(i, Byte.MIN_VALUE, Byte.MAX_VALUE, "byte");
    }

    /** ADR 0005: INTEGER 0 is false and 1 is true; any other value, and every other storage class, is an error. */
    @Override
    public boolean getBoolean(int i) throws SQLException {
        Object v = value(i);
        if (v == null) {
            return false;
        }
        if (v instanceof Long l && (l == 0 || l == 1)) {
            return l == 1;
        }
        throw Errors.data("only INTEGER 0 and 1 read as a boolean, not this " + storageClass(v), "22018");
    }

    @Override
    public double getDouble(int i) throws SQLException {
        Object v = value(i);
        return switch (v) {
            case null -> 0;
            case Double d -> d;
            case Long l -> l.doubleValue();
            case String s -> {
                try {
                    yield Double.parseDouble(s);
                } catch (NumberFormatException e) {
                    throw cannot(v, "a double");
                }
            }
            default -> throw cannot(v, "a double");
        };
    }

    @Override
    public float getFloat(int i) throws SQLException {
        double d = getDouble(i);
        float f = (float) d;
        if (Float.isInfinite(f) && !Double.isInfinite(d)) {
            throw Errors.data("REAL " + d + " is outside the range of float", "22003");
        }
        return f;
    }

    /** TEXT is read exactly, as ADR 0005 stores exact decimals. */
    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        Object v = value(i);
        return switch (v) {
            case null -> null;
            case Long l -> BigDecimal.valueOf(l);
            case Double d -> {
                if (Double.isInfinite(d)) {
                    throw Errors.data("an infinite REAL has no decimal value", "22003");
                }
                yield new BigDecimal(d.toString());
            }
            case String s -> {
                try {
                    yield new BigDecimal(s);
                } catch (NumberFormatException e) {
                    throw cannot(v, "a decimal");
                }
            }
            default -> throw cannot(v, "a decimal");
        };
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int i, int scale) throws SQLException {
        BigDecimal b = getBigDecimal(i);
        return b == null ? null : b.setScale(scale, RoundingMode.HALF_UP);
    }

    /** A BLOB's bytes, or a TEXT value's UTF-8 bytes (its own bytes, when they are not valid UTF-8). */
    @Override
    public byte[] getBytes(int i) throws SQLException {
        Object v = value(i);
        return switch (v) {
            case null -> null;
            case byte[] b -> b.clone();
            case String s -> s.getBytes(StandardCharsets.UTF_8);
            case TextBytes t -> t.bytes().clone();
            default -> throw cannot(v, "bytes");
        };
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        byte[] b = getBytes(i);
        return b == null ? null : new SerialBlob(b);
    }

    /** A TEXT value in a date or timestamp form; null for NULL. */
    private Temporal.Stamp stamp(int i, String to) throws SQLException {
        Object v = value(i);
        if (v == null) {
            return null;
        }
        Temporal.Stamp s = v instanceof String text ? Temporal.stamp(text) : null;
        if (s == null) {
            throw Errors.data("cannot read this " + storageClass(v) + " as " + to
                    + ": only TEXT in the date and timestamp forms of ADR 0005 is", "22007");
        }
        return s;
    }

    private LocalDateTime local(int i, String to) throws SQLException {
        Temporal.Stamp s = stamp(i, to);
        if (s == null) {
            return null;
        }
        if (s.offsetSeconds() != null) {
            throw Errors.data("text with an offset cannot be read as a local " + to, "22007");
        }
        return s.local();
    }

    private Instant instant(int i) throws SQLException {
        Temporal.Stamp s = stamp(i, "an instant");
        if (s == null) {
            return null;
        }
        Instant in = s.instant();
        if (in == null) {
            throw Errors.data("an instant outside the years 0001 to 9999 in UTC", "22008");
        }
        return in;
    }

    private LocalTime timeOfDay(int i) throws SQLException {
        Object v = value(i);
        if (v == null) {
            return null;
        }
        LocalTime t = v instanceof String s ? Temporal.timeOfDay(s) : null;
        if (t == null) {
            throw Errors.data("cannot read this " + storageClass(v) + " as a time of day", "22007");
        }
        return t;
    }

    /** A date: TEXT {@code YYYY-MM-DD}, or the date of a local timestamp. */
    @Override
    public Date getDate(int i) throws SQLException {
        LocalDateTime t = local(i, "date");
        return t == null ? null : Date.valueOf(t.toLocalDate());
    }

    @Override
    public Time getTime(int i) throws SQLException {
        LocalTime t = timeOfDay(i);
        return t == null ? null : Time.valueOf(t);
    }

    /** Text without an offset is a local timestamp in the JVM's zone, as JDBC's TIMESTAMP; with one, an instant. */
    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return getTimestamp(i, (Calendar) null);
    }

    @Override
    public Date getDate(int i, Calendar cal) throws SQLException {
        if (cal == null) {
            return getDate(i);
        }
        LocalDateTime t = local(i, "date");
        return t == null ? null : new Date(t.toLocalDate().atStartOfDay(zone(cal)).toInstant().toEpochMilli());
    }

    @Override
    public Time getTime(int i, Calendar cal) throws SQLException {
        if (cal == null) {
            return getTime(i);
        }
        LocalTime t = timeOfDay(i);
        return t == null
                ? null
                : new Time(t.atDate(LocalDate.EPOCH).atZone(zone(cal)).toInstant().toEpochMilli());
    }

    /** Text without an offset is read in the calendar's zone; text with one is the instant it names. */
    @Override
    public Timestamp getTimestamp(int i, Calendar cal) throws SQLException {
        Temporal.Stamp s = stamp(i, "a timestamp");
        if (s == null) {
            return null;
        }
        if (s.offsetSeconds() != null) {
            Instant in = s.instant();
            if (in == null) {
                throw Errors.data("an instant outside the years 0001 to 9999 in UTC", "22008");
            }
            return Timestamp.from(in);
        }
        return cal == null ? Timestamp.valueOf(s.local()) : Timestamp.from(s.local().atZone(zone(cal)).toInstant());
    }

    private static ZoneId zone(Calendar cal) {
        return cal.getTimeZone().toZoneId();
    }

    /**
     * The value by its storage class: Long, Double, String or byte[]; TEXT that is not valid UTF-8 is an error (read
     * it with getBytes). In a column whose type name is DATE, DATETIME or TIMESTAMP, TEXT in a date or timestamp form
     * reads as java.sql.Timestamp, as getTimestamp reads it, or as java.sql.Date for the date form alone in a DATE
     * column; other TEXT reads as a String.
     */
    @Override
    public Object getObject(int i) throws SQLException {
        Object v = value(i);
        switch (v) {
            case TextBytes t -> throw Errors.data("TEXT that is not valid UTF-8; read it with getBytes", "22021");
            case byte[] b -> {
                return b.clone();
            }
            case String s -> {
                int type = SkyliteResultSetMetaData.jdbcType(columns.get(i - 1).declType());
                Temporal.Stamp st = type == Types.DATE || type == Types.TIMESTAMP ? Temporal.stamp(s) : null;
                if (st == null || (st.offsetSeconds() != null && st.instant() == null)) {
                    return s;
                }
                if (type == Types.DATE && s.length() == 10) { // the date form alone
                    return Date.valueOf(st.local().toLocalDate());
                }
                return getTimestamp(i);
            }
            case null, default -> {
                return v;
            }
        }
    }

    @Override
    public <T> T getObject(int i, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLNonTransientException("skylite: getObject with a null type", "HY009");
        }
        Object v = value(i);
        if (v == null) {
            return null;
        }
        Object r;
        if (type == Object.class) {
            r = getObject(i);
        } else if (type == String.class) {
            r = getString(i);
        } else if (type == Long.class) {
            r = getLong(i);
        } else if (type == Integer.class) {
            r = getInt(i);
        } else if (type == Short.class) {
            r = getShort(i);
        } else if (type == Byte.class) {
            r = getByte(i);
        } else if (type == Double.class) {
            r = getDouble(i);
        } else if (type == Float.class) {
            r = getFloat(i);
        } else if (type == Boolean.class) {
            r = getBoolean(i);
        } else if (type == BigDecimal.class) {
            r = getBigDecimal(i);
        } else if (type == BigInteger.class) {
            try {
                // A REAL's exact binary value: its shortest decimal string can differ from the integer it holds.
                r = v instanceof Double d && Double.isFinite(d)
                        ? new BigDecimal(d).toBigIntegerExact()
                        : getBigDecimal(i).toBigIntegerExact();
            } catch (ArithmeticException e) {
                throw Errors.data("this " + storageClass(v) + " is not an integer", "22018");
            }
        } else if (type == Character.class) {
            String s = getString(i);
            if (s.length() != 1) {
                throw Errors.data("text of " + s.length() + " chars is not one character", "22018");
            }
            r = s.charAt(0);
        } else if (type == byte[].class) {
            r = getBytes(i);
        } else if (type == LocalDate.class) {
            r = local(i, "date").toLocalDate();
        } else if (type == LocalDateTime.class) {
            r = local(i, "timestamp");
        } else if (type == LocalTime.class) {
            r = timeOfDay(i);
        } else if (type == Instant.class) {
            r = instant(i);
        } else if (type == OffsetDateTime.class) {
            r = instant(i).atOffset(ZoneOffset.UTC);
        } else if (type == ZonedDateTime.class) {
            r = instant(i).atZone(ZoneOffset.UTC);
        } else if (type == Date.class) {
            r = getDate(i);
        } else if (type == Time.class) {
            r = getTime(i);
        } else if (type == Timestamp.class) {
            r = getTimestamp(i);
        } else if (type == Blob.class) {
            r = getBlob(i);
        } else {
            throw Errors.unsupported("getObject as " + type.getName());
        }
        return type.cast(r);
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getObject(i);
        }
        throw Errors.unsupported("a type map");
    }

    // ---- by label ----

    @Override
    public String getString(String l) throws SQLException {
        return getString(findColumn(l));
    }

    @Override
    public boolean getBoolean(String l) throws SQLException {
        return getBoolean(findColumn(l));
    }

    @Override
    public byte getByte(String l) throws SQLException {
        return getByte(findColumn(l));
    }

    @Override
    public short getShort(String l) throws SQLException {
        return getShort(findColumn(l));
    }

    @Override
    public int getInt(String l) throws SQLException {
        return getInt(findColumn(l));
    }

    @Override
    public long getLong(String l) throws SQLException {
        return getLong(findColumn(l));
    }

    @Override
    public float getFloat(String l) throws SQLException {
        return getFloat(findColumn(l));
    }

    @Override
    public double getDouble(String l) throws SQLException {
        return getDouble(findColumn(l));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String l, int scale) throws SQLException {
        return getBigDecimal(findColumn(l), scale);
    }

    @Override
    public byte[] getBytes(String l) throws SQLException {
        return getBytes(findColumn(l));
    }

    @Override
    public Date getDate(String l) throws SQLException {
        return getDate(findColumn(l));
    }

    @Override
    public Time getTime(String l) throws SQLException {
        return getTime(findColumn(l));
    }

    @Override
    public Timestamp getTimestamp(String l) throws SQLException {
        return getTimestamp(findColumn(l));
    }

    @Override
    public Object getObject(String l) throws SQLException {
        return getObject(findColumn(l));
    }

    @Override
    public BigDecimal getBigDecimal(String l) throws SQLException {
        return getBigDecimal(findColumn(l));
    }

    @Override
    public Object getObject(String l, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(l), map);
    }

    @Override
    public Blob getBlob(String l) throws SQLException {
        return getBlob(findColumn(l));
    }

    @Override
    public Date getDate(String l, Calendar cal) throws SQLException {
        return getDate(findColumn(l), cal);
    }

    @Override
    public Time getTime(String l, Calendar cal) throws SQLException {
        return getTime(findColumn(l), cal);
    }

    @Override
    public Timestamp getTimestamp(String l, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(l), cal);
    }

    @Override
    public <T> T getObject(String l, Class<T> type) throws SQLException {
        return getObject(findColumn(l), type);
    }

    @Override
    public String getNString(int i) throws SQLException {
        return getString(i);
    }

    @Override
    public String getNString(String l) throws SQLException {
        return getString(l);
    }

    // ---- state ----

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw Errors.unsupported("named cursors");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        return new SkyliteResultSetMetaData(columns);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw Errors.unsupported("isBeforeFirst on a forward-only result set");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkOpen();
        return done && rowNumber > 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkOpen();
        return row != null && rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw Errors.unsupported("isLast on a forward-only result set");
    }

    @Override
    public int getRow() throws SQLException {
        checkOpen();
        return row == null ? 0 : (int) Math.min(rowNumber, Integer.MAX_VALUE);
    }

    private static SQLException forwardOnly() {
        return Errors.unsupported("moving a TYPE_FORWARD_ONLY result set other than by next");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw forwardOnly();
    }

    @Override
    public void afterLast() throws SQLException {
        throw forwardOnly();
    }

    @Override
    public boolean first() throws SQLException {
        throw forwardOnly();
    }

    @Override
    public boolean last() throws SQLException {
        throw forwardOnly();
    }

    @Override
    public boolean absolute(int r) throws SQLException {
        throw forwardOnly();
    }

    @Override
    public boolean relative(int r) throws SQLException {
        throw forwardOnly();
    }

    @Override
    public boolean previous() throws SQLException {
        throw forwardOnly();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != FETCH_FORWARD) {
            throw forwardOnly();
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return FETCH_FORWARD;
    }

    /** A hint only, as on the statement, whose fetch size it starts with. */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) {
            throw new SQLNonTransientException("skylite: negative fetch size", "HY092");
        }
        fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        checkOpen();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkOpen();
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkOpen();
        return stmt;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface != null && iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLNonTransientException("skylite: not a wrapper for " + (iface == null ? null : iface.getName()), "HY000");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isInstance(this);
    }

    // ---- not supported: streams, LOB types other than Blob, and every update method ----

    private static SQLException notSupported(String what) {
        return Errors.unsupported(what.toLowerCase(Locale.ROOT));
    }

    private static SQLException readOnly() {
        return Errors.unsupported("updating a CONCUR_READ_ONLY result set");
    }

    @Override public InputStream getAsciiStream(int i) throws SQLException { throw notSupported("getAsciiStream"); }
    @Override @Deprecated public InputStream getUnicodeStream(int i) throws SQLException { throw notSupported("getUnicodeStream"); }
    @Override public InputStream getBinaryStream(int i) throws SQLException { throw notSupported("getBinaryStream"); }
    @Override public InputStream getAsciiStream(String l) throws SQLException { throw notSupported("getAsciiStream"); }
    @Override @Deprecated public InputStream getUnicodeStream(String l) throws SQLException { throw notSupported("getUnicodeStream"); }
    @Override public InputStream getBinaryStream(String l) throws SQLException { throw notSupported("getBinaryStream"); }
    @Override public Reader getCharacterStream(int i) throws SQLException { throw notSupported("getCharacterStream"); }
    @Override public Reader getCharacterStream(String l) throws SQLException { throw notSupported("getCharacterStream"); }
    @Override public Reader getNCharacterStream(int i) throws SQLException { throw notSupported("getNCharacterStream"); }
    @Override public Reader getNCharacterStream(String l) throws SQLException { throw notSupported("getNCharacterStream"); }
    @Override public Ref getRef(int i) throws SQLException { throw notSupported("getRef"); }
    @Override public Clob getClob(int i) throws SQLException { throw notSupported("getClob"); }
    @Override public Array getArray(int i) throws SQLException { throw notSupported("getArray"); }
    @Override public Ref getRef(String l) throws SQLException { throw notSupported("getRef"); }
    @Override public Clob getClob(String l) throws SQLException { throw notSupported("getClob"); }
    @Override public Array getArray(String l) throws SQLException { throw notSupported("getArray"); }
    @Override public URL getURL(int i) throws SQLException { throw notSupported("getURL"); }
    @Override public URL getURL(String l) throws SQLException { throw notSupported("getURL"); }
    @Override public RowId getRowId(int i) throws SQLException { throw notSupported("getRowId"); }
    @Override public RowId getRowId(String l) throws SQLException { throw notSupported("getRowId"); }
    @Override public NClob getNClob(int i) throws SQLException { throw notSupported("getNClob"); }
    @Override public NClob getNClob(String l) throws SQLException { throw notSupported("getNClob"); }
    @Override public SQLXML getSQLXML(int i) throws SQLException { throw notSupported("getSQLXML"); }
    @Override public SQLXML getSQLXML(String l) throws SQLException { throw notSupported("getSQLXML"); }

    @Override public void updateNull(int i) throws SQLException { throw readOnly(); }
    @Override public void updateBoolean(int i, boolean x) throws SQLException { throw readOnly(); }
    @Override public void updateByte(int i, byte x) throws SQLException { throw readOnly(); }
    @Override public void updateShort(int i, short x) throws SQLException { throw readOnly(); }
    @Override public void updateInt(int i, int x) throws SQLException { throw readOnly(); }
    @Override public void updateLong(int i, long x) throws SQLException { throw readOnly(); }
    @Override public void updateFloat(int i, float x) throws SQLException { throw readOnly(); }
    @Override public void updateDouble(int i, double x) throws SQLException { throw readOnly(); }
    @Override public void updateBigDecimal(int i, BigDecimal x) throws SQLException { throw readOnly(); }
    @Override public void updateString(int i, String x) throws SQLException { throw readOnly(); }
    @Override public void updateBytes(int i, byte[] x) throws SQLException { throw readOnly(); }
    @Override public void updateDate(int i, Date x) throws SQLException { throw readOnly(); }
    @Override public void updateTime(int i, Time x) throws SQLException { throw readOnly(); }
    @Override public void updateTimestamp(int i, Timestamp x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateObject(int i, Object x, int scale) throws SQLException { throw readOnly(); }
    @Override public void updateObject(int i, Object x) throws SQLException { throw readOnly(); }
    @Override public void updateNull(String l) throws SQLException { throw readOnly(); }
    @Override public void updateBoolean(String l, boolean x) throws SQLException { throw readOnly(); }
    @Override public void updateByte(String l, byte x) throws SQLException { throw readOnly(); }
    @Override public void updateShort(String l, short x) throws SQLException { throw readOnly(); }
    @Override public void updateInt(String l, int x) throws SQLException { throw readOnly(); }
    @Override public void updateLong(String l, long x) throws SQLException { throw readOnly(); }
    @Override public void updateFloat(String l, float x) throws SQLException { throw readOnly(); }
    @Override public void updateDouble(String l, double x) throws SQLException { throw readOnly(); }
    @Override public void updateBigDecimal(String l, BigDecimal x) throws SQLException { throw readOnly(); }
    @Override public void updateString(String l, String x) throws SQLException { throw readOnly(); }
    @Override public void updateBytes(String l, byte[] x) throws SQLException { throw readOnly(); }
    @Override public void updateDate(String l, Date x) throws SQLException { throw readOnly(); }
    @Override public void updateTime(String l, Time x) throws SQLException { throw readOnly(); }
    @Override public void updateTimestamp(String l, Timestamp x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String l, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String l, InputStream x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String l, Reader x, int n) throws SQLException { throw readOnly(); }
    @Override public void updateObject(String l, Object x, int scale) throws SQLException { throw readOnly(); }
    @Override public void updateObject(String l, Object x) throws SQLException { throw readOnly(); }
    @Override public void insertRow() throws SQLException { throw readOnly(); }
    @Override public void updateRow() throws SQLException { throw readOnly(); }
    @Override public void deleteRow() throws SQLException { throw readOnly(); }
    @Override public void refreshRow() throws SQLException { throw readOnly(); }
    @Override public void cancelRowUpdates() throws SQLException { throw readOnly(); }
    @Override public void moveToInsertRow() throws SQLException { throw readOnly(); }
    @Override public void moveToCurrentRow() throws SQLException { throw readOnly(); }
    @Override public void updateRef(int i, Ref x) throws SQLException { throw readOnly(); }
    @Override public void updateRef(String l, Ref x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, Blob x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String l, Blob x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Clob x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String l, Clob x) throws SQLException { throw readOnly(); }
    @Override public void updateArray(int i, Array x) throws SQLException { throw readOnly(); }
    @Override public void updateArray(String l, Array x) throws SQLException { throw readOnly(); }
    @Override public void updateRowId(int i, RowId x) throws SQLException { throw readOnly(); }
    @Override public void updateRowId(String l, RowId x) throws SQLException { throw readOnly(); }
    @Override public void updateNString(int i, String x) throws SQLException { throw readOnly(); }
    @Override public void updateNString(String l, String x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, NClob x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String l, NClob x) throws SQLException { throw readOnly(); }
    @Override public void updateSQLXML(int i, SQLXML x) throws SQLException { throw readOnly(); }
    @Override public void updateSQLXML(String l, SQLXML x) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(String l, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String l, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String l, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String l, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String l, InputStream x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String l, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String l, Reader x, long n) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNCharacterStream(String l, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateAsciiStream(String l, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBinaryStream(String l, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateCharacterStream(String l, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(int i, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateBlob(String l, InputStream x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateClob(String l, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(int i, Reader x) throws SQLException { throw readOnly(); }
    @Override public void updateNClob(String l, Reader x) throws SQLException { throw readOnly(); }
}
