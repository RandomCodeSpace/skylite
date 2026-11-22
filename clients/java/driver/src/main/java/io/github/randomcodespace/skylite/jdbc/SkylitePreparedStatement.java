package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Column;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Columns;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Params;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * A prepared statement sends nothing until it runs: each execution sends its SQL and parameters as one {@code query}.
 * Its metadata comes from a {@code describe}, sent only when asked for and kept for the statement's life. Parameters
 * bind by index, {@code ?}, {@code ?NNN} and named ones alike; a value the protocol cannot carry (NaN, a year outside
 * 0001 to 9999, a lone surrogate) is refused when it is set, before anything is sent.
 */
final class SkylitePreparedStatement extends SkyliteStatement implements PreparedStatement {
    private static final Object UNSET = new Object();
    private final String sql;
    private final String keyLabel; // null unless generated keys were asked for
    private final List<List<Object>> batch = new ArrayList<>();
    private Object[] params = new Object[0];
    private List<Column> describedColumns;
    private int describedParams;

    SkylitePreparedStatement(SkyliteConnection conn, String sql, String keyLabel) {
        super(conn);
        this.sql = sql;
        this.keyLabel = keyLabel;
    }

    private void set(int i, Object v) throws SQLException {
        checkOpen();
        if (i < 1) {
            throw new SQLNonTransientException("skylite: parameter index " + i + " is below 1", "07009");
        }
        if (i > params.length) {
            int old = params.length;
            params = Arrays.copyOf(params, Math.max(i, old * 2));
            Arrays.fill(params, old, params.length, UNSET);
        }
        params[i - 1] = v;
    }

    private List<Object> bound() throws SQLException {
        int n = params.length;
        while (n > 0 && params[n - 1] == UNSET) {
            n--;
        }
        List<Object> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (params[i] == UNSET) {
                throw new SQLNonTransientException("skylite: parameter " + (i + 1) + " is not set", "07001");
            }
            out.add(params[i]);
        }
        return out;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return query(run(sql, bound(), false, keyLabel));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return count(executeLargeUpdate());
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        return update(run(sql, bound(), !conn.readOnly(), keyLabel) != null);
    }

    @Override
    public boolean execute() throws SQLException {
        return run(sql, bound(), false, keyLabel) != null;
    }

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();
        params = new Object[0];
    }

    // ---- describe ----

    private void describe() throws SQLException {
        checkOpen();
        if (describedColumns != null) {
            return;
        }
        Session s = conn.session();
        List<Column> cols = List.of();
        int params = 0;
        begin();
        try (Exchange ex = Exchange.send(
                s, FrameReader.Kind.DESCRIBE, Request.describe(sql, s.watermark()), getQueryTimeout(), this)) {
            Frame f;
            while ((f = ex.next()) != null) {
                switch (f) {
                    case Columns c -> cols = c.columns();
                    case Params p -> params += p.names().size();
                    case Done d -> s.raise(d.txn());
                    case Failure e -> throw Errors.map(e);
                    default -> throw Errors.transport("malformed response: a row in a describe answer", null);
                }
            }
        }
        describedColumns = cols;
        describedParams = params;
    }

    /** The result columns: of the open result set, or from a describe. Null for a statement without columns. */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet rs = getResultSet();
        if (rs != null && !rs.isClosed()) {
            return rs.getMetaData();
        }
        describe();
        return describedColumns.isEmpty() ? null : new SkyliteResultSetMetaData(describedColumns);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        describe();
        return new SkyliteParameterMetaData(describedParams);
    }

    // ---- setters ----

    @Override
    public void setNull(int i, int sqlType) throws SQLException {
        set(i, null);
    }

    @Override
    public void setNull(int i, int sqlType, String typeName) throws SQLException {
        set(i, null);
    }

    /** INTEGER 0 or 1, ADR 0005's boolean. */
    @Override
    public void setBoolean(int i, boolean x) throws SQLException {
        set(i, x ? 1L : 0L);
    }

    @Override
    public void setByte(int i, byte x) throws SQLException {
        set(i, (long) x);
    }

    @Override
    public void setShort(int i, short x) throws SQLException {
        set(i, (long) x);
    }

    @Override
    public void setInt(int i, int x) throws SQLException {
        set(i, (long) x);
    }

    @Override
    public void setLong(int i, long x) throws SQLException {
        set(i, x);
    }

    @Override
    public void setFloat(int i, float x) throws SQLException {
        setDouble(i, x);
    }

    @Override
    public void setDouble(int i, double x) throws SQLException {
        if (Double.isNaN(x)) {
            throw Errors.data("NaN cannot be sent: binding it in SQLite stores NULL", "22023");
        }
        set(i, x);
    }

    /** TEXT in plain notation, as ADR 0005 keeps exact decimals. */
    @Override
    public void setBigDecimal(int i, BigDecimal x) throws SQLException {
        set(i, x == null ? null : x.toPlainString());
    }

    @Override
    public void setString(int i, String x) throws SQLException {
        if (x != null) {
            try {
                Values.checkString(x);
            } catch (Values.Malformed e) {
                throw Errors.data("a string with a lone surrogate cannot be sent as UTF-8", "22021");
            }
        }
        set(i, x);
    }

    @Override
    public void setNString(int i, String x) throws SQLException {
        setString(i, x);
    }

    @Override
    public void setBytes(int i, byte[] x) throws SQLException {
        set(i, x == null ? null : x.clone());
    }

    private static void year(int y) throws SQLException {
        if (!Temporal.validYear(y)) {
            throw Errors.data("a temporal value must be in the years 0001 to 9999", "22008");
        }
    }

    private void setText(int i, LocalDate d) throws SQLException {
        year(d.getYear());
        set(i, Temporal.format(d));
    }

    private void setText(int i, LocalTime t) throws SQLException {
        set(i, Temporal.format(t));
    }

    private void setText(int i, LocalDateTime t) throws SQLException {
        year(t.getYear());
        set(i, Temporal.format(t));
    }

    private void setText(int i, Instant t) throws SQLException {
        year(LocalDateTime.ofInstant(t, java.time.ZoneOffset.UTC).getYear());
        set(i, Temporal.format(t));
    }

    /** A date, written {@code YYYY-MM-DD}. */
    @Override
    public void setDate(int i, Date x) throws SQLException {
        if (x == null) {
            set(i, null);
        } else {
            setText(i, x.toLocalDate());
        }
    }

    @Override
    public void setTime(int i, Time x) throws SQLException {
        if (x == null) {
            set(i, null);
        } else {
            setText(i, x.toLocalTime());
        }
    }

    /** A local timestamp, its wall-clock fields in the JVM's zone, as JDBC's TIMESTAMP. */
    @Override
    public void setTimestamp(int i, Timestamp x) throws SQLException {
        if (x == null) {
            set(i, null);
        } else {
            setText(i, x.toLocalDateTime());
        }
    }

    @Override
    public void setDate(int i, Date x, Calendar cal) throws SQLException {
        if (x == null || cal == null) {
            setDate(i, x);
        } else {
            setText(i, Instant.ofEpochMilli(x.getTime()).atZone(cal.getTimeZone().toZoneId()).toLocalDate());
        }
    }

    @Override
    public void setTime(int i, Time x, Calendar cal) throws SQLException {
        if (x == null || cal == null) {
            setTime(i, x);
        } else {
            setText(i, Instant.ofEpochMilli(x.getTime()).atZone(cal.getTimeZone().toZoneId()).toLocalTime());
        }
    }

    /** The timestamp's wall-clock fields in the calendar's zone. */
    @Override
    public void setTimestamp(int i, Timestamp x, Calendar cal) throws SQLException {
        if (x == null || cal == null) {
            setTimestamp(i, x);
        } else {
            setText(i, x.toInstant().atZone(cal.getTimeZone().toZoneId()).toLocalDateTime());
        }
    }

    /**
     * Java values to protocol values: null, integers, floating point, BigDecimal, String, byte[], Boolean, the
     * java.time types (an Instant, OffsetDateTime or ZonedDateTime as its instant in UTC) and the java.sql temporal
     * types.
     */
    @Override
    public void setObject(int i, Object x) throws SQLException {
        switch (x) {
            case null -> set(i, null);
            case Long v -> setLong(i, v);
            case Integer v -> setInt(i, v);
            case Short v -> setShort(i, v);
            case Byte v -> setByte(i, v);
            case Boolean v -> setBoolean(i, v);
            case Double v -> setDouble(i, v);
            case Float v -> setFloat(i, v);
            case BigDecimal v -> setBigDecimal(i, v);
            case java.math.BigInteger v -> setBigDecimal(i, new BigDecimal(v));
            case String v -> setString(i, v);
            case Character v -> setString(i, v.toString());
            case byte[] v -> setBytes(i, v);
            case Timestamp v -> setTimestamp(i, v);
            case Date v -> setDate(i, v);
            case Time v -> setTime(i, v);
            case LocalDate v -> setText(i, v);
            case LocalTime v -> setText(i, v);
            case LocalDateTime v -> setText(i, v);
            case Instant v -> setText(i, v);
            case OffsetDateTime v -> setText(i, v.toInstant());
            case ZonedDateTime v -> setText(i, v.toInstant());
            default -> throw Errors.unsupported("a parameter of type " + x.getClass().getName());
        }
    }

    /** The target type is not used: the value's own type decides its storage class. */
    @Override
    public void setObject(int i, Object x, int targetSqlType) throws SQLException {
        setObject(i, x);
    }

    @Override
    public void setObject(int i, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(i, x);
    }

    @Override
    public void setObject(int i, Object x, SQLType targetSqlType) throws SQLException {
        setObject(i, x);
    }

    @Override
    public void setObject(int i, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(i, x);
    }

    // ---- the Statement methods that take SQL do not apply to a PreparedStatement ----

    private static SQLException sqlGiven() {
        return new SQLNonTransientException("skylite: a PreparedStatement runs its own SQL", "HY000");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        batch.add(bound());
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw sqlGiven();
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batch.clear();
    }

    /** Runs each parameter set as a statement of its own, in order, as {@link SkyliteStatement#executeLargeBatch}. */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkOpen();
        List<List<Object>> sets = List.copyOf(batch);
        batch.clear();
        long[] counts = new long[sets.size()];
        List<Long> keys = new ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            try {
                counts[i] = update(run(sql, sets.get(i), !conn.readOnly(), keyLabel) != null);
            } catch (SQLException e) {
                generatedKeys(keys); // the keys of the entries before it, as its update counts are
                throw batchFailed(e, Arrays.copyOf(counts, i));
            }
            keys.addAll(generatedKeys());
        }
        generatedKeys(keys);
        return counts;
    }

    // ---- not supported ----

    @Override public void setAsciiStream(int i, InputStream x, int n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override @Deprecated public void setUnicodeStream(int i, InputStream x, int n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setBinaryStream(int i, InputStream x, int n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setCharacterStream(int i, Reader x, int n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setRef(int i, Ref x) throws SQLException { throw Errors.unsupported("Ref parameters"); }
    @Override public void setBlob(int i, Blob x) throws SQLException { throw Errors.unsupported("Blob parameters"); }
    @Override public void setClob(int i, Clob x) throws SQLException { throw Errors.unsupported("Clob parameters"); }
    @Override public void setArray(int i, Array x) throws SQLException { throw Errors.unsupported("Array parameters"); }
    @Override public void setURL(int i, URL x) throws SQLException { throw Errors.unsupported("URL parameters"); }
    @Override public void setRowId(int i, RowId x) throws SQLException { throw Errors.unsupported("RowId parameters"); }
    @Override public void setNCharacterStream(int i, Reader x, long n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setNClob(int i, NClob x) throws SQLException { throw Errors.unsupported("NClob parameters"); }
    @Override public void setClob(int i, Reader x, long n) throws SQLException { throw Errors.unsupported("Clob parameters"); }
    @Override public void setBlob(int i, InputStream x, long n) throws SQLException { throw Errors.unsupported("Blob parameters"); }
    @Override public void setNClob(int i, Reader x, long n) throws SQLException { throw Errors.unsupported("NClob parameters"); }
    @Override public void setSQLXML(int i, SQLXML x) throws SQLException { throw Errors.unsupported("SQLXML parameters"); }
    @Override public void setAsciiStream(int i, InputStream x, long n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setBinaryStream(int i, InputStream x, long n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setCharacterStream(int i, Reader x, long n) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setAsciiStream(int i, InputStream x) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setBinaryStream(int i, InputStream x) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setCharacterStream(int i, Reader x) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setNCharacterStream(int i, Reader x) throws SQLException { throw Errors.unsupported("stream parameters"); }
    @Override public void setClob(int i, Reader x) throws SQLException { throw Errors.unsupported("Clob parameters"); }
    @Override public void setBlob(int i, InputStream x) throws SQLException { throw Errors.unsupported("Blob parameters"); }
    @Override public void setNClob(int i, Reader x) throws SQLException { throw Errors.unsupported("NClob parameters"); }
}
