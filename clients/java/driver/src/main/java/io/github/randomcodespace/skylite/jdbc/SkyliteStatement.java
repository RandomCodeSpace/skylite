package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Columns;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A statement sends each execution as one stateless {@code query}: the server runs pure reads and refuses anything
 * else with {@code needs_channel}, which raises {@link java.sql.SQLFeatureNotSupportedException}. Rows stream; at most
 * one result set is open, and the next execution or {@link #close} releases its response.
 */
class SkyliteStatement implements Statement, Exchange.Owner {
    final SkyliteConnection conn;
    /** Orders close, cancel, the submission of each request and the publication of its result set. */
    final Object lock = new Object();
    private final List<Exchange> live = new ArrayList<>(2); // guarded by lock: an open cursor's, a describe's
    private volatile SkyliteResultSet result; // written under lock
    private long updateCount = -1;
    private int queryTimeout;
    private long maxRows;
    private int fetchSize;
    private boolean poolable;
    private boolean closeOnCompletion;
    private volatile boolean closed; // written under lock
    private boolean cancelRequested; // guarded by lock

    SkyliteStatement(SkyliteConnection conn) {
        this.conn = conn;
    }

    void checkOpen() throws SQLException {
        if (closed) {
            throw Errors.closed("statement");
        }
        conn.checkOpen();
    }

    @Override
    public Object lock() {
        return lock;
    }

    @Override
    public SQLException admit(Exchange ex) {
        if (cancelRequested) {
            return Exchange.cancelled(null);
        }
        if (closed) {
            return Errors.closed("statement");
        }
        if (conn.isClosed()) {
            return Errors.closed("connection");
        }
        live.add(ex);
        return null;
    }

    @Override
    public void release(Exchange ex) {
        synchronized (lock) {
            live.remove(ex);
        }
    }

    @Override
    public SkyliteConnection connection() {
        return conn;
    }

    /** A new execution or describe: a cancel requested before it is not carried into it. */
    void begin() {
        synchronized (lock) {
            cancelRequested = false;
        }
    }

    /**
     * Sends one query and reads its first frame: the result set when the statement has result columns, otherwise
     * null, with its update count, which a read reports as 0.
     */
    SkyliteResultSet run(String sql, List<Object> params) throws SQLException {
        checkOpen();
        closeResult();
        begin();
        Session s = conn.session();
        byte[] body = Request.query(sql, params, s.watermark(), queryTimeout * 1000L);
        Exchange ex = Exchange.send(s, FrameReader.Kind.QUERY, body, queryTimeout, this);
        switch (ex.next()) {
            case Columns c -> {
                return publish(new SkyliteResultSet(this, ex, c.columns(), maxRows, fetchSize));
            }
            case Done d -> {
                ex.close();
                s.raise(d.txn());
                updateCount = d.rowsAffected();
                return null;
            }
            case Failure e -> {
                ex.close();
                throw Errors.map(e);
            }
            case null, default -> {
                ex.close();
                throw Errors.transport("malformed response: an unexpected first frame", null);
            }
        }
    }

    /** Makes rs the current result set, unless this statement closed first: then rs is released. */
    SkyliteResultSet publish(SkyliteResultSet rs) throws SQLException {
        synchronized (lock) {
            if (!closed) {
                result = rs;
                return rs;
            }
        }
        rs.close();
        throw Errors.closed("statement");
    }

    /** Called by the result set when it closes: with closeOnCompletion, that closes this statement. */
    void resultClosed(SkyliteResultSet rs) {
        boolean complete;
        synchronized (lock) {
            complete = rs == result && closeOnCompletion;
        }
        if (complete) {
            close();
        }
    }

    /** An update count as an int, for the methods that return one. */
    static int count(long n) throws SQLException {
        if (n > Integer.MAX_VALUE) {
            throw Errors.data("the update count " + n + " exceeds the range of int; use the Large method", "22003");
        }
        return (int) n;
    }

    /** Closes the current result set, if any, and returns it. */
    private SkyliteResultSet closeResult() {
        SkyliteResultSet r;
        synchronized (lock) {
            r = result;
            result = null;
            updateCount = -1;
        }
        if (r != null) {
            r.close();
        }
        return r;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return query(run(sql, List.of()));
    }

    static ResultSet query(SkyliteResultSet rs) throws SQLException {
        if (rs == null) {
            throw new SQLNonTransientException("skylite: the statement returned no result set", "07000");
        }
        return rs;
    }

    /** A write is refused by the server with needs_channel; a read without result columns returns its count, 0. */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        return count(executeLargeUpdate(sql));
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return update(run(sql, List.of()) != null);
    }

    long update(boolean hasResult) throws SQLException {
        if (hasResult) {
            closeResult();
            throw new SQLNonTransientException("skylite: executeUpdate on a statement that returns rows", "07000");
        }
        return updateCount;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return run(sql, List.of()) != null;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw Errors.writes();
        }
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw Errors.writes();
        }
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw Errors.writes();
        }
        return executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw Errors.writes();
    }

    /** Releases the result set's response, if any. Nothing is sent. */
    @Override
    public void close() {
        List<Exchange> running;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            running = List.copyOf(live);
        }
        closeResult();
        for (Exchange ex : running) {
            ex.close();
        }
        conn.forget(this);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();
        if (max != 0) {
            throw Errors.unsupported("a maximum field size");
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        return Math.toIntExact(Math.min(getLargeMaxRows(), Integer.MAX_VALUE));
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        checkOpen();
        return maxRows;
    }

    /** Rows past the limit are not read: the result set releases the response at the limit. */
    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkOpen();
        if (max < 0) {
            throw new SQLNonTransientException("skylite: negative max rows", "HY092");
        }
        maxRows = max;
    }

    /** Escape syntax is never processed: the SQL is sent as written. */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkOpen();
        return queryTimeout;
    }

    /**
     * Lowers the server's statement time for each execution, through timeout_ms, and abandons a response that has
     * not ended 2 s after it: either way {@link java.sql.SQLTimeoutException}, safe for a read, which leaves no
     * unknown outcome.
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();
        if (seconds < 0 || seconds > Integer.MAX_VALUE / 1000) {
            throw new SQLNonTransientException("skylite: query timeout outside 0 to 2147483 s", "HY092");
        }
        queryTimeout = seconds;
    }

    /**
     * Abandons what the statement runs, an execution or a describe and an open result set: each request whose
     * answer has not arrived, or its response, whose buffered rows are then not delivered. A stateless read
     * needs no cancel request (ADR 0005 decision 6): the server stops streaming when the response is dropped.
     */
    @Override
    public void cancel() throws SQLException {
        checkOpen();
        List<Exchange> running;
        synchronized (lock) {
            cancelRequested = true;
            running = List.copyOf(live);
        }
        for (Exchange ex : running) {
            ex.cancel();
        }
    }

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
    public void setCursorName(String name) throws SQLException {
        throw Errors.unsupported("named cursors");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return result;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return count(getLargeUpdateCount());
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkOpen();
        return result != null ? -1 : updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkOpen();
        if (closeResult() != null && closeOnCompletion) { // that was its last result set
            close();
        }
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (current == KEEP_CURRENT_RESULT && result != null) {
            throw Errors.unsupported("keeping the current result set");
        }
        return getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw Errors.unsupported("a fetch direction other than FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    /** A hint only: rows always stream as the server sends them. */
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
    public int getResultSetConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw Errors.writes();
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw Errors.writes();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        throw Errors.writes();
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw Errors.writes();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkOpen();
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkOpen();
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkOpen();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkOpen();
        return closeOnCompletion;
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
}
