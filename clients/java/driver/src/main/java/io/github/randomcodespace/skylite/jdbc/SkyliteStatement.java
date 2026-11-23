package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Column;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Columns;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Row;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A statement sends a read in auto-commit as one stateless {@code query}, whose rows stream. Everything else goes to
 * the connection's write channel: an executeUpdate, a statement the server answers {@code needs_channel}, and every
 * statement while auto-commit is off, unless the connection is read-only. A channel answer is read in full before its
 * result set is returned. At most one result set is open, and the next execution or {@link #close} releases it.
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
    boolean statelessOnly; // the driver's own metadata queries never use the channel
    private long runningSeq; // the channel request this statement runs, 0 if none; guarded by lock
    private String runningToken; // the channel runningSeq was sent on; guarded by lock
    private Exchange runningExchange; // runningSeq's current send; guarded by lock
    private List<Long> generatedKeys = List.of(); // the rowids of this statement's inserts, when keys were asked for
    private SkyliteResultSet keys; // the last getGeneratedKeys result set; guarded by lock
    private String keyLabel;
    private final List<String> batch = new ArrayList<>();
    static final String ROWID_LABEL = "last_insert_rowid()";
    private static final Done END = new Done(0, false, 0, 0, 0, 0, null, null, 0, null, false, 0, null, null);

    SkyliteStatement(SkyliteConnection conn) {
        this.conn = conn;
    }

    void checkOpen() throws SQLException {
        if (closed) {
            SQLException discarded = conn.channel().discarded(); // closed with its connection after an unknown outcome
            throw discarded != null ? discarded : Errors.closed("statement");
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
            return conn.closedError();
        }
        live.add(ex);
        return null;
    }

    /**
     * admit for a send of channel request seq on channel token: a cancel requested before its first send refuses it,
     * as for a query; a resend is not refused by one, since the server may already hold the request.
     */
    SQLException admitChannel(Exchange ex, String token, long seq, boolean resend) {
        if (cancelRequested && !resend) {
            return Exchange.cancelled(null);
        }
        if (closed) {
            return Errors.closed("statement");
        }
        if (conn.isClosed()) {
            return conn.closedError();
        }
        live.add(ex);
        runningSeq = seq;
        runningToken = token;
        runningExchange = ex;
        return null;
    }

    /** The channel request has its outcome, or it is unknown: a cancel no longer reaches it. */
    void channelDone() {
        synchronized (lock) {
            runningSeq = 0;
            runningToken = null;
            runningExchange = null;
        }
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
     * Runs one execution: the result set when the statement has result columns, otherwise null, with its update
     * count, which a read reports as 0. update is true for executeUpdate, which goes to the channel at once. With
     * keyLabel, the rowid of an insert is kept for {@link #getGeneratedKeys}.
     */
    SkyliteResultSet run(String sql, List<Object> params, boolean update, String keyLabel) throws SQLException {
        checkOpen();
        closeResult();
        closeKeys();
        begin();
        generatedKeys = List.of();
        this.keyLabel = keyLabel;
        if (!statelessOnly && conn.onChannel(update)) {
            return onChannel(sql, params, keyLabel);
        }
        return stateless(sql, params, keyLabel);
    }

    /**
     * Sends a stateless query, and again after a pause, with the watermark read again, while it is refused before it
     * ran with a code that may pass later ({@link Retry}), within the query timeout, or the default budget without
     * one. Each send's timeout is what is left of it.
     */
    private SkyliteResultSet stateless(String sql, List<Object> params, String keyLabel) throws SQLException {
        Session s = conn.session();
        long deadline = Retry.deadline(queryTimeout * 1000L);
        for (int attempt = 0;; ) {
            long ms = queryTimeout > 0 ? attempt == 0 ? queryTimeout * 1000L : Math.max(1, Retry.left(deadline)) : 0;
            byte[] body = Request.query(sql, params, s.watermark(), ms);
            checkBody(body.length, conn.channel().bodyLimit());
            Exchange ex = Exchange.send(s, FrameReader.Kind.QUERY, body, (int) Math.ceilDiv(ms, 1000), this);
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
                    if (e.code().equals("needs_channel") && !conn.readOnly() && !statelessOnly) {
                        return onChannel(sql, params, keyLabel); // refused before it ran
                    }
                    if (Retry.refused(e) && Retry.pause(attempt++, deadline)) {
                        continue;
                    }
                    throw Errors.map(e);
                }
                case null, default -> {
                    ex.close();
                    throw Errors.transport("malformed response: an unexpected first frame", null);
                }
            }
        }
    }

    /** Refuses a body above the server's advertised limit, when one is known, before it is sent. */
    static void checkBody(long length, long limit) throws SQLException {
        if (limit > 0 && length > limit) {
            throw new SQLNonTransientException("skylite: the request of " + length + " bytes is above the server's "
                    + "request body limit of " + limit + " bytes", "54000");
        }
    }

    /** Runs on the channel, or as a stateless read when the connection became read-only after it was chosen. */
    private SkyliteResultSet onChannel(String sql, List<Object> params, String keyLabel) throws SQLException {
        List<Frame> frames = conn.execute(sql, params, queryTimeout, this);
        if (frames == null) {
            return stateless(sql, params, keyLabel);
        }
        Done d = (Done) frames.get(frames.size() - 1);
        if (d.dataOmitted() && d.readOnly()) { // changed nothing: sent again as a new request (ADR 0005 decision 3)
            frames = conn.execute(sql, params, queryTimeout, this);
            if (frames == null) {
                return stateless(sql, params, keyLabel);
            }
            d = (Done) frames.get(frames.size() - 1);
            if (d.dataOmitted() && d.readOnly()) {
                throw Errors.transport("the read's answer was lost in transport again, and its replay has no rows; "
                        + "it changed nothing", null);
            }
        }
        if (d.dataOmitted()) {
            throw Channel.omitted(d);
        }
        if (frames.get(0) instanceof Columns c) {
            return publish(new SkyliteResultSet(this, FrameReader.Frames.of(frames.subList(1, frames.size())),
                    c.columns(), maxRows, fetchSize));
        }
        updateCount = d.rowsAffected();
        if (keyLabel != null && d.rowsAffected() > 0 && inserts(sql)) {
            generatedKeys = List.of(d.lastInsertRowid());
        }
        return null;
    }

    /**
     * Whether sql starts with INSERT or REPLACE after white space and comments: only then is the connection's last
     * insert rowid this statement's key. An INSERT inside WITH is not recognised, and returns no key.
     */
    static boolean inserts(String sql) {
        int i = 0, n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (sql.startsWith("--", i)) {
                int nl = sql.indexOf('\n', i);
                i = nl < 0 ? n : nl + 1;
            } else if (sql.startsWith("/*", i)) {
                int end = sql.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
            } else {
                break;
            }
        }
        return sql.regionMatches(true, i, "insert", 0, 6) || sql.regionMatches(true, i, "replace", 0, 7);
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
        return query(run(sql, List.of(), false, null));
    }

    static ResultSet query(SkyliteResultSet rs) throws SQLException {
        if (rs == null) {
            throw new SQLNonTransientException("skylite: the statement returned no result set", "07000");
        }
        return rs;
    }

    /** Runs on the write channel, unless the connection is read-only. */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        return count(executeLargeUpdate(sql));
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return update(run(sql, List.of(), !conn.readOnly(), null) != null);
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
        return run(sql, List.of(), false, null) != null;
    }

    /** The generated keys' column label for autoGeneratedKeys, or null for none. */
    static String keyLabel(int autoGeneratedKeys) throws SQLException {
        return switch (autoGeneratedKeys) {
            case RETURN_GENERATED_KEYS -> ROWID_LABEL;
            case NO_GENERATED_KEYS -> null;
            default -> throw new SQLNonTransientException("skylite: unknown autoGeneratedKeys " + autoGeneratedKeys, "HY092");
        };
    }

    /** The rowid is the only generated key, whichever columns are asked for; at least one, each from 1. */
    static String keyLabel(int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0 || Arrays.stream(columnIndexes).anyMatch(i -> i < 1)) {
            throw new SQLNonTransientException("skylite: the column indexes for generated keys are empty or below 1",
                    "HY092");
        }
        return ROWID_LABEL;
    }

    /** The rowid is the only generated key: it is labelled with the first column name asked for. */
    static String keyLabel(String[] columnNames) {
        return columnNames != null && columnNames.length > 0 && columnNames[0] != null ? columnNames[0] : ROWID_LABEL;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return run(sql, List.of(), false, keyLabel(autoGeneratedKeys)) != null;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return run(sql, List.of(), false, keyLabel(columnIndexes)) != null;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return run(sql, List.of(), false, keyLabel(columnNames)) != null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return count(executeLargeUpdate(sql, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return count(executeLargeUpdate(sql, columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return count(executeLargeUpdate(sql, columnNames));
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return update(run(sql, List.of(), !conn.readOnly(), keyLabel(autoGeneratedKeys)) != null);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return update(run(sql, List.of(), !conn.readOnly(), keyLabel(columnIndexes)) != null);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return update(run(sql, List.of(), !conn.readOnly(), keyLabel(columnNames)) != null);
    }

    /** Releases the result set's response, if any, and the generated keys. Nothing is sent. */
    @Override
    public void close() {
        List<Exchange> running;
        SkyliteResultSet k;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            running = List.copyOf(live);
            k = keys;
        }
        closeResult();
        if (k != null) {
            k.close();
        }
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
        long seq;
        String token;
        Exchange onChannel;
        synchronized (lock) {
            cancelRequested = true;
            running = List.copyOf(live);
            seq = runningSeq;
            token = runningToken;
            onChannel = runningExchange;
        }
        if (seq > 0) {
            // A channel request's answer is its outcome, so it is not abandoned: the server is asked to cancel it,
            // and the answer says whether it was cancelled or finished.
            conn.channel().cancel(token, seq);
        }
        for (Exchange ex : running) {
            if (ex != onChannel) {
                ex.cancel();
            }
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
        checkOpen();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return ints(executeLargeBatch());
    }

    static int[] ints(long[] counts) throws SQLException {
        int[] out = new int[counts.length];
        for (int i = 0; i < counts.length; i++) {
            out[i] = count(counts[i]);
        }
        return out;
    }

    /** Runs the batch's statements as one channel batch of statements, as {@link #runBatch} says. */
    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkOpen();
        List<String> entries = new ArrayList<>(batch.size());
        try {
            for (String sql : batch) {
                entries.add(Request.statement(sql));
            }
        } finally {
            batch.clear();
        }
        return runBatch(null, entries, null);
    }

    /**
     * Runs a batch on the channel ({@link Channel#batch}): sql once per entry of param_sets, or the statements in
     * entries when sql is null. Outside a transaction it is one autocommit write, all or nothing; inside one it stops
     * at its first failure. A failure is a {@link BatchUpdateException} holding the counts of the entries that ran,
     * none for an autocommit batch, whose failure leaves nothing, and its cause as its next exception. With keyLabel,
     * {@link #getGeneratedKeys} returns the rowid of each entry that ran and inserted a row.
     */
    long[] runBatch(String sql, List<String> entries, String keyLabel) throws SQLException {
        closeResult();
        closeKeys();
        begin();
        generatedKeys = List.of();
        this.keyLabel = keyLabel;
        if (entries.isEmpty()) {
            return new long[0];
        }
        Channel.Batched b = conn.channel().batch(sql, entries, queryTimeout, this);
        List<Long> keys = new ArrayList<>();
        for (int i = 0; keyLabel != null && sql != null && inserts(sql) && i < b.rows().length; i++) {
            if (b.rows()[i] > 0) {
                keys.add(b.rowids()[i]);
            }
        }
        generatedKeys = keys;
        if (b.failure() != null) {
            throw batchFailed(b.failure(), b.rows());
        }
        return b.rows();
    }

    static BatchUpdateException batchFailed(SQLException e, long[] counts) {
        BatchUpdateException b = new BatchUpdateException(e.getMessage(), e.getSQLState(), e.getErrorCode(), counts, e);
        b.setNextException(e);
        return b;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    /**
     * The rowid of the last execution's insert, one per batch entry for a batch, when it asked for keys and inserted
     * a row; otherwise no row. Its one column is labelled with the first column name asked for, or
     * {@code last_insert_rowid()}. An insert of several rows has only the last one's rowid, and an insert into a
     * WITHOUT ROWID table has none: what it returns is an earlier insert's rowid in the same transaction, or 0.
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();
        List<Frame> rows = new ArrayList<>();
        for (Long key : generatedKeys) {
            rows.add(new Row(new Object[] {key}));
        }
        rows.add(END);
        Column c = new Column(keyLabel != null ? keyLabel : ROWID_LABEL, "INTEGER", null, null, null);
        SkyliteResultSet rs = new SkyliteResultSet(this, FrameReader.Frames.of(rows), List.of(c), 0, 0);
        SkyliteResultSet old;
        synchronized (lock) {
            if (closed) {
                old = rs;
            } else {
                old = keys;
                keys = rs;
            }
        }
        if (old != null) {
            old.close();
        }
        if (old == rs) {
            throw Errors.closed("statement");
        }
        return rs;
    }

    /** Closes the last getGeneratedKeys result set, which a new execution replaces. */
    private void closeKeys() {
        SkyliteResultSet k;
        synchronized (lock) {
            k = keys;
            keys = null;
        }
        if (k != null) {
            k.close();
        }
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
