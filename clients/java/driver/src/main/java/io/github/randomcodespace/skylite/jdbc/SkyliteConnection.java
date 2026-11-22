package io.github.randomcodespace.skylite.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * A connection reads in auto-commit through stateless {@code query} requests, and writes through one write channel
 * of its own, opened on its first write and closed with it. With auto-commit off, the first statement begins a
 * transaction on the channel and every statement runs in it until commit or rollback; a read-only connection runs
 * every statement as a stateless read instead, each at the latest committed state. Savepoints are statements of the
 * transaction, sent as SAVEPOINT, ROLLBACK TO and RELEASE.
 */
final class SkyliteConnection implements Connection, Exchange.Owner {
    private final Session session;
    private final String url;
    private final Set<SkyliteStatement> statements = ConcurrentHashMap.newKeySet();
    private final Set<Exchange> validations = ConcurrentHashMap.newKeySet();
    private final Object validationLock = new Object();
    private final Channel channel;
    private volatile boolean closed;
    private volatile boolean autoCommit = true;
    private volatile boolean readOnly;
    private final List<SkyliteSavepoint> savepoints = new ArrayList<>(); // oldest first; guarded by channel
    private long savepointTxn; // Channel.transaction() the savepoints belong to; guarded by channel
    private int savepointIds; // guarded by channel

    SkyliteConnection(Session session, String url) {
        this.session = session;
        this.url = url;
        this.channel = new Channel(this);
    }

    Channel channel() {
        return channel;
    }

    /** Whether a statement runs on the channel: with auto-commit off, or for an update, unless read-only. */
    boolean onChannel(boolean update) {
        return !readOnly && (update || !autoCommit);
    }

    /** Runs a statement on the channel, in the transaction when auto-commit is off. */
    java.util.List<FrameReader.Frame> execute(String sql, java.util.List<Object> params, int timeoutSeconds,
            SkyliteStatement stmt) throws SQLException {
        return channel.execute(sql, params, timeoutSeconds, stmt);
    }

    /** getAutoCommit for the driver's own use, which checks openness itself; changed under the channel's lock. */
    boolean autoCommit() {
        return autoCommit;
    }

    Session session() {
        return session;
    }

    String url() {
        return url;
    }

    /** Refuses every use of a closed connection, and of one whose transaction was lost (ADR 0005). */
    void checkOpen() throws SQLException {
        if (closed) {
            throw closedError();
        }
        SQLException refused = channel.refusal();
        if (refused != null) {
            throw refused;
        }
    }

    /** The error for a use of the closed connection: 08003 when it was discarded after an unknown outcome. */
    SQLException closedError() {
        SQLException discarded = channel.discarded();
        return discarded != null ? discarded : Errors.closed("connection");
    }

    void forget(SkyliteStatement s) {
        statements.remove(s);
    }

    private <T extends SkyliteStatement> T track(T s) throws SQLException {
        statements.add(s);
        if (closed) { // closed while s was made, perhaps after close() went through the statements
            s.close();
            throw closedError();
        }
        return s;
    }

    private static void checkResultSet(int type, int concurrency, int holdability) throws SQLException {
        if (type != ResultSet.TYPE_FORWARD_ONLY || concurrency != ResultSet.CONCUR_READ_ONLY) {
            throw Errors.unsupported("a result set that is not TYPE_FORWARD_ONLY and CONCUR_READ_ONLY");
        }
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT && holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLNonTransientException("skylite: unknown holdability " + holdability, "HY092");
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return track(new SkyliteStatement(this));
    }

    @Override
    public Statement createStatement(int type, int concurrency) throws SQLException {
        return createStatement(type, concurrency, getHoldability());
    }

    @Override
    public Statement createStatement(int type, int concurrency, int holdability) throws SQLException {
        checkResultSet(type, concurrency, holdability);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        if (sql == null) {
            throw new SQLNonTransientException("skylite: the SQL is null", "HY009");
        }
        return track(new SkylitePreparedStatement(this, sql, null));
    }

    /** A statement of the driver's own metadata queries: always a stateless read, inside a transaction too. */
    PreparedStatement metadataStatement(String sql) throws SQLException {
        SkylitePreparedStatement s = (SkylitePreparedStatement) prepare(sql, null);
        s.statelessOnly = true;
        return s;
    }

    private PreparedStatement prepare(String sql, String keyLabel) throws SQLException {
        checkOpen();
        if (sql == null) {
            throw new SQLNonTransientException("skylite: the SQL is null", "HY009");
        }
        return track(new SkylitePreparedStatement(this, sql, keyLabel));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int type, int concurrency) throws SQLException {
        return prepareStatement(sql, type, concurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int type, int concurrency, int holdability)
            throws SQLException {
        checkResultSet(type, concurrency, holdability);
        return prepareStatement(sql);
    }

    /** With RETURN_GENERATED_KEYS, getGeneratedKeys returns the rowid of each insert. */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepare(sql, SkyliteStatement.keyLabel(autoGeneratedKeys));
    }

    /** The rowid is the only generated key, whichever columns are asked for. */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepare(sql, SkyliteStatement.keyLabel(columnIndexes));
    }

    /** The rowid is the only generated key, labelled with the first name asked for. */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepare(sql, SkyliteStatement.keyLabel(columnNames));
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw Errors.unsupported("CallableStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int type, int concurrency) throws SQLException {
        throw Errors.unsupported("CallableStatement");
    }

    @Override
    public CallableStatement prepareCall(String sql, int type, int concurrency, int holdability) throws SQLException {
        throw Errors.unsupported("CallableStatement");
    }

    /** Escape syntax is not processed; the SQL is returned unchanged. */
    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    /** Turning auto-commit on commits an open transaction, as JDBC requires. */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        synchronized (channel) { // after a statement in flight, which may begin a transaction
            if (autoCommit && !this.autoCommit) {
                channel.commit();
            }
            this.autoCommit = autoCommit;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    /**
     * Commits the open transaction; with none open, as when no statement ran since the last commit, nothing is sent.
     * A commit whose outcome is unknown raises {@code 08007}, and the connection is given up; a transaction lost
     * with its channel raises {@code SKR03} and sends nothing.
     */
    @Override
    public void commit() throws SQLException {
        checkOpen();
        synchronized (channel) {
            if (autoCommit) {
                throw new SQLNonTransientException("skylite: commit in auto-commit mode", "25000");
            }
            channel.commit();
        }
    }

    /**
     * Rolls back the open or aborted transaction; with none, nothing is sent. A transaction lost with its channel
     * rolls back locally. After an unknown commit the connection is closed, and rollback raises {@code 08003}.
     */
    @Override
    public void rollback() throws SQLException {
        if (closed) {
            throw closedError();
        }
        synchronized (channel) {
            if (autoCommit) {
                throw new SQLNonTransientException("skylite: rollback in auto-commit mode", "25000");
            }
            channel.rollback();
        }
    }

    /**
     * Closes the connection's statements and validations, which releases their responses and ends a statement in
     * flight, then its write channel, which rolls back an open transaction, waiting up to 2 s for the server's
     * answer. A commit or rollback in flight on another thread settles first.
     */
    @Override
    public void close() {
        release();
        channel.close();
    }

    /** {@link #close}, except that the channel's close, which waits for its answer, runs on another thread. */
    void closeLater() {
        release();
        Exchange.async(channel::close);
    }

    /**
     * Closes the connection after an unknown outcome (ADR 0005 decision 4), on the thread that learnt it, which holds
     * the channel's lock and has detached its token: nothing more is sent, and every later use raises {@code 08003}.
     */
    void discard() {
        release();
    }

    /** Marks the connection closed and closes its statements and validations, none of which waits. */
    private void release() {
        closed = true;
        for (SkyliteStatement s : statements) {
            s.close();
        }
        statements.clear();
        for (Exchange ex : validations) {
            ex.close();
        }
    }

    // ---- the owner of isValid's exchanges ----

    @Override
    public Object lock() {
        return validationLock;
    }

    @Override
    public SQLException admit(Exchange ex) {
        validations.add(ex);
        if (closed) { // close() either sees ex or is seen here
            validations.remove(ex);
            return Errors.closed("connection");
        }
        return null;
    }

    @Override
    public void release(Exchange ex) {
        validations.remove(ex);
    }

    @Override
    public SkyliteConnection connection() {
        return this;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new SkyliteDatabaseMetaData(this);
    }

    /**
     * A read-only connection sends no writes: every statement is a stateless read, and one that is not a pure read
     * fails with SQLState 25006. Refused while a transaction is open.
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        synchronized (channel) {
            if (readOnly != this.readOnly && !channel.state().equals("none")) {
                throw new SQLNonTransientException("skylite: setReadOnly inside a transaction", "25000");
            }
            this.readOnly = readOnly;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    /** isReadOnly for the driver's own use, which checks openness itself. */
    boolean readOnly() {
        return readOnly;
    }

    /** Ignored: a skylite URL names one database, and there are no catalogs to switch. */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return null;
    }

    /**
     * Every level is served at serializable (ADR 0005 decision 5): a weaker one requested is accepted, NONE is not.
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        switch (level) {
            case TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ,
                    TRANSACTION_SERIALIZABLE -> {}
            default -> throw new SQLNonTransientException("skylite: unsupported isolation level " + level, "HY024");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return TRANSACTION_SERIALIZABLE;
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
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw Errors.unsupported("a type map");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw Errors.unsupported("a type map");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw Errors.unsupported("holdability other than CLOSE_CURSORS_AT_COMMIT");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /** An unnamed savepoint, which begins the transaction when none is open. */
    @Override
    public Savepoint setSavepoint() throws SQLException {
        return savepoint(null);
    }

    /** A named savepoint, which begins the transaction when none is open. */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        if (name == null) {
            throw new SQLNonTransientException("skylite: the savepoint name is null", "HY009");
        }
        return savepoint(name);
    }

    private Savepoint savepoint(String name) throws SQLException {
        checkOpen();
        synchronized (channel) {
            checkSavepoints();
            SkyliteSavepoint sp = new SkyliteSavepoint(name == null ? ++savepointIds : 0, name);
            execute("SAVEPOINT " + sp.sql(), List.of(), 0, null);
            dropEnded();
            savepoints.add(sp);
            return sp;
        }
    }

    /** Rolls back to the savepoint, which stays; the savepoints set after it are released. */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        synchronized (channel) {
            checkSavepoints();
            int i = live(savepoint);
            execute("ROLLBACK TO SAVEPOINT " + savepoints.get(i).sql(), List.of(), 0, null);
            savepoints.subList(i + 1, savepoints.size()).clear();
        }
    }

    /** Releases the savepoint and the savepoints set after it. */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        synchronized (channel) {
            checkSavepoints();
            int i = live(savepoint);
            execute("RELEASE SAVEPOINT " + savepoints.get(i).sql(), List.of(), 0, null);
            savepoints.subList(i, savepoints.size()).clear();
        }
    }

    private void checkSavepoints() throws SQLException {
        if (autoCommit) {
            throw new SQLNonTransientException("skylite: a savepoint in auto-commit mode", "25000");
        }
        if (readOnly) {
            throw new SQLNonTransientException("skylite: a savepoint on a read-only connection, which runs no "
                    + "transaction", "25000");
        }
    }

    /** Forgets the savepoints of a transaction that ended. */
    private void dropEnded() {
        long txn = channel.transaction();
        if (txn != savepointTxn) {
            savepoints.clear();
            savepointTxn = txn;
        }
    }

    /** The index of savepoint, which must be one of this connection's open transaction not yet released. */
    private int live(Savepoint savepoint) throws SQLException {
        dropEnded();
        int i = savepoints.indexOf(savepoint);
        if (i < 0 || savepointTxn == 0) {
            throw new SQLNonTransientException("skylite: the savepoint is not one of this connection's open "
                    + "transaction, or was released or rolled back past", "3B001");
        }
        return i;
    }

    /**
     * A savepoint of one transaction, by name or by id. An unnamed one is sent as skylite_savepoint_&lt;id&gt;.
     * shortcut: two live savepoints of one name, or a name of that form, resolve to the latest, as in SQLite; refuse
     * duplicates if that bites.
     */
    private static final class SkyliteSavepoint implements Savepoint {
        private final int id;
        private final String name;

        SkyliteSavepoint(int id, String name) {
            this.id = id;
            this.name = name;
        }

        String sql() {
            String n = name != null ? name : "skylite_savepoint_" + id;
            return '"' + n.replace("\"", "\"\"") + '"';
        }

        @Override
        public int getSavepointId() throws SQLException {
            if (name != null) {
                throw new SQLNonTransientException("skylite: a named savepoint has no id", "HY000");
            }
            return id;
        }

        @Override
        public String getSavepointName() throws SQLException {
            if (name == null) {
                throw new SQLNonTransientException("skylite: an unnamed savepoint has no name", "HY000");
            }
            return name;
        }
    }

    @Override
    public Clob createClob() throws SQLException {
        throw Errors.unsupported("createClob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw Errors.unsupported("createBlob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw Errors.unsupported("createNClob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw Errors.unsupported("createSQLXML");
    }

    /**
     * Describes {@code SELECT 1}, as the Go driver's ping does: the cheapest request a server answers without a
     * channel, which proves the endpoint, the token and the database. One deadline, the shorter of the timeout and
     * the network timeout, covers the token, the headers and the body; when the network timeout is the one that
     * passes, the connection closes. Closing or aborting the connection ends a validation with false.
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLNonTransientException("skylite: negative timeout", "HY092");
        }
        if (closed) {
            return false;
        }
        long network = session.networkTimeoutMillis();
        long limit = timeout == 0 ? network : network == 0 ? timeout * 1000L : Math.min(network, timeout * 1000L);
        try (Exchange ex = Exchange.validate(session, Request.describe("SELECT 1", session.watermark()), limit, this)) {
            FrameReader.Frame f;
            while ((f = ex.next()) != null) {
                if (f instanceof FrameReader.Failure) {
                    return false;
                }
                if (f instanceof FrameReader.Done d) {
                    session.raise(d.txn());
                }
            }
            return true;
        } catch (SQLTimeoutException e) {
            if (network > 0 && limit == network) {
                close();
            }
            return false;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException("skylite: client info is not supported", "0A000", 0, Map.of());
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("skylite: client info is not supported", "0A000", 0, Map.of());
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkOpen();
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw Errors.unsupported("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Errors.unsupported("createStruct");
    }

    /** Ignored: a skylite database has no schemas to switch. */
    @Override
    public void setSchema(String schema) throws SQLException {
        checkOpen();
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLNonTransientException("skylite: abort needs an executor", "HY009");
        }
        release();
        channel.abort();
        executor.execute(channel::close); // the channel's close waits for its answer
    }

    /**
     * Bounds the wait for each response's headers, whatever the query timeout, and each wait for more of its body;
     * when it passes, the connection and its statements close. 0 means the driver's default: 60 s, or more for a
     * longer query timeout, for the headers, and no bound on the body. The executor is not used: the driver's own
     * timer closes the connection.
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkOpen();
        if (executor == null) {
            throw new SQLNonTransientException("skylite: setNetworkTimeout needs an executor", "HY009");
        }
        if (milliseconds < 0) {
            throw new SQLNonTransientException("skylite: negative network timeout", "HY092");
        }
        session.networkTimeoutMillis(milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkOpen();
        return session.networkTimeoutMillis();
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
