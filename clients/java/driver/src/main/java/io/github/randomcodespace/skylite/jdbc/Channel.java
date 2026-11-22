package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Kind;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A connection's write channel (ADR 0005 decision 2), opened on its first write and closed with it. Requests on it are
 * sent one at a time, each with the next sequence number, and each answer is read in full before the caller sees
 * any of it. A request whose answer is lost in transport is sent again with the same body and sequence number: the
 * server replays its recorded outcome, or answers in_flight while it still runs, so the resend never runs it twice.
 * It is not a retry policy: a request the server refused or failed is never sent again, and a request that may have
 * been accepted is never sent on another channel.
 *
 * <p>When the outcome of a sent request cannot be learnt, the driver raises {@code 08007} and gives the channel up,
 * closing it without waiting; the server does not serve {@code resolve} yet, so nothing settles it (ADR 0005 decision
 * 4). An autocommit write's or a commit's loss closes the connection, and every later use of it fails with
 * {@code 08003}. A statement's or a rollback's loss inside a transaction loses the transaction: its later statements
 * and its commit fail with {@code SKR03} and send nothing, its rollback succeeds locally, and the next statement after
 * that opens a new channel.
 *
 * <p>The channel's state is read and changed under this object's lock, which a request holds while it runs. Close
 * waits for it; cancel does not take it.
 */
final class Channel {
    /** Sends of one request when its answer is lost in transport: the first and two resends. */
    private static final int SENDS = 3;
    /** The wait before a request the server reports in_flight is sent again. */
    private static final long IN_FLIGHT_PAUSE_MILLIS = 200;
    /** ADR 0005's reopening rule: an idle channel with no answer for this long is replaced before it is used. */
    static volatile long reopenAfterNanos = TimeUnit.SECONDS.toNanos(60);

    private final SkyliteConnection conn;
    private final Session session;
    /** The owner of open, commit, rollback and cancel: refused once the connection closed, and stopped by abort. */
    private final Exchange.Owner control;
    /** The owner of close, which is never refused. */
    private final Exchange.Owner closer;
    private final Set<Exchange> controls = ConcurrentHashMap.newKeySet();
    private String token; // null until opened, and after the channel was closed or given up; guarded by this
    private long last; // the last sequence number sent and settled; guarded by this
    private long answered; // System.nanoTime() of the channel's last answer; guarded by this
    private String state = "none"; // txn_state after the last settled request; guarded by this
    private long begun; // begins sent on the connection's channels; guarded by this
    private volatile boolean lost; // written under this: the connection was discarded after an unknown outcome
    private volatile boolean dead; // written under this: the transaction was lost with its channel; rollback ends it

    Channel(SkyliteConnection conn) {
        this.conn = conn;
        this.session = conn.session();
        this.control = owner(true);
        this.closer = owner(false);
    }

    private Exchange.Owner owner(boolean refuseClosed) {
        Object lock = new Object();
        return new Exchange.Owner() {
            @Override
            public Object lock() {
                return lock;
            }

            @Override
            public SQLException admit(Exchange ex) {
                if (!refuseClosed) {
                    return null;
                }
                controls.add(ex);
                if (conn.isClosed()) { // abort either sees ex or is seen here
                    controls.remove(ex);
                    return Errors.closed("connection");
                }
                return null;
            }

            @Override
            public void release(Exchange ex) {
                controls.remove(ex);
            }

            @Override
            public SkyliteConnection connection() {
                return conn;
            }
        };
    }

    /** The channel's transaction state: none, open or aborted. */
    synchronized String state() {
        return state;
    }

    /** The number of the open transaction, distinct from every earlier one's, or 0 when none is open. */
    synchronized long transaction() {
        return state.equals("open") ? begun : 0;
    }

    /** Why the connection may not be used, or null: it was discarded, or its transaction was lost. */
    SQLException refusal() {
        if (lost) {
            return discarded();
        }
        if (dead) {
            return new SQLException("skylite: the transaction was lost with its write channel after an unknown "
                    + "outcome, and nothing more of it is sent; roll it back", "SKR03", 0);
        }
        return null;
    }

    /** The error for any use of a connection discarded after an unknown outcome, or null if it was not. */
    SQLException discarded() {
        return lost ? new SQLNonTransientConnectionException("skylite: the connection was closed after an unknown "
                + "outcome on its write channel; open a new connection", "08003") : null;
    }

    /**
     * Runs one statement on the channel. With auto-commit off, the first statement after a transaction ended is sent
     * as a begin's first statement, so that a transaction starts with its first statement; otherwise it is an execute,
     * which outside a transaction is an autocommit write. Returns every frame of the answer, its done last, or null,
     * sending nothing, when the connection became read-only after the statement chose the channel.
     */
    synchronized List<Frame> execute(String sql, List<Object> params, int timeoutSeconds, SkyliteStatement stmt)
            throws SQLException {
        conn.checkOpen();
        if (conn.readOnly()) { // setReadOnly holds this lock, so no transaction is open
            return null;
        }
        boolean transaction = !conn.autoCommit();
        open();
        boolean txnStatement = !state.equals("none");
        if (transaction && !txnStatement) {
            begun++;
        }
        long seq = last + 1;
        byte[] body = Request.execute(token, seq, transaction && !txnStatement, sql, params, session.watermark(),
                timeoutSeconds * 1000L);
        return send(seq, Kind.EXECUTE, body, timeoutSeconds * 1000L, stmt, txnStatement, transaction);
    }

    /** Commits the open transaction; with none open, nothing is sent. */
    synchronized void commit() throws SQLException {
        conn.checkOpen();
        if (!state.equals("none")) {
            step("commit", false);
        }
    }

    /**
     * Rolls back the open or aborted transaction; with none, nothing is sent. A transaction lost with its channel,
     * or whose rollback's outcome is unknown, rolls back locally: its commit is never sent, so it cannot commit, and
     * the next statement opens a new channel.
     */
    synchronized void rollback() throws SQLException {
        if (conn.isClosed()) {
            throw conn.closedError();
        }
        if (!dead && !state.equals("none")) {
            try {
                step("rollback", true);
            } catch (SQLException e) {
                if (!dead) {
                    throw e;
                }
            }
        }
        dead = false;
    }

    private void step(String op, boolean transaction) throws SQLException {
        open();
        long seq = last + 1;
        send(seq, Kind.STEP, Request.step(token, seq, op), 0, null, false, transaction);
    }

    private void open() throws SQLException {
        if (token != null && state.equals("none") && System.nanoTime() - answered > reopenAfterNanos) {
            String t = token; // idle too long: the server may end it at its idle limit before the next request
            token = null;
            Exchange.async(() -> closeQuietly(t));
        }
        if (token != null) {
            return;
        }
        List<Frame> frames = exchange("channels", Kind.OPEN, Request.open(), Exchange.RESPONSE_HEADER_TIMEOUT.toMillis(),
                control);
        Frame f = frames.get(frames.size() - 1);
        if (f instanceof Failure e) {
            throw Errors.map(e);
        }
        Done d = (Done) f;
        session.raise(d.txn());
        token = d.token();
        last = 0;
        state = "none";
        answered = System.nanoTime();
    }

    /**
     * Sends seq's body, and again with the same bytes while its answer is lost in transport or the server reports it
     * in_flight. stmt, if any, is the statement whose cancel reaches this request. txnStatement: an execute sent while
     * a transaction is open, whose recorded failure is its outcome. transaction: an application transaction is open,
     * from its begin, which an unknown outcome loses; otherwise, an autocommit write or a commit, an unknown outcome
     * discards the connection.
     */
    private List<Frame> send(long seq, Kind kind, byte[] body, long timeoutMillis, SkyliteStatement stmt,
            boolean txnStatement, boolean transaction) throws SQLException {
        try {
            return sendOnce(seq, kind, body, timeoutMillis, stmt, txnStatement, transaction);
        } finally {
            if (stmt != null) {
                stmt.channelDone();
            }
        }
    }

    private List<Frame> sendOnce(long seq, Kind kind, byte[] body, long timeoutMillis, SkyliteStatement stmt,
            boolean txnStatement, boolean transaction) throws SQLException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(Exchange.RESPONSE_HEADER_TIMEOUT.toMillis() + timeoutMillis);
        long wait = Exchange.RESPONSE_HEADER_TIMEOUT.toMillis() + timeoutMillis;
        boolean sent = false; // a send of seq may have reached the server
        boolean fresh = true; // no earlier send of seq may have been accepted
        int drops = 0; // sends whose answer was lost in transport
        for (;;) {
            boolean[] admitted = {false};
            List<Frame> frames;
            try {
                frames = exchange("channel", kind, body, wait, tracking(stmt, token, seq, admitted, sent));
            } catch (SQLException e) {
                sent |= admitted[0];
                if (!sent) {
                    throw e; // nothing was sent: seq is still free
                }
                if (!admitted[0] || !"08S01".equals(e.getSQLState()) || ++drops >= SENDS
                        || System.nanoTime() > deadline || Thread.currentThread().isInterrupted()) {
                    throw unknown(seq, e, transaction);
                }
                fresh = false;
                continue;
            }
            sent = true;
            answered = System.nanoTime();
            Frame f = frames.get(frames.size() - 1);
            if (f instanceof Done d) {
                settle(seq, d.txnState());
                session.raise(d.txn());
                return frames;
            }
            Failure e = (Failure) f;
            if (e.code().equals("in_flight") && System.nanoTime() < deadline) {
                fresh = false;
                try {
                    pause();
                } catch (SQLException interrupted) {
                    throw unknown(seq, interrupted, transaction);
                }
                continue;
            }
            // channel_lost is not a proof (ADR 0005 decision 4), and sequence_conflict says seq is not this body's.
            if (e.code().equals("channel_lost") || e.code().equals("sequence_conflict")) {
                throw unknown(seq, Errors.map(e), transaction);
            }
            // A recorded failure is the outcome when it was not executed, or of a statement inside a transaction.
            if (e.isFinal() && (e.notExecuted() || txnStatement)) {
                settle(seq, e.txnState() != null ? e.txnState() : state);
                if (e.txn() > 0) {
                    session.raise(e.txn());
                }
                throw Errors.map(e);
            }
            if (!e.isFinal() && e.notExecuted() && fresh) {
                throw Errors.map(e); // refused before acceptance: seq is still free
            }
            throw unknown(seq, Errors.map(e), transaction);
        }
    }

    /**
     * The owner of one send of seq on token t: stmt's, so that its cancel reaches seq, or the control owner; notes
     * admission.
     */
    private Exchange.Owner tracking(SkyliteStatement stmt, String t, long seq, boolean[] admitted, boolean resend) {
        Exchange.Owner base = stmt != null ? stmt : control;
        return new Exchange.Owner() {
            @Override
            public Object lock() {
                return base.lock();
            }

            @Override
            public SQLException admit(Exchange ex) {
                SQLException refused = stmt != null ? stmt.admitChannel(ex, t, seq, resend) : base.admit(ex);
                admitted[0] = refused == null;
                return refused;
            }

            @Override
            public void release(Exchange ex) {
                base.release(ex);
            }

            @Override
            public SkyliteConnection connection() {
                return conn;
            }
        };
    }

    private void settle(long seq, String txnState) {
        last = seq;
        state = txnState;
    }

    private static void pause() throws SQLException {
        try {
            Thread.sleep(IN_FLIGHT_PAUSE_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw Errors.transport("interrupted while waiting for a request in flight", ie);
        }
    }

    /**
     * The request's outcome cannot be learnt: the channel is given up, and with it the transaction, if one is open;
     * otherwise the connection is discarded (ADR 0005 decision 4). The channel is closed without waiting, unless the
     * connection is closing, whose close sends it and waits.
     */
    private SQLException unknown(long seq, SQLException cause, boolean transaction) {
        state = "none";
        if (!conn.isClosed()) {
            String t = token;
            token = null;
            Exchange.async(() -> closeQuietly(t));
        }
        if (transaction) {
            dead = true;
        } else {
            lost = true;
            conn.discard();
        }
        return new SQLNonTransientConnectionException("skylite: the outcome of request " + seq
                + " on the write channel is unknown; it may have committed (" + cause.getMessage() + ")", "08007", cause);
    }

    /**
     * Asks the server to cancel request seq on channel t, waiting up to the cancel wait for its answer, which is
     * ignored: the request's own answer carries what happened.
     */
    void cancel(String t, long seq) {
        if (t == null || lost) {
            return;
        }
        try {
            exchange("channel/cancel", Kind.CANCEL, Request.cancel(t, seq), Exchange.CANCEL_WAIT.toMillis(), control);
        } catch (SQLException ignored) {
            // the request's own answer, or its loss, decides
        }
    }

    /** Stops the open, commit, rollback or cancel in flight, as {@link java.sql.Connection#abort} does. */
    void abort() {
        for (Exchange ex : controls) {
            ex.close();
        }
    }

    /**
     * Closes the channel, which rolls back an open transaction, waiting up to the cancel wait. A request in flight on
     * it ends first.
     */
    void close() {
        String t;
        synchronized (this) {
            t = token;
            token = null;
        }
        closeQuietly(t);
    }

    private void closeQuietly(String t) {
        if (t == null) {
            return;
        }
        try {
            exchange("channel/close", Kind.CLOSE, Request.close(t), Exchange.CANCEL_WAIT.toMillis(), closer);
        } catch (SQLException ignored) {
            // the server ends the channel at its idle limit
        }
    }

    private List<Frame> exchange(String path, Kind kind, byte[] body, long waitMillis, Exchange.Owner owner)
            throws SQLException {
        try (Exchange ex = Exchange.channel(session, path, kind, body, waitMillis, owner)) {
            List<Frame> out = new ArrayList<>();
            Frame f;
            while ((f = ex.next()) != null) {
                out.add(f);
            }
            return out;
        }
    }
}
