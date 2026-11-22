package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Kind;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A connection's write channel (ADR 0005 decision 2), opened on its first write and closed with it. Requests on it are
 * sent one at a time, each with the next sequence number, and each answer is read in full before the caller sees
 * any of it. A request whose answer is lost in transport is sent again with the same body and sequence number: the
 * server replays its recorded outcome, or answers in_flight while it still runs, so the resend never runs it twice.
 * It is not a retry policy: a request the server refused or failed is never sent again.
 *
 * <p>When the outcome of a sent request cannot be learnt, the driver raises {@code 08007} and stops using the
 * channel, closing it without waiting; the server does not serve {@code resolve} yet, so nothing settles it.
 */
final class Channel {
    /** Sends of one request when its answer is lost in transport: the first and two resends. */
    private static final int SENDS = 3;
    /** The wait before a request the server reports in_flight is sent again. */
    private static final long IN_FLIGHT_PAUSE_MILLIS = 200;

    private final SkyliteConnection conn;
    private final Session session;
    /** Exchanges a connection's close must not stop: its own steps and the control requests. */
    private final Exchange.Owner control;
    private volatile String token; // null until opened, and after the channel ended with nothing pending
    private long last; // the last sequence number sent and settled; guarded by this
    private volatile String state = "none"; // txn_state after the last settled request
    private volatile boolean lost; // an outcome is unknown: the channel is not used again

    Channel(SkyliteConnection conn) {
        this.conn = conn;
        this.session = conn.session();
        Object lock = new Object();
        this.control = new Exchange.Owner() {
            @Override
            public Object lock() {
                return lock;
            }

            @Override
            public SQLException admit(Exchange ex) {
                return null;
            }

            @Override
            public void release(Exchange ex) {}

            @Override
            public SkyliteConnection connection() {
                return conn;
            }
        };
    }

    /** The channel's transaction state: none, open or aborted. */
    String state() {
        return state;
    }

    boolean lost() {
        return lost;
    }

    /**
     * Runs one statement on the channel. With transaction, the first statement after a transaction ended is sent as
     * a begin's first statement, so that a transaction starts with its first statement; otherwise it is an execute,
     * which outside a transaction is an autocommit write. Returns every frame of the answer, its done last.
     */
    synchronized List<Frame> execute(boolean transaction, String sql, List<Object> params, int timeoutSeconds,
            SkyliteStatement stmt) throws SQLException {
        for (int opens = 0; ; opens++) {
            open();
            boolean begin = transaction && state.equals("none");
            long seq = last + 1;
            byte[] body = Request.execute(token, seq, begin, sql, params, session.watermark(), timeoutSeconds * 1000L);
            try {
                return send(seq, Kind.EXECUTE, body, timeoutSeconds * 1000L, stmt);
            } catch (Ended e) {
                // A channel that ended while idle, as its idle limit ends it, is replaced once: nothing of this ran.
                if (!e.wasIdle || opens > 0) {
                    throw e.exception;
                }
            }
        }
    }

    /** The channel ended before seq reached it, so nothing of seq ran. */
    private static final class Ended extends SQLException {
        private static final long serialVersionUID = 1L;
        final transient SQLException exception;
        final boolean wasIdle;

        Ended(SQLException exception, boolean wasIdle) {
            this.exception = exception;
            this.wasIdle = wasIdle;
        }
    }

    /** commit or rollback of the open transaction. */
    synchronized Done step(String op) throws SQLException {
        open();
        long seq = last + 1;
        try {
            List<Frame> frames = send(seq, Kind.STEP, Request.step(token, seq, op), 0, null);
            return (Done) frames.get(frames.size() - 1);
        } catch (Ended e) {
            throw e.exception;
        }
    }

    private void open() throws SQLException {
        if (lost) {
            throw new SQLNonTransientConnectionException("skylite: the connection's write channel was given up after "
                    + "an unknown outcome; open a new connection", "08003");
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
    }

    /**
     * Sends seq's body, and again with the same bytes while its answer is lost in transport or the server reports it
     * in_flight. stmt, if any, is the statement whose cancel reaches this request.
     */
    private List<Frame> send(long seq, Kind kind, byte[] body, long timeoutMillis, SkyliteStatement stmt)
            throws SQLException {
        try {
            return sendOnce(seq, kind, body, timeoutMillis, stmt);
        } finally {
            if (stmt != null) {
                stmt.channelDone();
            }
        }
    }

    private List<Frame> sendOnce(long seq, Kind kind, byte[] body, long timeoutMillis, SkyliteStatement stmt)
            throws SQLException {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(Exchange.RESPONSE_HEADER_TIMEOUT.toMillis() + timeoutMillis);
        long wait = Exchange.RESPONSE_HEADER_TIMEOUT.toMillis() + timeoutMillis;
        boolean sent = false;
        SQLException transport = null;
        for (int sends = 0; ; ) {
            boolean[] admitted = {false};
            List<Frame> frames;
            try {
                frames = exchange("channel", kind, body, wait, tracking(stmt, seq, admitted, sent));
            } catch (SQLException e) {
                sent |= admitted[0];
                if (!sent) {
                    throw e; // nothing was sent: seq is still free
                }
                if (!admitted[0] || !"08S01".equals(e.getSQLState()) || ++sends == SENDS
                        || System.nanoTime() > deadline || Thread.currentThread().isInterrupted()) {
                    throw unknown(seq, e);
                }
                transport = e;
                continue;
            }
            sends++;
            sent = true;
            Frame f = frames.get(frames.size() - 1);
            if (f instanceof Done d) {
                settle(seq, d.txnState());
                session.raise(d.txn());
                return frames;
            }
            Failure e = (Failure) f;
            if (e.code().equals("in_flight") && System.nanoTime() < deadline) {
                pause();
                continue;
            }
            if (e.isFinal() && !e.code().equals("sequence_conflict")) {
                settle(seq, e.txnState() != null ? e.txnState() : state);
                if (e.txn() > 0) {
                    session.raise(e.txn());
                }
                throw Errors.map(e);
            }
            if (e.code().equals("channel_lost") && e.notExecuted() && sends == 1 && transport == null) {
                // The channel ended before this request reached it, so nothing of it ran.
                boolean wasIdle = state.equals("none");
                token = null;
                state = "none";
                throw new Ended(wasIdle ? Errors.map(e) : new SQLNonTransientConnectionException("skylite: the write "
                        + "channel ended and its transaction was rolled back (" + Errors.map(e).getMessage() + ")",
                        "08003"), wasIdle);
            }
            if (e.notExecuted() && sends == 1 && transport == null && !e.code().equals("sequence_conflict")) {
                throw Errors.map(e); // refused before acceptance: seq is still free
            }
            throw unknown(seq, Errors.map(e));
        }
    }

    /** The owner of one send of seq: stmt's, so that its cancel reaches seq, or the control owner; notes admission. */
    private Exchange.Owner tracking(SkyliteStatement stmt, long seq, boolean[] admitted, boolean resend) {
        Exchange.Owner base = stmt != null ? stmt : control;
        return new Exchange.Owner() {
            @Override
            public Object lock() {
                return base.lock();
            }

            @Override
            public SQLException admit(Exchange ex) {
                SQLException refused = stmt != null ? stmt.admitChannel(ex, seq, resend) : base.admit(ex);
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

    /** The request's outcome cannot be learnt: the channel is given up and closed without waiting. */
    private SQLException unknown(long seq, SQLException cause) {
        lost = true;
        state = "none";
        String t = token;
        CompletableFuture.runAsync(() -> closeQuietly(t));
        return new SQLNonTransientConnectionException("skylite: the outcome of request " + seq
                + " on the write channel is unknown; it may have committed (" + cause.getMessage() + ")", "08007", cause);
    }

    /**
     * Asks the server to cancel request seq, waiting up to the cancel wait for its answer, which is ignored: the
     * request's own answer carries what happened.
     */
    void cancel(long seq) {
        String t = token;
        if (t == null || lost) {
            return;
        }
        try {
            exchange("channel/cancel", Kind.CANCEL, Request.cancel(t, seq), Exchange.CANCEL_WAIT.toMillis(), control);
        } catch (SQLException ignored) {
            // the request's own answer, or its loss, decides
        }
    }

    /** Closes the channel, which rolls back an open transaction, waiting up to the cancel wait. */
    void close() {
        String t = token;
        token = null;
        if (t != null && !lost) {
            closeQuietly(t);
        }
    }

    private void closeQuietly(String t) {
        if (t == null) {
            return;
        }
        try {
            exchange("channel/close", Kind.CLOSE, Request.close(t), Exchange.CANCEL_WAIT.toMillis(), control);
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
