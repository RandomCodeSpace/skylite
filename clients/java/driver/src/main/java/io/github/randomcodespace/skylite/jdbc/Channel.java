package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Kind;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Limits;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Results;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A connection's channels (ADR 0005 decision 2): a write channel, and a read-only one for transactions of a read-only
 * connection, each opened on its first use and closed with the connection. Requests on a channel are sent one at a
 * time, each with the next sequence number, and each answer is read in full before the caller sees any of it. A
 * request whose answer is lost in transport is sent again with the same body and sequence number: the server replays
 * its recorded outcome, or answers in_flight while it still runs, so the resend never runs it twice. A request that a
 * refusal or a recorded failure proves did not run, and that may pass later, is sent again after a pause
 * ({@link Retry}), within the statement's deadline.
 *
 * <p>When the outcome of a sent autocommit write, batch or commit cannot be learnt, the driver settles it (ADR 0005
 * decision 4): it closes the channel, whose answer settles a request on a live channel, or, when the channel is lost
 * or the close does not answer, it sends {@code resolve} until the resolve wait ends. Committed is a success for a
 * commit, and for an executeUpdate whose count the close's receipt holds, and {@code SKR01} (committed, result
 * unavailable) for any other write; not committed sends an autocommit write once
 * more, on a new channel, and is {@code 40000} for a commit; otherwise the outcome is unknown, {@code 08007}, and the
 * connection is closed. A commit settled either way leaves the connection open: only the channel is gone, and the
 * next request opens a new one (ADR 0005, amended 2026-11-23). A statement's or a rollback's loss inside a
 * transaction loses the transaction: its later statements and its commit fail with {@code SKR03} and send nothing, its
 * rollback succeeds locally, and the next statement after that opens a new channel.
 *
 * <p>The channels' state is read and changed under this object's lock, which a request holds while it runs. Close
 * waits for it; cancel does not take it.
 */
final class Channel {
    /** Sends of one request when its answer is lost in transport: the first and two resends. */
    private static final int SENDS = 3;
    /** The wait before a request the server reports in_flight is sent again. */
    private static final long IN_FLIGHT_PAUSE_MILLIS = 200;
    /** ADR 0005's reopening rule: an idle channel with no answer for this long is replaced before it is used. */
    static volatile long reopenAfterNanos = TimeUnit.SECONDS.toNanos(60);
    /** ADR 0005's driver wait for a resolution. */
    static volatile long resolveWaitMillis = 30_000;
    /** Answers to resolve after which it is sent again. */
    private static final Set<String> RESOLVE_AGAIN =
            Set.of("no_leader", "behind", "overloaded", "not_serving", "owner_unreachable", "maintenance");

    /** What a request is, for what its loss means: ADR 0005 decision 4. */
    private enum Mode {
        AUTOCOMMIT, BEGIN, TXN, COMMIT, ROLLBACK;

        /** A loss loses the application's transaction. */
        boolean transaction() {
            return this == BEGIN || this == TXN || this == ROLLBACK;
        }
    }

    /** One channel's state; guarded by the Channel. */
    private static final class Slot {
        final boolean readOnly;
        String token; // null until opened, and after the channel was closed or given up
        long last; // the last sequence number sent and settled
        long answered; // System.nanoTime() of the channel's last answer
        String state = "none"; // txn_state after the last settled request
        long openHead; // the open answer's txn, which the token carries
        Limits limits = Limits.DEFAULT; // the open answer's

        Slot(boolean readOnly) {
            this.readOnly = readOnly;
        }
    }

    /** A request body for a token and a sequence number. */
    @FunctionalInterface
    private interface Body {
        byte[] build(String token, long seq) throws SQLException;
    }

    /** The counts and rowids of a batch's entries that ran, and the failure that stopped it, or null. */
    record Batched(long[] rows, long[] rowids, SQLException failure) {}

    private final SkyliteConnection conn;
    private final Session session;
    /** The owner of open, commit, rollback and cancel: refused once the connection closed, and stopped by abort. */
    private final Exchange.Owner control;
    /** The owner of close and resolve, which are never refused. */
    private final Exchange.Owner closer;
    private final Set<Exchange> controls = ConcurrentHashMap.newKeySet();
    private final Slot write = new Slot(false);
    private final Slot read = new Slot(true);
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

    /** The channel the connection uses now: the read-only one while the connection is read-only. */
    private Slot slot() {
        return conn.readOnly() ? read : write;
    }

    /** The channel with a transaction open or aborted, or else the one the connection uses now. */
    private Slot active() {
        return !read.state.equals("none") ? read : !write.state.equals("none") ? write : slot();
    }

    /** The transaction state: none, or open or aborted on either channel. */
    synchronized String state() {
        return active().state;
    }

    /** The number of the open transaction, distinct from every earlier one's, or 0 when none is open. */
    synchronized long transaction() {
        return active().state.equals("open") ? begun : 0;
    }

    /** The write channel's request body limit, from its open answer, or 0 before one. */
    synchronized long bodyLimit() {
        return write.token != null ? write.limits.requestBodyBytes() : 0;
    }

    /** Why the connection may not be used, or null: it was discarded, or its transaction was lost. */
    SQLException refusal() {
        if (lost) {
            return discarded();
        }
        if (dead) {
            return transactionLost(null);
        }
        return null;
    }

    private static SQLException transactionLost(SQLException cause) {
        return new SQLException("skylite: the transaction was lost with its channel, and nothing more of it is sent; "
                + "roll it back", "SKR03", 0, cause);
    }

    /** The error for any use of a connection discarded after an unknown outcome, or null if it was not. */
    SQLException discarded() {
        return lost ? new SQLNonTransientConnectionException("skylite: the connection was closed after an unknown "
                + "outcome on its write channel; open a new connection", "08003") : null;
    }

    /**
     * Runs one statement on a channel: the write channel, or the read-only one when the connection is read-only. With
     * auto-commit off, the first statement after a transaction ended is sent as a begin's first statement, so that a
     * transaction starts with its first statement; otherwise it is an execute, which outside a transaction is an
     * autocommit write. Returns every frame of the answer, its done last, or null, sending nothing, when the
     * connection is read-only in auto-commit, where a statement is a stateless read.
     */
    synchronized List<Frame> execute(String sql, List<Object> params, int timeoutSeconds, SkyliteStatement stmt)
            throws SQLException {
        conn.checkOpen();
        if (conn.readOnly() && conn.autoCommit()) { // setReadOnly holds this lock, so no transaction is open
            return null;
        }
        Slot s = slot();
        Mode mode = conn.autoCommit() ? Mode.AUTOCOMMIT : s.state.equals("none") ? Mode.BEGIN : Mode.TXN;
        if (mode == Mode.BEGIN) {
            begun++;
        }
        long ms = timeoutSeconds * 1000L;
        boolean begin = mode == Mode.BEGIN;
        List<Frame> frames = call(s, Kind.EXECUTE,
                (t, q) -> Request.execute(t, q, begin, sql, params, session.watermark(), ms), ms, stmt, mode);
        if (frames.get(frames.size() - 1) instanceof Failure e) {
            throw Errors.map(e);
        }
        return frames;
    }

    /**
     * Runs a batch on the channel the connection uses: one statement per parameter set when sql is given, otherwise
     * one statement per entry ({@link Request#paramSet}, {@link Request#statement}). Outside a transaction it is one
     * autocommit write, all or nothing, refused before anything is sent when it is above the channel's entry or body
     * limit. Inside one it is split at those limits, after a begin when the transaction has not started, and stops at
     * its first failure, after which the counts of the entries before it are returned with the failure.
     */
    synchronized Batched batch(String sql, List<String> entries, int timeoutSeconds, SkyliteStatement stmt)
            throws SQLException {
        conn.checkOpen();
        Slot s = slot();
        long ms = timeoutSeconds * 1000L;
        boolean auto = conn.autoCommit();
        if (s.readOnly && auto) {
            return new Batched(new long[0], new long[0], new SQLNonTransientException("skylite: a batch on a "
                    + "read-only connection in auto-commit, where every statement is a stateless read", "25006"));
        }
        long[] rows = new long[entries.size()], ids = new long[entries.size()];
        int from = 0;
        try {
            open(s, deadline(s, ms));
            List<Integer> ends = split(s, sql, entries, ms);
            if (auto && ends.size() > 1) {
                throw new SQLNonTransientException("skylite: the batch is above the server's limit of "
                        + s.limits.batchEntries() + " entries or " + s.limits.requestBodyBytes() + " bytes, and in "
                        + "auto-commit it is one transaction, which the driver does not split", "54000");
            }
            if (!auto && s.state.equals("none")) {
                begun++;
                step(s, Kind.STEP, (t, q) -> Request.begin(t, q, session.watermark()), ms, stmt, Mode.BEGIN);
            }
            for (int end : ends) {
                int start = from;
                List<Frame> frames = call(s, Kind.BATCH,
                        (t, q) -> Request.batch(t, q, sql, entries.subList(start, end), session.watermark(), ms), ms,
                        stmt, auto ? Mode.AUTOCOMMIT : Mode.TXN);
                for (Frame f : frames) {
                    if (f instanceof Results r) {
                        if (r.rowsAffected().length > end - from) {
                            throw Errors.transport("malformed response: more results than the batch's "
                                    + (end - start) + " entries", null);
                        }
                        System.arraycopy(r.rowsAffected(), 0, rows, from, r.rowsAffected().length);
                        System.arraycopy(r.lastInsertRowids(), 0, ids, from, r.lastInsertRowids().length);
                        from += r.rowsAffected().length;
                    }
                }
                switch (frames.get(frames.size() - 1)) {
                    case Failure e -> {
                        // In a transaction the entries before batch_index ran, though a replayed receipt has no results
                        for (long ran = auto ? 0 : Math.min(start + e.batchIndex(), end); from < ran; from++) {
                            rows[from] = Statement.SUCCESS_NO_INFO;
                        }
                        throw Errors.map(e);
                    }
                    case Done d when d.dataOmitted() -> throw omitted(d);
                    default -> from = end;
                }
            }
            return new Batched(rows, ids, null);
        } catch (SQLException e) {
            return new Batched(Arrays.copyOf(rows, from), Arrays.copyOf(ids, from), e);
        }
    }

    /** The error for a channel answer whose data the driver lost: committed (SKR01), or in a transaction (SKR02). */
    static SQLException omitted(Done d) {
        // ADR 0005 decision 3: the base class, which Spring and Hibernate treat as non-transient.
        if ("none".equals(d.txnState())) {
            return new SQLException("skylite: the statement committed as transaction " + d.txn() + ", but its "
                    + "answer was lost in transport and the server kept no copy of its result", "SKR01", 0);
        }
        return new SQLException("skylite: the statement ran inside the transaction, which is still open with its "
                + "effects, but its answer was lost in transport and the server kept no copy of its result",
                "SKR02", 0);
    }

    /** The end index of each part of a batch that fits the channel's entry and body limits, in order. */
    private List<Integer> split(Slot s, String sql, List<String> entries, long ms) throws SQLException {
        long room = s.limits.requestBodyBytes()
                - Request.batch(s.token, Long.MAX_VALUE, sql, List.of(), Long.MAX_VALUE, ms).length;
        List<Integer> ends = new ArrayList<>();
        long size = 0;
        for (int i = 0; i < entries.size(); i++) {
            long n = utf8Length(entries.get(i)) + 1; // and its comma
            int count = i - (ends.isEmpty() ? 0 : ends.get(ends.size() - 1));
            if (count > 0 && (count >= s.limits.batchEntries() || size + n > room)) {
                ends.add(i);
                size = 0;
            }
            size += n;
        }
        ends.add(entries.size());
        return ends;
    }

    private static long utf8Length(String s) {
        long n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            n += c < 0x80 ? 1 : c < 0x800 || Character.isSurrogate(c) ? 2 : 3; // a surrogate pair is 4 bytes
        }
        return n;
    }

    /** Commits the open transaction; with none open, nothing is sent. */
    synchronized void commit() throws SQLException {
        conn.checkOpen();
        Slot s = active();
        if (!s.state.equals("none")) {
            step(s, Kind.STEP, (t, q) -> Request.step(t, q, "commit"), 0, null, Mode.COMMIT);
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
        Slot s = active();
        if (!dead && !s.state.equals("none")) {
            try {
                step(s, Kind.STEP, (t, q) -> Request.step(t, q, "rollback"), 0, null, Mode.ROLLBACK);
            } catch (SQLException e) {
                if (!dead) {
                    throw e;
                }
            }
        }
        dead = false;
    }

    private void step(Slot s, Kind kind, Body body, long ms, SkyliteStatement stmt, Mode mode) throws SQLException {
        List<Frame> frames = call(s, kind, body, ms, stmt, mode);
        if (frames.get(frames.size() - 1) instanceof Failure e) {
            throw Errors.map(e);
        }
    }

    /** The deadline of a request: its query timeout, or else the server's statement time, plus the cancel wait. */
    private static long deadline(Slot s, long timeoutMillis) {
        long budget = timeoutMillis > 0 ? timeoutMillis : Math.min(Retry.defaultBudgetMillis, s.limits.statementMs());
        return Retry.deadline(budget + Exchange.CANCEL_WAIT.toMillis());
    }

    /**
     * Opens s unless it is open, replacing an idle channel first when it has had no answer for the reopening time or
     * its open head is more than half the resolve bound below the watermark (ADR 0005, reopening). A refused open is
     * sent again after a pause until deadline.
     */
    private void open(Slot s, long deadline) throws SQLException {
        if (s.token != null && s.state.equals("none") && (System.nanoTime() - s.answered > reopenAfterNanos
                || session.watermark() - s.openHead > s.limits.resolveBound() / 2)) {
            giveUp(s); // the server may end it at its idle limit, or a resolve may not reach back to its open head
        }
        for (int attempt = 0; s.token == null; ) {
            List<Frame> frames = exchange("channels", Kind.OPEN, Request.open(s.readOnly, session.watermark()),
                    headerWait(deadline), control);
            Frame f = frames.get(frames.size() - 1);
            if (f instanceof Failure e) {
                if (Retry.refused(e) && Retry.pause(attempt++, deadline)) {
                    continue;
                }
                throw Errors.map(e);
            }
            Done d = (Done) f;
            session.raise(d.txn());
            s.token = d.token();
            s.last = 0;
            s.state = "none";
            s.answered = System.nanoTime();
            s.openHead = d.txn();
            s.limits = d.limits();
        }
    }

    private static long headerWait(long deadline) {
        return Math.max(Retry.left(deadline), Exchange.CANCEL_WAIT.toMillis());
    }

    /** Sends a request and settles it; stmt, if any, is the statement whose cancel reaches it. */
    private List<Frame> call(Slot s, Kind kind, Body body, long timeoutMillis, SkyliteStatement stmt, Mode mode)
            throws SQLException {
        try {
            return attempt(s, kind, body, timeoutMillis, stmt, mode);
        } finally {
            if (stmt != null) {
                stmt.channelDone();
            }
        }
    }

    /**
     * Sends the request as the channel's next sequence number until it has an outcome: its answer, a recorded
     * failure, or one settled after its answer was lost. A recorded failure that ran nothing outside a transaction
     * and may pass later, and a behind refusal, which closes the channel, are sent again after a pause, as a new
     * request; an autocommit write settled as not committed is sent once more, on a new channel.
     */
    private List<Frame> attempt(Slot s, Kind kind, Body body, long timeoutMillis, SkyliteStatement stmt, Mode mode)
            throws SQLException {
        long deadline = deadline(s, timeoutMillis);
        boolean again = false; // the not_committed write was sent once more
        for (int attempt = 0;; ) {
            open(s, deadline);
            long seq = s.last + 1;
            byte[] b = body.build(s.token, seq);
            if (b.length > s.limits.requestBodyBytes()) {
                throw new SQLNonTransientException("skylite: the request of " + b.length + " bytes is above the "
                        + "server's request body limit of " + s.limits.requestBodyBytes() + " bytes", "54000");
            }
            Delivery d = deliver(s, seq, kind, b, deadline, stmt, mode);
            if (d.frames != null) {
                if (d.frames.get(d.frames.size() - 1) instanceof Failure e
                        && (mode == Mode.AUTOCOMMIT || mode == Mode.BEGIN) && Retry.recorded(e)
                        && Retry.pause(attempt++, deadline)) {
                    continue; // busy, overloaded or maintenance ran nothing: a new request may pass
                }
                return d.frames;
            }
            if (d.behind != null) { // the channel was given up, which settles seq as not executed
                if (mode != Mode.AUTOCOMMIT && mode != Mode.BEGIN) { // and loses the open transaction with it
                    s.state = "none";
                    dead = true;
                    throw transactionLost(Errors.map(d.behind));
                }
                if (Retry.pause(attempt++, deadline)) {
                    continue;
                }
                throw Errors.map(d.behind);
            }
            if (s.readOnly && mode == Mode.COMMIT) { // a read-only transaction has nothing to commit
                giveUp(s);
                s.state = "none";
                return List.of(new Done(0, true, 0, 0, 0, seq, "none", null, 0, null, false, 0, null, null));
            }
            if (mode.transaction() || s.readOnly || abandoned()) {
                throw unknown(s, seq, d.lost, mode);
            }
            String token = s.token;
            s.token = null; // given up; settle closes it
            s.state = "none";
            // Settled, even a commit keeps the connection: the next request opens a new channel (ADR 0005, amended)
            Frame settled = settle(token, seq, d.gone, timeoutMillis > 0 ? deadline : 0);
            switch (settled) {
                case Done done when "committed".equals(done.resolution()) -> {
                    return List.of(mode == Mode.COMMIT ? done : done.omitted());
                }
                case Done done when "not_committed".equals(done.resolution()) -> {
                    if (mode == Mode.AUTOCOMMIT && !again && Retry.left(deadline) > 0) {
                        again = true;
                        continue; // no effect, now or later: sent once more, on a new channel
                    }
                    throw new SQLTransactionRollbackException("skylite: request " + seq + " on the write channel "
                            + "was not committed: its answer was lost, and the server settled that it has no effect"
                            + (mode == Mode.COMMIT ? "; the transaction was rolled back" : "") + " ("
                            + d.lost.getMessage() + ")", "40000", d.lost);
                }
                case Failure f -> {
                    return List.of(f); // the close answer's receipt: the recorded failure
                }
                case null, default -> {
                    lost = true;
                    conn.discard();
                    throw unknownError(seq, d.lost);
                }
            }
        }
    }

    /** What one delivery loop learnt: the outcome's frames, a behind refusal, or a loss. */
    private record Delivery(List<Frame> frames, Failure behind, SQLException lost, boolean gone) {}

    /**
     * Sends seq's body, and again with the same bytes while its answer is lost in transport, the server reports it
     * in_flight, a refusal before acceptance may pass later, or a forwarding pod lost the owner's answer
     * (owner_unreachable, not_executed false), until deadline. mode TXN: an execute or batch sent
     * while a transaction is open, whose recorded failure is its outcome.
     */
    private Delivery deliver(Slot s, long seq, Kind kind, byte[] body, long deadline, SkyliteStatement stmt, Mode mode)
            throws SQLException {
        boolean sent = false; // a send of seq may have reached the server
        boolean fresh = true; // no earlier send of seq may have been accepted
        int drops = 0; // sends whose answer was lost in transport
        int refusals = 0; // refusals before acceptance sent again
        for (;;) {
            boolean[] admitted = {false};
            List<Frame> frames;
            try {
                // The first send waits out the deadline; a resend, whose answer is a replay, waits for what is left.
                long wait = sent ? Math.max(1, Retry.left(deadline)) : headerWait(deadline);
                frames = exchange("channel", kind, body, wait, tracking(stmt, s.token, seq, admitted, sent));
            } catch (SQLException e) {
                sent |= admitted[0];
                if (!sent) {
                    throw e; // nothing was sent: seq is still free
                }
                if (!admitted[0] || !"08S01".equals(e.getSQLState()) || ++drops >= SENDS
                        || Retry.left(deadline) == 0 || Thread.currentThread().isInterrupted()) {
                    return new Delivery(null, null, e, false);
                }
                fresh = false;
                continue;
            }
            sent = true;
            s.answered = System.nanoTime();
            Frame f = frames.get(frames.size() - 1);
            if (f instanceof Done d) {
                settle(s, seq, d.txnState());
                session.raise(d.txn());
                return new Delivery(frames, null, null, false);
            }
            Failure e = (Failure) f;
            if (e.code().equals("in_flight")) {
                fresh = false;
                if (Retry.left(deadline) == 0) {
                    return new Delivery(null, null, Errors.map(e), false);
                }
                try {
                    pause();
                } catch (SQLException interrupted) {
                    return new Delivery(null, null, interrupted, false);
                }
                continue;
            }
            // channel_lost is not a proof (ADR 0005 decision 4): the channel may have ended after running seq.
            if (e.code().equals("channel_lost") || e.code().equals("commit_outcome_unknown")) {
                return new Delivery(null, null, Errors.map(e), true);
            }
            // seq holds another body, so a resolve of seq would answer about that one.
            if (e.code().equals("sequence_conflict")) {
                throw unknown(s, seq, Errors.map(e), mode);
            }
            // A recorded failure is the outcome when it was not executed, or of a statement inside a transaction.
            if (e.isFinal() && (e.notExecuted() || mode == Mode.TXN)) {
                settle(s, seq, e.txnState() != null ? e.txnState() : s.state);
                if (e.txn() > 0) {
                    session.raise(e.txn());
                }
                return new Delivery(frames, null, null, false);
            }
            if (!e.isFinal() && e.notExecuted()) {
                if (fresh && e.code().equals("behind")) { // ADR 0005 decision 5: close it, then a new channel
                    giveUp(s);
                    return new Delivery(null, e, null, false);
                }
                boolean again;
                try {
                    again = Retry.refused(e) && Retry.pause(refusals++, deadline);
                } catch (SQLException interrupted) {
                    if (fresh) {
                        throw interrupted;
                    }
                    return new Delivery(null, null, interrupted, false);
                }
                if (again) {
                    continue; // the same bytes: an earlier send, if accepted, is replayed
                }
                if (fresh) {
                    throw Errors.map(e); // refused before acceptance: seq is still free
                }
            }
            // The hop may have delivered seq, as a lost answer may have: the same bytes replay its outcome.
            if (e.code().equals("owner_unreachable") && !e.notExecuted() && ++drops < SENDS
                    && Retry.left(deadline) > 0) {
                fresh = false;
                continue;
            }
            return new Delivery(null, null, Errors.map(e), false);
        }
    }

    /**
     * Settles request seq of channel t, which this driver gave up (ADR 0005 decision 4): a close answer settles it
     * unless the channel is gone (gone, or the close is answered channel_lost or not at all); otherwise resolve is
     * sent until the resolve wait ends, or callerDeadline when it is set and later than the cancel wait. Returns a
     * Done whose resolution is committed (the close's receipt, with its counts and itself as the outcome, or resolve's
     * Done carrying the commit's txn) or not_committed,
     * a recorded Failure from the close's receipt, or null when the outcome stays unknown.
     */
    private Frame settle(String t, long seq, boolean gone, long callerDeadline) {
        if (!gone) {
            try {
                List<Frame> frames = exchange("channel/close", Kind.CLOSE, Request.close(t),
                        Exchange.CANCEL_WAIT.toMillis(), closer);
                if (frames.get(frames.size() - 1) instanceof Done d && d.lastSeq() <= seq) {
                    if (d.lastSeq() < seq) { // ADR 0005 decision 4, case 4: never accepted, and now never will be
                        return resolution("not_committed", seq, d.outcome() instanceof Done r ? r.txn() : 0);
                    }
                    if (d.outcome() instanceof Done r) {
                        session.raise(r.txn());
                        return new Done(r.txn(), r.readOnly(), r.rowsAffected(), r.lastInsertRowid(), 0, seq,
                                "none", null, 0, r, false, r.entries(), "committed", null);
                    }
                    if (d.outcome() instanceof Failure r && r.isFinal() && r.notExecuted()) {
                        return r;
                    }
                }
            } catch (SQLException e) {
                // the channel's owner is gone or did not answer: resolve decides
            }
        }
        long now = System.nanoTime();
        long until = now + TimeUnit.MILLISECONDS.toNanos(resolveWaitMillis);
        if (callerDeadline != 0) { // sooner when the caller's own deadline is sooner, but never less than the cancel wait
            until = Math.min(until, Math.max(callerDeadline, now + TimeUnit.MILLISECONDS.toNanos(
                    Exchange.CANCEL_WAIT.toMillis())));
        }
        for (int attempt = 0; !abandoned(); ) {
            try {
                List<Frame> frames = exchange("resolve", Kind.RESOLVE, Request.cancel(t, seq),
                        Math.max(1, Retry.left(until)), closer);
                Frame f = frames.get(frames.size() - 1);
                if (f instanceof Done d) {
                    session.raise(d.txn());
                    switch (d.resolution()) {
                        case "committed", "not_committed" -> {
                            return resolution(d.resolution(), seq, d.txn());
                        }
                        case "unknown" -> {
                            return null;
                        }
                        default -> {} // pending: the channel may still be live; ask again
                    }
                } else if (!RESOLVE_AGAIN.contains(((Failure) f).code())) {
                    return null;
                }
            } catch (SQLException e) {
                if (!"08S01".equals(e.getSQLState())) {
                    return null;
                }
            }
            try {
                if (!Retry.pause(attempt++, until)) {
                    return null;
                }
            } catch (SQLException interrupted) {
                return null;
            }
        }
        return null;
    }

    private static Done resolution(String resolution, long seq, long txn) {
        return new Done(txn, false, 0, 0, 0, seq, "none", null, 0, null, false, 0, resolution, null);
    }

    /** The connection was closed by its user, not by its network timeout: nothing more is sent for it. */
    private boolean abandoned() {
        return conn.isClosed() && !conn.networkTimedOut();
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

    private static void settle(Slot s, long seq, String txnState) {
        s.last = seq;
        s.state = txnState;
    }

    private static void pause() throws SQLException {
        try {
            Thread.sleep(IN_FLIGHT_PAUSE_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw Errors.transport("interrupted while waiting for a request in flight", ie);
        }
    }

    /** Stops using s: it is closed without waiting, unless the connection is closing, whose close sends it. */
    private void giveUp(Slot s) {
        String t = s.token;
        if (t != null && !conn.isClosed()) {
            s.token = null;
            Exchange.async(() -> closeQuietly(t));
        }
    }

    /**
     * The request's outcome cannot be learnt and is not settled: the channel is given up, and with it the
     * transaction, if one is open; otherwise the connection is discarded (ADR 0005 decision 4).
     */
    private SQLException unknown(Slot s, long seq, SQLException cause, Mode mode) {
        s.state = "none";
        giveUp(s);
        if (mode.transaction()) {
            dead = true;
        } else {
            lost = true;
            conn.discard();
        }
        return unknownError(seq, cause);
    }

    private static SQLException unknownError(long seq, SQLException cause) {
        return new SQLNonTransientConnectionException("skylite: the outcome of request " + seq
                + " on the channel is unknown; it may have committed (" + cause.getMessage() + ")", "08007", cause);
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
     * Closes the channels, which rolls back an open transaction, waiting up to the cancel wait for each. A request in
     * flight on one ends first.
     */
    void close() {
        String w, r;
        synchronized (this) {
            w = write.token;
            r = read.token;
            write.token = null;
            read.token = null;
        }
        closeQuietly(w);
        closeQuietly(r);
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
