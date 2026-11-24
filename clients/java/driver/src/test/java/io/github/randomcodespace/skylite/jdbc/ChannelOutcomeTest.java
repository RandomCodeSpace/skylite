package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What the driver reports when a write channel request's outcome is unknown or its channel is lost (ADR 0005
 * decisions 3 and 4), and the races between a connection's transaction methods, its close and its statements.
 */
@Timeout(15)
class ChannelOutcomeTest {
    private FakeServer fake;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer(null, "outcome-tok");
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private Connection connect() throws SQLException {
        Properties p = new Properties();
        p.setProperty("allowPlaintext", "true");
        p.put("tokenProvider", (TokenProvider) () -> "outcome-tok");
        return DriverManager.getConnection(fake.url("outcome"), p);
    }

    private void script(String... answers) {
        AtomicInteger next = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.p" + next.get()));
            } else if (r.path().endsWith("/channel")) {
                int i = next.getAndIncrement();
                String a = answers[Math.min(i, answers.length - 1)];
                if (a.equals("drop")) {
                    ex.close();
                } else {
                    FakeServer.send(ex, a.contains("\"error\"") && !a.contains("\"final\":true") ? 409 : 200, a);
                }
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
    }

    /** channel_lost is not a proof (ADR 0005 decision 4): an autocommit write is never sent on a new channel. */
    @Test
    void channelLostOnFirstSendMustNotReexecuteAutocommitWrite() throws Exception {
        script(FakeServer.channelError("channel_lost", true, false, "none"), FakeServer.executed(1, "none", 1, 9));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("08007", e.getSQLState(), e.toString());
        }
        assertEquals(1, fake.requests.stream().filter(r -> r.path().endsWith("/channels")).count(),
                "must not open a second channel and re-execute the write");
    }

    /** A transaction lost with its channel fails its later statements and its commit, and opens no new channel. */
    @Test
    void channelLostMidTransactionMustFailCommitAndNotStartSplitTransaction() throws Exception {
        script(FakeServer.executed(1, "open", 1, 1),
                FakeServer.channelError("channel_lost", true, false, "none"),
                FakeServer.executed(1, "open", 1, 2));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertThrows(SQLException.class, () -> s.executeUpdate("INSERT INTO t VALUES (2)"));
            assertThrows(SQLException.class, c::commit, "commit silently succeeded on a lost transaction");
        }

        // Another statement after channel_lost opens no new channel.
        fake.requests.clear();
        script(FakeServer.executed(1, "open", 1, 1),
                FakeServer.channelError("channel_lost", true, false, "none"),
                FakeServer.executed(1, "open", 1, 2));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertThrows(SQLException.class, () -> s.executeUpdate("INSERT INTO t VALUES (2)"));
            assertThrows(SQLException.class, () -> s.executeUpdate("INSERT INTO t VALUES (3)"),
                    "must not start a new transaction on a new channel before rollback");
            assertEquals(1, fake.requests.stream().filter(r -> r.path().endsWith("/channels")).count(),
                    "must not open a new channel for a lost transaction");
        }
    }

    /** channel_lost on a commit leaves its outcome unknown, and the connection is given up. */
    @Test
    void channelLostOnCommitMustRaise08007Not08003() throws Exception {
        script(FakeServer.executed(1, "open", 1, 1),
                FakeServer.channelError("channel_lost", true, false, "none"));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            SQLException e = assertThrows(SQLNonTransientConnectionException.class, c::commit);
            assertEquals("08007", e.getSQLState(), "got " + e.getSQLState() + ": " + e.getMessage());
            assertFalse(c.isValid(2), "connection with unknown commit outcome must not be valid");
        }
    }

    /** After an unknown outcome inside a transaction, neither commit nor setAutoCommit(true) succeeds. */
    @Test
    void unknownOutcomeMidTransactionMustNotAllowSilentCommit() throws Exception {
        script(FakeServer.executed(1, "open", 1, 1), "drop", "drop", "drop");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            SQLException e = assertThrows(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (2)"));
            assertEquals("08007", e.getSQLState());
            assertThrows(SQLException.class, c::commit, "commit silently succeeded after 08007");
            assertThrows(SQLException.class, () -> c.setAutoCommit(true),
                    "setAutoCommit(true) silently succeeded after 08007");
        }
    }

    /** An interrupt in the pause after in_flight leaves the outcome unknown; the sequence number is never reused. */
    @Test
    void interruptDuringInFlightPauseMustRaise08007AndGiveUpChannel() throws Exception {
        CountDownLatch inFlightSent = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.inf"));
            } else if (r.path().endsWith("/channel")) {
                int n = calls.getAndIncrement();
                if (n == 0) {
                    ex.close(); // the first answer is lost
                } else if (n == 1) {
                    FakeServer.send(ex, 409, FakeServer.channelError("in_flight", true, false, "none"));
                    inFlightSent.countDown();
                } else {
                    // what a reuse of seq 1 would get
                    FakeServer.send(ex, 200, FakeServer.executed(1, "none", 1, 77));
                }
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
        try (Connection c = connect(); Statement s = c.createStatement()) {
            Thread caller = Thread.currentThread();
            var pool = Executors.newSingleThreadExecutor();
            try {
                pool.submit(() -> {
                    if (inFlightSent.await(5, TimeUnit.SECONDS)) {
                        Thread.sleep(50); // inside the pause after in_flight
                        caller.interrupt();
                    }
                    return null;
                });
                SQLException e = assertThrows(SQLException.class,
                        () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
                Thread.interrupted();
                assertEquals("08007", e.getSQLState(), "expected 08007, got " + e.getSQLState() + ": " + e);
                assertFalse(c.isValid(2), "channel must be given up after interrupted in_flight wait");
                assertThrows(SQLNonTransientConnectionException.class,
                        () -> s.executeUpdate("INSERT INTO t VALUES (1)"),
                        "must not reuse channel and seq=1 after interrupted in_flight");
            } finally {
                Thread.interrupted();
                pool.shutdownNow();
            }
        }
    }

    /** in_flight answers do not count against the resends after lost answers. */
    @Test
    void inFlightFollowedByTransportDropsMustStopAtResendLimit() throws Exception {
        AtomicInteger channelCalls = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.loop"));
            } else if (r.path().endsWith("/channel")) {
                int n = channelCalls.incrementAndGet();
                if (n <= 3) {
                    FakeServer.send(ex, 409, FakeServer.channelError("in_flight", true, false, "none"));
                } else if (n <= 10) {
                    ex.close(); // answers lost in transport
                } else {
                    // ends an unbounded resend loop with a failed assertion rather than a timeout
                    FakeServer.send(ex, 200, FakeServer.channelError("commit_outcome_unknown", false, false, "none"));
                }
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("08007", e.getSQLState());
            assertTrue(channelCalls.get() <= 6,
                    "expected bounded resends (<=6), but driver sent " + channelCalls.get() + " times");
        }
    }

    /** A final commit failure with not_executed false proves nothing: unknown, not a rollback. */
    @Test
    void commitFailureWithNotExecutedFalseMustRaise08007() throws Exception {
        script(FakeServer.executed(1, "open", 1, 1),
                FakeServer.channelError("commit_failed", false, true, "none"));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            SQLException e = assertThrows(SQLNonTransientConnectionException.class, c::commit);
            assertEquals("08007", e.getSQLState(), "got " + e.getClass().getSimpleName() + " " + e.getSQLState());
            assertFalse(c.isValid(2));
        }
    }

    /** A statement queued behind a commit while the connection closes opens no channel. */
    @Test
    void closeWhileStatementQueuedOnChannelMustNotOpenNewChannel() throws Exception {
        CountDownLatch commitAtServer = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        AtomicInteger opens = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                opens.incrementAndGet();
                FakeServer.send(ex, 200, FakeServer.opened("w1.q" + opens.get()));
            } else if (r.path().endsWith("/channel")) {
                if (r.bodyText().contains("\"op\":\"commit\"")) {
                    commitAtServer.countDown();
                    releaseCommit.await(5, TimeUnit.SECONDS);
                    FakeServer.send(ex, 200, FakeServer.stepped(2));
                } else {
                    FakeServer.send(ex, 200, FakeServer.executed(1, "open", 1, 1));
                }
            } else {
                FakeServer.send(ex, 200, "{\"done\":{\"last_seq\":1,\"outcome\":{\"seq\":1,\"ok\":true,"
                        + "\"txn_state\":\"none\",\"txn\":2}}}");
            }
        };
        var pool = Executors.newCachedThreadPool();
        try {
            Connection c = connect();
            Statement s1 = c.createStatement();
            Statement s2 = c.createStatement();
            c.setAutoCommit(false);
            s1.executeUpdate("INSERT INTO t VALUES (1)");
            assertEquals(1, opens.get());

            Future<?> commitFuture = pool.submit(() -> {
                c.commit();
                return null;
            });
            assertTrue(commitAtServer.await(5, TimeUnit.SECONDS));

            // s2 waits for the channel, which the commit holds.
            Future<SQLException> s2Future = pool.submit(() -> assertThrows(SQLException.class,
                    () -> s2.executeUpdate("INSERT INTO t VALUES (2)")));
            Thread.sleep(80);

            Future<?> closing = pool.submit(() -> {
                c.close(); // waits for the commit in flight
                return null;
            });
            Thread.sleep(80);
            releaseCommit.countDown();
            closing.get(5, TimeUnit.SECONDS);
            commitFuture.get(5, TimeUnit.SECONDS);
            s2Future.get(5, TimeUnit.SECONDS);

            assertEquals(1, opens.get(),
                    "the closed connection opened " + opens.get() + " channels");
        } finally {
            releaseCommit.countDown();
            pool.shutdownNow();
        }
    }

    /** A network timeout closes the connection, and the commit is not sent again on it. */
    @Test
    void networkTimeoutOnCommitMustNotResendOnClosedConnection() throws Exception {
        AtomicInteger commitSends = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.net"));
            } else if (r.path().endsWith("/channel")) {
                if (r.bodyText().contains("\"op\":\"commit\"")) {
                    commitSends.incrementAndGet();
                    Thread.sleep(400); // past the network timeout
                } else {
                    FakeServer.send(ex, 200, FakeServer.executed(1, "open", 1, 1));
                }
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
        var exec = Executors.newSingleThreadExecutor();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            c.setNetworkTimeout(exec, 100);
            assertThrows(SQLException.class, c::commit);
            assertEquals(1, commitSends.get(),
                    "commit was sent " + commitSends.get() + " times after network timeout closed the connection");
        } finally {
            exec.shutdownNow();
        }
    }

    /** setAutoCommit(true) waits for a begin in flight and commits its transaction. */
    @Test
    void setAutoCommitTrueRacingInFlightBeginMustNotLeaveOpenTransaction() throws Exception {
        CountDownLatch beginAtServer = new CountDownLatch(1);
        CountDownLatch releaseBegin = new CountDownLatch(1);
        AtomicInteger commits = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.ac"));
            } else if (r.path().endsWith("/channel")) {
                if (r.bodyText().contains("\"op\":\"begin\"")) {
                    beginAtServer.countDown();
                    releaseBegin.await(5, TimeUnit.SECONDS);
                    FakeServer.send(ex, 200, FakeServer.executed(1, "open", 1, 1));
                } else if (r.bodyText().contains("\"op\":\"commit\"")) {
                    commits.incrementAndGet();
                    FakeServer.send(ex, 200, FakeServer.stepped(2));
                } else {
                    FakeServer.send(ex, 200, FakeServer.executed(2, "none", 1, 2));
                }
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
        var pool = Executors.newCachedThreadPool();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            Future<Integer> beg = pool.submit(() -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertTrue(beginAtServer.await(5, TimeUnit.SECONDS));
            Future<?> flip = pool.submit(() -> {
                c.setAutoCommit(true);
                return null;
            });
            Thread.sleep(50);
            releaseBegin.countDown();
            beg.get(5, TimeUnit.SECONDS);
            flip.get(5, TimeUnit.SECONDS);
            assertTrue(c.getAutoCommit());
            assertEquals("none", c.unwrap(SkyliteConnection.class).channel().state(),
                    "autoCommit is true while channel still has an open transaction!");
            assertEquals(1, commits.get(), "setAutoCommit(true) failed to commit the in-flight begin");
        } finally {
            pool.shutdownNow();
        }
    }

    /** A committed autocommit write whose result was dropped is SKR01, with its transaction number. */
    @Test
    void dataOmittedMustUseSkr01AndSkr02() throws Exception {
        script("{\"done\":{\"seq\":1,\"txn_state\":\"none\",\"txn\":42,\"read_only\":false,"
                + "\"rows_affected\":1,\"last_insert_rowid\":1,\"data_omitted\":true}}");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("SKR01", e.getSQLState(), "got " + e.getSQLState() + ": " + e.getMessage());
            assertTrue(e.getMessage().contains("42"), "message lacks txn number: " + e.getMessage());
        }
    }

    /** A PreparedStatement refuses the Statement methods that take SQL and generated keys. */
    @Test
    void preparedStatementMustRefuseSqlOverloadsWithGeneratedKeys() throws Exception {
        script(FakeServer.executed(1, "none", 1, 1));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1")) {
            assertThrows(SQLException.class, () -> ps.executeUpdate("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS));
            assertEquals(0, fake.requests.size(), "PreparedStatement executed arbitrary SQL string");
        }
    }
}
