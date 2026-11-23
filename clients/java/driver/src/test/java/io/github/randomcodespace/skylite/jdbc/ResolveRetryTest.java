package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Settling a write whose answer was lost (ADR 0005 decision 4: close, then resolve), the driver's retries of
 * requests that were proved not to have run, read-your-writes, and the server's advertised body limit.
 */
@Timeout(20)
class ResolveRetryTest {
    private Scripted server;
    private long base, budget, resolveWait;

    @BeforeEach
    void start() throws Exception {
        server = new Scripted();
        base = Retry.baseMillis;
        budget = Retry.defaultBudgetMillis;
        resolveWait = Channel.resolveWaitMillis;
        Retry.baseMillis = 1;
    }

    @AfterEach
    void stop() {
        server.close();
        Retry.baseMillis = base;
        Retry.defaultBudgetMillis = budget;
        Channel.resolveWaitMillis = resolveWait;
    }

    private static String lost() {
        return FakeServer.channelError("channel_lost", false, false, "none");
    }

    private static String refused(String code) {
        return FakeServer.channelError(code, true, false, "none");
    }

    private static String recorded(String code) {
        return FakeServer.channelError(code, true, true, "none");
    }

    private static final String INSERT = "INSERT INTO t VALUES (1)";
    private static final String INSERTED = FakeServer.executed(1, "none", 1, 7);

    // ---- resolve ----

    @Test
    void aLostWriteResolvedCommittedIsSkr01AndTheConnectionStaysOpen() throws Exception {
        server.on("channel", lost()).on("resolve", FakeServer.resolved("committed", 42));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> s.executeUpdate(INSERT));
            assertEquals("SKR01", e.getSQLState(), e.toString());
            assertTrue(e.getMessage().contains("42"), e.getMessage());
            assertFalse(c.isClosed());
        }
        assertEquals(List.of("{\"channel\":\"w1.1\",\"seq\":1}"), server.bodies("resolve"),
                "the resolve names the unsettled request's own token and sequence number");
        assertEquals(1, server.bodies("channel").size(), "a committed write is never sent again");
    }

    @Test
    void aLostWriteResolvedNotCommittedIsSentOnceMoreOnANewChannel() throws Exception {
        server.on("channel", lost(), INSERTED).on("resolve", FakeServer.resolved("not_committed", 3));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        assertEquals(2, sent.size());
        assertTrue(sent.get(0).startsWith("{\"channel\":\"w1.1\",\"seq\":1,"), sent.get(0));
        assertTrue(sent.get(1).startsWith("{\"channel\":\"w1.2\",\"seq\":1,"), sent.get(1));
    }

    @Test
    void aWriteNotCommittedTwiceIsARollbackAndIsNotSentAThirdTime() throws Exception {
        server.on("channel", lost()).on("resolve", FakeServer.resolved("not_committed", 3));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLTransactionRollbackException.class, () -> s.executeUpdate(INSERT));
            assertEquals("40000", e.getSQLState());
            assertFalse(c.isClosed());
        }
        assertEquals(2, server.bodies("channel").size());
    }

    @Test
    void pendingIsResolvedAgainUntilItSettles() throws Exception {
        server.on("channel", lost()).on("resolve", FakeServer.resolved("pending", 3), "drop",
                FakeServer.error("no_leader", "m"), FakeServer.error("behind", "m"),
                FakeServer.resolved("committed", 9));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("SKR01", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
        assertEquals(5, server.bodies("resolve").size());
    }

    @Test
    void pendingToTheEndOfTheResolveWaitIsUnknownAndClosesTheConnection() throws Exception {
        Channel.resolveWaitMillis = 200;
        server.on("channel", lost()).on("resolve", FakeServer.resolved("pending", 3));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            SQLException e = assertThrows(SQLNonTransientConnectionException.class, () -> s.executeUpdate(INSERT));
            assertEquals("08007", e.getSQLState());
            assertTrue(c.isClosed());
        }
        assertTrue(server.bodies("resolve").size() > 1);
        assertEquals(1, server.bodies("channel").size());
    }

    @Test
    void unknownAndAnUnexpectedAnswerAreUnknown() throws Exception {
        for (String answer : List.of(FakeServer.resolved("unknown", 3), FakeServer.error("forbidden", "m"))) {
            server.on("channel", lost()).on("resolve", answer);
            try (Connection c = server.connect(); Statement s = c.createStatement()) {
                assertEquals("08007", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
                assertTrue(c.isClosed());
            }
        }
    }

    @Test
    void commitOutcomeUnknownIsResolved() throws Exception {
        server.on("channel", FakeServer.channelError("commit_outcome_unknown", false, false, "none"))
                .on("resolve", FakeServer.resolved("committed", 9));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("SKR01", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
    }

    @Test
    void aLostCommitResolvedCommittedSucceedsAndTheConnectionGoesOnOnANewChannel() throws Exception {
        afterASettledCommit("committed");
    }

    @Test
    void aLostCommitResolvedNotCommittedIsARollbackAndTheConnectionGoesOnOnANewChannel() throws Exception {
        afterASettledCommit("not_committed");
    }

    /**
     * ADR 0005, amended 2026-11-23: a commit settled by resolve leaves nothing about the session unknown, so the
     * connection stays open with its settings, and its next statement begins a transaction on a new channel at
     * seq 1, at the connection's watermark, which the resolve raised.
     */
    private void afterASettledCommit(String resolution) throws Exception {
        server.on("channel", FakeServer.executed(1, "open", 1, 1), lost(), FakeServer.executed(1, "open", 1, 1),
                        FakeServer.stepped(2))
                .on("resolve", FakeServer.resolved(resolution, 9));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            if (resolution.equals("committed")) {
                c.commit();
            } else {
                SQLException e = assertThrows(SQLTransactionRollbackException.class, c::commit);
                assertEquals("40000", e.getSQLState());
                c.rollback(); // nothing is open: sends nothing
            }
            assertFalse(c.isClosed(), "only the channel is gone");
            assertFalse(c.getAutoCommit());
            assertFalse(c.isReadOnly());
            assertEquals(1, s.executeUpdate(INSERT), "the connection's statements stay open too");
            c.commit();
        }
        assertEquals(List.of("{\"channel\":\"w1.1\",\"seq\":2}"), server.bodies("resolve"));
        List<String> sent = server.bodies("channel");
        assertEquals(4, sent.size(), sent.toString());
        assertTrue(sent.get(2).startsWith("{\"channel\":\"w1.2\",\"seq\":1,\"op\":\"begin\"")
                && sent.get(2).contains("\"min_txn\":9"), sent.get(2));
        assertTrue(sent.get(3).startsWith("{\"channel\":\"w1.2\",\"seq\":2,"), sent.get(3));
        assertEquals(2, server.bodies("channels").size());
    }

    /**
     * A pool returns a connection by resetting it, as HikariCP's ProxyConnection.close does: rollback when its commit
     * failed, the settings it changed, clearWarnings; and it may lend it again without validating it. After a settled
     * commit the connection is open, so that raises nothing and the next borrower's statement runs on a new channel;
     * after an unknown one it is closed, isValid is false, and the reset and every use raise 08003.
     */
    @Test
    void aPoolResetsAndReusesAConnectionAfterASettledCommit() throws Throwable {
        for (String resolution : List.of("committed", "not_committed", "unknown")) {
            server.on("channel", FakeServer.executed(1, "open", 1, 1), lost(), INSERTED)
                    .on("resolve", FakeServer.resolved(resolution, 9))
                    .on("describe", "{\"columns\":[{\"name\":\"1\"}]}\n" + FakeServer.describeDone(4, 0));
            Connection c = server.connect();
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.executeUpdate(INSERT);
            }
            if (resolution.equals("committed")) {
                c.commit();
            } else {
                assertThrows(SQLException.class, c::commit);
            }
            List<org.junit.jupiter.api.function.Executable> reset = List.of(c::rollback, () -> c.setReadOnly(false),
                    () -> c.setAutoCommit(true), () -> c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE),
                    () -> c.setCatalog("main"), () -> c.setNetworkTimeout(Runnable::run, 0), () -> c.setSchema("main"),
                    c::clearWarnings);
            if (resolution.equals("unknown")) {
                assertTrue(c.isClosed());
                assertFalse(c.isValid(1), "the pool drops it");
                for (var r : reset) {
                    assertEquals("08003", assertThrows(SQLException.class, r).getSQLState());
                }
                SQLException used = assertThrows(SQLException.class, c::createStatement);
                assertEquals("08003", used.getSQLState());
                assertTrue(used.getMessage().contains("unknown outcome"), used.getMessage());
            } else {
                for (var r : reset) {
                    r.execute();
                }
                assertFalse(c.isClosed(), resolution);
                assertTrue(c.isValid(1), resolution);
                int before = server.bodies("channel").size();
                try (Statement s = c.createStatement()) {
                    assertEquals(1, s.executeUpdate(INSERT), resolution);
                }
                List<String> sent = server.bodies("channel");
                assertEquals(before + 1, sent.size(), resolution);
                assertTrue(sent.get(before).contains("\"seq\":1,\"op\":\"execute\""), sent.get(before));
            }
            c.close();
        }
    }

    /**
     * ADR 0005 decision 4: behind inside a transaction would close the channel and the transaction with it. It is
     * lost, as after an unknown outcome: never sent again on a new channel, and never committed.
     */
    @Test
    void behindInsideATransactionLosesItAndOpensNoNewChannel() throws Exception {
        Retry.defaultBudgetMillis = 500; // a reopen would be sent again until then
        server.on("channel", FakeServer.executed(1, "open", 1, 1), refused("behind"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            assertEquals("SKR03", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
            assertEquals("SKR03", assertThrows(SQLException.class, c::commit).getSQLState());
            c.rollback();
        }
        assertEquals(2, server.bodies("channel").size());
        assertEquals(1, server.bodies("channels").size());

        server.on("channel", FakeServer.executed(1, "open", 1, 1), refused("behind")); // the commit's answer
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            assertEquals("SKR03", assertThrows(SQLException.class, c::commit).getSQLState());
            c.rollback();
            assertFalse(c.isClosed());
        }
        assertEquals(2 + 2, server.bodies("channel").size());
        assertEquals(2, server.bodies("channels").size());
    }

    /** behind of a resend may follow an accepted send: the same bytes again, never a new channel. */
    @Test
    void behindAfterALostAnswerIsSentAgainOnTheSameChannel() throws Exception {
        server.on("channel", "drop", refused("behind"), INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        assertEquals(3, sent.size());
        assertEquals(1, sent.stream().distinct().count(), sent.toString());
        assertEquals(1, server.bodies("channels").size());
    }

    /** A live channel is settled by its close: last_seq below the request proves it never ran. */
    @Test
    void aWriteWhoseAnswersAreLostIsSettledByTheClose() throws Exception {
        server.on("channel", "drop", "drop", "drop", INSERTED).on("channel/close", "{\"done\":{\"last_seq\":0}}");
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        assertEquals("{\"channel\":\"w1.1\"}", server.bodies("channel/close").get(0));
        assertEquals(0, server.bodies("resolve").size());
        assertEquals(4, server.bodies("channel").size());

        // last_seq of the request with its receipt: committed, with the count executeUpdate needs and the key.
        String receipt = "{\"done\":{\"last_seq\":1,\"outcome\":{\"seq\":1,\"ok\":true,"
                + "\"txn_state\":\"none\",\"txn\":5,\"read_only\":false,\"rows_affected\":3,\"last_insert_rowid\":7}}}";
        server.on("channel", "drop").on("channel/close", receipt);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(3, s.executeUpdate(INSERT, Statement.RETURN_GENERATED_KEYS));
            try (ResultSet keys = s.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertEquals(7, keys.getLong(1));
            }
        }

        // execute, which may want rows that the receipt does not hold: committed, and its result is not at hand.
        server.on("query", FakeServer.error("needs_channel", "m")).on("channel", "drop").on("channel/close", receipt);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("SKR01", assertThrows(SQLException.class, () -> s.execute(INSERT)).getSQLState());
        }
    }

    /** ADR 0005 decision 3: a forwarding pod that lost the owner's answer is no proof; the same bytes replay it. */
    @Test
    void ownerUnreachableThatMayHaveRunIsSentAgainWithTheSameBytes() throws Exception {
        String unreachable = FakeServer.channelError("owner_unreachable", false, false, "none");
        server.on("channel", unreachable, INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        assertEquals(2, sent.size());
        assertEquals(1, sent.stream().distinct().count(), sent.toString());
        assertEquals(0, server.bodies("resolve").size());

        // Unreachable on all three sends: then settled, and the close's receipt has the count.
        server.on("channel", unreachable).on("channel/close", "{\"done\":{\"last_seq\":1,\"outcome\":{\"seq\":1,"
                + "\"ok\":true,\"txn_state\":\"none\",\"txn\":5,\"read_only\":false,\"rows_affected\":1,"
                + "\"last_insert_rowid\":7}}}");
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        assertEquals(2 + 3, server.bodies("channel").size());
    }

    /** The close is lost too: resolve decides. */
    @Test
    void aCloseAnsweredChannelLostSendsResolve() throws Exception {
        server.on("channel", "drop").on("channel/close", lost()).on("resolve", FakeServer.resolved("committed", 4));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("SKR01", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
        assertEquals(1, server.bodies("resolve").size());
    }

    /** ADR 0005 decision 4: a resolve of a sequence number that holds another body would answer about that one. */
    @Test
    void sequenceConflictIsUnknownWithoutAResolve() throws Exception {
        server.on("channel", FakeServer.channelError("sequence_conflict", true, true, "none"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("08007", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
        assertEquals(0, server.bodies("resolve").size());
    }

    /** A commit whose answer never comes is bounded by the statement time, then settled. */
    @Test
    void aCommitThatHangsIsBoundedAndResolved() throws Exception {
        Retry.defaultBudgetMillis = 300;
        server.on("channel", FakeServer.executed(1, "open", 1, 1), "sleep:15000").on("channel/close", lost())
                .on("resolve", FakeServer.resolved("committed", 9));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            long t0 = System.nanoTime();
            c.commit();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 6_000, "commit took " + ms + " ms");
        }
    }

    /** A statement inside a transaction is never resolved: only a commit's number is stored (ADR 0005). */
    @Test
    void aLostStatementInsideATransactionIsNotResolved() throws Exception {
        server.on("channel", FakeServer.executed(1, "open", 1, 1), lost());
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            assertEquals("08007", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
            assertEquals("SKR03", assertThrows(SQLException.class, c::commit).getSQLState());
            c.rollback();
        }
        assertEquals(0, server.bodies("resolve").size());
    }

    // ---- retries ----

    @Test
    void aRefusedFirstSendIsSentAgainWithTheSameBody() throws Exception {
        server.on("channel", refused("overloaded"), refused("not_serving"), refused("maintenance"), INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        assertEquals(4, sent.size());
        assertEquals(1, sent.stream().distinct().count(), "every send is the same request: " + sent);
    }

    @Test
    void aRefusalOfAResendAfterALostAnswerIsSentAgain() throws Exception {
        server.on("channel", "drop", refused("overloaded"), INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        assertEquals(1, server.bodies("channel").stream().distinct().count());
    }

    /** An answer that says the request may have run is never a reason to send it again: it is settled instead. */
    @Test
    void aRefusalThatMayHaveRunIsSettledNotSentAgain() throws Exception {
        server.on("channel", FakeServer.channelError("overloaded", false, false, "none"), INSERTED)
                .on("channel/close", lost()).on("resolve", FakeServer.resolved("committed", 5));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals("SKR01", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
        assertEquals(1, server.bodies("channel").size());
    }

    @Test
    void recordedBusyIsSentAgainAsANewRequest() throws Exception {
        server.on("channel", recorded("busy"), recorded("overloaded"), FakeServer.executed(3, "none", 1, 7));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        for (int i = 0; i < 3; i++) {
            assertTrue(sent.get(i).startsWith("{\"channel\":\"w1.1\",\"seq\":" + (i + 1) + ","), sent.get(i));
        }
    }

    @Test
    void aBusyBeginIsSentAgainAsANewBegin() throws Exception {
        server.on("channel", recorded("busy"), FakeServer.executed(2, "open", 1, 7), FakeServer.stepped(3));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            assertEquals(1, s.executeUpdate(INSERT));
            c.commit();
        }
        List<String> sent = server.bodies("channel");
        assertTrue(sent.get(0).contains("\"op\":\"begin\"") && sent.get(1).contains("\"op\":\"begin\""), sent.toString());
    }

    /** Never sent again: a recorded failure that ran, or that will fail again. */
    @Test
    void failuresThatRanOrWillFailAgainAreNotSentAgain() throws Exception {
        server.on("channel", recorded("sql_error"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeUpdate(INSERT));
        }
        assertEquals(1, server.bodies("channel").size());

        server.on("channel", FakeServer.executed(1, "open", 1, 1), FakeServer.channelError("busy", false, true, "open"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate(INSERT);
            assertEquals("40001", assertThrows(SQLException.class, () -> s.executeUpdate(INSERT)).getSQLState());
        }
        assertEquals(1 + 2, server.bodies("channel").size(), "a statement inside a transaction is never sent again");
    }

    @Test
    void retriesEndAtTheQueryTimeout() throws Exception {
        server.on("channel", refused("overloaded"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.setQueryTimeout(1);
            long t0 = System.nanoTime();
            SQLException e = assertThrows(SQLTransientConnectionException.class, () -> s.executeUpdate(INSERT));
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals("08004", e.getSQLState());
            assertTrue(ms >= 1_000 && ms < 15_000, "gave up after " + ms + " ms"); // 3 s, and slack for CI
            assertFalse(c.isClosed(), "nothing ran: the connection stays usable");
        }
        assertTrue(server.bodies("channel").size() > 2);
    }

    @Test
    void aRefusedQueryIsSentAgainWithTheWatermarkReadAgain() throws Exception {
        server.on("query", FakeServer.error("overloaded", "m"), FakeServer.error("behind", "m"),
                FakeServer.error("not_serving", "m"), "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n" + FakeServer.done(1));
        try (Connection c = server.connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        assertEquals(4, server.bodies("query").size());

        server.on("query", FakeServer.error("invalid_request", "m"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1"));
        }
        assertEquals(5, server.bodies("query").size(), "a request that is wrong is not sent again");
    }

    @Test
    void aRefusedOpenIsSentAgain() throws Exception {
        server.on("channels", FakeServer.error("too_many_channels", "m"), FakeServer.error("no_leader", "m"),
                FakeServer.opened("w1.x")).on("channel", INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        assertEquals(3, server.bodies("channels").size());
    }

    /** ADR 0005 decision 5: behind on a channel closes it, and the request goes on a new channel. */
    @Test
    void behindOnAChannelClosesItAndUsesANewOne() throws Exception {
        server.on("channel", refused("behind"), INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
        }
        List<String> sent = server.bodies("channel");
        assertTrue(sent.get(1).startsWith("{\"channel\":\"w1.2\",\"seq\":1,"), sent.toString());
        long until = System.nanoTime() + 5_000_000_000L; // the behind channel is closed on another thread
        while (!server.bodies("channel/close").contains("{\"channel\":\"w1.1\"}") && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        assertTrue(server.bodies("channel/close").contains("{\"channel\":\"w1.1\"}"), "the behind channel is closed");
    }

    @Test
    void pausesAreJitteredGrowAndStopAtTheDeadline() throws Exception {
        Retry.baseMillis = 40;
        long t0 = System.nanoTime();
        assertTrue(Retry.pause(0, System.nanoTime() + 10_000_000_000L));
        long first = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(first >= 20 && first < 450, "first pause " + first + " ms"); // the 1 s bound: 500 or more
        t0 = System.nanoTime();
        assertTrue(Retry.pause(2, System.nanoTime() + 10_000_000_000L));
        long third = (System.nanoTime() - t0) / 1_000_000;
        assertTrue(third >= 80, "third pause " + third + " ms");
        t0 = System.nanoTime();
        assertFalse(Retry.pause(10, System.nanoTime() + 100_000_000L), "a pause past the deadline is not taken");
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 450, "a pause taken would be 500 ms or more");
    }

    // ---- read-your-writes and limits ----

    @Test
    void theWatermarkGoesWithEveryReadBeginAndExecute() throws Exception {
        server.on("channel", FakeServer.executed(1, "none", 1, 7));
        server.on("query", "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n" + FakeServer.done(1));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.executeUpdate(INSERT); // answers txn 2
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                rs.next();
            }
            c.setReadOnly(true);
            c.setAutoCommit(false);
            server.on("channel", "{\"columns\":[{\"name\":\"x\"}]}\n" + FakeServer.executed(1, "open", 0, 0));
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertFalse(rs.next());
            }
        }
        assertTrue(server.bodies("query").get(0).contains("\"min_txn\":2"), server.bodies("query").toString());
        assertTrue(server.bodies("channels").get(1).equals("{\"mode\":\"read_only\",\"min_txn\":2}"),
                server.bodies("channels").toString());
        List<String> sent = server.bodies("channel");
        assertTrue(sent.get(1).startsWith("{\"channel\":\"r1.2\",\"seq\":1,\"op\":\"begin\"") && sent.get(1).contains("\"min_txn\":2"),
                sent.toString());
    }

    @Test
    void aChannelFarBelowTheWatermarkIsReplaced() throws Exception {
        server.limits = "{\"resolve_bound\":10}";
        server.on("channel", "{\"done\":{\"seq\":1,\"txn_state\":\"none\",\"txn\":100,\"read_only\":false,"
                + "\"rows_affected\":1,\"last_insert_rowid\":1}}", INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.executeUpdate(INSERT); // raises the watermark to 100, far above the open head 1
            s.executeUpdate(INSERT);
        }
        assertEquals(2, server.bodies("channels").size());
    }

    @Test
    void aRequestAboveTheAdvertisedBodyLimitIsRefusedBeforeItIsSent() throws Exception {
        server.limits = "{\"request_body_bytes\":100}";
        server.on("channel", INSERTED);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate(INSERT));
            SQLException e = assertThrows(SQLException.class, () -> s.executeUpdate("INSERT INTO t VALUES ('" + "x".repeat(200) + "')"));
            assertEquals("54000", e.getSQLState());
            e = assertThrows(SQLException.class, () -> s.executeQuery("SELECT '" + "x".repeat(200) + "'"));
            assertEquals("54000", e.getSQLState());
            assertFalse(c.isClosed());
        }
        assertEquals(1, server.bodies("channel").size());
        assertEquals(0, server.bodies("query").size());
    }

    @Test
    void aReadOnlyConnectionInAutoCommitStillReadsStatelessly() throws Exception {
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            assertInstanceOf(ResultSet.class, s.executeQuery("SELECT 1"));
        }
        assertEquals(0, server.bodies("channels").size());
    }
}
