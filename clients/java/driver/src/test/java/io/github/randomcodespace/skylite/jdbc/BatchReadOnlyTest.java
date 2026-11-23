package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** executeBatch on the channel's batch request, and read-only transactions on a read-only channel. */
@Timeout(20)
class BatchReadOnlyTest {
    private Scripted server;
    private long base;

    @BeforeEach
    void start() throws Exception {
        server = new Scripted();
        base = Retry.baseMillis;
        Retry.baseMillis = 1;
    }

    @AfterEach
    void stop() {
        server.close();
        Retry.baseMillis = base;
    }

    private static String begun(long seq) {
        return "{\"done\":{\"seq\":" + seq + ",\"txn_state\":\"open\",\"txn\":2}}";
    }

    private List<String> batches() {
        return server.bodies("channel").stream().filter(b -> b.contains("\"op\":\"batch\"")).toList();
    }

    @Test
    void aPreparedBatchInAutoCommitIsOneBatchRequest() throws Exception {
        server.on("channel", FakeServer.results(0, 1, 1, 0) + "\n" + FakeServer.batched(1, "none", 2, 3));
        try (Connection c = server.connect();
                PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 1; i <= 3; i++) {
                p.setInt(1, i);
                p.addBatch();
            }
            assertArrayEquals(new int[] {1, 1, 0}, p.executeBatch());
            List<Long> keys = new ArrayList<>();
            try (ResultSet rs = p.getGeneratedKeys()) {
                while (rs.next()) {
                    keys.add(rs.getLong(1));
                }
            }
            assertEquals(List.of(100L, 101L), keys, "a key for each entry that inserted a row");
            assertArrayEquals(new int[0], p.executeBatch(), "an empty batch sends nothing");
        }
        assertEquals(1, server.bodies("channel").size());
        assertTrue(server.bodies("channel").get(0).startsWith("{\"channel\":\"w1.1\",\"seq\":1,\"op\":\"batch\","
                + "\"sql\":\"INSERT INTO t VALUES (?)\",\"param_sets\":[[1],[2],[3]]"), server.bodies("channel").get(0));
    }

    @Test
    void aStatementBatchIsOneBatchOfStatements() throws Exception {
        server.on("channel", FakeServer.results(0, 1, 2) + "\n" + FakeServer.batched(1, "none", 3, 2));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.addBatch("DELETE FROM a");
            s.addBatch("DELETE FROM b");
            assertArrayEquals(new long[] {1, 2}, s.executeLargeBatch());
        }
        assertTrue(server.bodies("channel").get(0).contains(
                "\"op\":\"batch\",\"statements\":[{\"sql\":\"DELETE FROM a\"},{\"sql\":\"DELETE FROM b\"}]"),
                server.bodies("channel").get(0));
    }

    /** An autocommit batch is one transaction: a failure leaves nothing, so no entry counts as run. */
    @Test
    void aFailedAutocommitBatchHasNoCounts() throws Exception {
        server.on("channel", "{\"error\":{\"code\":\"sql_error\",\"not_executed\":true,\"final\":true,\"message\":\"m\","
                + "\"request_id\":\"r\",\"txn_state\":\"none\",\"sqlite_code\":19,\"batch_index\":1}}");
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.addBatch("INSERT INTO a VALUES (1)");
            s.addBatch("INSERT INTO a VALUES (1)");
            BatchUpdateException e = assertThrows(BatchUpdateException.class, s::executeBatch);
            assertArrayEquals(new int[0], e.getUpdateCounts());
            assertEquals("23000", e.getSQLState());
            assertSame(e.getCause(), e.getNextException());
        }
    }

    /** Inside a transaction a batch stops at its first failure; the entries before it ran and keep their counts. */
    @Test
    void aFailedBatchInATransactionHasTheCountsBeforeItsFailedEntry() throws Exception {
        server.on("channel", begun(1), FakeServer.results(0, 1) + "\n{\"error\":{\"code\":\"sql_error\","
                + "\"not_executed\":false,\"final\":true,\"message\":\"m\",\"request_id\":\"r\",\"txn_state\":\"open\","
                + "\"sqlite_code\":19,\"batch_index\":1}}", FakeServer.stepped(3));
        try (Connection c = server.connect();
                PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            c.setAutoCommit(false);
            for (int i = 0; i < 3; i++) {
                p.setInt(1, i);
                p.addBatch();
            }
            BatchUpdateException e = assertThrows(BatchUpdateException.class, p::executeBatch);
            assertArrayEquals(new int[] {1}, e.getUpdateCounts());
            try (ResultSet rs = p.getGeneratedKeys()) {
                assertTrue(rs.next());
                assertEquals(100, rs.getLong(1));
                assertFalse(rs.next());
            }
            c.rollback();
        }
        List<String> sent = server.bodies("channel");
        assertTrue(sent.get(0).startsWith("{\"channel\":\"w1.1\",\"seq\":1,\"op\":\"begin\""), sent.get(0));
        assertTrue(sent.get(1).startsWith("{\"channel\":\"w1.1\",\"seq\":2,\"op\":\"batch\""), sent.get(1));
    }

    /** Inside a transaction a batch above the entry limit is split; outside one it is refused, unsent. */
    @Test
    void aBatchAboveTheEntryLimitIsSplitOnlyInsideATransaction() throws Exception {
        server.limits = "{\"batch_entries\":2}";
        server.on("channel", begun(1), FakeServer.results(0, 1, 1) + "\n" + FakeServer.batched(2, "open", 2, 2),
                FakeServer.results(0, 1, 1) + "\n" + FakeServer.batched(3, "open", 2, 2),
                FakeServer.results(0, 1) + "\n" + FakeServer.batched(4, "open", 1, 1), FakeServer.stepped(5));
        try (Connection c = server.connect(); PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES (?)")) {
            c.setAutoCommit(false);
            for (int i = 0; i < 5; i++) {
                p.setInt(1, i);
                p.addBatch();
            }
            assertArrayEquals(new int[] {1, 1, 1, 1, 1}, p.executeBatch());
            c.commit();
            assertEquals(3, batches().size());
            assertTrue(batches().get(2).contains("\"param_sets\":[[4]]"), batches().get(2));

            c.setAutoCommit(true);
            for (int i = 0; i < 3; i++) {
                p.setInt(1, i);
                p.addBatch();
            }
            BatchUpdateException e = assertThrows(BatchUpdateException.class, p::executeBatch);
            assertEquals("54000", e.getSQLState());
            assertArrayEquals(new int[0], e.getUpdateCounts());
        }
        assertEquals(3, batches().size(), "the autocommit batch is not sent");
    }

    @Test
    void aBatchAboveTheBodyLimitIsSplitInsideATransaction() throws Exception {
        server.limits = "{\"request_body_bytes\":300}";
        String one = FakeServer.results(0, 1) + "\n" + FakeServer.batched(2, "open", 1, 1);
        server.on("channel", begun(1), one, one, one, FakeServer.stepped(5));
        try (Connection c = server.connect(); PreparedStatement p = c.prepareStatement("INSERT INTO t VALUES (?)")) {
            c.setAutoCommit(false);
            for (int i = 0; i < 3; i++) {
                p.setString(1, "v".repeat(100));
                p.addBatch();
            }
            assertArrayEquals(new int[] {1, 1, 1}, p.executeBatch());
            c.commit();
        }
        assertEquals(3, batches().size());
        for (String b : batches()) {
            assertTrue(b.length() <= 300, b.length() + " bytes");
        }
    }

    /** A batch whose answer was replayed without its results: committed in auto-commit, still open in a transaction. */
    @Test
    void aBatchReplayedWithoutItsResultsIsSkr01OrSkr02() throws Exception {
        String omitted = "{\"done\":{\"seq\":1,\"txn_state\":\"none\",\"txn\":2,\"rows_affected\":2,"
                + "\"last_insert_rowid\":0,\"entries\":2,\"data_omitted\":true}}";
        server.on("channel", "drop", omitted);
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            s.addBatch("DELETE FROM a");
            s.addBatch("DELETE FROM b");
            assertEquals("SKR01", assertThrows(BatchUpdateException.class, s::executeBatch).getSQLState());
        }
    }

    @Test
    void aBatchOnAReadOnlyConnectionInAutoCommitIs25006Unsent() throws Exception {
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            s.addBatch("DELETE FROM a");
            assertEquals("25006", assertThrows(BatchUpdateException.class, s::executeBatch).getSQLState());
        }
        assertEquals(0, server.bodies("channels").size());
    }

    // ---- read-only channels ----

    @Test
    void aReadOnlyTransactionRunsOnAReadOnlyChannelFromASnapshotBegin() throws Exception {
        String row = "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n";
        server.on("channel", row + FakeServer.executed(1, "open", 0, 0), row + FakeServer.executed(2, "open", 0, 0),
                FakeServer.stepped(3), row + FakeServer.executed(1, "open", 0, 0), FakeServer.stepped(2));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            c.setAutoCommit(false);
            for (int i = 0; i < 2; i++) {
                try (ResultSet rs = s.executeQuery("SELECT x FROM t")) {
                    assertTrue(rs.next());
                }
            }
            assertThrows(SQLException.class, () -> c.setReadOnly(false), "refused inside the transaction");
            c.commit();
            c.setReadOnly(false);
            s.executeQuery("SELECT 2").close(); // a write transaction now, on the write channel
            c.rollback();
        }
        assertEquals(List.of("{\"mode\":\"read_only\"}", "{\"mode\":\"write\"}"), server.bodies("channels"));
        List<String> sent = server.bodies("channel");
        assertTrue(sent.get(0).startsWith("{\"channel\":\"r1.1\",\"seq\":1,\"op\":\"begin\",\"execute\":"), sent.get(0));
        assertTrue(sent.get(1).startsWith("{\"channel\":\"r1.1\",\"seq\":2,\"op\":\"execute\""), sent.get(1));
        assertEquals("{\"channel\":\"r1.1\",\"seq\":3,\"op\":\"commit\"}", sent.get(2));
        assertTrue(sent.get(3).startsWith("{\"channel\":\"w1.2\",\"seq\":1,\"op\":\"begin\""), sent.get(3));
    }

    /** At the read-only transactions' caps a begin is refused, not executed: it is sent again after a pause. */
    @Test
    void aReadOnlyBeginRefusedAtTheCapsIsSentAgain() throws Exception {
        String row = "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n";
        server.on("channel", FakeServer.channelError("overloaded", true, false, "none"),
                row + FakeServer.executed(1, "open", 0, 0), FakeServer.stepped(2));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            c.setAutoCommit(false);
            try (ResultSet rs = s.executeQuery("SELECT x FROM t")) {
                assertTrue(rs.next());
            }
            c.commit();
        }
        assertEquals(3, server.bodies("channel").size());
    }

    /** A read-only commit whose answer is lost commits nothing either way: it succeeds, and nothing is resolved. */
    @Test
    void aLostReadOnlyCommitSucceedsWithoutAResolve() throws Exception {
        String row = "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n";
        server.on("channel", row + FakeServer.executed(1, "open", 0, 0),
                FakeServer.channelError("channel_lost", false, false, "none"));
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            c.setAutoCommit(false);
            s.executeQuery("SELECT x FROM t").close();
            c.commit();
            assertFalse(c.isClosed());
        }
        assertEquals(0, server.bodies("resolve").size());
    }

    /** A write inside a read-only transaction fails as SQLite's READONLY. */
    @Test
    void aWriteInAReadOnlyTransactionIs25006() throws Exception {
        server.on("channel", "{\"error\":{\"code\":\"sql_error\",\"not_executed\":true,\"final\":true,\"message\":\"m\","
                + "\"request_id\":\"r\",\"txn_state\":\"none\",\"sqlite_code\":8}}");
        try (Connection c = server.connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            c.setAutoCommit(false);
            assertEquals("25006", assertThrows(SQLException.class, () -> s.executeUpdate("DELETE FROM t")).getSQLState());
        }
    }
}
