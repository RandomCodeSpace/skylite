package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Discarding a connection after an unknown outcome (ADR 0005 decision 4), losing and recovering a transaction, and
 * the lifecycle rules that go with them.
 */
@Timeout(20)
class GiveUpTest {
    private FakeServer fake;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer(null, "giveup-tok");
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private Properties properties() {
        Properties p = new Properties();
        p.setProperty("allowPlaintext", "true");
        p.put("tokenProvider", (TokenProvider) () -> "giveup-tok");
        return p;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(fake.url("giveup"), properties());
    }

    /** Answers channel requests in order, the last one repeated; "drop" closes the exchange without an answer. */
    private void script(String... answers) {
        AtomicInteger next = new AtomicInteger();
        AtomicInteger opens = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.g" + opens.incrementAndGet()));
            } else if (r.path().endsWith("/channel")) {
                String a = answers[Math.min(next.getAndIncrement(), answers.length - 1)];
                if (a.equals("drop")) {
                    ex.close();
                } else {
                    FakeServer.send(ex, a.contains("\"error\"") ? 409 : 200, a);
                }
            } else if (r.path().endsWith("/query")) {
                FakeServer.send(ex, 200, "{\"columns\":[{\"name\":\"x\"}]}", "[1]", "[2]", FakeServer.done(1));
            } else if (r.path().endsWith("/resolve")) {
                FakeServer.send(ex, 200, FakeServer.resolved("unknown", 1)); // nothing settles it
            } else {
                FakeServer.send(ex, 410, FakeServer.channelError("channel_lost", true, false, "none"));
            }
        };
    }

    private long count(String suffix) {
        return fake.requests.stream().filter(r -> r.path().endsWith(suffix)).count();
    }

    /** A refusal before acceptance that a later send would not pass, so the driver does not send it again. */
    private static String refused() {
        return FakeServer.channelError("invalid_request", true, false, "none");
    }

    private static String lost() {
        return FakeServer.channelError("channel_lost", true, false, "none");
    }

    @Test
    void aRefusalOfTheFirstSendLeavesTheConnectionUsable() throws Exception {
        script(refused(), FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals("42000", assertThrows(SQLException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
        }
    }

    @Test
    void aRefusalOfAResendAfterALostAnswerIsUnknown() throws Exception {
        script("drop", refused(), FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals("08007", assertThrows(SQLException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            assertTrue(c.isClosed());
        }
        assertEquals(2, count("/channel"));
    }

    @Test
    void aRefusalOfAResendAfterInFlightIsUnknown() throws Exception {
        script(FakeServer.channelError("in_flight", true, false, "none"), refused(),
                FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals("08007", assertThrows(SQLException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            assertTrue(c.isClosed());
        }
        assertEquals(2, count("/channel"));
    }

    @Test
    void rollbackAfterAnUnknownCommitRaises08003() throws Exception {
        script(FakeServer.executed(1, "open", 1, 4), lost());
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertEquals("08007", assertThrows(SQLException.class, c::commit).getSQLState());
            assertTrue(c.isClosed());
            assertEquals("08003", assertThrows(SQLException.class, c::rollback).getSQLState());
            assertEquals("08003", assertThrows(SQLException.class, c::getAutoCommit).getSQLState());
        }
        assertEquals(2, count("/channel"));
    }

    @Test
    void anUnknownAutocommitWriteClosesTheConnectionAndItsCursors() throws Exception {
        script(lost());
        try (Connection c = connect(); Statement r = c.createStatement(); Statement w = c.createStatement()) {
            ResultSet rows = r.executeQuery("SELECT 1");
            assertTrue(rows.next());
            assertEquals("08007", assertThrows(SQLException.class,
                    () -> w.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            assertTrue(c.isClosed());
            assertFalse(c.isValid(1));
            assertTrue(r.isClosed());
            assertTrue(rows.isClosed());
            assertThrows(SQLException.class, rows::next);
            assertEquals("08003", assertThrows(SQLException.class, () -> r.executeQuery("SELECT 1")).getSQLState());
        }
        assertEquals(1, count("/query"));
    }

    @Test
    void aReadPausedInTheTokenProviderIsNotSentAfterTheConnectionIsDiscarded() throws Exception {
        CountDownLatch atProvider = new CountDownLatch(1);
        CountDownLatch go = new CountDownLatch(1);
        ThreadLocal<Boolean> pause = new ThreadLocal<>();
        Properties p = properties();
        p.put("tokenProvider", (TokenProvider) () -> {
            if (Boolean.TRUE.equals(pause.get())) {
                atProvider.countDown();
                go.await(5, TimeUnit.SECONDS);
            }
            return "giveup-tok";
        });
        script(lost());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection c = DriverManager.getConnection(fake.url("giveup"), p);
                Statement a = c.createStatement(); Statement b = c.createStatement()) {
            Future<Boolean> read = pool.submit(() -> {
                pause.set(true);
                try (ResultSet rs = a.executeQuery("SELECT 1")) {
                    return rs.next();
                }
            });
            assertTrue(atProvider.await(5, TimeUnit.SECONDS));
            try {
                assertEquals("08007", assertThrows(SQLException.class,
                        () -> b.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            } finally {
                go.countDown();
            }
            assertThrows(ExecutionException.class, () -> read.get(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, count("/query"), "a read was sent after the connection was discarded");
    }

    /** It runs where a statement of a read-only transaction runs: on the read-only channel, which refuses it. */
    @Test
    void aWriteQueuedBehindSetReadOnlyRunsOnTheReadOnlyChannel() throws Exception {
        script("{\"error\":{\"code\":\"sql_error\",\"not_executed\":true,\"final\":true,\"message\":\"m\","
                + "\"request_id\":\"r\",\"txn_state\":\"none\",\"sqlite_code\":8}}");
        fake.responder = wrapQuery(fake.responder);
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SkyliteConnection impl = (SkyliteConnection) c;
            c.setAutoCommit(false);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    s.executeUpdate("INSERT INTO t VALUES (1)");
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            synchronized (impl.channel()) {
                writer.start();
                long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (writer.getState() != Thread.State.BLOCKED && System.nanoTime() < limit) {
                    Thread.sleep(1);
                }
                assertEquals(Thread.State.BLOCKED, writer.getState());
                c.setReadOnly(true);
            }
            writer.join(5000);
            assertFalse(writer.isAlive());
            assertTrue(c.isReadOnly());
            assertEquals("none", impl.channel().state());
            assertEquals("25006", ((SQLException) failure.get()).getSQLState(), String.valueOf(failure.get()));
        }
        assertEquals(1, count("/channel"));
        assertEquals(0, count("/query"));
        assertTrue(fake.requests.stream().anyMatch(r -> r.bodyText().startsWith("{\"mode\":\"read_only\"")));
    }

    /** A query answered needs_channel, as the server answers a write. */
    private static FakeServer.Responder wrapQuery(FakeServer.Responder inner) {
        return (r, ex) -> {
            if (r.path().endsWith("/query")) {
                FakeServer.send(ex, 409, FakeServer.error("needs_channel", "not a pure read"));
            } else {
                inner.respond(r, ex);
            }
        };
    }

    @Test
    void aTransactionLostWithItsChannelRecoversAfterRollback() throws Exception {
        script(FakeServer.executed(1, "open", 1, 4), lost(), FakeServer.executed(1, "open", 1, 5));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertEquals("08007", assertThrows(SQLException.class,
                    () -> s.executeUpdate("UPDATE t SET x = 2")).getSQLState());
            assertEquals("SKR03", assertThrows(SQLException.class, c::commit).getSQLState());
            c.rollback();
            assertFalse(c.isClosed());
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (3)"));
        }
        assertEquals(2, count("/channels"));
        List<String> bodies = fake.requests.stream().filter(r -> r.path().endsWith("/channel"))
                .map(FakeServer.Received::bodyText).toList();
        assertTrue(bodies.get(2).startsWith("{\"channel\":\"w1.g2\",\"seq\":1,\"op\":\"begin\","), bodies.get(2));
    }

    @Test
    void aReadWhoseRowsAreDroppedTwiceIsNotReportedAsAWrite() throws Exception {
        String omitted = FakeServer.executed(1, "open", 0, 0).replace("\"read_only\":false",
                "\"read_only\":true,\"data_omitted\":true");
        script(omitted);
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            SQLException e = assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1"));
            assertEquals("08S01", e.getSQLState(), e.toString());
        }
        assertEquals(2, count("/channel"));
    }

    @Test
    void generatedKeysResultSetsCloseWithTheStatementAndOnReexecution() throws Exception {
        script(FakeServer.executed(1, "none", 1, 7), FakeServer.executed(2, "none", 1, 8));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.executeUpdate("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS);
            ResultSet k1 = s.getGeneratedKeys();
            ResultSet k2 = s.getGeneratedKeys();
            assertTrue(k1.isClosed());
            s.executeUpdate("INSERT INTO t VALUES (2)", Statement.RETURN_GENERATED_KEYS);
            assertTrue(k2.isClosed());
            ResultSet k3 = s.getGeneratedKeys();
            s.close();
            assertTrue(k3.isClosed());
            assertThrows(SQLException.class, s::getGeneratedKeys);
        }
    }

    @Test
    void closeWaitsAtMostTheCancelWaitForTheWholeAnswer() throws Exception {
        CountDownLatch headers = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.c"));
            } else if (r.path().endsWith("/channel")) {
                FakeServer.send(ex, 200, FakeServer.executed(1, "none", 1, 7));
            } else {
                FakeServer.start(ex, 200, true);
                ex.getResponseBody().flush();
                headers.countDown();
                finish.await(10, TimeUnit.SECONDS);
                ex.getResponseBody().write("{\"done\":{\"last_seq\":1}}\n".getBytes());
                ex.close();
            }
        };
        Connection c = connect();
        try (Statement w = c.createStatement()) {
            w.executeUpdate("INSERT INTO t VALUES (1)");
        }
        long t0 = System.nanoTime();
        Thread closer = new Thread(((SkyliteConnection) c)::close);
        closer.start();
        try {
            assertTrue(headers.await(5, TimeUnit.SECONDS));
            closer.join(4000);
            assertFalse(closer.isAlive(), "close still waiting for the body");
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(4));
        } finally {
            finish.countDown();
            closer.join(5000);
        }
    }

    /**
     * A proxy lookup runs while the statement's lock is held. A stop that fires while it does must not hold the
     * shared timer: another statement's query timeout still fires on time.
     */
    @Test
    void aStopWaitingForAStatementLockDoesNotHoldTheTimer() throws Exception {
        CountDownLatch inProxy = new CountDownLatch(1);
        CountDownLatch releaseProxy = new CountDownLatch(1);
        ThreadLocal<Boolean> block = new ThreadLocal<>();
        ProxySelector proxy = new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                if (Boolean.TRUE.equals(block.get())) {
                    inProxy.countDown();
                    try {
                        releaseProxy.await(15, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return List.of(Proxy.NO_PROXY);
            }

            @Override
            public void connectFailed(URI uri, SocketAddress address, IOException cause) {}
        };
        CountDownLatch never = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            never.await(15, TimeUnit.SECONDS); // no answer: the query timeout ends it
            ex.close();
        };
        ExecutorService pool = Executors.newFixedThreadPool(2);
        String url = fake.url("giveup");
        HttpClient http = HttpClient.newBuilder().proxy(proxy).build();
        try (SkyliteConnection c = new SkyliteConnection(new Session(Endpoint.parse(url, true),
                        () -> "giveup-tok", null, http, new AtomicLong()), url);
                Statement held = c.createStatement(); Statement timed = c.createStatement()) {
            held.setQueryTimeout(1);
            timed.setQueryTimeout(1);
            Future<?> a = pool.submit(() -> {
                block.set(true);
                return held.executeQuery("SELECT 1");
            });
            assertTrue(inProxy.await(5, TimeUnit.SECONDS));
            Thread.sleep(200); // held's stop is scheduled first, and fires first
            long t0 = System.nanoTime();
            Future<?> b = pool.submit(() -> timed.executeQuery("SELECT 1"));
            ExecutionException e = assertThrows(ExecutionException.class, () -> b.get(6, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof SQLTimeoutException, String.valueOf(e.getCause()));
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(5));
            releaseProxy.countDown();
            assertThrows(ExecutionException.class, () -> a.get(6, TimeUnit.SECONDS));
        } finally {
            releaseProxy.countDown();
            never.countDown();
            pool.shutdownNow();
            http.shutdownNow();
        }
    }
}
