package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A network timeout that closes a connection never blocks the driver's timer thread, so that other connections'
 * query timeouts still fire while that close waits for its answer.
 */
class TimerTest {
    private FakeServer fake;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer(null, "timer-tok");
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private Connection connect() throws SQLException {
        Properties p = new Properties();
        p.setProperty("allowPlaintext", "true");
        p.put("tokenProvider", (TokenProvider) () -> "timer-tok");
        return DriverManager.getConnection(fake.url("timer"), p);
    }

    @Test
    void networkTimeoutStallDuringChannelCloseMustNotDeadlockTimerThread() throws Exception {
        CountDownLatch closeStalled = new CountDownLatch(1);
        CountDownLatch releaseAll = new CountDownLatch(1);
        AtomicInteger channelCalls = new AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.dl"));
            } else if (r.path().endsWith("/channel")) {
                if (channelCalls.incrementAndGet() == 1) {
                    FakeServer.send(ex, 200, FakeServer.executed(1, "none", 1, 1));
                } else {
                    // headers and a keepalive, then nothing: the network timeout closes the connection
                    FakeServer.start(ex, 200, true);
                    ex.getResponseBody().write("{\"keepalive\":{}}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    ex.getResponseBody().flush();
                    releaseAll.await(10, TimeUnit.SECONDS);
                }
            } else if (r.path().endsWith("/channel/close")) {
                // the close's answer stalls too, which a close on the timer thread would wait for there
                FakeServer.start(ex, 200, true);
                ex.getResponseBody().write("{\"keepalive\":{}}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ex.getResponseBody().flush();
                closeStalled.countDown();
                releaseAll.await(10, TimeUnit.SECONDS);
            } else {
                // the other connection's query stalls until its query timeout
                FakeServer.start(ex, 200, true);
                ex.getResponseBody().write("{\"keepalive\":{}}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                ex.getResponseBody().flush();
                releaseAll.await(10, TimeUnit.SECONDS);
            }
        };
        var exec = Executors.newCachedThreadPool();
        try {
            Connection c1 = connect();
            Statement s1 = c1.createStatement();
            s1.executeUpdate("INSERT INTO t VALUES (1)"); // opens the channel
            c1.setNetworkTimeout(exec, 150);
            var f1 = exec.submit(() -> assertThrows(SQLException.class, () -> s1.executeUpdate("INSERT INTO t VALUES (2)")));
            assertTrue(closeStalled.await(5, TimeUnit.SECONDS), "the network timeout did not close the channel");

            // While that close waits, another connection's query timeout must still fire.
            try (Connection c2 = connect(); Statement s2 = c2.createStatement()) {
                s2.setQueryTimeout(1);
                var f2 = exec.submit(() -> {
                    try (ResultSet rs = s2.executeQuery("SELECT 1")) {
                        rs.next();
                    }
                    return null;
                });
                // the query timeout of 1 s plus the cancel wait of 2 s
                try {
                    f2.get(4500, TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.ExecutionException expected) {
                    return;
                } finally {
                    releaseAll.countDown();
                    f1.cancel(true);
                }
            }
        } finally {
            releaseAll.countDown();
            exec.shutdownNow();
        }
    }
}
