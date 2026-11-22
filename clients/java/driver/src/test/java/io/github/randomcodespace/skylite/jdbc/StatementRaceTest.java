package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Close racing a statement's submission and its publication, stopped at the statement's own seams. */
class StatementRaceTest {
    private FakeServer fake;
    private SkyliteConnection conn;

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer(null, "race");
        fake.responder = (r, ex) -> FakeServer.send(ex, 200, DriverTest.COLUMN_X, "[1]", FakeServer.done(1));
        Properties p = new Properties();
        p.setProperty("allowPlaintext", "true");
        p.put("tokenProvider", (TokenProvider) () -> "race");
        conn = DriverManager.getConnection(fake.url("race"), p).unwrap(SkyliteConnection.class);
    }

    @AfterEach
    void stop() {
        conn.close();
        fake.close();
    }

    @Test
    void aCloseThatCompletesBeforeTheSubmissionSendsNothing() throws Exception {
        SkyliteStatement s = new SkyliteStatement(conn) {
            @Override
            public SQLException admit(Exchange ex) {
                close(); // the last moment before the request is sent
                return super.admit(ex);
            }
        };
        assertEquals("HY010", assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1")).getSQLState());
        assertEquals(0, fake.requests.size());
    }

    @Test
    void aCloseDuringTheSubmissionWaitsForItAndThenStopsIt() throws Exception {
        CountDownLatch closed = new CountDownLatch(1);
        fake.responder = (r, ex) -> { // answers only after the close, so that the close is what ends the query
            closed.await(10, TimeUnit.SECONDS);
            FakeServer.send(ex, 200, DriverTest.COLUMN_X, "[1]", FakeServer.done(1));
        };
        SkyliteStatement s = new SkyliteStatement(conn) {
            @Override
            public SQLException admit(Exchange ex) {
                SQLException r = super.admit(ex);
                Thread.ofVirtual().start(() -> {
                    close();
                    closed.countDown();
                });
                try {
                    assertFalse(closed.await(200, TimeUnit.MILLISECONDS), "close finished between check and send");
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return r;
            }
        };
        assertEquals("HY010", assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1")).getSQLState());
        assertTrue(closed.await(10, TimeUnit.SECONDS));
    }

    @Test
    void aCloseBeforeThePublicationReleasesTheResultSet() throws Exception {
        SkyliteResultSet[] made = new SkyliteResultSet[1];
        SkyliteStatement s = new SkyliteStatement(conn) {
            @Override
            SkyliteResultSet publish(SkyliteResultSet rs) throws SQLException {
                made[0] = rs;
                close(); // after the columns are decoded, before the result set is the statement's
                return super.publish(rs);
            }
        };
        assertEquals("HY010", assertThrows(SQLException.class, () -> s.executeQuery("SELECT 1")).getSQLState());
        assertTrue(s.isClosed());
        assertNotNull(made[0]);
        assertTrue(made[0].isClosed(), "a closed statement left its result set open");
    }
}
