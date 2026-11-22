package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Wrapper;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** The driver end to end against {@link FakeServer}, over HTTPS unless a test says otherwise. */
class DriverTest {
    static final String TOKEN = "eyJhbGciOiJFUzI1NiJ9.SECRET-test-token-" + UUID.randomUUID() + ".c2ln";
    static TestCerts certs;
    static Path tmp;

    FakeServer fake;
    String db;
    final List<Throwable> seen = new ArrayList<>();
    final CountDownLatch release = new CountDownLatch(1);

    @BeforeAll
    static void certs(@TempDir Path dir) throws Exception {
        tmp = dir;
        certs = TestCerts.create(dir);
    }

    @AfterAll
    static void done() {
        certs = null;
    }

    @BeforeEach
    void start() throws Exception {
        fake = new FakeServer(certs, TOKEN);
        db = "db-" + UUID.randomUUID().toString().substring(0, 8); // its own watermark
    }

    @AfterEach
    void stop() {
        release.countDown();
        fake.close();
        assertEquals(List.of(), fake.violations, "requests that break ADR 0005");
        for (Throwable t : seen) {
            assertFalse(trace(t).contains(TOKEN), "an exception holds the token: " + trace(t));
        }
    }

    static String trace(Throwable t) {
        StringWriter w = new StringWriter();
        t.printStackTrace(new PrintWriter(w));
        return w.toString();
    }

    Properties props() {
        Properties p = new Properties();
        p.put("tokenProvider", (TokenProvider) () -> TOKEN);
        p.setProperty("tlsRootCertificates", certs.certPem.toString());
        return p;
    }

    Connection connect() throws SQLException {
        return connect(props());
    }

    Connection connect(Properties p) throws SQLException {
        return DriverManager.getConnection(fake.url(db), p);
    }

    /** Runs a call that must fail, keeps its exception for the token check, and returns it. */
    <T extends Throwable> T fails(Class<T> type, org.junit.jupiter.api.function.Executable call) {
        T e = assertThrows(type, call);
        seen.add(e);
        return e;
    }

    void answer(String... frames) {
        fake.responder = (r, ex) -> FakeServer.send(ex, 200, frames);
    }

    // ---- requests ----

    @Test
    void sendsTheQueryWithItsHeadersBodyAndWatermark() throws Exception {
        answer("{\"columns\":[{\"name\":\"x\",\"decltype\":null}]}", "[1]", FakeServer.done(41));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?")) {
            ps.setLong(1, -9223372036854775808L);
            ps.setDouble(2, 0.1);
            ps.setString(3, "a\"\\\n\u0001/ é");
            ps.setNull(4, Types.VARCHAR);
            ps.setBytes(5, new byte[] {0, 1});
            ps.setBoolean(6, true);
            ps.setObject(7, LocalDateTime.of(2026, 11, 16, 10, 20, 30, 5));
            ps.setBigDecimal(8, new BigDecimal("1E+3"));
            ps.setQueryTimeout(3);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // drains to the done frame, which raises the watermark
                }
            }
            FakeServer.Received r = fake.last();
            assertEquals("POST", r.method());
            assertEquals("/v1/databases/" + db + "/query", r.path());
            assertEquals(List.of("Bearer " + TOKEN), r.headers().get("Authorization"));
            assertEquals("application/json", r.headers().getFirst("Content-Type"));
            assertEquals(String.valueOf(r.body().length), r.headers().getFirst("Content-Length"));
            assertNull(r.headers().getFirst("Transfer-Encoding"));
            assertEquals("{\"sql\":\"SELECT ?, ?, ?, ?, ?, ?, ?, ?\",\"params\":[-9223372036854775808,1e-1,"
                    + "\"a\\\"\\\\\\n\\u0001/ é\",null,{\"blob\":\"AAE=\"},1,\"2026-11-16 10:20:30.000000005\",\"1000\"],"
                    + "\"timeout_ms\":3000}", r.bodyText());

            // The next request, on another connection to the same database, carries the watermark.
            try (Connection c2 = DriverManager.getConnection(
                            fake.url(db).replace("localhost", "LOCALHOST").replace("/" + db, "//" + db), props());
                    Statement s = c2.createStatement()) {
                s.executeQuery("SELECT 1").close();
            }
            assertEquals("{\"sql\":\"SELECT 1\",\"min_txn\":41}", fake.last().bodyText());
        }
    }

    @Test
    void theWatermarkNeverGoesDown() throws Exception {
        answer("{\"columns\":[]}", FakeServer.done(9));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.executeQuery("SELECT 1").next();
            answer("{\"columns\":[]}", FakeServer.done(3));
            s.executeQuery("SELECT 1").next();
            s.executeQuery("SELECT 1").close();
            assertEquals("{\"sql\":\"SELECT 1\",\"min_txn\":9}", fake.last().bodyText());
        }
    }

    @Test
    void readsTheTokenFileAgainForEveryRequest() throws Exception {
        Path f = tmp.resolve("token-" + db);
        Files.writeString(f, TOKEN + "\n");
        Properties p = props();
        p.remove("tokenProvider");
        p.setProperty("tokenFile", f.toString());
        try (Connection c = connect(p); Statement s = c.createStatement()) {
            s.executeQuery("SELECT 1").close();
            Files.writeString(f, "rotated-token");
            SQLException e = fails(SQLInvalidAuthorizationSpecException.class, () -> s.executeQuery("SELECT 1"));
            assertEquals("28000", e.getSQLState());
            assertEquals("Bearer rotated-token", fake.last().headers().getFirst("Authorization"));
        }
    }

    /** Loaded by name through the tokenProvider property. */
    public static final class NamedProvider implements TokenProvider {
        @Override
        public String token() {
            return TOKEN;
        }
    }

    @Test
    void loadsATokenProviderByClassName() throws Exception {
        Properties p = props();
        p.setProperty("tokenProvider", NamedProvider.class.getName());
        try (Connection c = connect(p); Statement s = c.createStatement()) {
            s.executeQuery("SELECT 1").close();
        }
    }

    @Test
    void refusesAMissingOrAmbiguousTokenSource() {
        Properties none = props();
        none.remove("tokenProvider");
        fails(SQLNonTransientConnectionException.class, () -> connect(none));
        Properties both = props();
        both.setProperty("tokenFile", "/nonexistent");
        fails(SQLNonTransientConnectionException.class, () -> connect(both));
        Properties bad = props();
        bad.setProperty("tokenProvider", String.class.getName());
        fails(SQLNonTransientConnectionException.class, () -> connect(bad));
        assertEquals(0, fake.requests.size());
    }

    @Test
    void neverPutsTheTokenInAnException() throws Exception {
        Properties failing = props();
        failing.put("tokenProvider", (TokenProvider) () -> {
            throw new IOException("vault refused " + TOKEN);
        });
        try (Connection c = connect(failing); Statement s = c.createStatement()) {
            SQLException e = fails(SQLInvalidAuthorizationSpecException.class, () -> s.executeQuery("SELECT 1"));
            assertTrue(e.getMessage().contains("java.io.IOException"), e.getMessage());
            assertNull(e.getCause(), "the provider's exception, which may hold a token, is dropped");
        }
        Properties spaced = props();
        spaced.put("tokenProvider", (TokenProvider) () -> TOKEN + " trailing");
        try (Connection c = connect(spaced); Statement s = c.createStatement()) {
            fails(SQLInvalidAuthorizationSpecException.class, () -> s.executeQuery("SELECT 1"));
        }
        assertEquals(0, fake.requests.size(), "a bad token is refused before anything is sent");
        Properties wrong = props();
        wrong.put("tokenProvider", (TokenProvider) () -> TOKEN + "x");
        try (Connection c = connect(wrong); Statement s = c.createStatement()) {
            fails(SQLInvalidAuthorizationSpecException.class, () -> s.executeQuery("SELECT 1"));
        }
        try (Connection c = connect(); Statement s = c.createStatement()) {
            fake.responder = (r, ex) -> FakeServer.send(ex, 200, "{\"columns\":[]}", "garbage");
            fails(SQLNonTransientConnectionException.class, () -> s.executeQuery("SELECT 1").next());
            fake.responder = (r, ex) -> FakeServer.start(ex, 200, false);
            fails(SQLNonTransientConnectionException.class, () -> s.executeQuery("SELECT 1"));
        }
        // stop() checks every exception kept above.
    }

    // ---- transport ----

    @Test
    void refusesPlaintextWithoutTheOptIn() throws Exception {
        try (FakeServer plain = new FakeServer(null, TOKEN)) {
            Properties p = props();
            SQLException e = fails(SQLNonTransientConnectionException.class,
                    () -> DriverManager.getConnection(plain.url(db), p));
            assertTrue(e.getMessage().contains("allowPlaintext"));
            assertEquals(0, plain.requests.size());
            p.setProperty("allowPlaintext", "true");
            try (Connection c = DriverManager.getConnection(plain.url(db), p); Statement s = c.createStatement()) {
                s.executeQuery("SELECT 1").close();
            }
            assertEquals(1, plain.requests.size());
        }
    }

    @Test
    void refusesARedirect() throws Exception {
        fake.responder = (r, ex) -> {
            ex.getResponseHeaders().add("Location", "/v1/databases/" + db + "/describe");
            FakeServer.start(ex, 307, true);
            ex.getResponseBody().close();
        };
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = fails(SQLNonTransientConnectionException.class, () -> s.executeQuery("SELECT 1"));
            assertTrue(e.getMessage().contains("redirect"), e.getMessage());
        }
        assertEquals(1, fake.requests.size(), "the redirect was not followed");
    }

    @Test
    void refusesAnAnswerWithoutTheMarker() throws Exception {
        fake.responder = (r, ex) -> {
            FakeServer.start(ex, 200, false);
            try (OutputStream out = ex.getResponseBody()) {
                out.write((FakeServer.done(1) + "\n").getBytes(StandardCharsets.UTF_8));
            }
        };
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = fails(SQLNonTransientConnectionException.class, () -> s.execute("SELECT 1"));
            assertTrue(e.getMessage().contains("Skylite-Protocol"), e.getMessage());
        }
    }

    @Test
    void refusesAServerItDoesNotTrust() throws Exception {
        Properties p = props();
        p.remove("tlsRootCertificates"); // the JVM's roots do not hold the test certificate
        try (Connection c = connect(p); Statement s = c.createStatement()) {
            fails(SQLNonTransientConnectionException.class, () -> s.executeQuery("SELECT 1"));
        }
    }

    // ---- results ----

    static final String TYPED_COLS = "{\"columns\":["
            + "{\"name\":\"i\",\"decltype\":\"INTEGER\",\"database\":\"main\",\"table\":\"t\",\"origin\":\"i\"},"
            + "{\"name\":\"r\",\"decltype\":\"REAL\"},{\"name\":\"s\",\"decltype\":\"TEXT\"},"
            + "{\"name\":\"tb\",\"decltype\":\"TEXT\"},{\"name\":\"b\",\"decltype\":\"BLOB\"},"
            + "{\"name\":\"n\",\"decltype\":null},{\"name\":\"inf\",\"decltype\":\"double\"},"
            + "{\"name\":\"d\",\"decltype\":\"date\"},{\"name\":\"ts\",\"decltype\":\"datetime(6)\"},"
            + "{\"name\":\"dec\",\"decltype\":\"TEXT\"}]}";

    @Test
    void readsEachStorageClass() throws Exception {
        answer(TYPED_COLS,
                "[9223372036854775807,1.5e0,\"Dune\",{\"text_bytes\":\"/w==\"},{\"blob\":\"\"},null,"
                        + "{\"real\":\"-Infinity\"},\"2026-11-16\",\"2026-11-16 10:20:30.5\",\"12345678901234567890.1\"]",
                "[0,-0e0,\"\",\"x\",{\"blob\":\"AAEC\"},null,1e2,\"not a date\",\"2026-11-16T10:20:30Z\",\"1\"]",
                FakeServer.done(5));
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT …")) {
            assertTrue(rs.next());
            assertEquals(Long.MAX_VALUE, rs.getObject(1));
            assertEquals(Long.MAX_VALUE, rs.getLong("I"));
            SQLDataException range = fails(SQLDataException.class, () -> rs.getInt(1));
            assertEquals("22003", range.getSQLState());
            assertEquals(1.5, rs.getObject("r"));
            assertEquals("1.5", rs.getString(2));
            assertEquals("Dune", rs.getObject(3));
            fails(SQLDataException.class, () -> rs.getString(4));
            assertArrayEquals(new byte[] {(byte) 0xff}, rs.getBytes(4));
            assertArrayEquals(new byte[0], (byte[]) rs.getObject(5));
            assertEquals(0, rs.getBlob(5).length());
            assertNull(rs.getObject(6));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getLong(6));
            assertTrue(rs.wasNull());
            assertFalse(rs.getBoolean(6));
            assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble(7));
            assertFalse(rs.wasNull());
            assertEquals(Date.valueOf("2026-11-16"), rs.getObject(8));
            assertEquals(LocalDate.of(2026, 11, 16), rs.getObject(8, LocalDate.class));
            assertEquals(Timestamp.valueOf("2026-11-16 10:20:30.5"), rs.getObject(9));
            assertEquals(LocalDateTime.of(2026, 11, 16, 10, 20, 30, 500_000_000), rs.getObject(9, LocalDateTime.class));
            assertEquals(Instant.parse("2026-11-16T10:20:30.5Z"), rs.getObject(9, Instant.class));
            assertEquals(new BigDecimal("12345678901234567890.1"), rs.getBigDecimal(10));
            fails(SQLDataException.class, () -> rs.getBoolean(1));

            assertTrue(rs.next());
            assertEquals(0L, rs.getObject(1));
            assertFalse(rs.getBoolean(1));
            assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(rs.getDouble(2)));
            assertEquals("", rs.getString(3));
            assertEquals("x", rs.getString(4));
            assertArrayEquals(new byte[] {0, 1, 2}, rs.getBytes("b"));
            assertEquals(100L, rs.getLong(7), "an integral REAL reads as a long");
            assertEquals("not a date", rs.getObject(8), "text that is not a date stays text");
            fails(SQLDataException.class, () -> rs.getDate(8));
            assertEquals(Timestamp.from(Instant.parse("2026-11-16T10:20:30Z")), rs.getTimestamp(9));
            assertEquals(OffsetDateTime.of(2026, 11, 16, 10, 20, 30, 0, ZoneOffset.UTC),
                    rs.getObject(9, OffsetDateTime.class));
            fails(SQLDataException.class, () -> rs.getObject(9, LocalDateTime.class));
            // TEXT "1" is not a boolean.
            fails(SQLDataException.class, () -> rs.getBoolean(10));

            assertFalse(rs.next());
            assertFalse(rs.next());
            fails(SQLNonTransientException.class, () -> rs.getObject(1));
        }
    }

    @Test
    void reportsColumnMetadata() throws Exception {
        answer(TYPED_COLS, FakeServer.done(5));
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT …")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(10, md.getColumnCount());
            assertEquals(Types.BIGINT, md.getColumnType(1));
            assertEquals("INTEGER", md.getColumnTypeName(1));
            assertEquals("t", md.getTableName(1));
            assertEquals(Types.DOUBLE, md.getColumnType(2));
            assertEquals(Types.VARCHAR, md.getColumnType(3));
            assertEquals(Types.BLOB, md.getColumnType(5));
            assertEquals(Types.OTHER, md.getColumnType(6));
            assertEquals("", md.getColumnTypeName(6));
            assertEquals(Types.DATE, md.getColumnType(8));
            assertEquals("DATETIME", md.getColumnTypeName(9));
            assertEquals(Types.TIMESTAMP, md.getColumnType(9));
            assertEquals(ResultSetMetaData.columnNullableUnknown, md.isNullable(1));
            assertEquals(Long.class.getName(), md.getColumnClassName(1));
            assertEquals(2, rs.findColumn("R"));
        }
    }

    @Test
    void aStatementWithoutColumnsHasAnUpdateCountAndNoResultSet() throws Exception {
        answer(FakeServer.done(2));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertFalse(s.execute("SELECT 1 WHERE 0"));
            assertEquals(0, s.getUpdateCount());
            assertNull(s.getResultSet());
            c.setReadOnly(true); // so that executeUpdate is a stateless read too
            assertEquals(0, s.executeUpdate("PRAGMA nothing"));
            fails(SQLNonTransientException.class, () -> s.executeQuery("SELECT 1 WHERE 0"));
        }
    }

    @Test
    void anIncompleteBodyIsAnErrorNeverAShortResult() throws Exception {
        answer("{\"columns\":[{\"name\":\"x\"}]}", "[1]", "[2]");
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT x")) {
            assertTrue(rs.next());
            assertTrue(rs.next());
            SQLException e = fails(SQLNonTransientConnectionException.class, rs::next);
            assertEquals("08S01", e.getSQLState());
            assertTrue(e.getMessage().contains("incomplete"), e.getMessage());
        }
    }

    @Test
    void anErrorAfterRowsIsRaisedByNext() throws Exception {
        answer("{\"columns\":[{\"name\":\"x\"}]}", "[1]", FakeServer.error("result_too_large", "too large"));
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT x")) {
            assertTrue(rs.next());
            assertEquals("54000", fails(SQLNonTransientException.class, rs::next).getSQLState());
        }
    }

    @Test
    void maxRowsStopsReading() throws Exception {
        answer("{\"columns\":[{\"name\":\"x\"}]}", "[1]", "[2]", "[3]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.setMaxRows(2);
            try (ResultSet rs = s.executeQuery("SELECT x")) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
        }
    }

    /** Streams rows until the client goes away, then counts down. */
    CountDownLatch endlessRows() {
        CountDownLatch gone = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            FakeServer.start(ex, 200, true);
            OutputStream out = ex.getResponseBody();
            try {
                out.write("{\"columns\":[{\"name\":\"x\"}]}\n".getBytes(StandardCharsets.UTF_8));
                byte[] row = ("[\"" + "r".repeat(1000) + "\"]\n").getBytes(StandardCharsets.UTF_8);
                while (true) {
                    out.write(row);
                    out.flush();
                }
            } catch (IOException e) {
                gone.countDown();
            }
        };
        return gone;
    }

    @Test
    void closingAResultSetMidStreamReleasesTheResponse() throws Exception {
        CountDownLatch gone = endlessRows();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT x FROM endless");
            for (int i = 0; i < 3; i++) {
                assertTrue(rs.next());
            }
            rs.close();
            assertTrue(gone.await(10, TimeUnit.SECONDS), "the server saw the response released");
            assertTrue(rs.isClosed());
            fails(SQLException.class, rs::next);
        }
    }

    @Test
    void closingTheConnectionReleasesItsResponses() throws Exception {
        CountDownLatch gone = endlessRows();
        Connection c = connect();
        ResultSet rs = c.createStatement().executeQuery("SELECT x FROM endless");
        assertTrue(rs.next());
        c.close();
        assertTrue(gone.await(10, TimeUnit.SECONDS));
        assertTrue(rs.isClosed());
    }

    /** Sends columns and one row, then holds the response open until the test ends. */
    void stallAfterOneRow() {
        fake.responder = (r, ex) -> {
            FakeServer.start(ex, 200, true);
            OutputStream out = ex.getResponseBody();
            out.write("{\"columns\":[{\"name\":\"x\"}]}\n[1]\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            release.await(30, TimeUnit.SECONDS);
        };
    }

    @Test
    void cancelEndsARunningResultSet() throws Exception {
        stallAfterOneRow();
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT x")) {
            assertTrue(rs.next());
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(200);
                    s.cancel();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            long t0 = System.nanoTime();
            SQLException e = fails(SQLNonTransientException.class, rs::next);
            assertEquals("57014", e.getSQLState());
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(5));
        }
    }

    @Test
    void cancelEndsARequestWaitingForItsHeaders() throws Exception {
        fake.responder = (r, ex) -> release.await(30, TimeUnit.SECONDS);
        try (Connection c = connect(); Statement s = c.createStatement()) {
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(200);
                    s.cancel();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            assertEquals("57014", fails(SQLNonTransientException.class, () -> s.executeQuery("SELECT x")).getSQLState());
        }
    }

    @Test
    void theQueryTimeoutEndsAStalledResponse() throws Exception {
        stallAfterOneRow();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.setQueryTimeout(1);
            long t0 = System.nanoTime();
            try (ResultSet rs = s.executeQuery("SELECT x")) {
                assertTrue(rs.next());
                SQLTimeoutException e = fails(SQLTimeoutException.class, rs::next);
                assertEquals("HYT00", e.getSQLState());
            }
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(ms >= 2900 && ms < 10_000, "gave up after " + ms + " ms: the timeout plus the 2 s cancel wait");
            assertTrue(fake.last().bodyText().endsWith(",\"timeout_ms\":1000}"));
            assertFalse(c.isClosed() || s.isClosed(), "a query timeout leaves the statement and connection open");
        }
    }

    @Test
    void theQueryTimeoutEndsARequestWaitingForItsHeaders() throws Exception {
        fake.responder = (r, ex) -> release.await(30, TimeUnit.SECONDS);
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1")) {
            ps.setQueryTimeout(1);
            fails(SQLTimeoutException.class, ps::executeQuery);
        }
    }

    @Test
    void theHeaderWaitOutlastsTheStopTimer() { // else the JDK's own timeout could end it as a transport failure
        for (int t = 1; t <= 120; t++) {
            long query = (t + Exchange.CANCEL_WAIT.toSeconds()) * 1000, describe = t * 1000L;
            for (long stop : new long[] {query, describe}) {
                assertTrue(Exchange.headerWait(0, stop).toMillis() > stop, t + " s, stop at " + stop + " ms");
            }
        }
        assertEquals(Exchange.RESPONSE_HEADER_TIMEOUT, Exchange.headerWait(0, 0));
        assertEquals(250, Exchange.headerWait(250, 5000).toMillis()); // the network timeout bounds the headers
    }

    // ---- errors ----

    @ParameterizedTest
    @CsvSource({
        "invalid_request, 400, java.sql.SQLSyntaxErrorException, 42000",
        "unauthenticated, 401, java.sql.SQLInvalidAuthorizationSpecException, 28000",
        "forbidden, 403, java.sql.SQLInvalidAuthorizationSpecException, 28000",
        "statement_forbidden, 403, java.sql.SQLSyntaxErrorException, 42501",
        "database_not_found, 404, java.sql.SQLNonTransientConnectionException, 08001",
        "database_exists, 409, java.sql.SQLNonTransientException, 42710",
        "needs_channel, 409, java.sql.SQLNonTransientException, 25006",
        "channel_busy, 409, java.sql.SQLNonTransientException, HY010",
        "in_flight, 409, java.sql.SQLNonTransientException, HY010",
        "stale_sequence, 409, java.sql.SQLNonTransientException, HY010",
        "sequence_gap, 409, java.sql.SQLNonTransientException, HY010",
        "sequence_conflict, 409, java.sql.SQLNonTransientException, HY010",
        "channel_lost, 410, java.sql.SQLNonTransientConnectionException, 08003",
        "request_too_large, 413, java.sql.SQLNonTransientException, 54000",
        "too_many_channels, 429, java.sql.SQLTransientConnectionException, 08004",
        "overloaded, 429, java.sql.SQLTransientConnectionException, 08004",
        "invalid_state, 409, java.sql.SQLNonTransientException, 25000",
        "transaction_aborted, 200, java.sql.SQLNonTransientException, 25000",
        "busy, 200, java.sql.SQLTransactionRollbackException, 40001",
        "timeout, 200, java.sql.SQLTimeoutException, HYT00",
        "cancelled, 200, java.sql.SQLNonTransientException, 57014",
        "result_too_large, 200, java.sql.SQLNonTransientException, 54000",
        "sql_error, 400, java.sql.SQLException, HY000",
        "commit_failed, 200, java.sql.SQLTransactionRollbackException, 40000",
        "commit_outcome_unknown, 200, java.sql.SQLNonTransientConnectionException, 08007",
        "no_leader, 503, java.sql.SQLTransientConnectionException, 08001",
        "not_serving, 503, java.sql.SQLTransientConnectionException, 08001",
        "owner_unreachable, 503, java.sql.SQLTransientConnectionException, 08001",
        "behind, 503, java.sql.SQLTransientConnectionException, 08001",
        "maintenance, 503, java.sql.SQLTransientConnectionException, 08001",
        "internal, 500, java.sql.SQLException, HY000",
        "a_code_from_the_future, 500, java.sql.SQLException, HY000",
    })
    void mapsEveryErrorCode(String code, int status, String type, String state) throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, status, FakeServer.error(code, "message " + code));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true); // needs_channel goes no further
            SQLException e = fails(SQLException.class, () -> s.executeQuery("SELECT 1"));
            assertSame(Class.forName(type), e.getClass(), e.toString());
            assertEquals(state, e.getSQLState());
            assertEquals(0, e.getErrorCode());
            assertTrue(e.getMessage().contains(code) && e.getMessage().contains("message " + code)
                    && e.getMessage().contains("req-1"), e.getMessage());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "1, 1, java.sql.SQLSyntaxErrorException, 42000",
        "8, 1032, java.sql.SQLNonTransientException, 25006",
        "18, 18, java.sql.SQLDataException, 22001",
        "19, 2067, java.sql.SQLIntegrityConstraintViolationException, 23000",
        "20, 20, java.sql.SQLDataException, 22000",
        "25, 25, java.sql.SQLDataException, 22023",
        "5, 517, java.sql.SQLException, HY000",
    })
    void mapsSqliteErrorsByPrimaryCode(int code, int extended, String type, String state) throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, 400, "{\"error\":{\"code\":\"sql_error\",\"not_executed\":true,"
                + "\"final\":false,\"message\":\"m\",\"request_id\":\"r\",\"sqlite_code\":" + code
                + ",\"sqlite_extended_code\":" + extended + "}}");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLException e = fails(SQLException.class, () -> s.executeQuery("SELECT 1"));
            assertSame(Class.forName(type), e.getClass());
            assertEquals(state, e.getSQLState());
            assertEquals(extended, e.getErrorCode());
        }
    }

    @Test
    void maintenanceThatAbortedATransactionIsRetryable() throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, 200, "{\"error\":{\"code\":\"transaction_aborted\","
                + "\"not_executed\":true,\"final\":true,\"message\":\"m\",\"request_id\":\"r\",\"cause\":\"maintenance\"}}");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals("40001", fails(SQLTransactionRollbackException.class, () -> s.executeQuery("SELECT 1")).getSQLState());
        }
    }

    // ---- writes, transactions and what this version refuses ----

    @Test
    void aReadOnlyConnectionRefusesWritesAndSavepointsAreNotSupported() throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, 409, FakeServer.error("needs_channel", "not a pure read"));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertFalse(c.isReadOnly());
            assertTrue(c.getAutoCommit());
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, c.getTransactionIsolation());
            fails(SQLFeatureNotSupportedException.class, () -> c.setSavepoint());
            c.setReadOnly(true);
            SQLNonTransientException e = fails(SQLNonTransientException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("25006", e.getSQLState());
            assertTrue(e.getMessage().contains("needs_channel"), e.getMessage());
            fails(SQLNonTransientException.class, () -> s.executeQuery("DELETE FROM t RETURNING *"));
            assertTrue(fake.requests.stream().noneMatch(r -> r.path().contains("/channel")), "nothing on a channel");
        }
    }

    /** Answers a channel's requests in order: the open, then each scripted answer for the next channel request. */
    private void channel(String... answers) {
        java.util.concurrent.atomic.AtomicInteger next = new java.util.concurrent.atomic.AtomicInteger();
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.t" + next.get()));
            } else if (r.path().endsWith("/channel")) {
                String a = answers[next.getAndIncrement()];
                if (a.equals("drop")) {
                    ex.close(); // no answer: the response is lost in transport
                } else {
                    FakeServer.send(ex, a.contains("\"error\"") && !a.contains("\"final\":true") ? 409 : 200, a);
                }
            } else {
                FakeServer.send(ex, 200, "{\"done\":{\"last_seq\":0}}");
            }
        };
    }

    private List<String> channelBodies() {
        return fake.requests.stream().filter(r -> r.path().endsWith("/channel")).map(FakeServer.Received::bodyText).toList();
    }

    @Test
    void writesTakeTheNextSequenceNumberAndATransactionBeginsWithItsFirstStatement() throws Exception {
        channel(FakeServer.executed(1, "none", 1, 41), FakeServer.executed(2, "open", 1, 42),
                FakeServer.executed(3, "open", 2, 42), FakeServer.stepped(4));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)", Statement.RETURN_GENERATED_KEYS));
            try (ResultSet keys = s.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertEquals(41, keys.getLong(1));
            }
            c.setAutoCommit(false);
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (2)"));
            assertEquals(2, s.executeUpdate("UPDATE t SET x = 1"));
            c.commit();
            c.commit(); // nothing open: nothing sent
        }
        assertEquals(List.of(
                "{\"channel\":\"w1.t0\",\"seq\":1,\"op\":\"execute\",\"sql\":\"INSERT INTO t VALUES (1)\",\"min_txn\":1}",
                "{\"channel\":\"w1.t0\",\"seq\":2,\"op\":\"begin\",\"execute\":{\"sql\":\"INSERT INTO t VALUES (2)\"},"
                        + "\"min_txn\":2}",
                "{\"channel\":\"w1.t0\",\"seq\":3,\"op\":\"execute\",\"sql\":\"UPDATE t SET x = 1\",\"min_txn\":2}",
                "{\"channel\":\"w1.t0\",\"seq\":4,\"op\":\"commit\"}"), channelBodies());
        assertTrue(fake.requests.get(fake.requests.size() - 1).path().endsWith("/channel/close"), "closed with the connection");
    }

    @Test
    void aLostAnswerIsSentAgainWithTheSameBodyAndItsReplayIsTheOutcome() throws Exception {
        channel("drop", FakeServer.channelError("in_flight", true, false, "none"), FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
        }
        List<String> bodies = channelBodies();
        assertEquals(3, bodies.size());
        assertEquals(bodies.get(0), bodies.get(1));
        assertEquals(bodies.get(0), bodies.get(2));
    }

    @Test
    void aRefusalBeforeAcceptanceLeavesTheSequenceNumberFree() throws Exception {
        channel(FakeServer.channelError("overloaded", true, false, "none"), FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            fails(SQLTransientConnectionException.class, () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
        }
        assertTrue(channelBodies().stream().allMatch(b -> b.contains("\"seq\":1,")), channelBodies().toString());
    }

    @Test
    void aRecordedFailureIsTheOutcomeAndTheTransactionGoesOn() throws Exception {
        channel(FakeServer.executed(1, "open", 1, 1),
                "{\"error\":{\"code\":\"sql_error\",\"not_executed\":false,\"final\":true,\"message\":\"UNIQUE\","
                        + "\"request_id\":\"r\",\"txn_state\":\"open\",\"sqlite_code\":19,\"sqlite_extended_code\":2067}}",
                FakeServer.executed(3, "open", 1, 2), FakeServer.stepped(4));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertEquals("23000", fails(java.sql.SQLIntegrityConstraintViolationException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)")).getSQLState());
            s.executeUpdate("INSERT INTO t VALUES (2)");
            c.commit();
        }
        List<String> bodies = channelBodies();
        assertTrue(bodies.get(2).contains("\"seq\":3,\"op\":\"execute\""), bodies.get(2));
        assertTrue(bodies.get(3).contains("\"seq\":4,\"op\":\"commit\""), bodies.get(3));
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"channel_lost", "commit_outcome_unknown", "drop"})
    void anUnknownOutcomeIs08007AndTheChannelIsGivenUp(String code) throws Exception {
        String lost = code.equals("drop") ? "drop" : FakeServer.channelError(code, false, false, "none");
        channel(lost, lost, lost);
        try (Connection c = connect(); Statement s = c.createStatement()) {
            SQLNonTransientConnectionException e = fails(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (1)"));
            assertEquals("08007", e.getSQLState(), e.toString());
            assertFalse(c.isValid(5));
            assertEquals("08003", fails(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (2)")).getSQLState());
        }
    }

    @Test
    void aChannelThatEndedWhileIdleIsReplaced() throws Exception {
        channel(FakeServer.channelError("channel_lost", true, false, "none"), FakeServer.executed(1, "none", 1, 7));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals(1, s.executeUpdate("INSERT INTO t VALUES (1)"));
        }
        assertEquals(2, fake.requests.stream().filter(r -> r.path().endsWith("/channels")).count());
    }

    @Test
    void aTransactionLostWithItsChannelIsAnError() throws Exception {
        channel(FakeServer.executed(1, "open", 1, 1), FakeServer.channelError("channel_lost", true, false, "none"));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("INSERT INTO t VALUES (1)");
            assertEquals("08003", fails(SQLNonTransientConnectionException.class,
                    () -> s.executeUpdate("INSERT INTO t VALUES (2)")).getSQLState());
            c.rollback(); // nothing open any more: nothing sent
        }
    }

    @Test
    void cancelReachesTheServerAndTheAnswerSaysWhatHappened() throws Exception {
        CountDownLatch cancelled = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/channels")) {
                FakeServer.send(ex, 200, FakeServer.opened("w1.c"));
            } else if (r.path().endsWith("/channel/cancel")) {
                cancelled.countDown();
                FakeServer.send(ex, 200, "{\"done\":{\"seq\":1,\"txn_state\":\"none\",\"outcome\":"
                        + "{\"seq\":1,\"ok\":false,\"error\":{\"code\":\"cancelled\",\"not_executed\":true,"
                        + "\"final\":true,\"message\":\"cancelled\",\"request_id\":\"r\"}}}}");
            } else if (r.path().endsWith("/channel")) {
                assertTrue(cancelled.await(10, TimeUnit.SECONDS));
                FakeServer.send(ex, 200, FakeServer.channelError("cancelled", true, true, "none"));
            } else {
                FakeServer.send(ex, 200, "{\"done\":{\"last_seq\":1,\"outcome\":{\"seq\":1,\"ok\":true,"
                        + "\"txn_state\":\"none\",\"txn\":1}}}");
            }
        };
        try (Connection c = connect(); Statement s = c.createStatement()) {
            Future<SQLException> run = Executors.newSingleThreadExecutor().submit(
                    () -> fails(SQLNonTransientException.class, () -> s.executeUpdate("DELETE FROM t")));
            while (fake.requests.stream().noneMatch(r -> r.path().endsWith("/channel"))) {
                Thread.sleep(10);
            }
            s.cancel();
            assertEquals("57014", run.get(10, TimeUnit.SECONDS).getSQLState());
        }
        assertTrue(fake.requests.stream().anyMatch(r -> r.path().endsWith("/channel/cancel")
                && r.bodyText().equals("{\"channel\":\"w1.c\",\"seq\":1}")));
    }

    @Test
    void aFailedBatchEntryStopsTheBatchWithTheCountsBeforeIt() throws Exception {
        channel(FakeServer.executed(1, "none", 1, 1),
                FakeServer.channelError("sql_error", true, true, "none"));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?)")) {
            for (int i = 0; i < 3; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            java.sql.BatchUpdateException e = fails(java.sql.BatchUpdateException.class, ps::executeBatch);
            assertArrayEquals(new int[] {1}, e.getUpdateCounts());
        }
        assertEquals(2, channelBodies().size());
    }

    @Test
    void refusesValuesTheProtocolCannotCarryBeforeSending() throws Exception {
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT ?")) {
            fails(SQLDataException.class, () -> ps.setDouble(1, Double.NaN));
            fails(SQLDataException.class, () -> ps.setObject(1, LocalDate.of(10000, 1, 1)));
            fails(SQLDataException.class, () -> ps.setObject(1, Instant.parse("0000-12-31T23:59:59Z")));
            fails(SQLDataException.class, () -> ps.setString(1, "\ud800"));
            fails(SQLNonTransientException.class, () -> ps.setObject(0, 1));
            PreparedStatement gap = c.prepareStatement("SELECT ?, ?");
            gap.setInt(2, 1);
            assertEquals("07001", fails(SQLNonTransientException.class, gap::executeQuery).getSQLState());
            fails(SQLNonTransientException.class, () -> c.createStatement().executeQuery("SELECT '\u0000'"));
        }
        assertEquals(0, fake.requests.size());
    }

    // ---- describe and metadata ----

    @Test
    void describesParametersAndColumnsOnceWhenAsked() throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, 200,
                "{\"columns\":[{\"name\":\"a\",\"decltype\":\"TEXT\"}]}",
                "{\"params\":{\"from\":0,\"names\":[\":t\",null]}}",
                FakeServer.describeDone(12, 2));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT a FROM t WHERE a = :t OR ?")) {
            assertEquals(0, fake.requests.size(), "preparing sends nothing");
            ParameterMetaData pm = ps.getParameterMetaData();
            assertEquals(2, pm.getParameterCount());
            assertEquals(ParameterMetaData.parameterModeIn, pm.getParameterMode(1));
            assertEquals(Types.VARCHAR, ps.getMetaData().getColumnType(1));
            assertEquals(1, fake.requests.size());
            FakeServer.Received r = fake.last();
            assertEquals("/v1/databases/" + db + "/describe", r.path());
            assertEquals("{\"sql\":\"SELECT a FROM t WHERE a = :t OR ?\"}", r.bodyText());
            fails(SQLNonTransientException.class, () -> pm.getParameterType(3));
        }
    }

    @Test
    void isValidDescribesSelect1() throws Exception {
        fake.responder = (r, ex) -> FakeServer.send(ex, 200, "{\"columns\":[{\"name\":\"1\"}]}", FakeServer.describeDone(4, 0));
        Connection c = connect();
        assertTrue(c.isValid(2));
        assertEquals("{\"sql\":\"SELECT 1\"}", fake.last().bodyText());
        fake.responder = (r, ex) -> FakeServer.send(ex, 503, FakeServer.error("not_serving", "x"));
        assertFalse(c.isValid(2));
        c.close();
        assertFalse(c.isValid(2));
    }

    @Test
    void reportsTheProductFromTheServer() throws Exception {
        answer("{\"columns\":[{\"name\":\"sqlite_version()\",\"decltype\":null}]}", "[\"3.53.4\"]", FakeServer.done(1));
        Driver d = DriverManager.getDriver(fake.url(db));
        assertInstanceOf(SkyliteDriver.class, d);
        assertFalse(d.jdbcCompliant());
        try (Connection c = connect()) {
            var md = c.getMetaData();
            assertEquals("SQLite", md.getDatabaseProductName());
            assertEquals("3.53.4", md.getDatabaseProductVersion());
            assertEquals(3, md.getDatabaseMajorVersion());
            assertEquals(53, md.getDatabaseMinorVersion());
            assertEquals(fake.url(db), md.getURL());
            assertEquals(SkyliteDriver.VERSION, md.getDriverVersion());
            assertFalse(md.getDriverVersion().contains("${"), "the version resource was filtered");
            assertTrue(md.supportsTransactions());
            assertTrue(md.supportsGetGeneratedKeys());
            assertTrue(md.supportsBatchUpdates());
            assertFalse(md.supportsSavepoints());
            assertTrue(md.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
            fails(SQLFeatureNotSupportedException.class, () -> md.getPrimaryKeys(null, null, "t"));
            assertEquals("{\"sql\":\"SELECT sqlite_version()\"}", fake.last().bodyText());
        }
    }

    @Test
    void ignoresOtherDriversUrls() throws Exception {
        assertNull(new SkyliteDriver().connect("jdbc:sqlite:x.db", props()));
        assertFalse(new SkyliteDriver().acceptsURL("jdbc:postgresql://h/d"));
    }

    @Test
    void closedObjectsRefuseUse() throws Exception {
        Connection c = connect();
        Statement s = c.createStatement();
        s.close();
        fails(SQLException.class, () -> s.executeQuery("SELECT 1"));
        c.close();
        fails(SQLException.class, c::createStatement);
        assertEquals(0, fake.requests.size());
    }

    // ---- statement and result set lifecycle ----

    static final String COLUMN_X = "{\"columns\":[{\"name\":\"x\"}]}";

    @Test
    void closeOnCompletionClosesTheStatementWhenItsResultSetCloses() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.closeOnCompletion();
            ResultSet first = s.executeQuery("SELECT 1");
            ResultSet rs = s.executeQuery("SELECT 1"); // closes the first, which leaves the statement open
            assertTrue(first.isClosed());
            assertFalse(s.isClosed());
            assertTrue(rs.next());
            assertFalse(rs.next());
            assertFalse(rs.isClosed(), "the end of the rows does not close the result set");
            assertTrue(rs.isAfterLast());
            assertFalse(s.isClosed());
            rs.close();
            assertTrue(s.isClosed());
        }
    }

    @Test
    void getMoreResultsClosesTheResultSet() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1");
            assertFalse(s.getMoreResults());
            assertTrue(rs.isClosed());
            assertNull(s.getResultSet());
            assertEquals(-1, s.getUpdateCount());
        }
    }

    @Test
    void getMoreResultsPastTheLastResultSetCompletesTheStatement() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1");
            s.closeOnCompletion();
            assertFalse(s.getMoreResults());
            assertTrue(rs.isClosed());
            assertTrue(s.isClosed());
        }
    }

    @Test
    void aPreparedStatementDescribesItsColumnsOnceItsResultSetIsClosed() throws Exception {
        String cols = "{\"columns\":[{\"name\":\"a\",\"decltype\":\"INTEGER\"}]}";
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/describe")) {
                FakeServer.send(ex, 200, cols, FakeServer.describeDone(1, 0));
            } else {
                FakeServer.send(ex, 200, cols, "[1]", FakeServer.done(1));
            }
        };
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1 AS a")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("a", ps.getMetaData().getColumnName(1));
            }
            assertEquals(1, fake.requests.size(), "the open result set's columns needed no describe");
            assertEquals("a", ps.getMetaData().getColumnName(1));
            assertEquals("/v1/databases/" + db + "/describe", fake.last().path());
        }
    }

    @Test
    void aCancelWithNothingRunningDoesNotReachALaterDescribe() throws Exception {
        answer("{\"columns\":[{\"name\":\"a\"}]}", FakeServer.describeDone(1, 0));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1 AS a")) {
            ps.cancel();
            assertEquals(0, ps.getParameterMetaData().getParameterCount());
            assertEquals("a", ps.getMetaData().getColumnName(1));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aStatementStoppedWhileItsTokenIsReadSendsNothing(boolean cancel) throws Exception {
        CountDownLatch inToken = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        Properties p = props();
        p.put("tokenProvider", (TokenProvider) () -> {
            inToken.countDown();
            proceed.await(10, TimeUnit.SECONDS);
            return TOKEN;
        });
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(p)) {
            Statement s = c.createStatement();
            CompletableFuture<Throwable> failed = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    s.executeQuery("SELECT 1");
                    failed.complete(null);
                } catch (Throwable e) {
                    failed.complete(e);
                }
            });
            assertTrue(inToken.await(10, TimeUnit.SECONDS));
            if (cancel) {
                s.cancel();
            } else {
                s.close();
            }
            proceed.countDown();
            SQLException e = assertInstanceOf(SQLException.class, failed.get(10, TimeUnit.SECONDS));
            seen.add(e);
            assertEquals(cancel ? "57014" : "HY010", e.getSQLState());
        }
        assertEquals(0, fake.requests.size());
    }

    @Test
    void aResultSetStartsWithItsStatementsFetchSize() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.setFetchSize(10);
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertEquals(10, rs.getFetchSize());
                rs.setFetchSize(25);
                assertEquals(25, rs.getFetchSize());
                assertEquals("HY092", fails(SQLNonTransientException.class, () -> rs.setFetchSize(-1)).getSQLState());
            }
        }
    }

    @Test
    void anUpdateCountAboveIntIsADataError() throws Exception {
        answer("{\"done\":{\"txn\":1,\"read_only\":true,\"rows_affected\":2147483648,\"last_insert_rowid\":0}}");
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setReadOnly(true);
            assertEquals(2147483648L, s.executeLargeUpdate("SELECT 1 WHERE 0"));
            assertEquals("22003", fails(SQLDataException.class, s::getUpdateCount).getSQLState());
            fails(SQLDataException.class, () -> s.executeUpdate("SELECT 1 WHERE 0"));
        }
    }

    @Test
    void wrappersRefuseANullInterface() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1"); ResultSet rs = ps.executeQuery()) {
            for (Wrapper w : List.of(c, ps, rs, rs.getMetaData(), c.getMetaData(), new SkyliteParameterMetaData(0))) {
                assertFalse(w.isWrapperFor(null));
                fails(SQLNonTransientException.class, () -> w.unwrap(null));
                assertSame(w, w.unwrap(w.getClass()));
            }
        }
    }

    @Test
    void aNullProductVersionIsAnError() throws Exception {
        answer("{\"columns\":[{\"name\":\"sqlite_version()\"}]}", "[null]", FakeServer.done(1));
        try (Connection c = connect()) {
            fails(SQLNonTransientException.class, () -> c.getMetaData().getDatabaseMajorVersion());
        }
    }

    // ---- values ----

    @Test
    void getObjectReadsTheTypesSetObjectSends() throws Exception {
        answer("{\"columns\":[{\"name\":\"i\"},{\"name\":\"t\"},{\"name\":\"c\"},{\"name\":\"r\"},{\"name\":\"big\"},"
                + "{\"name\":\"huge\"},{\"name\":\"b\"},{\"name\":\"local\"}]}",
                "[42,\"2026-11-16 10:20:30Z\",\"x\",1.5e0,1e19,1e300,{\"blob\":\"AQI=\"},\"2026-11-16 12:20:30\"]",
                FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT *")) {
            assertTrue(rs.next());
            assertEquals(BigInteger.valueOf(42), rs.getObject(1, BigInteger.class));
            assertEquals(ZonedDateTime.of(2026, 11, 16, 10, 20, 30, 0, ZoneOffset.UTC), rs.getObject(2, ZonedDateTime.class));
            assertEquals('x', rs.getObject(3, Character.class));
            fails(SQLDataException.class, () -> rs.getObject(2, Character.class));
            fails(SQLDataException.class, () -> rs.getObject(4, BigInteger.class));
            assertEquals("22003", fails(SQLDataException.class, () -> rs.getLong(5)).getSQLState());
            assertEquals("22003", fails(SQLDataException.class, () -> rs.getFloat(6)).getSQLState());
            byte[] b = rs.getBytes(7);
            b[0] = 9;
            assertArrayEquals(new byte[] {1, 2}, rs.getBytes(7), "getBytes returns a copy");
            Calendar plus2 = Calendar.getInstance(TimeZone.getTimeZone("GMT+02:00"));
            assertEquals(Instant.parse("2026-11-16T10:20:30Z"), rs.getTimestamp(8, plus2).toInstant());
        }
    }

    @Test
    void parametersAreCopiedWhenSetAndRemovedByClearParameters() throws Exception {
        answer(COLUMN_X, FakeServer.done(1));
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT ?")) {
            byte[] b = {1, 2};
            ps.setBytes(1, b);
            b[0] = 9;
            ps.executeQuery().close();
            assertTrue(fake.last().bodyText().contains("\"params\":[{\"blob\":\"AQI=\"}]"), fake.last().bodyText());
            Calendar plus2 = Calendar.getInstance(TimeZone.getTimeZone("GMT+02:00"));
            ps.setTimestamp(1, Timestamp.from(Instant.parse("2026-11-16T10:20:30Z")), plus2);
            ps.executeQuery().close();
            assertTrue(fake.last().bodyText().contains("\"params\":[\"2026-11-16 12:20:30"), fake.last().bodyText());
            ps.clearParameters();
            ps.executeQuery().close();
            assertFalse(fake.last().bodyText().contains("params"), fake.last().bodyText());
        }
    }

    // ---- timeouts ----

    @Test
    void isValidReturnsWithinItsTimeout() throws Exception {
        fake.responder = (r, ex) -> release.await(30, TimeUnit.SECONDS);
        try (Connection c = connect()) {
            long t0 = System.nanoTime();
            assertFalse(c.isValid(1));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(ms >= 900 && ms < 1900, "isValid(1) took " + ms + " ms");
        }
    }

    @Test
    void aDescribeAnswerWithARowIsInvalid() throws Exception {
        answer("{\"columns\":[{\"name\":\"1\"}]}", "[1]", FakeServer.describeDone(1, 0));
        try (Connection c = connect()) {
            assertFalse(c.isValid(2));
        }
    }

    @Test
    void theNetworkTimeoutBoundsEachWaitForTheBody() throws Exception {
        stallAfterOneRow();
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setNetworkTimeout(Runnable::run, 500);
            s.setQueryTimeout(30);
            try (ResultSet rs = s.executeQuery("SELECT x")) {
                assertTrue(rs.next());
                long t0 = System.nanoTime();
                SQLException e = fails(SQLNonTransientConnectionException.class, rs::next);
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertEquals("08S01", e.getSQLState());
                assertTrue(e.getMessage().contains("network timeout of 500 ms"), e.getMessage());
                assertTrue(ms >= 400 && ms < 5000, "gave up after " + ms + " ms");
                assertTrue(c.isClosed() && s.isClosed() && rs.isClosed(), "the expiry closes the connection");
            }
        }
    }

    @Test
    void aQueryTimeoutDoesNotRaiseTheNetworkTimeout() throws Exception {
        fake.responder = (r, ex) -> release.await(30, TimeUnit.SECONDS);
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setNetworkTimeout(Runnable::run, 300);
            s.setQueryTimeout(2);
            long t0 = System.nanoTime();
            SQLException e = fails(SQLNonTransientConnectionException.class, () -> s.executeQuery("SELECT 1"));
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertTrue(ms < 1900, "gave up after " + ms + " ms, past the 300 ms network timeout");
            assertEquals("08S01", e.getSQLState());
            assertTrue(c.isClosed() && s.isClosed(), "the expiry closes the connection");
        }
    }

    // ---- connection properties ----

    /** Set when NotAProvider's static code runs; kept outside that class so reading it initialises nothing. */
    static final AtomicBoolean NOT_A_PROVIDER_INITIALISED = new AtomicBoolean();

    /** Not a TokenProvider. */
    public static final class NotAProvider {
        static {
            NOT_A_PROVIDER_INITIALISED.set(true);
        }
    }

    @Test
    void neverRunsTheStaticCodeOfATokenProviderClassThatIsNotOne() {
        Properties p = props();
        p.setProperty("tokenProvider", NotAProvider.class.getName());
        SQLException e = fails(SQLNonTransientConnectionException.class, () -> connect(p));
        assertTrue(e.getMessage().contains("does not implement"), e.getMessage());
        assertFalse(NOT_A_PROVIDER_INITIALISED.get(), "the class was initialised");
    }

    @Test
    void readsPropertiesFromTheDefaults() throws Exception {
        answer(COLUMN_X, "[1]", FakeServer.done(1));
        Properties defaults = new Properties();
        defaults.setProperty("tokenProvider", NamedProvider.class.getName());
        defaults.setProperty("tlsRootCertificates", certs.certPem.toString());
        try (Connection c = connect(new Properties(defaults)); Statement s = c.createStatement()) {
            s.executeQuery("SELECT 1").close();
        }
        defaults.setProperty("allowPlaintext", "true");
        new SkyliteDriver().connect("jdbc:skylite:http://db.example/library", new Properties(defaults)).close();
    }

    // ---- stops racing executions ----

    @Test
    void aDescribeStaysCancellableAfterAnEarlierResultSetCloses() throws Exception {
        CountDownLatch described = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            if (r.path().endsWith("/query")) {
                FakeServer.send(ex, 200, COLUMN_X, "[1]", FakeServer.done(1));
            } else {
                described.countDown();
                release.await(30, TimeUnit.SECONDS);
                FakeServer.send(ex, 200, COLUMN_X, FakeServer.describeDone(1, 0));
            }
        };
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT 1")) {
            ResultSet rs = ps.executeQuery();
            CompletableFuture<Throwable> failed = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    ps.getParameterMetaData();
                    failed.complete(null);
                } catch (Throwable e) {
                    failed.complete(e);
                }
            });
            assertTrue(described.await(10, TimeUnit.SECONDS));
            rs.close();
            ps.cancel();
            SQLException e = assertInstanceOf(SQLException.class, failed.get(10, TimeUnit.SECONDS));
            seen.add(e);
            assertEquals("57014", e.getSQLState());
            assertEquals(1, release.getCount(), "the describe ended by the cancel, not by its answer");
        }
    }

    @Test
    void aCancelledResultSetDeliversNoBufferedRow() throws Exception {
        fake.responder = (r, ex) -> {
            FakeServer.start(ex, 200, true);
            OutputStream out = ex.getResponseBody();
            out.write((COLUMN_X + "\n[1]\n[2]\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            release.await(30, TimeUnit.SECONDS);
        };
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            s.cancel();
            assertEquals("57014", fails(SQLException.class, rs::next).getSQLState());
        }
    }

    @Test
    void abortEndsAValidationWithFalse() throws Exception {
        CountDownLatch described = new CountDownLatch(1);
        CountDownLatch answered = new CountDownLatch(1);
        fake.responder = (r, ex) -> {
            described.countDown();
            release.await(30, TimeUnit.SECONDS);
            FakeServer.send(ex, 200, COLUMN_X, FakeServer.describeDone(1, 0));
            answered.countDown();
        };
        Connection c = connect();
        CompletableFuture<Boolean> valid = CompletableFuture.supplyAsync(() -> {
            try {
                return c.isValid(0);
            } catch (SQLException e) {
                throw new AssertionError(e);
            }
        });
        assertTrue(described.await(10, TimeUnit.SECONDS));
        c.abort(Runnable::run);
        assertTrue(c.isClosed());
        release.countDown();
        assertFalse(valid.get(10, TimeUnit.SECONDS));
    }

    @Test
    void validationEndsAtTheShorterOfItsTimeoutAndTheNetworkTimeout() throws Exception {
        fake.responder = (r, ex) -> {
            FakeServer.start(ex, 200, true);
            OutputStream out = ex.getResponseBody();
            for (int i = 0; i < 10; i++) { // each keepalive within the network timeout, the answer past it
                out.write("{\"keepalive\":{}}\n".getBytes(StandardCharsets.UTF_8));
                out.flush();
                Thread.sleep(100);
            }
            out.write((COLUMN_X + "\n" + FakeServer.describeDone(1, 0) + "\n").getBytes(StandardCharsets.UTF_8));
        };
        Connection c = connect();
        c.setNetworkTimeout(Runnable::run, 250); // HikariCP's order: the network timeout, then isValid(1)
        long t0 = System.nanoTime();
        assertFalse(c.isValid(1));
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(ms >= 200 && ms < 900, "validation took " + ms + " ms");
        assertTrue(c.isClosed(), "the network timeout passed, which closes the connection");
    }

    @Test
    void validationCountsTheTokenAgainstItsTimeout() throws Exception {
        Properties p = props();
        p.put("tokenProvider", (TokenProvider) () -> {
            Thread.sleep(1100);
            return TOKEN;
        });
        answer(COLUMN_X, FakeServer.describeDone(1, 0));
        try (Connection c = connect(p)) {
            assertFalse(c.isValid(1));
            assertFalse(c.isClosed(), "isValid's own timeout leaves the connection open");
        }
        assertEquals(0, fake.requests.size());
    }

    @Test
    void setNetworkTimeoutNeedsAnExecutor() throws Exception {
        try (Connection c = connect()) {
            assertEquals("HY009", fails(SQLException.class, () -> c.setNetworkTimeout(null, 10)).getSQLState());
        }
    }

    @Test
    void aRealIntegerIsReadAsItsExactBigInteger() throws Exception {
        answer(COLUMN_X, "[1.15292150460684698e18]", FakeServer.done(1));
        try (Connection c = connect(); Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1L << 60, rs.getLong(1));
            assertEquals(BigInteger.ONE.shiftLeft(60), rs.getObject(1, BigInteger.class));
        }
    }
}
