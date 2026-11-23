package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The driver against a real {@code skylite serve}, built from this repository with Go, over HTTPS with a test OIDC
 * issuer: a JWKS file and an ES256 token signed here. The database is created with a PUT; each test that writes
 * uses tables of its own.
 */
class SkyliteServerIT {
    static final String ISSUER = "https://issuer.skylite.test";
    static final String DB = "jdbc-it";

    static Process server;
    static Path log;
    static String url;
    static String token;
    static TestCerts certs;

    @BeforeAll
    static void startServer(@TempDir Path dir) throws Exception {
        Path repo = Path.of(System.getProperty("skylite.repo", "../../..")).toRealPath();
        Path bin = Path.of(System.getProperty("skylite.target", "target")).toAbsolutePath().resolve("skylite");
        ProcessBuilder build = new ProcessBuilder("go", "build", "-o", bin.toString(), "./cmd/skylite")
                .directory(repo.toFile())
                .redirectErrorStream(true);
        Map<String, String> env = build.environment();
        env.putIfAbsent("GOTOOLCHAIN", "go1.26.11");
        env.put("GOFLAGS", "-buildvcs=false");
        env.put("CGO_ENABLED", "0");
        Process p = build.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.waitFor(), "go build ./cmd/skylite failed:\n" + out);

        certs = TestCerts.create(dir);
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair key = g.generateKeyPair();
        Path jwks = dir.resolve("jwks.json");
        ECPublicKey pub = (ECPublicKey) key.getPublic();
        Files.writeString(jwks, "{\"keys\":[{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"it\",\"alg\":\"ES256\",\"use\":\"sig\","
                + "\"x\":\"" + b64(coordinate(pub.getW().getAffineX())) + "\",\"y\":\""
                + b64(coordinate(pub.getW().getAffineY())) + "\"}]}");
        token = jwt(key, "admin");

        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Path data = Files.createDirectories(dir.resolve("data"));
        Path config = dir.resolve("skylite.json");
        Files.writeString(config, """
                {
                  "listen": "127.0.0.1:%d",
                  "tls": {"cert_file": "%s", "key_file": "%s"},
                  "auth": {
                    "issuers": [{"issuer": "%s", "audience": "skylite", "jwks_file": "%s"}],
                    "rules": [{"issuer": "%s", "subject": "admin", "database": "*", "role": "admin"}]
                  },
                  "storage": {"prefix": "it", "localfs": {"path": "%s"}},
                  "databases": ["%s"]
                }
                """.formatted(port, certs.certPem, certs.keyPem, ISSUER, jwks, ISSUER, data, DB));
        log = dir.resolve("skylite.log");
        ProcessBuilder serve = new ProcessBuilder(bin.toString(), "serve", "-config", config.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        serve.environment().put("TMPDIR", dir.toString()); // the leaders' local files
        server = serve.start();
        waitForPort(port);
        url = Endpoint.PREFIX + "https://localhost:" + port + "/" + DB;
        put(port);
        waitUntilServing();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (server != null) {
            server.destroy(); // SIGTERM: the server leaves office and exits
            if (!server.waitFor(70, TimeUnit.SECONDS)) {
                server.destroyForcibly();
            }
        }
    }

    static byte[] coordinate(BigInteger v) {
        byte[] b = v.toByteArray();
        byte[] out = new byte[32];
        int n = Math.min(b.length, 32);
        System.arraycopy(b, b.length - n, out, 32 - n, n);
        return out;
    }

    static String b64(byte[] b) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    static String jwt(KeyPair key, String subject) throws Exception {
        long now = Instant.now().getEpochSecond();
        String header = b64("{\"alg\":\"ES256\",\"kid\":\"it\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String claims = b64(("{\"iss\":\"" + ISSUER + "\",\"sub\":\"" + subject + "\",\"aud\":\"skylite\",\"iat\":" + now
                        + ",\"exp\":" + (now + 3600) + "}")
                .getBytes(StandardCharsets.UTF_8));
        Signature sig = Signature.getInstance("SHA256withECDSAinP1363Format");
        sig.initSign(key.getPrivate());
        sig.update((header + "." + claims).getBytes(StandardCharsets.US_ASCII));
        return header + "." + claims + "." + b64(sig.sign());
    }

    static void waitForPort(int port) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try {
                new Socket("127.0.0.1", port).close();
                return;
            } catch (IOException e) {
                if (!server.isAlive() || System.nanoTime() > deadline) {
                    throw new AssertionError("skylite serve did not start:\n" + Files.readString(log), e);
                }
                Thread.sleep(100);
            }
        }
    }

    /** Creates the database with the protocol's PUT, outside the driver, which does not create databases. */
    static void put(int port) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        ks.load(null, null);
        ks.setCertificateEntry("it", java.security.cert.CertificateFactory.getInstance("X.509")
                .generateCertificate(Files.newInputStream(certs.certPem)));
        tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        try (HttpClient http = HttpClient.newBuilder().sslContext(ctx).build()) {
            HttpResponse<String> r = http.send(
                    HttpRequest.newBuilder(URI.create("https://localhost:" + port + "/v1/databases/" + DB))
                            .header("Authorization", "Bearer " + token)
                            .header("Content-Type", "application/json")
                            .PUT(HttpRequest.BodyPublishers.ofString("{}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, r.statusCode(), "PUT: " + r.body());
            assertEquals("1", r.headers().firstValue("Skylite-Protocol").orElse(""));
        }
    }

    static Properties props() {
        Properties p = new Properties();
        p.put("tokenProvider", (TokenProvider) () -> token);
        p.setProperty("tlsRootCertificates", certs.certPem.toString());
        return p;
    }

    /** The server takes office within its retry backoff once the database exists; until then not_serving. */
    static void waitUntilServing() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            try (Connection c = DriverManager.getConnection(url, props());
                    Statement s = c.createStatement();
                    ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                return;
            } catch (SQLTransientConnectionException e) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the server did not lead " + DB + ":\n" + Files.readString(log), e);
                }
                Thread.sleep(250);
            }
        }
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, props());
    }

    @Test
    void readsConstantRowsOfEveryStorageClass() throws Exception {
        try (Connection c = connect();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT 9223372036854775807 AS i, 1.5 AS r, 'Dune' AS t, x'0001' AS b, "
                        + "NULL AS n, 1e999 AS inf, CAST(x'ff' AS TEXT) AS bad")) {
            assertTrue(rs.next());
            assertEquals(Long.MAX_VALUE, rs.getLong("i"));
            assertEquals(1.5, rs.getDouble("r"));
            assertEquals("Dune", rs.getString("t"));
            assertArrayEquals(new byte[] {0, 1}, rs.getBytes("b"));
            assertNull(rs.getObject("n"));
            assertTrue(rs.wasNull());
            assertEquals(Double.POSITIVE_INFINITY, rs.getDouble("inf"));
            assertArrayEquals(new byte[] {(byte) 0xff}, rs.getBytes("bad"), "TEXT that is not UTF-8 keeps its bytes");
            assertFalse(rs.next());
        }
    }

    @Test
    void bindsParametersAndRoundTripsThem() throws Exception {
        try (Connection c = connect(); PreparedStatement ps = c.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, :named")) {
            ps.setLong(1, Long.MIN_VALUE);
            ps.setDouble(2, -0.1);
            ps.setString(3, "é\u0000\n\"");
            ps.setBytes(4, new byte[] {1, 2, 3});
            ps.setNull(5, java.sql.Types.NULL);
            ps.setObject(6, LocalDateTime.of(2026, 11, 16, 10, 20, 30, 1));
            ps.setBoolean(7, true);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(Long.MIN_VALUE, rs.getObject(1));
                assertEquals(-0.1, rs.getObject(2));
                assertEquals("é\u0000\n\"", rs.getObject(3));
                assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) rs.getObject(4));
                assertNull(rs.getObject(5));
                assertEquals(LocalDateTime.of(2026, 11, 16, 10, 20, 30, 1), rs.getObject(6, LocalDateTime.class));
                assertTrue(rs.getBoolean(7));
            }
            assertEquals(7, ps.getParameterMetaData().getParameterCount());
        }
    }

    @Test
    void streamsALargeResultAndReleasesItMidway() throws Exception {
        String sql = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200000) "
                + "SELECT i, printf('%0100d', i) FROM n";
        try (Connection c = connect(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(sql)) {
                long sum = 0;
                while (rs.next()) {
                    sum += rs.getLong(1);
                }
                assertEquals(200000L * 200001 / 2, sum);
            }
            ResultSet rs = s.executeQuery(sql);
            for (int i = 1; i <= 10; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            rs.close();
            try (ResultSet again = s.executeQuery("VALUES (1), (2)")) {
                assertTrue(again.next() && again.next() && !again.next());
            }
        }
    }

    @Test
    void reportsMetadataFromTheServer() throws Exception {
        try (Connection c = connect()) {
            var md = c.getMetaData();
            String version;
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT sqlite_version()")) {
                assertTrue(rs.next());
                version = rs.getString(1);
            }
            assertEquals("SQLite", md.getDatabaseProductName());
            assertEquals(version, md.getDatabaseProductVersion());
            assertEquals(Integer.parseInt(version.split("\\.")[0]), md.getDatabaseMajorVersion());
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 AS one, 'x' AS two WHERE ? > 0")) {
                assertEquals(2, ps.getMetaData().getColumnCount());
                assertEquals("two", ps.getMetaData().getColumnLabel(2));
                assertEquals(1, ps.getParameterMetaData().getParameterCount());
            }
            assertTrue(c.isValid(5));
        }
    }

    @Test
    void refusesBadSql() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertThrows(SQLSyntaxErrorException.class, () -> s.executeQuery("SELEC 1"));
            assertThrows(SQLSyntaxErrorException.class, () -> s.executeQuery("SELECT * FROM no_such_table"));
        }
    }

    /** DDL, autocommit inserts with their keys and counts, batches, and the catalog queries over what they made. */
    @Test
    void writesInAutoCommit() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            assertEquals(0, s.executeUpdate("CREATE TABLE w_auto (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)"));
            assertFalse(s.execute("CREATE INDEX w_auto_name ON w_auto (name)"), "DDL through execute");
            try (PreparedStatement ins = c.prepareStatement("INSERT INTO w_auto (name) VALUES (?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                ins.setString(1, "a");
                assertEquals(1, ins.executeUpdate());
                try (ResultSet keys = ins.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    assertEquals(1, keys.getLong(1));
                    assertFalse(keys.next());
                }
                ins.setString(1, "b");
                ins.addBatch();
                ins.setString(1, "c");
                ins.addBatch();
                assertArrayEquals(new int[] {1, 1}, ins.executeBatch());
                ins.setString(1, "a");
                SQLException dup = assertThrows(SQLException.class, ins::executeUpdate);
                assertTrue(dup instanceof java.sql.SQLIntegrityConstraintViolationException, dup.toString());
                assertEquals("23000", dup.getSQLState());
            }
            assertEquals(3, s.executeUpdate("UPDATE w_auto SET name = name || '!'"));
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM w_auto WHERE name LIKE '%!'")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getLong(1), "the next read sees the autocommit writes");
            }
            try (ResultSet rs = c.getMetaData().getTables(null, null, "w_auto", new String[] {"TABLE"})) {
                assertTrue(rs.next());
                assertEquals("w_auto", rs.getString("TABLE_NAME"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = c.getMetaData().getColumns(null, null, "w_auto", "%")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals(java.sql.Types.BIGINT, rs.getInt("DATA_TYPE"));
                assertTrue(rs.next());
                assertEquals("name", rs.getString("COLUMN_NAME"));
                assertEquals("TEXT", rs.getString("TYPE_NAME"));
                assertEquals("NO", rs.getString("IS_NULLABLE"));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * A transaction is invisible to another connection until its commit, survives a failed statement, and leaves
     * nothing after a rollback; a read-only connection refuses writes; a rollback to a savepoint keeps what came before
     * it.
     */
    @Test
    void runsTransactions() throws Exception {
        try (Connection writer = connect(); Connection reader = connect(); Statement w = writer.createStatement()) {
            w.executeUpdate("CREATE TABLE w_txn (id INTEGER PRIMARY KEY, v TEXT UNIQUE)");
            writer.setAutoCommit(false);
            w.executeUpdate("INSERT INTO w_txn (v) VALUES ('one')");
            assertThrows(java.sql.SQLIntegrityConstraintViolationException.class,
                    () -> w.executeUpdate("INSERT INTO w_txn (v) VALUES ('one')"));
            w.executeUpdate("INSERT INTO w_txn (v) VALUES ('two')");
            try (ResultSet rs = w.executeQuery("SELECT count(*) FROM w_txn")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1), "the transaction reads its own writes");
            }
            assertEquals(0, count(reader, "w_txn"));
            writer.commit();
            assertEquals(2, count(reader, "w_txn"));

            w.executeUpdate("DELETE FROM w_txn");
            writer.rollback();
            writer.setAutoCommit(true);
            assertEquals(2, count(reader, "w_txn"));

            reader.setReadOnly(true);
            SQLException ro = assertThrows(SQLException.class,
                    () -> reader.createStatement().executeUpdate("DELETE FROM w_txn"));
            assertEquals("25006", ro.getSQLState(), ro.toString());

            writer.setAutoCommit(false);
            java.sql.Savepoint first = writer.setSavepoint(); // begins the transaction
            w.executeUpdate("INSERT INTO w_txn (v) VALUES ('three')");
            java.sql.Savepoint named = writer.setSavepoint("x\"y");
            w.executeUpdate("INSERT INTO w_txn (v) VALUES ('four')");
            writer.rollback(named);
            writer.releaseSavepoint(first);
            writer.commit();
            try (ResultSet rs = w.executeQuery("SELECT group_concat(v) FROM (SELECT v FROM w_txn ORDER BY id)")) {
                assertTrue(rs.next());
                assertEquals("one,two,three", rs.getString(1), "rolled back to the savepoint, then committed");
            }
        }
    }

    /**
     * A batch is one request: all or nothing in auto-commit, and inside a transaction it stops at its first failure,
     * reporting the counts and keys of the entries before it.
     */
    @Test
    void runsBatches() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement();
                PreparedStatement ins = c.prepareStatement("INSERT INTO w_batch (v) VALUES (?)",
                        Statement.RETURN_GENERATED_KEYS)) {
            s.executeUpdate("CREATE TABLE w_batch (id INTEGER PRIMARY KEY, v TEXT UNIQUE)");
            for (String v : java.util.List.of("a", "b", "a")) {
                ins.setString(1, v);
                ins.addBatch();
            }
            java.sql.BatchUpdateException auto = assertThrows(java.sql.BatchUpdateException.class, ins::executeBatch);
            assertEquals("23000", auto.getSQLState(), auto.toString());
            assertArrayEquals(new int[0], auto.getUpdateCounts(), "an autocommit batch leaves nothing");
            assertEquals(0, count(c, "w_batch"));

            c.setAutoCommit(false);
            for (String v : java.util.List.of("a", "b", "a", "c")) {
                ins.setString(1, v);
                ins.addBatch();
            }
            java.sql.BatchUpdateException txn = assertThrows(java.sql.BatchUpdateException.class, ins::executeBatch);
            assertArrayEquals(new int[] {1, 1}, txn.getUpdateCounts());
            try (ResultSet keys = ins.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertTrue(keys.next());
                assertFalse(keys.next());
            }
            c.commit();
            assertEquals(2, count(c, "w_batch"), "the entries before the failure, committed");

            c.setAutoCommit(true);
            s.addBatch("UPDATE w_batch SET v = v || '!'");
            s.addBatch("DELETE FROM w_batch WHERE v = 'a!'");
            assertArrayEquals(new long[] {2, 1}, s.executeLargeBatch());
            assertEquals(1, count(c, "w_batch"));
        }
    }

    /**
     * Closing a connection with its transaction open rolls it back and releases the write lock: another connection
     * writes at once.
     */
    @Test
    void closingRollsBackAndReleasesTheLock() throws Exception {
        try (Connection c = connect()) {
            c.createStatement().executeUpdate("CREATE TABLE w_close (v TEXT)");
        }
        Connection open = connect();
        open.setAutoCommit(false);
        open.createStatement().executeUpdate("INSERT INTO w_close VALUES ('lost')");
        open.close();
        try (Connection c = connect()) {
            long t0 = System.nanoTime();
            assertEquals(1, c.createStatement().executeUpdate("INSERT INTO w_close VALUES ('kept')"));
            assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() < 4_000, "the lock was released");
            assertEquals(1, count(c, "w_close"));
        }
    }

    /** A query timeout inside a transaction reaches the server, which ends the statement; the transaction goes on. */
    @Test
    void theStatementDeadlineEndsAChannelStatement() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.executeUpdate("CREATE TABLE w_deadline (v)");
            s.setQueryTimeout(1);
            long t0 = System.nanoTime();
            SQLException e = assertThrows(SQLException.class, () -> s.executeQuery("WITH RECURSIVE n(i) AS "
                    + "(SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT count(*) FROM n"));
            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            assertTrue(e instanceof java.sql.SQLTimeoutException, e.toString());
            assertTrue(ms >= 900 && ms < 2_900, "ended after " + ms + " ms");
            s.executeUpdate("INSERT INTO w_deadline VALUES (1)");
            c.commit();
            assertEquals(1, count(c, "w_deadline"));
        }
    }

    static long count(Connection c, String table) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    @Test
    void refusesAnUnknownPrincipal() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        String forged = jwt(g.generateKeyPair(), "admin");
        Properties p = props();
        p.put("tokenProvider", (TokenProvider) () -> forged);
        try (Connection c = DriverManager.getConnection(url, p); Statement s = c.createStatement()) {
            SQLInvalidAuthorizationSpecException e =
                    assertThrows(SQLInvalidAuthorizationSpecException.class, () -> s.executeQuery("SELECT 1"));
            assertTrue(e.getMessage().contains("unauthenticated"), e.getMessage());
            assertFalse(e.getMessage().contains(forged));
        }
    }

    /**
     * timeout_ms reaches the server and its deadline stops the query with ADR 0005's {@code timeout} terminal frame,
     * which the driver raises as {@link java.sql.SQLTimeoutException}. The query ends at the deadline, not at the
     * driver's backstop 2 s later.
     */
    @Test
    void theStatementDeadlineReachesTheServer() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            s.setQueryTimeout(1);
            try (ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
            long t0 = System.nanoTime();
            SQLException e = assertThrows(SQLException.class, () -> {
                try (ResultSet rs = s.executeQuery("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) "
                        + "SELECT count(*) FROM n")) {
                    rs.next();
                }
            });
            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            assertTrue(e instanceof java.sql.SQLTimeoutException, e.toString());
            assertTrue(ms >= 900 && ms < 2_900, "ended after " + ms + " ms");
        }
    }
    /**
     * A statement stopped just after its request is submitted gives the HTTP/2 stream back. The JDK client leaks a
     * stream slot when sendAsync's future is cancelled before the request body is subscribed; after the server's 16
     * it refuses every request. Distinct roots give this test its own HttpClient, so that a leak stays here.
     */
    @Test
    void aStopJustAfterTheSubmissionGivesTheStreamBack(@TempDir Path dir) throws Exception {
        String forever = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i FROM n";
        Path pem = dir.resolve("roots.pem");
        Files.writeString(pem, Files.readString(certs.certPem) + "\n");
        Properties p = props();
        p.setProperty("tlsRootCertificates", pem.toString());
        ExecutorService pool = Executors.newCachedThreadPool();
        try (Connection c = DriverManager.getConnection(url, p)) {
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next()); // the HTTP/2 connection exists before the races
            }
            for (int i = 0; i < 400; i++) {
                boolean close = i % 2 == 1;
                try (Statement s = c.createStatement()) {
                    CountDownLatch go = new CountDownLatch(1);
                    Future<?> query = pool.submit(() -> {
                        go.await();
                        try (ResultSet rs = s.executeQuery(forever)) {
                            rs.next();
                        } catch (SQLException e) { // cancelled or closed; never out of streams
                            assertFalse(String.valueOf(e.getMessage()).contains("concurrent streams"), e.toString());
                        }
                        return null;
                    });
                    Future<?> stop = pool.submit(() -> {
                        go.await();
                        long t = System.nanoTime(), w = ThreadLocalRandom.current().nextLong(2_000_000);
                        while (System.nanoTime() - t < w) {
                            Thread.onSpinWait();
                        }
                        if (close) {
                            s.close();
                        } else {
                            s.cancel();
                        }
                        return null;
                    });
                    go.countDown();
                    query.get(30, TimeUnit.SECONDS);
                    stop.get(30, TimeUnit.SECONDS);
                }
            }
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
