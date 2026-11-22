package io.github.randomcodespace.skylite.acceptance;

import java.io.IOException;
import java.io.InputStream;
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
import java.security.KeyStore;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * With {@code -Dskylite.serve=true} (the {@code skylite} profile), starts a {@code skylite serve} built from this
 * repository before any test, as the JDBC driver's SkyliteServerIT does: HTTPS with a self-signed certificate,
 * JWT authentication by a test issuer whose ES256 token is signed here, and storage on the local file system. It
 * creates the database, then sets {@code jdbc.url} and the pool's driver properties {@code tokenFile} and
 * {@code tlsRootCertificates} as system properties, which the application and {@link ConnectionReport} read. The
 * server stops when the test run ends. Without the property it does nothing.
 */
public class SkyliteServer implements LauncherSessionListener {
    static final String DB = "acceptance";
    private static final String ISSUER = "https://issuer.skylite.test";
    private static final String DRIVER = "io.github.randomcodespace.skylite.jdbc.SkyliteDriver";
    private Process server;

    @Override
    public void launcherSessionOpened(LauncherSession session) {
        if (!Boolean.getBoolean("skylite.serve") || server != null) {
            return;
        }
        try {
            start();
        } catch (Exception e) {
            throw new IllegalStateException("cannot start skylite serve", e);
        }
    }

    @Override
    public void launcherSessionClosed(LauncherSession session) {
        if (server != null) {
            server.destroy(); // SIGTERM: the server leaves office and exits
            try {
                if (!server.waitFor(70, TimeUnit.SECONDS)) {
                    server.destroyForcibly();
                }
            } catch (InterruptedException e) {
                server.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void start() throws Exception {
        Path repo = Path.of(System.getProperty("skylite.repo", "../../..")).toRealPath();
        Path dir = Files.createDirectories(Path.of("target", "skylite")).toAbsolutePath();
        Path bin = dir.resolve("skylite");
        run(new ProcessBuilder("go", "build", "-o", bin.toString(), "./cmd/skylite").directory(repo.toFile()), "go build");
        Path run = Files.createTempDirectory(dir, "run-");

        Path p12 = run.resolve("tls.p12");
        run(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "skylite", "-keyalg", "EC", "-groupname", "secp256r1",
                "-sigalg", "SHA256withECDSA", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "2", "-keystore", p12.toString(), "-storetype", "PKCS12",
                "-storepass", "changeit", "-keypass", "changeit"), "keytool");
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(p12)) {
            ks.load(in, "changeit".toCharArray());
        }
        Path cert = run.resolve("tls.crt");
        Path key = run.resolve("tls.key");
        Files.writeString(cert, pem("CERTIFICATE", ks.getCertificate("skylite").getEncoded()));
        Files.writeString(key, pem("PRIVATE KEY", ks.getKey("skylite", "changeit".toCharArray()).getEncoded()));

        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair signer = g.generateKeyPair();
        ECPublicKey pub = (ECPublicKey) signer.getPublic();
        Path jwks = run.resolve("jwks.json");
        Files.writeString(jwks, "{\"keys\":[{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"it\",\"alg\":\"ES256\","
                + "\"use\":\"sig\",\"x\":\"" + b64(coordinate(pub.getW().getAffineX())) + "\",\"y\":\""
                + b64(coordinate(pub.getW().getAffineY())) + "\"}]}");
        String token = jwt(signer);
        Path tokenFile = run.resolve("token");
        Files.writeString(tokenFile, token);

        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Path config = run.resolve("skylite.json");
        Files.writeString(config, """
                {
                  "listen": "127.0.0.1:%d",
                  "tls": {"cert_file": "%s", "key_file": "%s"},
                  "auth": {
                    "issuers": [{"issuer": "%s", "audience": "skylite", "jwks_file": "%s"}],
                    "rules": [{"issuer": "%s", "subject": "acceptance", "database": "*", "role": "admin"}]
                  },
                  "storage": {"prefix": "acceptance", "localfs": {"path": "%s"}},
                  "databases": ["%s"]
                }
                """.formatted(port, cert, key, ISSUER, jwks, ISSUER, Files.createDirectories(run.resolve("data")), DB));
        Path log = run.resolve("skylite.log");
        ProcessBuilder serve = new ProcessBuilder(bin.toString(), "serve", "-config", config.toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        serve.environment().put("TMPDIR", run.toString()); // the leader's local files
        server = serve.start();
        String url = "jdbc:skylite:https://localhost:" + port + "/" + DB;
        Properties props = new Properties();
        props.setProperty("tokenFile", tokenFile.toString());
        props.setProperty("tlsRootCertificates", cert.toString());
        try {
            waitForPort(port, log);
            put(port, cert, token);
            waitUntilServing(url, props, log);
        } catch (Exception | Error e) {
            server.destroyForcibly(); // no orphan server when the run cannot start
            server = null;
            throw e;
        }
        System.setProperty("jdbc.url", url);
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            System.setProperty("spring.datasource.hikari.data-source-properties." + e.getKey(), (String) e.getValue());
        }
        System.out.println("skylite serve on port " + port + ", log " + log);
    }

    private static void run(ProcessBuilder pb, String what) throws Exception {
        Map<String, String> env = pb.environment();
        env.putIfAbsent("GOTOOLCHAIN", "go1.26.11");
        env.put("GOFLAGS", "-buildvcs=false");
        env.put("CGO_ENABLED", "0");
        Process p = pb.redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IOException(what + " failed:\n" + out);
        }
    }

    private void waitForPort(int port, Path log) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try {
                new Socket("127.0.0.1", port).close();
                return;
            } catch (IOException e) {
                if (!server.isAlive() || System.nanoTime() > deadline) {
                    throw new IOException("skylite serve did not start:\n" + Files.readString(log), e);
                }
                Thread.sleep(100);
            }
        }
    }

    /** Creates the database with the protocol's PUT: the driver does not create databases. */
    private static void put(int port, Path cert, String token) throws Exception {
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        try (InputStream in = Files.newInputStream(cert)) {
            trust.setCertificateEntry("skylite", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        try (HttpClient http = HttpClient.newBuilder().sslContext(ctx).build()) {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("https://localhost:" + port
                            + "/v1/databases/" + DB))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != 200) {
                throw new IOException("PUT /v1/databases/" + DB + ": " + r.statusCode() + " " + r.body());
            }
        }
    }

    /** The server takes office within its retry backoff once the database exists; until then not_serving. */
    private void waitUntilServing(String url, Properties props, Path log) throws Exception {
        Driver driver = (Driver) Class.forName(DRIVER).getDeclaredConstructor().newInstance();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            try (Connection c = driver.connect(url, props); var rs = c.createStatement().executeQuery("SELECT 1")) {
                rs.next();
                return;
            } catch (SQLTransientConnectionException e) {
                if (System.nanoTime() > deadline) {
                    throw new IOException("the server did not lead " + DB + ":\n" + Files.readString(log), e);
                }
                Thread.sleep(250);
            }
        }
    }

    private static String jwt(KeyPair key) throws Exception {
        long now = Instant.now().getEpochSecond();
        String header = b64("{\"alg\":\"ES256\",\"kid\":\"it\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String claims = b64(("{\"iss\":\"" + ISSUER + "\",\"sub\":\"acceptance\",\"aud\":\"skylite\",\"iat\":" + now
                + ",\"exp\":" + (now + 4 * 3600) + "}").getBytes(StandardCharsets.UTF_8));
        Signature sig = Signature.getInstance("SHA256withECDSAinP1363Format");
        sig.initSign(key.getPrivate());
        sig.update((header + "." + claims).getBytes(StandardCharsets.US_ASCII));
        return header + "." + claims + "." + b64(sig.sign());
    }

    private static byte[] coordinate(BigInteger v) {
        byte[] b = v.toByteArray();
        byte[] out = new byte[32];
        int n = Math.min(b.length, 32);
        System.arraycopy(b, b.length - n, out, 32 - n, n);
        return out;
    }

    private static String b64(byte[] b) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
