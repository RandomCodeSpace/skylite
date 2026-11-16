package io.github.randomcodespace.skylite.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * The skylite JDBC driver: {@code jdbc:skylite:https://host[:port][/base]/<database>}, speaking ADR 0005 over HTTPS.
 * This version serves reads only. Connection properties:
 *
 * <ul>
 *   <li>{@code tokenProvider}: a {@link TokenProvider} class name, or an instance;
 *   <li>{@code tokenFile}: a file holding the token, read again before every request;
 *   <li>{@code tlsRootCertificates}: a PEM file of the certificates to trust instead of the JVM's;
 *   <li>{@code allowPlaintext}: {@code true} permits an {@code http://} URL.
 * </ul>
 *
 * Exactly one of {@code tokenProvider} and {@code tokenFile} is required. No secret goes in the URL, which may not carry
 * user information or a query. {@code user} and {@code password} are ignored.
 */
public final class SkyliteDriver implements Driver {
    static final String VERSION = version();

    /** ADR 0005 decision 5: one watermark per canonical base URL and database, for the driver class's life. */
    private static final Map<String, AtomicLong> WATERMARKS = new ConcurrentHashMap<>();
    /** One HTTP client per trust configuration, keyed by the roots file's digest ("" for the JVM's roots). */
    private static final Map<String, HttpClient> CLIENTS = new ConcurrentHashMap<>();

    static {
        try {
            DriverManager.registerDriver(new SkyliteDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // another driver's URL, as the Driver contract requires
        }
        Properties p = info == null ? new Properties() : info;
        boolean plaintext = Boolean.parseBoolean(String.valueOf(p.getOrDefault("allowPlaintext", "false")));
        Endpoint endpoint = Endpoint.parse(url, plaintext);
        Object providerValue = p.get("tokenProvider");
        String tokenFile = p.getProperty("tokenFile");
        if ((providerValue == null) == (tokenFile == null)) {
            throw config("set exactly one of the properties tokenProvider and tokenFile");
        }
        TokenProvider provider = providerValue == null ? null : provider(providerValue);
        HttpClient http = client(p.getProperty("tlsRootCertificates"));
        AtomicLong watermark = WATERMARKS.computeIfAbsent(endpoint.key(), k -> new AtomicLong());
        Session s = new Session(endpoint, provider, tokenFile == null ? null : Path.of(tokenFile), http, watermark);
        return new SkyliteConnection(s, url);
    }

    private static TokenProvider provider(Object v) throws SQLException {
        if (v instanceof TokenProvider t) {
            return t;
        }
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            // Not initialised until it is known to be a TokenProvider: no other class's static code runs.
            Class<?> c = Class.forName(String.valueOf(v), false, cl != null ? cl : SkyliteDriver.class.getClassLoader());
            if (!TokenProvider.class.isAssignableFrom(c)) {
                throw config("the tokenProvider class does not implement " + TokenProvider.class.getName());
            }
            return (TokenProvider) c.getConstructor().newInstance();
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new SQLNonTransientConnectionException(
                    "skylite: cannot instantiate the tokenProvider class with a public no-argument constructor",
                    "08001",
                    e);
        }
    }

    private static HttpClient client(String rootsFile) throws SQLException {
        if (rootsFile == null) {
            return CLIENTS.computeIfAbsent("", k -> build(null));
        }
        byte[] pem;
        try {
            pem = Files.readAllBytes(Path.of(rootsFile));
        } catch (IOException e) {
            throw new SQLNonTransientConnectionException("skylite: cannot read tlsRootCertificates", "08001", e);
        }
        SSLContext ctx;
        String key;
        try {
            key = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(pem));
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            int i = 0;
            try (InputStream in = new java.io.ByteArrayInputStream(pem)) {
                for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                    ks.setCertificateEntry("root-" + i++, c);
                }
            }
            if (i == 0) {
                throw config("tlsRootCertificates holds no certificate");
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
        } catch (GeneralSecurityException | IOException e) {
            throw new SQLNonTransientConnectionException("skylite: cannot load tlsRootCertificates", "08001", e);
        }
        return CLIENTS.computeIfAbsent(key, k -> build(ctx));
    }

    /** HTTP/2 where the server offers it, and no redirect followed, so a request and its token reach only the URL. */
    private static HttpClient build(SSLContext ctx) {
        HttpClient.Builder b = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(30));
        if (ctx != null) {
            b.sslContext(ctx);
        }
        return b.build();
    }

    private static SQLException config(String what) {
        return new SQLNonTransientConnectionException("skylite: " + what, "08001");
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(Endpoint.PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo provider = new DriverPropertyInfo("tokenProvider", null);
        provider.description = "Class name of a TokenProvider that supplies each request's bearer token";
        DriverPropertyInfo file = new DriverPropertyInfo("tokenFile", null);
        file.description = "File holding the bearer token, read again before every request";
        DriverPropertyInfo roots = new DriverPropertyInfo("tlsRootCertificates", null);
        roots.description = "PEM file of the certificates to trust instead of the JVM's";
        DriverPropertyInfo plain = new DriverPropertyInfo("allowPlaintext", "false");
        plain.description = "true permits an http:// URL";
        plain.choices = new String[] {"true", "false"};
        return new DriverPropertyInfo[] {provider, file, roots, plain};
    }

    @Override
    public int getMajorVersion() {
        return versionPart(0);
    }

    @Override
    public int getMinorVersion() {
        return versionPart(1);
    }

    static int versionPart(int i) {
        String[] parts = VERSION.split("[.-]");
        try {
            return Integer.parseInt(parts[i]);
        } catch (RuntimeException e) {
            return 0;
        }
    }

    /** Not JDBC compliant: no transactions or writes in this version, and SQLite is not SQL-92 entry level. */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw Errors.unsupported("a parent logger (the driver does not log)");
    }

    private static String version() {
        Properties p = new Properties();
        try (InputStream in = SkyliteDriver.class.getResourceAsStream("version.properties")) {
            if (in != null) {
                p.load(in);
            }
        } catch (IOException e) {
            // fall through to the unknown version
        }
        return p.getProperty("version", "0.0.0");
    }
}
