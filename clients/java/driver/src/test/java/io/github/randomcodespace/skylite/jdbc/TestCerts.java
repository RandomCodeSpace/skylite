package io.github.randomcodespace.skylite.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * A self-signed P-256 certificate for localhost and 127.0.0.1, made with the JDK's keytool, written as PEM files for
 * the driver's tlsRootCertificates and for skylite serve, and as an SSLContext for the fake server.
 */
final class TestCerts {
    private static final char[] PASS = "changeit".toCharArray();

    final Path certPem;
    final Path keyPem;
    final SSLContext serverContext;

    private TestCerts(Path certPem, Path keyPem, SSLContext serverContext) {
        this.certPem = certPem;
        this.keyPem = keyPem;
        this.serverContext = serverContext;
    }

    static TestCerts create(Path dir) throws Exception {
        Path p12 = dir.resolve("tls.p12");
        Process proc = new ProcessBuilder(
                        Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                        "-genkeypair", "-alias", "skylite", "-keyalg", "EC", "-groupname", "secp256r1",
                        "-sigalg", "SHA256withECDSA", "-dname", "CN=localhost",
                        "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
                        "-keystore", p12.toString(), "-storetype", "PKCS12",
                        "-storepass", "changeit", "-keypass", "changeit")
                .redirectErrorStream(true)
                .start();
        String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (proc.waitFor() != 0) {
            throw new IOException("keytool failed: " + out);
        }
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(p12)) {
            ks.load(in, PASS);
        }
        Path cert = dir.resolve("tls.crt");
        Path key = dir.resolve("tls.key");
        Files.writeString(cert, pem("CERTIFICATE", ks.getCertificate("skylite").getEncoded()));
        Files.writeString(key, pem("PRIVATE KEY", ks.getKey("skylite", PASS).getEncoded()));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, PASS);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);
        return new TestCerts(cert, key, ctx);
    }

    private static String pem(String type, byte[] der) {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }
}
