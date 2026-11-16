package io.github.randomcodespace.skylite.jdbc;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a connection needs to send requests: the endpoint, the token source, the HTTP client and the shared
 * read-your-writes watermark. The token comes from exactly one of a {@link TokenProvider} and a token file, and is
 * fetched again for every request; it is never kept.
 */
final class Session {
    private final Endpoint endpoint;
    private final TokenProvider provider;
    private final Path tokenFile;
    private final HttpClient http;
    private final AtomicLong watermark;
    private volatile int networkTimeoutMillis;

    Session(Endpoint endpoint, TokenProvider provider, Path tokenFile, HttpClient http, AtomicLong watermark) {
        this.endpoint = endpoint;
        this.provider = provider;
        this.tokenFile = tokenFile;
        this.http = http;
        this.watermark = watermark;
    }

    Endpoint endpoint() {
        return endpoint;
    }

    HttpClient http() {
        return http;
    }

    int networkTimeoutMillis() {
        return networkTimeoutMillis;
    }

    void networkTimeoutMillis(int ms) {
        networkTimeoutMillis = ms;
    }

    long watermark() {
        return watermark.get();
    }

    /** Raises the watermark to txn, never lowering it. */
    void raise(long txn) {
        watermark.accumulateAndGet(txn, Math::max);
    }

    /**
     * The bearer token of the next request: from the provider, or the token file read again, trimmed. Refused
     * before anything is sent when empty or holding characters a bearer token cannot hold. Errors never hold it.
     */
    String token() throws SQLException {
        String t;
        try {
            t = provider != null ? provider.token() : Files.readString(tokenFile, StandardCharsets.UTF_8).strip();
        } catch (Exception e) {
            throw new SQLInvalidAuthorizationSpecException(
                    "skylite: the token source failed (" + e.getClass().getName() + ")", "28000", e);
        }
        if (!validBearer(t)) {
            throw new SQLInvalidAuthorizationSpecException(
                    "skylite: the token source returned no token, or one with characters a bearer token cannot hold",
                    "28000");
        }
        return t;
    }

    /** Printable ASCII without spaces, so that it cannot break the header. */
    static boolean validBearer(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c <= ' ' || c > '~') {
                return false;
            }
        }
        return true;
    }
}
