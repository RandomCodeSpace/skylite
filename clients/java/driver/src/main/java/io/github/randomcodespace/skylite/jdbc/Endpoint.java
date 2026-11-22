package io.github.randomcodespace.skylite.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A JDBC URL {@code jdbc:skylite:https://host[:port][/base]/<database>}: the server's base URL, then the database
 * name as the last path segment. The base URL is kept in ADR 0005's canonical form (decision 5), which also keys the
 * read-your-writes watermark, so URLs that differ only in canonicalisation share one. Errors never echo the URL.
 */
record Endpoint(String base, String database) {
    static final String PREFIX = "jdbc:skylite:";
    private static final Pattern DATABASE = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");
    private static final String UNRESERVED =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

    static Endpoint parse(String url, boolean allowPlaintext) throws SQLException {
        URI u;
        try {
            u = new URI(url.substring(PREFIX.length()));
        } catch (URISyntaxException e) {
            throw invalid("is not a valid URL");
        }
        String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
        if (u.isOpaque() || u.getHost() == null || u.getHost().isEmpty()) {
            throw invalid("must be an absolute URL with a host");
        }
        if (u.getRawUserInfo() != null || u.getRawQuery() != null || u.getRawFragment() != null || url.contains("#")) {
            throw invalid("may not carry user information, a query or a fragment");
        }
        if (scheme.equals("http") && !allowPlaintext) {
            throw invalid("is plain http; set the property allowPlaintext=true to permit it");
        }
        if (!scheme.equals("https") && !scheme.equals("http")) {
            throw invalid("must be https:// (or http:// with allowPlaintext=true)");
        }
        String path = u.getRawPath() == null ? "" : u.getRawPath();
        int slash = path.lastIndexOf('/');
        String db = slash < 0 ? "" : path.substring(slash + 1);
        if (!DATABASE.matcher(db).matches()) {
            throw invalid("must end with the database name: 1 to 63 characters from a-z, 0-9 and -, "
                    + "starting with a letter or digit");
        }
        StringBuilder canon = new StringBuilder(scheme).append("://").append(u.getHost().toLowerCase(Locale.ROOT));
        int port = u.getPort();
        if (port == 0 || port > 65535) {
            throw invalid("has a port outside 1 to 65535");
        }
        if (port >= 0 && !(port == 443 && scheme.equals("https")) && !(port == 80 && scheme.equals("http"))) {
            canon.append(':').append(port);
        }
        String basePath = path.substring(0, slash);
        for (String seg : basePath.split("/", -1)) {
            String decoded = decodeSegment(seg);
            if (decoded.equals(".") || decoded.equals("..")) {
                throw invalid("may not have a \".\" or \"..\" path segment");
            }
        }
        canon.append(canonicalPath(basePath));
        int end = canon.length();
        while (end > 0 && canon.charAt(end - 1) == '/') {
            end--;
        }
        canon.setLength(end);
        return new Endpoint(canon.toString(), db);
    }

    /** Decodes percent-encoded octets of unreserved characters, writes every other one with upper-case hex. */
    private static String canonicalPath(String p) throws SQLException {
        StringBuilder sb = new StringBuilder(p.length());
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (c != '%') {
                sb.append(c);
                continue;
            }
            int v = hexOctet(p, i);
            if (UNRESERVED.indexOf(v) >= 0) {
                sb.append((char) v);
            } else {
                sb.append('%').append(String.format("%02X", v));
            }
            i += 2;
        }
        return sb.toString();
    }

    /** Decodes each percent-encoded octet to the char of that value: enough to find a "." or ".." segment. */
    private static String decodeSegment(String seg) throws SQLException {
        StringBuilder out = new StringBuilder(seg.length());
        for (int i = 0; i < seg.length(); i++) {
            char c = seg.charAt(i);
            if (c == '%') {
                out.append((char) hexOctet(seg, i));
                i += 2;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static int hexOctet(String s, int i) throws SQLException {
        if (i + 2 >= s.length()) {
            throw invalid("has an incomplete percent-encoding");
        }
        int hi = Character.digit(s.charAt(i + 1), 16), lo = Character.digit(s.charAt(i + 2), 16);
        if (hi < 0 || lo < 0) {
            throw invalid("has an invalid percent-encoding");
        }
        return hi << 4 | lo;
    }

    private static SQLException invalid(String what) {
        return new SQLNonTransientConnectionException("skylite: the JDBC URL " + what, "08001");
    }

    /** The URL of a request on this database: query or describe. */
    URI uri(String request) {
        return URI.create(base + "/v1/databases/" + database + "/" + request);
    }

    /** The watermark's key: the canonical base URL and the database name, nothing else. */
    String key() {
        return base + " " + database;
    }
}
