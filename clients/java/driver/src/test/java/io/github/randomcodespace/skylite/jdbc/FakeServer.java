package io.github.randomcodespace.skylite.jdbc;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;

/**
 * An in-process server that checks every request against ADR 0005's rules for its path, answering a violation with
 * 400 invalid_request and recording it, then answers valid requests with a scripted responder. It keeps no channel
 * state: the responder scripts the channel's answers.
 */
final class FakeServer implements AutoCloseable {
    /** A request as received. */
    record Received(String method, String path, Headers headers, byte[] body) {
        String bodyText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    @FunctionalInterface
    interface Responder {
        void respond(Received r, HttpExchange ex) throws Exception;
    }

    private static final Pattern PATH = Pattern.compile(
            "/v1/databases/([a-z0-9][a-z0-9-]{0,62})/(query|describe|channels|channel|channel/cancel|channel/close)");
    private static final Map<String, Set<String>> FIELDS = Map.of(
            "query", Set.of("sql", "params", "named", "min_txn", "timeout_ms"),
            "describe", Set.of("sql", "min_txn"),
            "channels", Set.of("mode", "min_txn"),
            "channel", Set.of("channel", "seq", "op", "sql", "params", "named", "min_txn", "timeout_ms", "execute"),
            "channel/cancel", Set.of("channel", "seq"),
            "channel/close", Set.of("channel"));

    final List<Received> requests = new CopyOnWriteArrayList<>();
    final List<String> violations = new CopyOnWriteArrayList<>();
    private final HttpServer server;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final String token;
    volatile Responder responder = (r, ex) -> send(ex, 200, "{\"columns\":[]}", done(1));

    FakeServer(TestCerts certs, String token) throws IOException {
        this.token = token;
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        if (certs == null) {
            server = HttpServer.create(addr, 0);
        } else {
            HttpsServer s = HttpsServer.create(addr, 0);
            s.setHttpsConfigurator(new HttpsConfigurator(certs.serverContext));
            server = s;
        }
        server.setExecutor(pool);
        server.createContext("/", this::handle);
        server.start();
    }

    /** The JDBC URL of database db on this server. */
    String url(String db) {
        String scheme = server instanceof HttpsServer ? "https" : "http";
        return Endpoint.PREFIX + scheme + "://localhost:" + server.getAddress().getPort() + "/" + db;
    }

    Received last() {
        return requests.get(requests.size() - 1);
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            byte[] body = ex.getRequestBody().readAllBytes();
            Received r = new Received(ex.getRequestMethod(), ex.getRequestURI().getRawPath(), ex.getRequestHeaders(), body);
            requests.add(r);
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (ex.getRequestHeaders().get("Authorization") == null
                    || ex.getRequestHeaders().get("Authorization").size() != 1
                    || !auth.equals("Bearer " + token)) {
                ex.getResponseHeaders().add("WWW-Authenticate", "Bearer");
                send(ex, 401, error("unauthenticated", "the bearer token is missing or does not verify"));
                return;
            }
            String v = violation(ex, r);
            if (v != null) {
                violations.add(v);
                send(ex, 400, error("invalid_request", v));
                return;
            }
            responder.respond(r, ex);
        } catch (Exception e) {
            // the client went away, or the responder failed on purpose
        }
    }

    private static String violation(HttpExchange ex, Received r) {
        Matcher m = PATH.matcher(r.path());
        if (!r.method().equals("POST") || !m.matches() || ex.getRequestURI().getRawQuery() != null) {
            return "unknown method or path " + r.method() + " " + r.path();
        }
        Headers h = ex.getRequestHeaders();
        String ct = h.getFirst("Content-Type");
        if (ct == null || !ct.split(";")[0].strip().toLowerCase(Locale.ROOT).equals("application/json")) {
            return "Content-Type is not application/json";
        }
        if (h.getFirst("Transfer-Encoding") != null) {
            return "Transfer-Encoding is set";
        }
        String cl = h.getFirst("Content-Length");
        if (cl == null || Long.parseLong(cl) != r.body().length) {
            return "Content-Length is missing or wrong";
        }
        byte[] b = r.body();
        if (b.length == 0 || b[0] != '{' || b[b.length - 1] != '}') {
            return "the body does not start with { and end with }";
        }
        try {
            Values.checkUtf8(b, 0, b.length);
            return fields(m.group(2), b);
        } catch (Values.Malformed | JacksonException e) {
            return "the body is not one valid JSON object: " + e.getMessage();
        }
    }

    private static String fields(String kind, byte[] b) throws Values.Malformed {
        boolean sql = false, params = false, named = false;
        try (JsonParser p = Values.parser(b, 0, b.length)) {
            p.nextToken();
            String name;
            while ((name = p.nextName()) != null) {
                JsonToken t = p.nextToken();
                if (!FIELDS.get(kind).contains(name)) {
                    return "field " + name + " is not allowed in a " + kind;
                }
                switch (name) {
                    case "sql" -> {
                        if (t != JsonToken.VALUE_STRING || p.getString().isEmpty() || p.getString().indexOf(0) >= 0) {
                            return "sql is not a non-empty string without U+0000";
                        }
                        Values.checkString(p.getString());
                        sql = true;
                    }
                    case "params" -> {
                        if (t != JsonToken.START_ARRAY) {
                            return "params is not an array";
                        }
                        while (p.nextToken() != JsonToken.END_ARRAY) {
                            Values.read(p);
                        }
                        params = true;
                    }
                    case "named" -> {
                        named = true;
                        p.skipChildren();
                    }
                    case "min_txn", "timeout_ms", "seq" -> {
                        if (t != JsonToken.VALUE_NUMBER_INT || p.getNumberType() == JsonParser.NumberType.BIG_INTEGER) {
                            return name + " is not an integer";
                        }
                        long v = p.getLongValue();
                        if (v < (name.equals("min_txn") ? 0 : 1) || (name.equals("timeout_ms") && v > Integer.MAX_VALUE)) {
                            return name + " is out of range";
                        }
                    }
                    default -> p.skipChildren();
                }
            }
            if (p.nextToken() != null) {
                return "bytes follow the object";
            }
        }
        if (!sql && (kind.equals("query") || kind.equals("describe"))) {
            return "sql is missing";
        }
        if (params && named) {
            return "params and named together";
        }
        return null;
    }

    static String done(long txn) {
        return "{\"done\":{\"txn\":" + txn + ",\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0}}";
    }

    /** An open answer with the given token. */
    static String opened(String token) {
        return "{\"done\":{\"txn\":1,\"channel\":{\"token\":\"" + token + "\",\"id\":\"00\",\"mode\":\"write\","
                + "\"epoch\":1,\"limits\":{}}}}";
    }

    /** The answer to an execute, or to a begin with its first statement. */
    static String executed(long seq, String state, long rows, long rowid) {
        return "{\"done\":{\"seq\":" + seq + ",\"txn_state\":\"" + state + "\",\"txn\":2,\"read_only\":false,"
                + "\"rows_affected\":" + rows + ",\"last_insert_rowid\":" + rowid + "}}";
    }

    /** The answer to a commit or a rollback. */
    static String stepped(long seq) {
        return "{\"done\":{\"seq\":" + seq + ",\"txn_state\":\"none\",\"txn\":3,\"committed\":true,\"wrote\":true}}";
    }

    /** An error of a channel request, with its flags. */
    static String channelError(String code, boolean notExecuted, boolean isFinal, String state) {
        return "{\"error\":{\"code\":\"" + code + "\",\"not_executed\":" + notExecuted + ",\"final\":" + isFinal
                + ",\"message\":\"m\",\"request_id\":\"req-1\",\"txn_state\":\"" + state + "\"}}";
    }

    static String describeDone(long txn, int params) {
        return "{\"done\":{\"txn\":" + txn + ",\"param_count\":" + params + "}}";
    }

    static String error(String code, String message) {
        return "{\"error\":{\"code\":\"" + code + "\",\"not_executed\":true,\"final\":false,\"message\":\""
                + message + "\",\"request_id\":\"req-1\"}}";
    }

    /** Sends a complete answer: the marker, NDJSON, then each frame and a newline. */
    static void send(HttpExchange ex, int status, String... frames) throws IOException {
        start(ex, status, true);
        try (OutputStream out = ex.getResponseBody()) {
            for (String f : frames) {
                out.write((f + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /** Sends the headers of a streamed answer; the caller writes the body. */
    static void start(HttpExchange ex, int status, boolean marker) throws IOException {
        Headers h = ex.getResponseHeaders();
        if (marker) {
            h.add("Skylite-Protocol", "1");
        }
        h.add("Skylite-Request-Id", "req-1");
        h.add("Content-Type", "application/x-ndjson");
        h.add("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, 0);
    }

    @Override
    public void close() {
        server.stop(0);
        pool.shutdownNow();
    }
}
