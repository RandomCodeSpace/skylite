package io.github.randomcodespace.skylite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fake server whose answers are scripted per path: each request to a path gets that path's next answer, the last
 * one repeated. An answer "drop" closes the exchange without one, "sleep:N" waits N ms and then drops it. An open's
 * default answer names a new token, w1.N or r1.N by the mode asked for, and a close's says nothing was accepted.
 */
final class Scripted implements AutoCloseable {
    final FakeServer fake;
    private final Map<String, List<String>> answers = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> next = new ConcurrentHashMap<>();
    private final AtomicInteger opens = new AtomicInteger();
    /** The limits object of the default open answer. */
    volatile String limits = "{}";

    Scripted() throws Exception {
        fake = new FakeServer(null, "script-tok");
        fake.responder = (r, ex) -> {
            String path = r.path().substring(r.path().indexOf('/', "/v1/databases/".length()) + 1);
            List<String> script = answers.get(path);
            String a;
            if (script != null) {
                a = script.get(Math.min(next.computeIfAbsent(path, k -> new AtomicInteger()).getAndIncrement(),
                        script.size() - 1));
            } else if (path.equals("channels")) {
                boolean ro = r.bodyText().contains("read_only");
                a = "{\"done\":{\"txn\":1,\"channel\":{\"token\":\"" + (ro ? "r1." : "w1.") + opens.incrementAndGet()
                        + "\",\"id\":\"00\",\"mode\":\"" + (ro ? "read_only" : "write") + "\",\"limits\":" + limits
                        + "}}}";
            } else if (path.equals("channel/close")) {
                a = "{\"done\":{\"last_seq\":0}}";
            } else {
                a = "{\"columns\":[{\"name\":\"x\"}]}\n[1]\n" + FakeServer.done(1);
            }
            if (a.startsWith("sleep:")) {
                Thread.sleep(Long.parseLong(a.substring(6)));
                ex.close();
            } else if (a.equals("drop")) {
                ex.close();
            } else {
                FakeServer.send(ex, a.contains("\"error\"") && !a.contains("\"final\":true") ? 409 : 200,
                        a.split("\n"));
            }
        };
    }

    /** Scripts the answers to path, such as "channel", "resolve" or "query". */
    Scripted on(String path, String... script) {
        next.remove(path);
        answers.put(path, List.of(script));
        return this;
    }

    Connection connect() throws SQLException {
        Properties p = new Properties();
        p.setProperty("allowPlaintext", "true");
        p.put("tokenProvider", (TokenProvider) () -> "script-tok");
        return DriverManager.getConnection(fake.url("script"), p);
    }

    /** The bodies sent to path, in order. */
    List<String> bodies(String path) {
        return fake.requests.stream().filter(r -> r.path().endsWith("/" + path)).map(FakeServer.Received::bodyText)
                .toList();
    }

    @Override
    public void close() {
        fake.close();
    }
}
