package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.Values.Malformed;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * One stateless request and its response: query or describe. It is sent once and never retried. Closing it, before
 * or during the body, releases the HTTP response; {@link #cancel} and the query timeout do that from another thread.
 */
final class Exchange implements AutoCloseable {
    /** ADR 0005's driver wait for a cancel answer: how long past a query timeout the server's own answer may take. */
    static final Duration CANCEL_WAIT = Duration.ofSeconds(2);
    /** The wait for a response's headers, as the Go driver's default: ADR 0005's request time T at the defaults. */
    static final Duration RESPONSE_HEADER_TIMEOUT = Duration.ofSeconds(60);

    private static final class Timer {
        static final ScheduledExecutorService INSTANCE = timer();

        private static ScheduledExecutorService timer() {
            ScheduledThreadPoolExecutor t = new ScheduledThreadPoolExecutor(1, r -> {
                Thread th = new Thread(r, "skylite-jdbc-timeout");
                th.setDaemon(true);
                return th;
            });
            t.setRemoveOnCancelPolicy(true);
            return t;
        }
    }

    private volatile CompletableFuture<HttpResponse<InputStream>> future;
    private volatile InputStream body;
    private volatile FrameReader reader;
    private volatile ScheduledFuture<?> timer;
    private volatile boolean cancelled, timedOut, stalled, closed;
    private final int timeoutSeconds;
    private final int networkMillis;

    private Exchange(int timeoutSeconds, int networkMillis) {
        this.timeoutSeconds = timeoutSeconds;
        this.networkMillis = networkMillis;
    }

    /**
     * Sends a request and waits for its headers. With a query timeout, the server's statement time is lowered to it
     * through the body's timeout_ms, and the response is abandoned {@link #CANCEL_WAIT} later if the server has not
     * ended it: a read leaves no unknown outcome, so that is a {@link SQLTimeoutException}. A describe carries no
     * timeout_ms, so it is abandoned at the timeout itself. A network timeout bounds the wait for the headers and
     * each wait for body bytes, as a socket read timeout would.
     */
    static Exchange send(Session s, FrameReader.Kind kind, byte[] body, int timeoutSeconds, Owner owner)
            throws SQLException {
        int network = s.networkTimeoutMillis();
        Exchange ex = new Exchange(timeoutSeconds, network);
        if (owner != null) {
            owner.started(ex);
        }
        String token = s.token(); // before the timer, so a failing token source leaves nothing scheduled
        if (ex.stopping()) { // cancelled or closed while the token was read: nothing is sent
            ex.close();
            throw ex.stopped(null);
        }
        Duration wait = network > 0 ? Duration.ofMillis(network) : RESPONSE_HEADER_TIMEOUT;
        if (timeoutSeconds > 0) {
            Duration t = Duration.ofSeconds(timeoutSeconds);
            if (kind == FrameReader.Kind.QUERY) {
                t = t.plus(CANCEL_WAIT);
            }
            if (network == 0 && t.compareTo(wait) > 0) {
                wait = t;
            }
            ex.timer = Timer.INSTANCE.schedule(ex::timeOut, t.toMillis(), TimeUnit.MILLISECONDS);
        }
        HttpRequest req = HttpRequest.newBuilder(s.endpoint().uri(kind == FrameReader.Kind.QUERY ? "query" : "describe"))
                .timeout(wait)
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<InputStream> resp;
        try {
            ex.future = s.http().sendAsync(req, HttpResponse.BodyHandlers.ofInputStream());
            // A response that arrives after a stop, or after this thread stopped waiting, is released here.
            ex.future.thenAccept(r -> ex.arrived(r.body()));
            if (ex.stopping()) {
                ex.future.cancel(true);
            }
            resp = ex.future.get();
        } catch (CancellationException e) {
            ex.close();
            throw ex.stopped(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ex.close();
            throw Errors.transport("interrupted while waiting for the response", e);
        } catch (ExecutionException e) {
            ex.close();
            if (ex.cancelled || ex.timedOut) {
                throw ex.stopped(e.getCause());
            }
            // The cause's text can name the host and port, never the token.
            throw Errors.transport("the request failed: " + e.getCause(), e.getCause());
        }
        if (!ex.arrived(resp.body())) {
            ex.close();
            throw ex.stopped(null);
        }
        int status = resp.statusCode();
        if (status >= 300 && status < 400) {
            ex.close();
            throw Errors.transport("the server answered with a redirect (HTTP status " + status
                    + "), which the driver never follows", null);
        }
        if (!resp.headers().allValues("Skylite-Protocol").equals(java.util.List.of("1"))) {
            ex.close();
            throw Errors.transport("the answer (HTTP status " + status
                    + ") has no Skylite-Protocol: 1 header, so it is not from a skylite server", null);
        }
        ex.reader = new FrameReader(network > 0 ? ex.new Watched(ex.body) : ex.body, kind, FrameReader.MAX_FRAME);
        return ex;
    }

    private boolean stopping() {
        return cancelled || timedOut || closed;
    }

    /**
     * Keeps the response's body so that a stop can release it, and releases it at once if the exchange was already
     * stopped: one of this and {@link #abort} always sees the other's write. False when it was stopped.
     */
    private boolean arrived(InputStream b) {
        body = b;
        if (stopping()) {
            closeBody();
            return false;
        }
        return true;
    }

    /** The next frame, or null after the terminal frame. An error frame is returned, not thrown. */
    Frame next() throws SQLException {
        try {
            Frame f = reader.next();
            if (f == null || f instanceof FrameReader.Done || f instanceof Failure) {
                stopTimer();
            }
            return f;
        } catch (IOException e) {
            boolean stopped = cancelled || timedOut || stalled || closed; // before close() sets closed
            close();
            if (stopped) {
                throw stopped(e);
            }
            throw e instanceof EOFException
                    ? Errors.transport(e.getMessage() + ", so it is incomplete", e)
                    : Errors.transport("reading the response failed: " + e, e);
        } catch (Malformed e) {
            close();
            throw Errors.transport("malformed response: " + e.getMessage(), null);
        }
    }

    /** The exception for a request this driver stopped: cancelled, timed out or closed. */
    private SQLException stopped(Throwable cause) {
        if (timedOut) {
            return new SQLTimeoutException("skylite: the query timeout of " + timeoutSeconds + " s passed", "HYT00", cause);
        }
        if (cancelled) {
            return new SQLNonTransientException("skylite: the statement was cancelled", "57014", cause);
        }
        if (stalled) {
            return Errors.transport("no response bytes within the network timeout of " + networkMillis + " ms", cause);
        }
        return Errors.closed("result set");
    }

    private void timeOut() {
        timedOut = true;
        abort();
    }

    private void stall() {
        stalled = true;
        abort();
    }

    /** Statement.cancel: abandons the request, or its response if it has one. */
    void cancel() {
        cancelled = true;
        abort();
    }

    private void abort() {
        CompletableFuture<HttpResponse<InputStream>> f = future;
        if (f != null) {
            f.cancel(true);
        }
        closeBody();
    }

    private void stopTimer() {
        ScheduledFuture<?> t = timer;
        if (t != null) {
            t.cancel(false);
        }
    }

    @Override
    public void close() {
        closed = true;
        stopTimer();
        CompletableFuture<HttpResponse<InputStream>> f = future;
        if (f != null && !f.isDone()) {
            f.cancel(true);
        }
        closeBody();
    }

    private void closeBody() {
        InputStream b = body;
        if (b != null) {
            try {
                b.close(); // before the end of the body, this resets the stream or closes the connection
            } catch (IOException ignored) {
                // nothing more to release
            }
        }
    }

    /** The body, each read of which the network timeout bounds. */
    private final class Watched extends FilterInputStream {
        Watched(InputStream in) {
            super(in);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ScheduledFuture<?> w = Timer.INSTANCE.schedule(Exchange.this::stall, networkMillis, TimeUnit.MILLISECONDS);
            try {
                return in.read(b, off, len);
            } finally {
                w.cancel(false);
            }
        }
    }

    /** What a statement is told about its running exchange, so that cancel can reach it. */
    interface Owner {
        void started(Exchange ex);
    }
}
