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
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * One request and its response: a stateless query or describe, or a channel request. It is sent once; only
 * {@link Channel} sends a channel request's body again. Closing it, before or during the body, releases the HTTP
 * response; {@link #cancel} and the query timeout do that from another thread.
 */
final class Exchange implements FrameReader.Frames {
    /** ADR 0005's driver wait for a cancel answer: how long past a query timeout the server's own answer may take. */
    static final Duration CANCEL_WAIT = Duration.ofSeconds(2);
    /** The wait for a response's headers, as the Go driver's default: ADR 0005's request time T at the defaults. */
    static final Duration RESPONSE_HEADER_TIMEOUT = Duration.ofSeconds(60);

    private static final class Timer {
        static final ScheduledExecutorService INSTANCE = timer();
        // The driver's own threads for stops and channel closes, which can wait on a token provider or a lock: the
        // common pool has only as many threads as CPUs, and blocked closes there would hold back every stop.
        static final ExecutorService WORK = Executors.newCachedThreadPool(r -> {
            Thread th = new Thread(r, "skylite-jdbc-work");
            th.setDaemon(true);
            return th;
        });

        private static ScheduledExecutorService timer() {
            ScheduledThreadPoolExecutor t = new ScheduledThreadPoolExecutor(1, r -> {
                Thread th = new Thread(r, "skylite-jdbc-timeout");
                th.setDaemon(true);
                return th;
            });
            t.setRemoveOnCancelPolicy(true);
            return t;
        }

        /**
         * Runs task after millis on another thread: a stop takes its owner's lock, which a submission on another
         * statement can hold while the JDK selects a proxy, and the timer must never wait.
         */
        static ScheduledFuture<?> later(Runnable task, long millis) {
            return INSTANCE.schedule(() -> async(task), millis, TimeUnit.MILLISECONDS);
        }
    }

    /** Runs task on one of the driver's own threads, never the timer's or the common pool's. */
    static void async(Runnable task) {
        CompletableFuture.runAsync(task, Timer.WORK);
    }

    private final Owner owner;
    private final Object lock;
    // Written under lock, so that a stop and the submission or the arrival of the response each see the other.
    private volatile CompletableFuture<HttpResponse<InputStream>> future; // the JDK's
    private volatile CompletableFuture<HttpResponse<InputStream>> waited; // its copy, which a stop cancels at once
    // Done when the JDK subscribes to the request body, or when its future completes without that.
    private final CompletableFuture<Void> subscribed = new CompletableFuture<>();
    private volatile InputStream body;
    private volatile FrameReader reader;
    private volatile ScheduledFuture<?> timer;
    private volatile boolean cancelled, timedOut, stalled, closed;
    private final int timeoutSeconds;
    private final int networkMillis;

    private Exchange(Owner owner, int timeoutSeconds, int networkMillis) {
        this.owner = owner;
        this.lock = owner.lock();
        this.timeoutSeconds = timeoutSeconds;
        this.networkMillis = networkMillis;
    }

    /**
     * Sends a statement's request and waits for its headers. With a query timeout, the server's statement time is
     * lowered to it through the body's timeout_ms, and the response is abandoned {@link #CANCEL_WAIT} later if the
     * server has not ended it: a read leaves no unknown outcome, so that is a {@link SQLTimeoutException}. A describe
     * carries no timeout_ms, so it is abandoned at the timeout itself. A network timeout bounds the wait for the
     * headers and each wait for body bytes, as a socket read timeout would; when it passes, the connection closes.
     */
    static Exchange send(Session s, FrameReader.Kind kind, byte[] body, int timeoutSeconds, Owner owner)
            throws SQLException {
        int network = s.networkTimeoutMillis();
        String token = s.token();
        long stop = 0;
        if (timeoutSeconds > 0) {
            Duration t = Duration.ofSeconds(timeoutSeconds);
            if (kind == FrameReader.Kind.QUERY) {
                t = t.plus(CANCEL_WAIT);
            }
            stop = t.toMillis();
        }
        return start(s, kind == FrameReader.Kind.QUERY ? "query" : "describe", kind, body, token, owner, timeoutSeconds,
                network, stop, headerWait(network, stop));
    }

    /**
     * Sends a request of a channel to path. No timer stops a request that carries an outcome: a write's answer is its
     * outcome, and the server ends the statement at its timeout_ms. The network timeout, or else waitMillis, bounds
     * the wait for the headers. A cancel, a close or a resolve, which changes nothing, is stopped waitMillis after it
     * is sent, body included.
     */
    static Exchange channel(Session s, String path, FrameReader.Kind kind, byte[] body, long waitMillis, Owner owner)
            throws SQLException {
        int network = s.networkTimeoutMillis();
        String token = s.token();
        boolean bounded = kind == FrameReader.Kind.CANCEL || kind == FrameReader.Kind.CLOSE
                || kind == FrameReader.Kind.RESOLVE;
        return start(s, path, kind, body, token, owner, 0, network, bounded ? waitMillis : 0,
                Duration.ofMillis(network > 0 ? network : waitMillis));
    }

    /**
     * The JDK's wait for the headers: the network timeout, or else long enough that the stop timer, not the JDK's
     * own timeout, ends a statement with a query timeout.
     */
    static Duration headerWait(int networkMillis, long stopMillis) {
        if (networkMillis > 0) {
            return Duration.ofMillis(networkMillis);
        }
        Duration past = Duration.ofMillis(stopMillis).plus(CANCEL_WAIT);
        return stopMillis > 0 && past.compareTo(RESPONSE_HEADER_TIMEOUT) > 0 ? past : RESPONSE_HEADER_TIMEOUT;
    }

    /**
     * Sends a validation describe that must end within limitMillis of this call, token included; 0 is no limit. Its
     * expiry is a {@link SQLTimeoutException}.
     */
    static Exchange validate(Session s, byte[] body, long limitMillis, Owner owner) throws SQLException {
        long t0 = System.nanoTime();
        String token = s.token();
        long left = 0;
        Duration wait = RESPONSE_HEADER_TIMEOUT;
        if (limitMillis > 0) {
            left = limitMillis - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            if (left <= 0) {
                throw new SQLTimeoutException("skylite: the validation timeout passed while the token was read", "HYT00");
            }
            wait = Duration.ofMillis(left).plus(CANCEL_WAIT); // the stop timer, not this wait, ends it
        }
        return start(s, "describe", FrameReader.Kind.DESCRIBE, body, token, owner, 0, 0, left, wait);
    }

    private static Exchange start(Session s, String path, FrameReader.Kind kind, byte[] body, String token,
            Owner owner, int timeoutSeconds, int network, long stopMillis, Duration wait) throws SQLException {
        Exchange ex = new Exchange(owner, timeoutSeconds, network);
        HttpRequest.BodyPublisher bytes = HttpRequest.BodyPublishers.ofByteArray(body);
        HttpRequest req = HttpRequest.newBuilder(s.endpoint().uri(path))
                .timeout(wait)
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(new HttpRequest.BodyPublisher() {
                    @Override
                    public long contentLength() {
                        return bytes.contentLength();
                    }

                    @Override
                    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                        bytes.subscribe(subscriber);
                        ex.subscribed.complete(null);
                    }
                })
                .build();
        // The owner's stop check, the submission and the registration are one step: a stop that completes first
        // sends nothing, and a later one finds the request to cancel. The headers are awaited outside it.
        synchronized (ex.lock) {
            SQLException refused = owner.admit(ex);
            if (refused != null) {
                throw refused;
            }
            if (stopMillis > 0) {
                ex.timer = Timer.later(ex::timeOut, stopMillis);
            }
            ex.future = s.http().sendAsync(req, HttpResponse.BodyHandlers.ofInputStream());
            ex.waited = ex.future.copy();
        }
        HttpResponse<InputStream> resp;
        try {
            // A request that ends before its body is subscribed leaves no cancel waiting for that.
            ex.future.whenComplete((r, e) -> ex.subscribed.complete(null));
            // A response that arrives after a stop, or after this thread stopped waiting, is released here.
            ex.future.thenAccept(r -> ex.arrived(r.body()));
            resp = ex.waited.get();
        } catch (CancellationException e) {
            ex.close();
            throw ex.stopped(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ex.close();
            throw Errors.transport("interrupted while waiting for the response", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (network > 0 && cause instanceof HttpTimeoutException && !ex.stopping()) {
                ex.stall(); // the wait for the headers was the network timeout
            }
            boolean stopped = ex.stopping(); // a stop can fail the request before it cancels the future
            ex.close();
            if (stopped) {
                throw ex.stopped(cause);
            }
            // The cause's text can name the host and port, never the token.
            throw Errors.transport("the request failed: " + cause, cause);
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
        ex.reader = new FrameReader(network > 0 ? ex.new Watched(resp.body()) : resp.body(), kind, FrameReader.MAX_FRAME);
        return ex;
    }

    private boolean stopping() {
        return cancelled || timedOut || stalled || closed;
    }

    /**
     * Keeps the response's body so that a stop can release it, and releases it at once if the exchange was already
     * stopped. False when it was stopped.
     */
    private boolean arrived(InputStream b) {
        synchronized (lock) {
            body = b;
            if (!stopping()) {
                return true;
            }
        }
        close(b);
        return false;
    }

    /** The next frame, or null after the terminal frame. An error frame is returned, not thrown. */
    @Override
    public Frame next() throws SQLException {
        try {
            Frame f = reader.next();
            if (stopping()) { // a frame read after a stop, perhaps from a buffer, is not delivered
                close();
                throw stopped(null);
            }
            if (f == null || f instanceof FrameReader.Done || f instanceof Failure) {
                stopTimer();
            }
            return f;
        } catch (IOException e) {
            boolean stopped = stopping(); // before close() sets closed
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
            return cancelled(cause);
        }
        if (stalled) {
            return Errors.transport("no response bytes within the network timeout of " + networkMillis + " ms", cause);
        }
        SkyliteConnection c = owner.connection();
        return c.isClosed() ? c.closedError() : Errors.closed("result set");
    }

    static SQLException cancelled(Throwable cause) {
        return new SQLNonTransientException("skylite: the statement was cancelled", "57014", cause);
    }

    private void timeOut() {
        synchronized (lock) {
            timedOut = true;
        }
        abort();
    }

    /**
     * The network timeout passed: like a socket timeout, that ends the logical connection and all it holds. The
     * connection's close, which waits for its channel's answer, runs on another thread.
     */
    private void stall() {
        synchronized (lock) {
            stalled = true;
        }
        owner.connection().closeLater(); // closed before the reader wakes, which closing this exchange's body does
        abort();
    }

    /** Statement.cancel: abandons the request, or its response if it has one. */
    void cancel() {
        synchronized (lock) {
            cancelled = true;
        }
        abort();
    }

    private void abort() {
        CompletableFuture<HttpResponse<InputStream>> f, w;
        InputStream b;
        synchronized (lock) {
            f = future;
            w = waited;
            b = body;
        }
        if (f != null && !f.isDone()) {
            w.cancel(false); // the waiting thread returns at once
            // The JDK leaks an HTTP/2 stream slot when the request is cancelled before its body is subscribed.
            subscribed.thenRunAsync(() -> f.cancel(true), Timer.INSTANCE);
        }
        close(b);
    }

    private void stopTimer() {
        ScheduledFuture<?> t = timer;
        if (t != null) {
            t.cancel(false);
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
        }
        stopTimer();
        abort();
        owner.release(this);
    }

    private static void close(InputStream b) {
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
            ScheduledFuture<?> w = Timer.later(Exchange.this::stall, networkMillis);
            try {
                return in.read(b, off, len);
            } finally {
                w.cancel(false);
            }
        }
    }

    /** A statement or a connection's validation: what stops its exchanges and what each belongs to. */
    interface Owner {
        /** The lock that orders the owner's stops with each exchange's submission, arrival and stop. */
        Object lock();

        /** Called under {@link #lock} just before the request is sent: refuses it, or keeps ex as running. */
        SQLException admit(Exchange ex);

        /** Forgets ex, by identity, when it closes. */
        void release(Exchange ex);

        /** The logical connection that a network timeout closes. */
        SkyliteConnection connection();
    }
}
