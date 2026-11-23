package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The driver's own retries of requests that ADR 0005 proves did not run: a refusal before acceptance with
 * {@code not_executed: true}, and a recorded failure outside a transaction with {@code not_executed: true} whose code
 * says that the server was busy, not that the request was wrong. Each pause is drawn at random from the upper half
 * of a bound that doubles from {@link #baseMillis} to {@link #MAX_MILLIS}, so that refused clients do not come back
 * together, and no pause passes the caller's deadline.
 */
final class Retry {
    private Retry() {}

    /** Refusals before acceptance that a later delivery of the same request can pass. */
    private static final Set<String> REFUSED = Set.of(
            "overloaded", "too_many_channels", "no_leader", "not_serving", "owner_unreachable", "behind", "maintenance");
    /** Recorded failures that ran nothing and that the same statement, sent as a new request, can pass. */
    private static final Set<String> RECORDED = Set.of("busy", "overloaded", "maintenance");
    /** The longest pause. */
    static final long MAX_MILLIS = 1_000;
    /** The first pause's bound; tests lower it. */
    static volatile long baseMillis = 10;
    /** How long the driver retries when the caller set no query timeout: ADR 0005's statement time. */
    static volatile long defaultBudgetMillis = 30_000;

    /** A refusal before acceptance that proves this delivery ran nothing and that may pass later. */
    static boolean refused(Failure f) {
        return f.notExecuted() && !f.isFinal() && REFUSED.contains(f.code());
    }

    /** A recorded failure, outside a transaction, of a request that ran nothing and may pass as a new request. */
    static boolean recorded(Failure f) {
        return f.notExecuted() && f.isFinal() && RECORDED.contains(f.code()) && !"open".equals(f.txnState());
    }

    /** The deadline of a request with timeoutMillis, or the default budget when it is 0, from now. */
    static long deadline(long timeoutMillis) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis > 0 ? timeoutMillis : defaultBudgetMillis);
    }

    /** Milliseconds left before deadline, at least 0. */
    static long left(long deadline) {
        return Math.max(0, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
    }

    /**
     * Sleeps before attempt (from 0), unless the pause would pass deadline: then returns false at once. An interrupt
     * is raised as a transport failure, the thread's interrupt status kept.
     */
    static boolean pause(int attempt, long deadline) throws SQLException {
        long bound = Math.min(MAX_MILLIS, baseMillis << Math.min(attempt, 20));
        long millis = bound / 2 + ThreadLocalRandom.current().nextLong(bound / 2 + 1);
        if (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis) - deadline > 0) {
            return false;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Errors.transport("interrupted while waiting to send a request again", e);
        }
        return true;
    }
}
