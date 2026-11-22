package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;

/**
 * Maps ADR 0005's error codes to JDBC exception classes and SQLStates, by meaning, within ADR 0005 decision 6: no
 * transient class and no class-40 state for an outcome that may be unknown, vendor code 0 except for SQLite's own
 * errors. The table is documented on docs/content/docs/jdbc-driver.md; change both together.
 */
final class Errors {
    private Errors() {}

    /** An exception for an error frame. Its message holds the code, the server's message and the request id. */
    static SQLException map(Failure f) {
        String msg = "skylite: " + f.code() + ": " + f.message() + " (request " + f.requestId() + ")";
        return switch (f.code()) {
            case "invalid_request" -> new SQLSyntaxErrorException(msg, "42000");
            case "unauthenticated", "forbidden" -> new SQLInvalidAuthorizationSpecException(msg, "28000");
            case "statement_forbidden" -> new SQLSyntaxErrorException(msg, "42501");
            case "database_not_found" -> new SQLNonTransientConnectionException(msg, "08001");
            case "database_exists" -> new SQLNonTransientException(msg, "42710");
            case "needs_channel" -> new SQLFeatureNotSupportedException(
                    "skylite: writes are not supported in this driver version; the server answered " + msg, "0A000");
            // Refused before acceptance: not executed, so a new attempt is safe.
            case "overloaded", "too_many_channels" -> new SQLTransientConnectionException(msg, "08004");
            case "no_leader", "not_serving", "owner_unreachable", "behind" ->
                new SQLTransientConnectionException(msg, "08001");
            case "maintenance" -> new SQLTransientConnectionException(msg, "08001");
            case "request_too_large", "result_too_large" -> new SQLNonTransientException(msg, "54000");
            case "timeout" -> new SQLTimeoutException(msg, "HYT00");
            case "cancelled" -> new SQLNonTransientException(msg, "57014");
            case "busy" -> new SQLTransactionRollbackException(msg, "40001");
            case "transaction_aborted" -> "maintenance".equals(f.cause())
                    ? new SQLTransactionRollbackException(msg, "40001")
                    : new SQLNonTransientException(msg, "25000");
            case "invalid_state" -> new SQLNonTransientException(msg, "25000");
            case "channel_busy", "in_flight", "stale_sequence", "sequence_gap", "sequence_conflict" ->
                new SQLNonTransientException(msg, "HY010");
            case "channel_lost" -> new SQLNonTransientConnectionException(msg, "08003");
            case "commit_failed" -> new SQLTransactionRollbackException(msg, "40000");
            case "commit_outcome_unknown" -> new SQLNonTransientConnectionException(msg, "08007");
            case "sql_error" -> sqlError(f, msg);
            default -> new SQLException(msg, "HY000"); // internal, or a code outside protocol version 1
        };
    }

    /** sql_error by SQLite's primary code; the vendor code is SQLite's extended code, or its primary one. */
    private static SQLException sqlError(Failure f, String msg) {
        int vendor = (int) (f.sqliteExtendedCode() >= 0 ? f.sqliteExtendedCode() : Math.max(f.sqliteCode(), 0));
        return switch ((int) f.sqliteCode()) {
            case 1 -> new SQLSyntaxErrorException(msg, "42000", vendor); // SQLITE_ERROR: no such table, syntax
            case 8 -> new SQLNonTransientException(msg, "25006", vendor); // SQLITE_READONLY
            case 18 -> new SQLDataException(msg, "22001", vendor); // SQLITE_TOOBIG
            case 19 -> new SQLIntegrityConstraintViolationException(msg, "23000", vendor); // SQLITE_CONSTRAINT
            case 20 -> new SQLDataException(msg, "22000", vendor); // SQLITE_MISMATCH
            case 25 -> new SQLDataException(msg, "22023", vendor); // SQLITE_RANGE
            default -> new SQLException(msg, "HY000", vendor);
        };
    }

    /** A response that breaks the protocol, ends early or fails in transport. Never retried. */
    static SQLException transport(String what, Throwable cause) {
        return new SQLNonTransientConnectionException("skylite: " + what, "08S01", cause);
    }

    static SQLFeatureNotSupportedException unsupported(String what) {
        return new SQLFeatureNotSupportedException("skylite: " + what + " is not supported in this driver version", "0A000");
    }

    static SQLFeatureNotSupportedException writes() {
        return unsupported("writing (transactions, updates, batches, generated keys)");
    }

    static SQLException closed(String what) {
        return new SQLNonTransientException("skylite: the " + what + " is closed", "HY010");
    }

    static SQLDataException data(String what, String state) {
        return new SQLDataException("skylite: " + what, state);
    }
}
