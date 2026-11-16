---
title: JDBC driver
description: The JDBC driver's read path, which is all it has in this version, how to configure it, and what it does not do yet.
weight: 21
status: current
---

`clients/java/driver` is a JDBC driver, artifact `io.github.randomcodespace:skylite-jdbc`, that speaks the protocol of [0005: Client protocol](/docs/decisions/0005-client-protocol/) over HTTPS, as [0003: JDBC transport](/docs/decisions/0003-jdbc-transport/) decided. It needs Java 25. This version has the read path only: statements outside a transaction that the server accepts as pure reads. It is not published to a Maven repository; build it with `mvn -f clients/java/driver/pom.xml verify`.

Its tests run against an in-process fake server that checks every request against 0005, and against a real `skylite serve` built from this repository. That server has no write channels yet, so the integration test queries constant SQL only. Nothing has been run through Hibernate, Spring or a connection pool.

## Usage

```java
import io.github.randomcodespace.skylite.jdbc.TokenProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public final class ProjectedToken implements TokenProvider {
    // A projected service account token, read again for every request.
    @Override
    public String token() throws Exception {
        return Files.readString(Path.of("/var/run/secrets/tokens/skylite")).strip();
    }

    public static void main(String[] args) throws SQLException {
        Properties p = new Properties();
        p.setProperty("tokenProvider", ProjectedToken.class.getName());
        try (Connection c = DriverManager.getConnection("jdbc:skylite:https://skylite.example.internal/library", p);
                PreparedStatement ps = c.prepareStatement("SELECT id, title FROM books WHERE id > ?")) {
            ps.setLong(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println(rs.getLong("id") + " " + rs.getString("title"));
                }
            }
        }
    }
}
```

The driver registers itself through `META-INF/services/java.sql.Driver`; its class is `io.github.randomcodespace.skylite.jdbc.SkyliteDriver`. For a token file alone, `p.setProperty("tokenFile", "/var/run/secrets/tokens/skylite")` does the same as the provider above.

## Configuration

The URL is `jdbc:skylite:https://host[:port][/base]/<database>`: the server's base URL, then the database name as the last path segment. The name is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit. User information, a query, a fragment, an empty host and `.` or `..` path segments are refused, as by the [Go driver](/docs/go-driver/), and the error does not repeat the URL.

| Property | Meaning |
|---|---|
| `tokenProvider` | The name of a class that implements `TokenProvider` and has a public no-argument constructor, or a `TokenProvider` instance put into the `Properties` object. Each connection created from a class name gets its own instance. The driver calls `token()` before every HTTP request. |
| `tokenFile` | A file that holds the token. The driver reads it again before every request and strips white space around it, so a rotated projected token is picked up at once. |
| `tlsRootCertificates` | A PEM file of the certificates to trust, such as a private CA, instead of the JVM's trust store. The file is read when a connection is made. |
| `allowPlaintext` | `true` permits an `http://` URL. Without it only `https://` is accepted. |

Exactly one of `tokenProvider` and `tokenFile` is required. `user` and `password` are ignored. No secret goes in the URL. The driver never logs; its exceptions never hold the token or the request URL, though an exception from the network or TLS can name the host and port it could not reach. A token that is empty or holds characters a bearer token cannot hold, such as a space, is refused before anything is sent.

The driver uses HTTP/2 when the server offers it, and refuses every redirect, so a request and its token go only to the URL's host. It waits up to 60 seconds for the headers of each response, or `Connection.setNetworkTimeout`'s value when that is set. HTTP clients are shared by connections with the same trust settings.

## What works

- **Queries.** `Statement` and `PreparedStatement` send each `executeQuery` and `execute` to the stateless `query` endpoint, with the SQL and its parameters. Rows are decoded as their frames arrive, never buffered whole; `ResultSet` is forward-only and read-only. Closing a result set, its statement or its connection before the end releases the HTTP response. A response that ends before its terminal frame is an error, never a short result.
- **Prepared statements.** Preparing sends nothing. `getParameterMetaData()` and, before an execution, `getMetaData()` send one `describe`, whose answer the statement keeps. Parameters bind by index: `?`, `?NNN` and named parameters such as `:t` alike. Each execution sends its SQL again; there are no server-side handles.
- **Metadata.** `ResultSetMetaData` from the result columns; `ParameterMetaData` with the parameter count; `DatabaseMetaData` with the product name `SQLite`, the server's `sqlite_version()` as the product version, the driver version and the capabilities of this version. Catalog queries such as `getTables` and `getColumns` are not supported yet.
- **Validity.** `Connection.isValid` describes `SELECT 1`, as the Go driver's ping does.
- **Cancel and timeout.** `Statement.cancel()` abandons the running request, or its response: a stateless read needs no cancel request, because the server stops streaming when the response is dropped. The exception has SQLState `57014`. `setQueryTimeout(n)` sends `timeout_ms` so that the server's statement time is at most *n* seconds, and abandons a response that has not ended 2 seconds after that, the driver's cancel wait in 0005. Either way the driver raises `SQLTimeoutException`, which is safe for a read: it leaves no unknown outcome.
- **Read-your-writes.** One watermark per database, keyed by the canonical base URL and the database name and kept for the driver class's life, is raised from each answer's `txn` and sent as `min_txn`, so connections and URLs that differ only in canonicalisation share it.

## Values

| Storage class | `getObject` | Also readable as |
|---|---|---|
| NULL | `null` | 0, `false` or `null` from the typed getters, with `wasNull()` true |
| INTEGER | `Long` | `getInt`, `getShort` and `getByte` within their range, otherwise SQLState `22003`; `getDouble`, `getBigDecimal`, `getString`; `getBoolean` for 0 and 1 only |
| REAL | `Double`, infinities included | `getLong` for an integral value within range; `getBigDecimal` for a finite value; `getString` |
| TEXT | `String` | `getBytes` (its UTF-8 bytes); `getLong`, `getDouble` and `getBigDecimal` when the text is a number; the temporal getters below |
| TEXT that is not valid UTF-8 | an error | `getBytes` returns its own bytes; `getString` raises SQLState `22021` |
| BLOB | `byte[]` | `getBytes`, `getBlob` |

Temporal values are TEXT ([0005 decision 8](/docs/decisions/0005-client-protocol/#8-values-and-shared-conventions)). `getObject(i, LocalDate.class)`, `LocalDateTime.class` and `LocalTime.class` read the text's fields; text with an offset is refused as a local value. `Instant.class` and `OffsetDateTime.class` read text with an offset by it, and text without one as UTC. `getTimestamp` reads text without an offset as a local timestamp in the JVM's zone, or the calendar's, and text with one as the instant it names. Only the forms 0005 lists are read, with years 0001 to 9999.

A column's type name is its declared type without the arguments in parentheses, trimmed, in upper case, as in the Go driver: `datetime(6)` is `DATETIME`. In a column named `DATE`, `DATETIME` or `TIMESTAMP`, `getObject` returns TEXT in a date or timestamp form as `java.sql.Timestamp`, or as `java.sql.Date` for a date alone in a `DATE` column. `getColumnType` reports `DATE`, `TIMESTAMP` for the other two, and otherwise follows SQLite's affinity rules: `BIGINT` for a name containing `INT`; `VARCHAR` for `CHAR`, `CLOB` or `TEXT`; `BLOB` for `BLOB` or no type; `DOUBLE` for `REAL`, `FLOA` or `DOUB`; `NUMERIC` otherwise; `OTHER` for an expression. Nullability, precision and scale are reported as unknown.

Parameters: `setLong` and the other integer setters send INTEGER, `setBoolean` INTEGER 0 or 1, `setDouble` REAL (a NaN is refused), `setBigDecimal` TEXT in plain notation, `setString` TEXT, `setBytes` BLOB. `setObject` also takes `LocalDate`, `LocalTime` and `LocalDateTime`, written as their fields, `Instant`, `OffsetDateTime` and `ZonedDateTime`, written in UTC, and `java.sql.Date`, `Time` and `Timestamp`, written as their wall-clock fields in the JVM's zone or the given calendar's. A year outside 0001 to 9999, a NaN and a string with a lone surrogate are refused when they are set, before anything is sent.

## Errors

An error the server answered becomes an `SQLException` whose message holds 0005's error code, the server's message and the request ID. The class and SQLState follow the code's meaning. Codes for requests refused before acceptance, which never executed, map to `SQLTransientConnectionException`; no code whose outcome can be unknown does. The vendor code is 0, except for `sql_error`.

| Code | Exception | SQLState |
|---|---|---|
| `invalid_request` | `SQLSyntaxErrorException` | `42000` |
| `unauthenticated`, `forbidden` | `SQLInvalidAuthorizationSpecException` | `28000` |
| `statement_forbidden` | `SQLSyntaxErrorException` | `42501` |
| `database_not_found` | `SQLNonTransientConnectionException` | `08001` |
| `needs_channel` | `SQLFeatureNotSupportedException`: writes are not supported in this version | `0A000` |
| `overloaded`, `too_many_channels` | `SQLTransientConnectionException` | `08004` |
| `no_leader`, `not_serving`, `owner_unreachable`, `behind`, `maintenance` | `SQLTransientConnectionException` | `08001` |
| `request_too_large`, `result_too_large` | `SQLNonTransientException` | `54000` |
| `timeout` | `SQLTimeoutException` | `HYT00` |
| `cancelled` | `SQLNonTransientException` | `57014` |
| `sql_error` | by SQLite's primary code: 1 `SQLSyntaxErrorException` `42000`, 8 `SQLNonTransientException` `25006`, 18 `SQLDataException` `22001`, 19 `SQLIntegrityConstraintViolationException` `23000`, 20 `SQLDataException` `22000`, 25 `SQLDataException` `22023`, others `SQLException` `HY000`; the vendor code is SQLite's extended code | as listed |
| `database_exists` | `SQLNonTransientException` | `42710` |
| `busy` | `SQLTransactionRollbackException` | `40001` |
| `transaction_aborted` | `SQLTransactionRollbackException` with `cause: "maintenance"`, which 0005 makes retryable; otherwise `SQLNonTransientException` | `40001`; `25000` |
| `invalid_state` | `SQLNonTransientException` | `25000` |
| `channel_busy`, `in_flight`, `stale_sequence`, `sequence_gap`, `sequence_conflict` | `SQLNonTransientException` | `HY010` |
| `channel_lost` | `SQLNonTransientConnectionException` | `08003` |
| `commit_failed` | `SQLTransactionRollbackException` | `40000` |
| `commit_outcome_unknown` | `SQLNonTransientConnectionException` | `08007` |
| `internal`, or a code outside protocol version 1 | `SQLException` | `HY000` |

The rows from `busy` down answer channel requests, which this version never sends; they are mapped so that the table is complete. A response that breaks the protocol, ends before its terminal frame, lacks the `Skylite-Protocol: 1` header or is a redirect, and a transport failure, raise `SQLNonTransientConnectionException` with SQLState `08S01`. Nothing is retried.

## Not yet

- Writes and transactions. `setAutoCommit(false)`, `setReadOnly(false)`, savepoints, batches and generated keys raise `SQLFeatureNotSupportedException`; a write statement reaches the server, which refuses it with `needs_channel`. They need the server's write channels, replay and `resolve`.
- The Hibernate dialect, and a run of the Spring Data JPA acceptance suite in `clients/java/acceptance/` against skylite.
- Catalog metadata (`DatabaseMetaData.getTables` and the like), streams and LOB parameters, and JDBC escape syntax, which is sent unprocessed.
- Retries. A read refused before acceptance, such as `overloaded` or `not_serving`, is raised to the caller.
- The server's statement deadline ends a running query by resetting the HTTP/2 stream rather than with its `timeout` frame, so a query the server stops reports `08S01` rather than `SQLTimeoutException`; the driver's own timeout, 2 seconds later, is never reached.
