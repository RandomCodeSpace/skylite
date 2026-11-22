---
title: JDBC driver
description: The JDBC driver and its Hibernate dialect, how to configure them, how writes and transactions reach the server, and what they do not do yet.
weight: 21
status: current
---

`clients/java/driver` is a JDBC driver, artifact `io.github.randomcodespace:skylite-jdbc`, that speaks the protocol of [0005: Client protocol](/docs/decisions/0005-client-protocol/) over HTTPS, as [0003: JDBC transport](/docs/decisions/0003-jdbc-transport/) decided. It needs Java 21 or later. It reads through the stateless `query` endpoint and writes through a write channel per connection, with autocommit writes and transactions. `clients/java/hibernate` is a Hibernate ORM dialect for it, artifact `io.github.randomcodespace:skylite-hibernate` ([below](#hibernate-dialect)). Neither is published to a Maven repository; build them with `mvn -f clients/java/driver/pom.xml verify` and `mvn -f clients/java/hibernate/pom.xml verify`.

The driver's tests run against an in-process fake server that checks every request against 0005, and against a real `skylite serve` built from this repository. The [Spring Data JPA acceptance suite](#spring-data-jpa) runs through the driver, the dialect, HikariCP and Hibernate against a real `skylite serve` as well.

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

The URL is `jdbc:skylite:https://host[:port][/base]/<database>`: the server's base URL, then the database name as the last path segment. The name is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit. User information, a query, a fragment, an empty host and `.` or `..` path segments are refused, as by the [Go driver](/docs/go-driver/), and so is a port outside 1 to 65535; the error does not repeat the URL.

| Property | Meaning |
|---|---|
| `tokenProvider` | The name of a class that implements `TokenProvider` and has a public no-argument constructor, or a `TokenProvider` instance put directly into the `Properties` object passed to the driver: an instance in its defaults is not seen, though a class name is. Each connection created from a class name gets its own instance. The driver calls `token()` before every HTTP request. |
| `tokenFile` | A file that holds the token. The driver reads it again before every request and strips white space around it, so a rotated projected token is picked up at once. |
| `tlsRootCertificates` | A PEM file of the certificates to trust, such as a private CA, instead of the JVM's trust store. The file is read when a connection is made. |
| `allowPlaintext` | `true` permits an `http://` URL. Without it only `https://` is accepted. |

Exactly one of `tokenProvider` and `tokenFile` is required. Each property is read from the `Properties` object or, failing that, its defaults. Connection properties are trusted configuration: whoever sets them chooses the class the driver instantiates and the file whose contents it sends, as a bearer token, to the URL's host, so never build them from untrusted input. `user` and `password` are ignored. No secret goes in the URL. The driver never logs, and its own exceptions never hold the token or the request URL, though an exception from the network or TLS can name the host and port it could not reach. When the token source fails, the exception names only the type of the provider's or file read's exception, without its message or cause, which could hold a token. A token that is empty or holds anything but printable ASCII without spaces is refused before anything is sent; the driver does not check the finer bearer-token grammar of RFC 6750.

The driver uses HTTP/2 when the server offers it, and refuses every redirect, so a request and its token go only to the URL's host. It waits up to 60 seconds for the headers of each response, or longer for a query timeout above that. `Connection.setNetworkTimeout` replaces that wait, whatever the query timeout, and also bounds each wait for more of a response's body, as a socket read timeout would; when it passes, the driver closes the connection, its statements and their result sets, and raises `SQLNonTransientConnectionException` with SQLState `08S01`, or `08007` for a channel request the server may have accepted, as [Unknown outcomes](#what-works) says. A query timeout leaves the connection open. HTTP clients are shared by connections with the same trust settings.

## What works

- **Queries.** In auto-commit, `Statement` and `PreparedStatement` send each `executeQuery` and `execute` to the stateless `query` endpoint, with the SQL and its parameters. Rows are decoded as their frames arrive, never buffered whole; `ResultSet` is forward-only and read-only. Closing a result set, its statement or its connection before the end releases the HTTP response. A response that ends before its terminal frame is an error, never a short result. A statement the server answers `needs_channel`, because it is not a pure read, was refused before it ran, and goes to the write channel instead.
- **Writes.** A connection opens one write channel on its first write and closes it when it closes. In auto-commit, `executeUpdate`, and a statement refused with `needs_channel`, is an `execute` on the channel: an autocommit write, committed or failed as a whole before its answer arrives. Every channel request carries the channel's next sequence number, and its answer, rows included, is read in full before the caller sees any of it. When the answer is lost in transport, the driver sends the same bytes again with the same sequence number, at most twice more: the server answers with the recorded outcome, or `in_flight` while the request still runs, after which the driver waits 200 ms and sends it again, for up to 60 seconds plus the query timeout. Answers lost in transport are counted apart from `in_flight` answers. That is not a retry: a request the server refused or failed is never sent again, a refusal before acceptance of a request's first send leaves its sequence number free for the next request, and a request that may have been accepted is never sent on another channel. A channel with no transaction open and no answer for 60 seconds is closed and replaced before its next request, so that the server's channel idle limit (10 minutes) rarely ends a channel the driver still uses. That is the 60-second part of 0005's reopening rule; its open-head part, which replaces a channel opened more than half the resolve bound below the pool's highest transaction, is not implemented.
- **Unknown outcomes.** When a sent request's outcome cannot be learnt, the driver raises `SQLNonTransientConnectionException` with SQLState `08007` and never reports success or a rollback. That covers answers that stay lost after the third send, `channel_lost` with either flag, since 0005 makes it no proof that nothing ran, `commit_outcome_unknown`, `sequence_conflict`, a final failure with `not_executed: false` of anything but a statement inside a transaction (so a commit's), a refusal before acceptance that answers a resend, since an earlier send may have been accepted, and an interrupt or a closed connection while the driver waits to send a request again. It then gives up the channel, closing it without waiting. After an autocommit write or a commit, it closes the connection too, as 0005 requires: `isClosed()` returns `true`, `isValid` returns `false`, its statements and result sets close, so an open cursor delivers no more rows, a statement waiting to be sent, on the channel or in the token provider, is never sent, and every later use of the connection, `rollback()` included, raises `08003`. Inside a transaction, after a statement or a rollback, the transaction is lost instead: its later statements, its `commit()` and `setAutoCommit(true)` raise `SQLException` with SQLState `SKR03` and vendor code 0 and send nothing, and `rollback()` succeeds locally, since a transaction whose commit is never sent cannot commit. The next statement after that opens a new channel. The server does not serve `resolve` yet, so nothing settles such a request.
- **Transactions.** With `setAutoCommit(false)`, the first statement is sent as a `begin` with that statement, so a transaction starts with its first statement and an empty one sends nothing, and every later statement, reads included, runs in it on the channel until `commit()` or `rollback()`. Turning auto-commit back on commits, as JDBC requires; closing the connection closes the channel, which rolls back an open transaction, waiting up to 2 seconds for the answer. A transaction holds the database's write lock from its first statement until it ends, so other writers wait for it ([Server: Transactions](/docs/server/#transactions)). A failed statement inside a transaction leaves it open unless SQLite rolled it back; then, and after a transaction limit or maintenance ended it, every request but `rollback` fails with `transaction_aborted`. A transaction whose channel ended is lost, as [Unknown outcomes](#what-works) says. `setAutoCommit`, `commit`, `rollback` and `setReadOnly` wait for a statement in flight on another thread, so that they see the transaction it begins. Isolation is always serializable: a weaker level requested is accepted and served at serializable, as JDBC allows, and `TRANSACTION_NONE` is refused.
- **Read-only connections.** After `setReadOnly(true)`, every statement is a stateless read, in a transaction too, where each reads the latest committed state rather than one snapshot; one that is not a pure read fails with SQLState `25006`. `setReadOnly` is refused while a transaction is open. A statement that chose the write channel before `setReadOnly(true)` returned, and was not sent yet, runs as a stateless read.
- **Generated keys.** With `RETURN_GENERATED_KEYS`, or column names or indexes, `getGeneratedKeys()` returns one row holding the insert's `last_insert_rowid` from the same answer, when the statement starts with `INSERT` or `REPLACE` and changed a row; otherwise no row. After a prepared statement's batch, it returns one such row for each entry. Its one column is labelled with the first column name asked for, or `last_insert_rowid()`. The rowid is the only key returned, and two limits follow from it: an insert of several rows returns only the last row's rowid, and an insert into a `WITHOUT ROWID` table returns the rowid of an earlier insert in the same transaction, or 0. Each `getGeneratedKeys()` closes the result set the previous one returned, and executing the statement again or closing it closes the last one. Column indexes must be 1 or more. `DatabaseMetaData.supportsGetGeneratedKeys()` is `true`.
- **Batches.** The server does not serve `batch` yet, so `executeBatch` runs the batch's statements one after another, each a request of its own, inside the transaction when auto-commit is off. The first failure stops the batch with a `BatchUpdateException` holding the counts of the statements before it, with the failure as its cause and its next exception; `getGeneratedKeys()` then returns the keys of the statements before it. `supportsBatchUpdates()` is `true`, so that Hibernate batches with one prepared statement.
- **Prepared statements.** Preparing sends nothing. `getParameterMetaData()` and, before an execution, `getMetaData()` send one `describe`, whose answer the statement keeps. Parameters bind by index: `?`, `?NNN` and named parameters such as `:t` alike. Each execution sends its SQL again; there are no server-side handles.
- **Metadata.** `ResultSetMetaData` from the result columns; `ParameterMetaData` with the parameter count; `DatabaseMetaData` with the product name `SQLite`, the server's `sqlite_version()` as the product version, the driver version, the capabilities of this version, and `getTables` and `getColumns` over the main database's tables and views, from `sqlite_schema` and `pragma_table_info`, as stateless reads. `getColumns` reports `DATA_TYPE` as `getColumnType` would for the declared type, `TYPE_NAME` as the declared type without its arguments, and the arguments as `COLUMN_SIZE` and `DECIMAL_DIGITS`. Catalog and schema arguments are ignored.
- **Validity.** `Connection.isValid` describes `SELECT 1`, as the Go driver's ping does. One deadline, the shorter of *n* seconds and the network timeout, covers reading the token and the whole answer: `isValid(n)` returns `false` when it passes, and when the network timeout was the shorter, the connection closes too. A token provider that is still running is not interrupted, so a slow one can make `isValid` return `false` late. Closing or aborting the connection ends a running `isValid` with `false`.
- **Cancel and timeout of reads.** `Statement.cancel()` abandons the stateless reads the statement runs, an execution, a `describe` and an open result set, before or after its answer arrives: a stateless read needs no cancel request, because the server stops streaming when the response is dropped. Rows already received are not delivered after it. The exception has SQLState `57014`. A `close()` that returns before a request is sent stops it from being sent; one that comes later releases it. `setQueryTimeout(n)` sends `timeout_ms` so that the server's statement time is at most *n* seconds, and abandons a response that has not ended 2 seconds after that, the driver's cancel wait in 0005. Either way the driver raises `SQLTimeoutException`, which is safe for a read: it leaves no unknown outcome.
- **Cancel and timeout on the channel.** A channel request's answer is its outcome, so the driver never abandons it. `Statement.cancel()` sends the server a `cancel` for the request the statement runs, waiting up to 2 seconds for its whole answer, and the request's own answer then says what happened: `cancelled` (`57014`) when the server stopped it before its commit point, or its result when it had finished. A cancel before the request is sent stops it from being sent. `setQueryTimeout(n)` sends `timeout_ms`, and the server ends a statement that reaches it before its commit point with `timeout`, which raises `SQLTimeoutException`; a statement past its commit point finishes, and its result is returned although the timeout passed, as 0005 decides. The driver sends no `cancel` of its own at the timeout, because the server ends the statement at the same time. Closing the statement or the connection while a channel request runs makes its outcome unknown (`08007`). Closing the connection then sends the channel's `close`, which rolls back an open transaction, and waits up to 2 seconds for its whole answer; a commit or rollback in flight on another thread settles first. `abort` stops that commit or rollback as well, and sends the close from its executor. When the network timeout closes a connection, its channel's close runs on another thread, never on the driver's timer.
- **Results the server dropped.** When a channel request's answer was lost and its replay comes without the rows, which the server keeps only up to 64 KiB (0005 decision 3), a read-only statement is sent again as a new request, once: when that answer comes without its rows too, the driver raises `SQLNonTransientConnectionException` with SQLState `08S01`, since the read changed nothing. A write is not sent again: an autocommit write raises `SQLException` with SQLState `SKR01`, vendor code 0 and its transaction number in the message, since it committed, and a write inside a transaction raises `SQLException` with SQLState `SKR02`, the transaction still open with its effects.
- **Read-your-writes.** One watermark per database, keyed by the canonical base URL and the database name and kept for the driver class's life, is raised from each answer's `txn` and sent as `min_txn`, so connections and URLs that differ only in canonicalisation share it.

## Values

| Storage class | `getObject` | Also readable as |
|---|---|---|
| NULL | `null` | 0, `false` or `null` from the typed getters, with `wasNull()` true |
| INTEGER | `Long` | `getInt`, `getShort` and `getByte` within their range, otherwise SQLState `22003`; `getDouble`, `getBigDecimal`, `getObject(i, BigInteger.class)`, `getString`; `getBoolean` for 0 and 1 only |
| REAL | `Double`, infinities included | `getLong` for an integral value within range; `getBigDecimal` for a finite value; `getObject(i, BigInteger.class)` for a finite integral value, exactly; `getString` |
| TEXT | `String` | `getBytes` (its UTF-8 bytes); `getLong`, `getDouble` and `getBigDecimal` when the text is a number; `getObject(i, Character.class)` for one character; the temporal getters below |
| TEXT that is not valid UTF-8 | an error | `getBytes` returns its own bytes; `getString` raises SQLState `22021` |
| BLOB | `byte[]` | `getBytes`, `getBlob` |

Temporal values are TEXT ([0005 decision 8](/docs/decisions/0005-client-protocol/#8-values-and-shared-conventions)). `getObject(i, LocalDate.class)`, `LocalDateTime.class` and `LocalTime.class` read the text's fields; text with an offset is refused as a local value. `Instant.class`, `OffsetDateTime.class` and `ZonedDateTime.class` read text with an offset by it, and text without one as UTC; the last two are in UTC. `getTimestamp` reads text without an offset as a local timestamp in the JVM's zone, or the calendar's, and text with one as the instant it names. Only the forms 0005 lists are read, with years 0001 to 9999.

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
| `needs_channel`, on a read-only connection | `SQLNonTransientException` | `25006` |
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
| `channel_lost` | `SQLNonTransientConnectionException` | `08003`, or `08007` on the write channel |
| `commit_failed` | `SQLTransactionRollbackException` | `40000` |
| `commit_outcome_unknown` | `SQLNonTransientConnectionException` | `08007` |
| `internal`, or a code outside protocol version 1 | `SQLException` | `HY000` |

The rows from `busy` down answer channel requests. A recorded failure is the request's outcome and is raised as the table says; an answer that leaves a sent request's outcome unknown raises `08007` instead, as [Unknown outcomes](#what-works) says, whatever its code. A stateless response that breaks the protocol, ends before its terminal frame, lacks the `Skylite-Protocol: 1` header or is a redirect, and a transport failure, raise `SQLNonTransientConnectionException` with SQLState `08S01`; a read is never sent again.

## Hibernate dialect

`io.github.randomcodespace.skylite.hibernate.SkyliteDialect` extends Hibernate's community `SQLiteDialect` (`hibernate-community-dialects`, 7.4.13.Final, the version Spring Boot 4.1.2 manages) with two changes:

- Generated identifiers come from `getGeneratedKeys`, which the driver answers from the insert's own answer, never from a separate `select last_insert_rowid()`, which skylite would run on another SQLite connection.
- Foreign keys are declared inside `create table`, with their `on delete` action, because SQLite has no `alter table ... add constraint` and the community dialect therefore emits none; skylite's SQLite connections enforce them. Dropping a table first drops the tables that reference it, so that the implicit delete of `drop table` never fails on their rows.

Set it with `spring.jpa.database-platform` or `hibernate.dialect`. The module is Apache-2.0 and depends on Hibernate ORM, which is LGPL-2.1-or-later; it copies nothing from Hibernate, and the driver depends on neither.

## Spring Data JPA

`make java-acceptance-skylite` runs the 29-test acceptance suite in `clients/java/acceptance/` through the driver, the dialect, HikariCP and Hibernate against a `skylite serve` that the test run builds from this tree and starts on the local file system, with HTTPS and JWT authentication; the CI job `java-acceptance-skylite` runs it on every pull request. 28 tests pass. `TransactionTest.rollbackToSavepointKeepsEarlierWrites` is skipped, with its reason in the log, because the server refuses `SAVEPOINT` on a channel and the driver therefore supports no savepoints. The suite's README says how it is run.

## Not yet

- Savepoints: `setSavepoint` and `rollback(Savepoint)` raise `SQLFeatureNotSupportedException`, because the server refuses `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` on a channel as transaction control.
- `resolve`, which the server does not serve: an unknown outcome stays unknown. Server-side `batch`, read-only channels and snapshot reads in a read-only transaction.
- Catalog metadata other than `getTables` and `getColumns`, streams and LOB parameters, and JDBC escape syntax, which is sent unprocessed.
- Retries. A request that did not run, refused with `overloaded` or `not_serving` or failed with `busy`, is raised to the caller.
