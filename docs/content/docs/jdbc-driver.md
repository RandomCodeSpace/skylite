---
title: JDBC driver
description: Connect a Java application to skylite with JDBC, Hibernate or Spring Data JPA. Build and install, connection URL, tokens, Spring Boot settings, transactions, and errors and retries.
weight: 50
navgroup: guides
status: current
---

The JDBC driver (`io.github.randomcodespace:skylite-jdbc`) talks to the server over HTTPS and needs Java 21 or later. A Hibernate dialect (`io.github.randomcodespace:skylite-hibernate`) comes with it. Both are tested against a real `skylite serve` on every pull request, including a 33-test Spring Data JPA acceptance suite through HikariCP and Hibernate.

## Build and install

The driver and the dialect are not published to any Maven repository yet. Build them from a clone and install them into your local Maven repository, or into your organisation's repository manager:

```sh
git clone https://github.com/RandomCodeSpace/skylite.git
cd skylite
mvn -q -f clients/java/driver/pom.xml install -DskipTests
mvn -q -f clients/java/hibernate/pom.xml install -DskipTests
```

Leave out `-DskipTests` to run the tests as well; the driver's integration tests build the server, so they need Go.

Both have version `0.0.0-SNAPSHOT`:

```xml
<dependency>
  <groupId>io.github.randomcodespace</groupId>
  <artifactId>skylite-jdbc</artifactId>
  <version>0.0.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.github.randomcodespace</groupId>
  <artifactId>skylite-hibernate</artifactId>
  <version>0.0.0-SNAPSHOT</version>
</dependency>
```

The driver's only runtime dependency is `jackson-core` 3. The dialect depends on Hibernate ORM's community dialects, which are LGPL-2.1-or-later; the driver does not.

## Connect

The URL is `jdbc:skylite:https://host[:port][/base]/<database>`. The driver registers itself with `DriverManager`; its class is `io.github.randomcodespace.skylite.jdbc.SkyliteDriver`.

```java
Properties p = new Properties();
p.setProperty("tokenFile", "/var/run/secrets/tokens/skylite");
try (Connection c = DriverManager.getConnection("jdbc:skylite:https://skylite.example.internal/orders", p)) {
    // ...
}
```

| Property | Meaning |
|---|---|
| `tokenFile` | A file holding the bearer token, read again before every request, so a rotated token is picked up at once. |
| `tokenProvider` | Instead of `tokenFile`: the name of a class implementing `io.github.randomcodespace.skylite.jdbc.TokenProvider`, whose `token()` is called before every request. |
| `tlsRootCertificates` | A PEM file of CA certificates to trust instead of the JVM's trust store, for a private CA. |
| `allowPlaintext` | `true` permits an `http://` URL, for a local trial or a server behind a mesh that encrypts traffic. |

Exactly one of `tokenFile` and `tokenProvider` is required. `user` and `password` are ignored. The token must come from an issuer in the server's [`auth` configuration](/docs/configuration/#auth), with a rule granting `read` for queries and `write` for writes and transactions. Connection properties are trusted configuration: never build them from user input. The driver never logs, and its exceptions never contain the token or the URL.

## Spring Boot and Spring Data JPA

```properties
spring.datasource.url=jdbc:skylite:https://skylite.example.internal/orders
spring.datasource.driver-class-name=io.github.randomcodespace.skylite.jdbc.SkyliteDriver
spring.datasource.hikari.data-source-properties.tokenFile=/var/run/secrets/tokens/skylite
spring.jpa.database-platform=io.github.randomcodespace.skylite.hibernate.SkyliteDialect
spring.jpa.open-in-view=false
```

HikariCP passes `data-source-properties` to the driver, which is how the acceptance suite sets `tokenFile` and `tlsRootCertificates`. The suite runs Spring Boot 4.1.2 with Hibernate 7.4.13.Final, HikariCP with at most 4 connections, `ddl-auto=create`, and Hibernate batching (`hibernate.jdbc.batch_size=50`, `order_inserts=true`); other versions have not been tested.

The dialect extends Hibernate's community `SQLiteDialect` in three ways: generated identifiers come from `getGeneratedKeys` on the insert's own answer, foreign keys are declared inside `create table`, where skylite enforces them, and JDBC batching is on by default, 50 statements a batch, since the driver sends each batch as one request.

Each pooled connection holds one write channel on the server, and one read-only channel once it has run a read-only transaction. Writes to one database run one at a time, so a large pool does not speed up writes. Size the pool for your read concurrency.

`@Transactional(readOnly = true)` runs on a read-only channel: Spring sets the connection read-only before the transaction's first statement, and every statement of the transaction then reads one snapshot, without the write lock.

## Queries, writes and transactions

- In auto-commit mode, a query is a stateless read, which any server in a deployment can answer, and an update is an autocommit write of one statement on the leader.
- With `setAutoCommit(false)`, the transaction starts with its first statement and runs on the leader. It holds the database's write lock until `commit()` or `rollback()`, so other writers wait. Keep transactions short: the server aborts one that is idle for 30 seconds or older than 5 minutes. Savepoints work.
- After `setReadOnly(true)`, a statement in auto-commit is a stateless read, and a transaction runs on a read-only channel, from one snapshot, without the write lock. A write fails with SQLState `25006`.
- `executeBatch` sends the whole batch as one request. In auto-commit it is one transaction: all of it commits or none of it. Inside a transaction it stops at its first failure, and the entries before it keep their effect. Hibernate's JDBC batching (`hibernate.jdbc.batch_size`) uses it.
- `Statement.setQueryTimeout` and `Statement.cancel()` work for reads and writes.

The driver remembers the newest transaction each database has answered and sends it with later requests, so a read after your own write, or after any read the pool has seen, never sees an older state, even when a different server answers it.

## Errors and retries

Server errors become `SQLException`s whose class and SQLState follow the error code, and whose message holds the code, the server's message and the request id. The full table is under [Errors](#errors).

| What you see | Meaning | What to do |
|---|---|---|
| `SQLTransientConnectionException`, SQLState `08004` (`overloaded`, `too_many_channels`) | The server stayed at capacity for the whole query timeout, or 30 seconds without one, while the driver retried. Nothing ran. | Retry later, or shed load. |
| `SQLTransientConnectionException`, SQLState `08001` (`no_leader`, `not_serving`, `owner_unreachable`, `behind`, `maintenance`) | No server could serve the request within that time, as during a long failover. Nothing ran. | Retry later. |
| `SQLTransactionRollbackException`, SQLState `40001` (`busy`, or `transaction_aborted` caused by maintenance) | The write lock was not granted in time, after the driver's retries, or maintenance rolled the transaction back. | Retry the whole transaction. |
| `SQLTransactionRollbackException`, SQLState `40000` (`commit_failed`, or a lost answer settled as not committed) | The commit or write definitely did not happen. | Retry the whole transaction or write. |
| `SQLException`, SQLState `SKR01` | The write committed, but its answer was lost and its result (update count, generated key, rows) is not available. | Do not repeat it; read the data if you need the result. |
| `SQLNonTransientConnectionException`, SQLState `08007` | The outcome of a write or commit is unknown: it may have committed, and the server could not settle it in time. The connection is closed. | Check the data before repeating the write. |
| `SQLIntegrityConstraintViolationException`, SQLState `23000` | A constraint failed. | Do not retry. |

The driver retries for you what the server proves did not run, so these exceptions mean that its retries ran out:

- A request the server refused before running it (`overloaded`, `too_many_channels`, `no_leader`, `not_serving`, `owner_unreachable`, `behind`, `maintenance`) is sent again, and so is a write outside a transaction, or a transaction's first statement, that failed without running with `busy`, `overloaded` or `maintenance`. The pauses grow from 5–10 ms to at most 0.5–1 s, each drawn at random, and stop at the statement's query timeout, or after 30 seconds without one. A statement inside a transaction is never sent again, and nor is a request whose answer says it may have run.
- A write or commit whose answer was lost, by a dropped connection, a failover or a pod that restarted, is settled before you see an error: the driver asks the server whether it committed. A commit that committed returns normally; a write that committed raises `SKR01`; one that did not commit is sent once more on a new channel, since it had no effect; a commit that did not commit raises `40000`. Only when the server cannot tell within 30 seconds does the driver raise `08007`. A commit waits at most the server's statement time, 30 seconds by default, or the network timeout, for its answer before it is settled this way.

[Running in production](/docs/operations/#errors-to-handle) explains each error code.

## Reference

The rest of this page is the driver's exact behaviour.

### Configuration

The URL is `jdbc:skylite:https://host[:port][/base]/<database>`: the server's base URL, then the database name as the last path segment. The name is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit. User information, a query, a fragment, an empty host and `.` or `..` path segments are refused, as by the [Go driver](/docs/go-driver/), and so is a port outside 1 to 65535; the error does not repeat the URL.

| Property | Meaning |
|---|---|
| `tokenProvider` | The name of a class that implements `TokenProvider` and has a public no-argument constructor, or a `TokenProvider` instance put directly into the `Properties` object passed to the driver: an instance in its defaults is not seen, though a class name is. Each connection created from a class name gets its own instance. The driver calls `token()` before every HTTP request. |
| `tokenFile` | A file that holds the token. The driver reads it again before every request and strips white space around it, so a rotated projected token is picked up at once. |
| `tlsRootCertificates` | A PEM file of the certificates to trust, such as a private CA, instead of the JVM's trust store. The file is read when a connection is made. |
| `allowPlaintext` | `true` permits an `http://` URL. Without it only `https://` is accepted. |

Exactly one of `tokenProvider` and `tokenFile` is required. Each property is read from the `Properties` object or, failing that, its defaults. Connection properties are trusted configuration: whoever sets them chooses the class the driver instantiates and the file whose contents it sends, as a bearer token, to the URL's host, so never build them from untrusted input. `user` and `password` are ignored. No secret goes in the URL. The driver never logs, and its own exceptions never hold the token or the request URL, though an exception from the network or TLS can name the host and port it could not reach. When the token source fails, the exception names only the type of the provider's or file read's exception, without its message or cause, which could hold a token. A token that is empty or holds anything but printable ASCII without spaces is refused before anything is sent; the driver does not check the finer bearer-token grammar of RFC 6750.

The driver uses HTTP/2 when the server offers it, and refuses every redirect, so a request and its token go only to the URL's host. It waits up to 60 seconds for the headers of a stateless response, or longer for a query timeout above that, and for a channel request, a commit included, up to its query timeout, or else the server's statement time (`limits.statement_ms`, 30 seconds by default), plus 2 seconds. `Connection.setNetworkTimeout` replaces that wait, whatever the query timeout, and also bounds each wait for more of a response's body, as a socket read timeout would; when it passes, the driver closes the connection, its statements and their result sets, and raises `SQLNonTransientConnectionException` with SQLState `08S01`, or `08007` for a channel request the server may have accepted, as [Unknown outcomes](#what-works) says. A query timeout leaves the connection open. HTTP clients are shared by connections with the same trust settings.

### What works

- **Queries.** In auto-commit, `Statement` and `PreparedStatement` send each `executeQuery` and `execute` to the stateless `query` endpoint, with the SQL and its parameters. Rows are decoded as their frames arrive, never buffered whole; `ResultSet` is forward-only and read-only. Closing a result set, its statement or its connection before the end releases the HTTP response. A response that ends before its terminal frame is an error, never a short result. A statement the server answers `needs_channel`, because it is not a pure read, was refused before it ran, and goes to the write channel instead.
- **Writes.** A connection opens one write channel on its first write and closes it when it closes. In auto-commit, `executeUpdate`, and a statement refused with `needs_channel`, is an `execute` on the channel: an autocommit write, committed or failed as a whole before its answer arrives. Every channel request carries the channel's next sequence number, and its answer, rows included, is read in full before the caller sees any of it. When the answer is lost in transport, or a forwarding pod answers `owner_unreachable` with `not_executed: false` because the request may have reached the channel's owner, the driver sends the same bytes again with the same sequence number, at most twice more: the server answers with the recorded outcome, or `in_flight` while the request still runs, after which the driver waits 200 ms and sends it again, until the request's deadline: its query timeout, or else the server's statement time, plus 2 seconds. Answers lost in transport are counted apart from `in_flight` answers. A channel with no transaction open is closed and replaced before its next request when it has had no answer for 60 seconds, so that the server's channel idle limit (10 minutes) rarely ends a channel the driver still uses, or when its open head is more than half the server's resolve bound (`limits.resolve_bound`) below the watermark, so that a resolve can still reach back to it.
- **Retries.** A request is sent again only when an answer proves that it did not run: a refusal before acceptance (`not_executed: true`, `final: false`) with `overloaded`, `too_many_channels`, `no_leader`, `not_serving`, `owner_unreachable`, `behind` or `maintenance`, to a stateless query or describe, a channel open, or a channel request, which is sent again with the same bytes and sequence number; and a recorded failure with `not_executed: true` and `busy`, `overloaded` or `maintenance` of an autocommit write or batch or of a transaction's first statement, which is sent again as the channel's next sequence number. `behind` on a channel closes it, which settles the request as not executed, and the request goes on a new channel; inside a transaction, where closing the channel rolls the transaction back, the transaction is lost instead, as [Unknown outcomes](#what-works) says, and nothing is sent on a new channel. Each pause is drawn at random from the upper half of a bound that starts at 10 ms and doubles to 1 s, and no pause passes the request's deadline: for a stateless query or describe, its query timeout, or 30 seconds without one; for a channel open or request, its query timeout, or else the smaller of 30 seconds and the server's statement time, plus 2 seconds. A stateless query is sent again with what is left of its query timeout and with the watermark read again. When the deadline comes, the last answer is raised as the table under [Errors](#errors) maps it. A failure with `not_executed: false`, but for the resends of `owner_unreachable` under Writes, any failure of a statement inside a transaction, and any other code are never sent again.
- **Unknown outcomes and resolve.** When a sent autocommit write, batch or commit has no answer that settles it, the driver settles it as below before it reports anything. That covers answers that stay lost after the third send or past the deadline, `owner_unreachable` with `not_executed: false` included, `channel_lost` with either flag, since it does not prove that nothing ran, `commit_outcome_unknown`, a final failure with `not_executed: false` of anything but a statement inside a transaction, a refusal of a resend that may not pass later, and the connection's network timeout. On a channel that may still be live it first sends the channel's `close`, waiting up to 2 seconds: a `last_seq` below the request proves it never ran, and one equal to it carries its receipt. Otherwise, or when the close is answered `channel_lost` or not at all, it sends `resolve` with the request's own token and sequence number, again after `pending`, `no_leader`, `behind`, `overloaded`, `not_serving`, `owner_unreachable`, `maintenance` or a transport failure, with the pauses above, for up to 30 seconds, or less when the statement's query timeout ends sooner, but never less than 2 seconds. Then:

  | Settled as | Commit | Autocommit write or batch |
  |---|---|---|
  | Committed (`committed`, or the close's successful receipt) | Returns normally | `SQLException`, SQLState `SKR01`, vendor code 0: committed, the result unavailable, the transaction number in the message; but `executeUpdate` settled by the close's receipt returns the receipt's count, and its generated key |
  | Not committed (`not_committed`, or `last_seq` below the request) | `SQLTransactionRollbackException`, SQLState `40000`: rolled back | Sent once more, on a new channel, as a new request; if that is settled as not committed too, `SQLTransactionRollbackException`, SQLState `40000` |
  | A recorded failure in the close's receipt | Raised as the table under [Errors](#errors) maps it | The same |
  | `unknown`, `pending` to the end, any other answer, or a connection its user closed meanwhile | `SQLNonTransientConnectionException`, SQLState `08007`, vendor code 0 | The same |

  After an `08007`, the driver closes the connection: `isClosed()` returns `true`, `isValid` returns `false`, its statements and result sets close, so an open cursor delivers no more rows, a statement waiting to be sent, on the channel or in the token provider, is never sent, and every later use of the connection, `rollback()` and the other calls a pool makes to reset it included, raises `08003`. After a write or a commit settled as committed or not committed, the connection stays open with its settings, auto-commit and read-only included, and its next request opens a new channel; a pool such as HikariCP resets and lends it again as usual. `sequence_conflict` raises `08007` without a resolve, which would answer about the other body that holds the sequence number. Inside a transaction, after a statement, a `begin` or a rollback whose outcome stays unknown, nothing is resolved, since only a commit's sequence number reaches the database: the statement raises `08007`, and the transaction is lost instead: its later statements, its `commit()` and `setAutoCommit(true)` raise `SQLException` with SQLState `SKR03` and vendor code 0 and send nothing, and `rollback()` succeeds locally, since a transaction whose commit is never sent cannot commit. The next statement after that opens a new channel.
- **Transactions.** With `setAutoCommit(false)`, the first statement is sent as a `begin` with that statement, so a transaction starts with its first statement and an empty one sends nothing, and every later statement, reads included, runs in it on the channel until `commit()` or `rollback()`. Turning auto-commit back on commits, as JDBC requires; closing the connection closes the channel, which rolls back an open transaction, waiting up to 2 seconds for the answer. A transaction holds the database's write lock from its first statement until it ends, so other writers wait for it ([Server: Transactions](/docs/server/#transactions)). A failed statement inside a transaction leaves it open unless SQLite rolled it back; then, and after a transaction limit or maintenance ended it, every request but `rollback` fails with `transaction_aborted`. A transaction whose channel ended is lost, as [Unknown outcomes](#what-works) says. `setAutoCommit`, `commit`, `rollback` and `setReadOnly` wait for a statement in flight on another thread, so that they see the transaction it begins. Isolation is always serializable: a weaker level requested is accepted and served at serializable, as JDBC allows, and `TRANSACTION_NONE` is refused.
- **Savepoints.** With auto-commit off, `setSavepoint()` and `setSavepoint(name)` send `SAVEPOINT`, `rollback(Savepoint)` sends `ROLLBACK TO SAVEPOINT` and `releaseSavepoint` sends `RELEASE SAVEPOINT`, each as a statement of the transaction, with the outcomes, unknown ones included, of any other statement in it; a savepoint set with no transaction open begins one. The name is sent as a quoted identifier with its double quotes doubled; an unnamed savepoint is sent as `skylite_savepoint_<id>`, where its id counts from 1 on each connection. `rollback(Savepoint)` keeps the savepoint and releases those set after it; `releaseSavepoint` releases it and those set after it. A savepoint of another connection, one released or rolled back past, one whose transaction ended, or one shadowed by a later savepoint sent under the same name (which SQLite would act on instead) raises `SQLNonTransientException` with SQLState `3B001` and sends nothing. In auto-commit, and on a read-only connection, which runs no transaction, the four methods raise SQLState `25000`. `DatabaseMetaData.supportsSavepoints()` is `true`.
- **Read-only connections.** After `setReadOnly(true)`, every statement in auto-commit is a stateless read; one that is not a pure read fails with SQLState `25006`. With auto-commit off, the transaction runs on the connection's read-only channel (a token starting `r1.`), opened on its first use with the watermark as `min_txn`: the first statement is sent as a `begin`, which takes the snapshot, and every statement after it reads that snapshot until `commit()` or `rollback()`. It holds no write lock; a write in it fails with `sql_error` `SQLITE_READONLY`, SQLState `25006`. The server bounds read-only transactions (half the pod's SQLite connections, and a principal's quota less one, 30 seconds idle, 120 seconds in all) and refuses a `begin` at a bound with `overloaded`, which the driver retries as above. A read-only commit whose answer is lost succeeds, since it committed nothing either way, and is not resolved. `setReadOnly` is refused while a transaction is open. A statement that chose the write channel before `setReadOnly(true)` returned, and was not sent yet, runs on the read-only channel, or as a stateless read in auto-commit. Savepoints are refused on a read-only connection.
- **Generated keys.** With `RETURN_GENERATED_KEYS`, or column names or indexes, `getGeneratedKeys()` returns one row holding the insert's `last_insert_rowid` from the same answer, when the statement starts with `INSERT` or `REPLACE` and changed a row; otherwise no row. After a prepared statement's batch, it returns one such row for each entry that ran and changed a row, from the batch's `results`. Its one column is labelled with the first column name asked for, or `last_insert_rowid()`. The rowid is the only key returned, and two limits follow from it: an insert of several rows returns only the last row's rowid, and an insert into a `WITHOUT ROWID` table returns the rowid of an earlier insert in the same transaction, or 0. Each `getGeneratedKeys()` closes the result set the previous one returned, and executing the statement again or closing it closes the last one. Column indexes must be 1 or more. `DatabaseMetaData.supportsGetGeneratedKeys()` is `true`.
- **Batches.** `executeBatch` and `executeLargeBatch` send the channel's `batch` request: a prepared statement's parameter sets as `sql` with `param_sets`, a `Statement`'s statements as `statements`, each with the watermark as `min_txn`. An empty batch sends nothing. In auto-commit the batch is one autocommit write, all or nothing: a failure leaves nothing, so its `BatchUpdateException` holds no counts. Because splitting it would break that, a batch above the channel's advertised entry limit (`limits.batch_entries`, at most 10,000) or body limit (`limits.request_body_bytes`) is refused before anything is sent, with SQLState `54000`; run it inside a transaction, or in smaller batches. Inside a transaction a batch that has not begun sends a `begin` first, is split into requests that fit both limits, and stops at its first failure: the `BatchUpdateException` holds the counts of the entries before the failed one (the server's `batch_index`), from every earlier request and the failed one's `results`, or `Statement.SUCCESS_NO_INFO` for each when the failed one's answer was a replay of its receipt alone, with the failure as its cause and its next exception, and `getGeneratedKeys()` returns those entries' keys. Counts above `Integer.MAX_VALUE` need `executeLargeBatch`. A batch whose answer was lost and replayed without its `results` raises `SKR01` in auto-commit and `SKR02` in a transaction, as below. On a read-only connection in auto-commit a batch raises `25006` and sends nothing. `supportsBatchUpdates()` is `true`, so that Hibernate batches with one prepared statement.
- **Prepared statements.** Preparing sends nothing. `getParameterMetaData()` and, before an execution, `getMetaData()` send one `describe`, whose answer the statement keeps. Parameters bind by index: `?`, `?NNN` and named parameters such as `:t` alike. Each execution sends its SQL again; there are no server-side handles.
- **Metadata.** `ResultSetMetaData` from the result columns; `ParameterMetaData` with the parameter count; `DatabaseMetaData` with the product name `SQLite`, the server's `sqlite_version()` as the product version, the driver version, the capabilities of this version, and `getTables` and `getColumns` over the main database's tables and views, from `sqlite_schema` and `pragma_table_info`, as stateless reads. `getColumns` reports `DATA_TYPE` as `getColumnType` would for the declared type, `TYPE_NAME` as the declared type without its arguments, and the arguments as `COLUMN_SIZE` and `DECIMAL_DIGITS`. Catalog and schema arguments are ignored.
- **Validity.** `Connection.isValid` describes `SELECT 1`, as the Go driver's ping does. One deadline, the shorter of *n* seconds and the network timeout, covers reading the token and the whole answer: `isValid(n)` returns `false` when it passes, and when the network timeout was the shorter, the connection closes too. A token provider that is still running is not interrupted, so a slow one can make `isValid` return `false` late. Closing or aborting the connection ends a running `isValid` with `false`.
- **Cancel and timeout of reads.** `Statement.cancel()` abandons the stateless reads the statement runs, an execution, a `describe` and an open result set, before or after its answer arrives: a stateless read needs no cancel request, because the server stops streaming when the response is dropped. Rows already received are not delivered after it. The exception has SQLState `57014`. A `close()` that returns before a request is sent stops it from being sent; one that comes later releases it. `setQueryTimeout(n)` sends `timeout_ms` so that the server's statement time is at most *n* seconds, and abandons a response that has not ended 2 seconds after that, the driver's cancel wait. Either way the driver raises `SQLTimeoutException`, which is safe for a read: it leaves no unknown outcome.
- **Cancel and timeout on the channel.** A channel request's answer is its outcome, so the driver never abandons it. `Statement.cancel()` sends the server a `cancel` for the request the statement runs, waiting up to 2 seconds for its whole answer, and the request's own answer then says what happened: `cancelled` (`57014`) when the server stopped it before its commit point, or its result when it had finished. A cancel before the request is sent stops it from being sent. `setQueryTimeout(n)` sends `timeout_ms`, and the server ends a statement that reaches it before its commit point with `timeout`, which raises `SQLTimeoutException`; a statement past its commit point finishes, and its result is returned although the timeout passed. The driver sends no `cancel` of its own at the timeout, because the server ends the statement at the same time. Closing the statement or the connection while a channel request runs makes its outcome unknown (`08007`). Closing the connection then sends the channel's `close`, which rolls back an open transaction, and waits up to 2 seconds for its whole answer; a commit or rollback in flight on another thread settles first. `abort` stops that commit or rollback as well, and sends the close from its executor. When the network timeout closes a connection, its channel's close runs on another thread, never on the driver's timer.
- **Results the server dropped.** When a channel request's answer was lost and its replay comes without the rows, which the server keeps only up to 64 KiB, a read-only statement is sent again as a new request, once: when that answer comes without its rows too, the driver raises `SQLNonTransientConnectionException` with SQLState `08S01`, since the read changed nothing. A write is not sent again: an autocommit write raises `SQLException` with SQLState `SKR01`, vendor code 0 and its transaction number in the message, since it committed, and a write inside a transaction raises `SQLException` with SQLState `SKR02`, the transaction still open with its effects.
- **Read-your-writes.** One watermark per database, keyed by the canonical base URL and the database name and kept for the driver class's life, is raised from each answer's `txn`, a resolve's and an error's included, and sent as `min_txn` with every stateless query and describe, every channel open of a read-only channel, every `begin`, and every `execute` and `batch`, inside a transaction too, where the server ignores it, so that no connection of the pool reads an older state than one it has seen, and connections and URLs that differ only in canonicalisation share it. A pod below it waits for its follower or forwards the request; one that cannot answers `behind`, which the driver retries as above. The watermark is raised from a query's `txn` when its result set reaches its end: rows read from a result set closed before its end do not raise it.
- **Limits.** A channel keeps the `limits` of its open answer for its life. A request body above `limits.request_body_bytes`, which the server scales with its memory budget, is refused before it is sent with `SQLNonTransientException`, SQLState `54000`, and so is a stateless query above the write channel's limit once the connection has opened one; before that, the server's own `request_too_large` (also `54000`) answers it.

### Values

| Storage class | `getObject` | Also readable as |
|---|---|---|
| NULL | `null` | 0, `false` or `null` from the typed getters, with `wasNull()` true |
| INTEGER | `Long` | `getInt`, `getShort` and `getByte` within their range, otherwise SQLState `22003`; `getDouble`, `getBigDecimal`, `getObject(i, BigInteger.class)`, `getString`; `getBoolean` for 0 and 1 only |
| REAL | `Double`, infinities included | `getLong` for an integral value within range; `getBigDecimal` for a finite value; `getObject(i, BigInteger.class)` for a finite integral value, exactly; `getString` |
| TEXT | `String` | `getBytes` (its UTF-8 bytes); `getLong`, `getDouble` and `getBigDecimal` when the text is a number; `getObject(i, Character.class)` for one character; the temporal getters below |
| TEXT that is not valid UTF-8 | an error | `getBytes` returns its own bytes; `getString` raises SQLState `22021` |
| BLOB | `byte[]` | `getBytes`, `getBlob` |

Temporal values are TEXT. `getObject(i, LocalDate.class)`, `LocalDateTime.class` and `LocalTime.class` read the text's fields; text with an offset is refused as a local value. `Instant.class`, `OffsetDateTime.class` and `ZonedDateTime.class` read text with an offset by it, and text without one as UTC; the last two are in UTC. `getTimestamp` reads text without an offset as a local timestamp in the JVM's zone, or the calendar's, and text with one as the instant it names. Only these forms are read, with years 0001 to 9999: a date `YYYY-MM-DD`, then optionally a space or `T` and `HH:MM`, `HH:MM:SS` or `HH:MM:SS` with 1 to 9 fraction digits, then, after a time, optionally `Z` or an offset `+HH:MM` or `-HH:MM`; and a time of day alone, `HH:MM`, `HH:MM:SS` or `HH:MM:SS` with 1 to 9 fraction digits.

A column's type name is its declared type without the arguments in parentheses, trimmed, in upper case, as in the Go driver: `datetime(6)` is `DATETIME`. In a column named `DATE`, `DATETIME` or `TIMESTAMP`, `getObject` returns TEXT in a date or timestamp form as `java.sql.Timestamp`, or as `java.sql.Date` for a date alone in a `DATE` column. `getColumnType` reports `DATE`, `TIMESTAMP` for the other two, and otherwise follows SQLite's affinity rules: `BIGINT` for a name containing `INT`; `VARCHAR` for `CHAR`, `CLOB` or `TEXT`; `BLOB` for `BLOB` or no type; `DOUBLE` for `REAL`, `FLOA` or `DOUB`; `NUMERIC` otherwise; `OTHER` for an expression. Nullability, precision and scale are reported as unknown.

Parameters: `setLong` and the other integer setters send INTEGER, `setBoolean` INTEGER 0 or 1, `setDouble` REAL (a NaN is refused), `setBigDecimal` TEXT in plain notation, `setString` TEXT, `setBytes` BLOB. `setObject` also takes `LocalDate`, `LocalTime` and `LocalDateTime`, written as their fields, `Instant`, `OffsetDateTime` and `ZonedDateTime`, written in UTC, and `java.sql.Date`, `Time` and `Timestamp`, written as their wall-clock fields in the JVM's zone or the given calendar's. A year outside 0001 to 9999, a NaN and a string with a lone surrogate are refused when they are set, before anything is sent.

### Errors

An error the server answered becomes an `SQLException` whose message holds the server's error code, the server's message and the request ID. The class and SQLState follow the code's meaning. Codes for requests refused before acceptance, which never executed, map to `SQLTransientConnectionException`; no code whose outcome can be unknown does. The vendor code is 0, except for `sql_error`.

| Code | Exception | SQLState |
|---|---|---|
| `invalid_request` | `SQLSyntaxErrorException` | `42000` |
| `unauthenticated`, `forbidden` | `SQLInvalidAuthorizationSpecException` | `28000` |
| `statement_forbidden` | `SQLSyntaxErrorException` | `42501` |
| `database_not_found` | `SQLNonTransientConnectionException` | `08001` |
| `needs_channel`, on a read-only connection | `SQLNonTransientException` | `25006` |
| `overloaded`, `too_many_channels` | `SQLTransientConnectionException` | `08004` |
| `no_leader`, `not_serving`, `owner_unreachable`, `behind`, `maintenance` | `SQLTransientConnectionException` | `08001` |
| `request_too_large`, `result_too_large`, or a body above the advertised limit | `SQLNonTransientException` | `54000` |
| `timeout` | `SQLTimeoutException` | `HYT00` |
| `cancelled` | `SQLNonTransientException` | `57014` |
| `sql_error` | by SQLite's primary code: 1 `SQLSyntaxErrorException` `42000`, 8 `SQLNonTransientException` `25006`, 18 `SQLDataException` `22001`, 19 `SQLIntegrityConstraintViolationException` `23000`, 20 `SQLDataException` `22000`, 25 `SQLDataException` `22023`, others `SQLException` `HY000`; the vendor code is SQLite's extended code | as listed |
| `database_exists` | `SQLNonTransientException` | `42710` |
| `busy` | `SQLTransactionRollbackException` | `40001` |
| `transaction_aborted` | `SQLTransactionRollbackException` with `cause: "maintenance"`, which can be retried; otherwise `SQLNonTransientException` | `40001`; `25000` |
| `invalid_state` | `SQLNonTransientException` | `25000` |
| `channel_busy`, `in_flight`, `stale_sequence`, `sequence_gap`, `sequence_conflict` | `SQLNonTransientException` | `HY010` |
| `channel_lost` | `SQLNonTransientConnectionException` | `08003`, or `08007` on the write channel |
| `commit_failed`, or a lost answer settled as not committed | `SQLTransactionRollbackException` | `40000` |
| A lost answer settled as committed, of an autocommit write or batch | `SQLException` | `SKR01` |
| `commit_outcome_unknown`, or an outcome that stays unknown after resolve | `SQLNonTransientConnectionException` | `08007` |
| `internal`, or a code outside protocol version 1 | `SQLException` | `HY000` |

The rows from `busy` down answer channel requests. A recorded failure is the request's outcome and is raised as the table says; an answer that leaves a sent request's outcome unknown is settled first, as [Unknown outcomes and resolve](#what-works) says, whatever its code. The codes the driver retries are raised only once its retries end. A stateless response that breaks the protocol, ends before its terminal frame, lacks the `Skylite-Protocol: 1` header or is a redirect, and a transport failure, raise `SQLNonTransientConnectionException` with SQLState `08S01`; a read is never sent again.

### Hibernate dialect

`io.github.randomcodespace.skylite.hibernate.SkyliteDialect` extends Hibernate's community `SQLiteDialect` (`hibernate-community-dialects`, 7.4.13.Final, the version Spring Boot 4.1.2 manages) with three changes:

- Generated identifiers come from `getGeneratedKeys`, which the driver answers from the insert's own answer, never from a separate `select last_insert_rowid()`, which skylite would run on another SQLite connection.
- Foreign keys are declared inside `create table`, with their `on delete` action, because SQLite has no `alter table ... add constraint` and the community dialect therefore emits none; skylite's SQLite connections enforce them. Dropping a table first drops the tables that reference it, so that the implicit delete of `drop table` never fails on their rows.
- `getDefaultStatementBatchSize()` is 50, so Hibernate batches inserts of application-generated identifiers, updates and deletes without `hibernate.jdbc.batch_size`, which still overrides it. The driver sends each batch as one channel `batch` request; Hibernate flushes inside a transaction, where the driver splits a batch above the server's limits.

Set it with `spring.jpa.database-platform` or `hibernate.dialect`. The module is Apache-2.0 and depends on Hibernate ORM, which is LGPL-2.1-or-later; it copies nothing from Hibernate, and the driver depends on neither.

### Spring Data JPA

`make java-acceptance-skylite` runs the 33-test acceptance suite in `clients/java/acceptance/` through the driver, the dialect, HikariCP and Hibernate against a `skylite serve` that the test run builds from this tree and starts on the local file system, with HTTPS and JWT authentication; the CI job `java-acceptance-skylite` runs it on every pull request. All 33 tests pass. The suite's README says how it is run.

### Not yet

- Catalog metadata other than `getTables` and `getColumns`, streams and LOB parameters, and JDBC escape syntax, which is sent unprocessed.
- Splitting an autocommit batch above the server's limits, which would break its atomicity; it is refused instead.
- A savepoint in a read-only transaction.
