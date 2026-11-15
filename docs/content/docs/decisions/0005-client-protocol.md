---
title: "0005: Client protocol"
description: "skylite will define its own protocol between the server and its Go and JDBC drivers: JSON over ordinary HTTP requests, newline-delimited JSON results, and channels with sequence numbers for every request that can write, so that no request takes effect twice and no driver reports an outcome it cannot prove."
weight: 5
---

**Status:** Accepted

**Date:** 2026-11-15

Nothing in this record exists yet. Every statement about skylite, its server and its drivers is a requirement or a plan. The Decision section and the [wire specification](#wire-specification) are a specification: the server and the two drivers are to be written from them independently. Statements about other software carry a link pinned to a version and a line, or are labelled inference or unverified.

## Context

The server and both drivers are still to be written, and they need one protocol. The constraints were fixed by the project owner:

- Services reach skylite over HTTPS only, through Istio or an ingress. There is no raw TCP port and no requirement for gRPC.
- Several pods serve one database. One leader takes writes; followers serve reads and take over on failure. Pods have no persistent volumes.
- A write is acknowledged only after it is confirmed in object storage. A node's commit attempt has three outcomes: committed, definitely not committed, or unknown, after which that node stops serving the database ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)).
- Clients are a Go `database/sql` driver, which must pass a GORM test suite, and a JDBC driver with a Hibernate dialect, which must pass the Spring Data JPA suite in `clients/java/acceptance/` ([0003](/docs/decisions/0003-jdbc-transport/)).
- Endpoints authenticate callers with OIDC JWT bearer tokens.

A research report prepared for this record (not in this repository) compared Hrana over HTTP, the rqlite HTTP API and Arrow Flight SQL. Its conclusion, accepted by the project owner, is that none is reusable: the Go Hrana client is deprecated, the rqlite Go driver's transactions are no-ops, and none of the inspected protocols defines what a client may conclude when a response to a write is lost. [0003](/docs/decisions/0003-jdbc-transport/) separately rejected Arrow Flight SQL for JDBC.

An earlier draft of this record identified stateless writes by a client-chosen commit identifier, deduplicated through a table bounded by a transaction-number window, with an outcome lookup on top. Its design review found that client-chosen identity invites collisions and reuse, that a table of refusals has no memory bound, and that the lookup was being stretched into exactly-once delivery for the application. The project owner replaced the mechanism with channels and narrowed the promises. The [response to the review](#response-to-the-design-review) goes through every finding.

## Decision

The protocol is version 1, as described below and in the [wire specification](#wire-specification). The server must implement it as written, and both drivers will.

### 1. What the protocol promises

- **At most once.** No request changes a database more than once, whatever intermediaries do: lose, delay without bound, truncate, reorder or duplicate requests and responses. A stateless read can be executed more than once if an intermediary duplicates it; that has no effect, because the stateless endpoint executes nothing that can change a database ([decision 5](#5-reads)).
- **Honest outcomes.** A driver reports success only for a write that is durably committed. It reports "not executed" or "not committed" only with one of the proofs in [decision 4](#4-when-a-driver-may-report-not-executed). Otherwise it reports "unknown".
- **Not promised:**
  - Exactly-once for the application. An application that retries after an error issues a new operation; that is the application's business, as with any database.
  - Recovery of a result after its response was lost, beyond the replay in [decision 3](#3-replay-of-the-last-outcome).
  - Resolution of an unknown outcome across a leader change, in this version.

### 2. Channels

Every request that can write goes through a **channel**: a small piece of server state on one pod, opened by the driver. A driver connection (`driver.Conn`, `java.sql.Connection`) holds at most one write channel and at most one read-only channel.

**Opening.** `open` returns an opaque, unguessable **token** and a **channel identifier** that is not secret, for logs and errors. The channel is bound to the database, the authenticated principal (the JWT's issuer and subject), its owning pod and, for a write channel, the leader epoch. Write channels live on the leader. A read-only channel lives on the pod that received the open. The token names the owning pod by an identifier that is not an address, and travels only in request bodies, never in a URL or a header.

**Sequence numbers.** Every request on a channel, other than `cancel` and `close`, carries a sequence number. The owner keeps, per channel: the last accepted sequence number, whether that request is still running, its recorded outcome ([decision 3](#3-replay-of-the-last-outcome)) and the SHA-256 digest of its request body. On arrival, after authenticating the caller and matching the principal:

| Sequence number | Answer |
|---|---|
| last + 1, nothing running | Accepted and executed |
| last + 1, the previous request still running | `channel_busy` |
| last, same body digest, still running | `in_flight` |
| last, same body digest, finished | The recorded outcome again |
| last, different body digest | `sequence_conflict` |
| below last | `stale_sequence` |
| above last + 1 | `sequence_gap` |

**A refused sequence number never changes any state.** It never rolls anything back and never ends the channel. A driver resends a request with a byte-identical body. A different body under an accepted sequence number is refused, and that refusal is final for the body that was refused: it can never be accepted, because its sequence number is taken. So a driver that shares a token with another, by mistake, is never told the other's outcome as its own.

**Transactions are a state of the channel.** Begin, statements, commit and rollback are requests on it. While a write transaction is open, the channel holds one SQLite connection and the database's write lock. An autocommit write is a single request on the channel. When SQLite rolls a transaction back by itself after an error, or a transaction limit ends it, the channel enters the **aborted** state: it holds nothing, and refuses every request except `rollback` until it gets one. So a statement is never run outside the transaction the driver believes it is in.

**Ending.** A channel ends when the driver closes it, when it has been idle past the channel idle limit, when its owning pod restarts or stops serving the database, or, for a write channel, when the owner stops holding office at the channel's epoch. A read-only channel's "epoch change" is its pod applying an epoch-bump commit. Requests on a channel that has ended are refused, for ever, with `channel_lost`: tokens are never reused, and channel state never moves between pods. An open transaction is rolled back, except a commit already past its commit point, which completes or fails on its own. The **commit point** is the moment the storage layer begins the create of the commit object ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)). Nothing interrupts a request after it.

**Closing** is idempotent and is how a driver makes sure that nothing still in transit on that channel can ever execute. The owner answers a close only after any running request has finished, interrupting it if it is before its commit point, and the answer carries the last accepted sequence number and that request's recorded outcome. A close of a channel that has already ended gets `channel_lost`.

**Opening is not deduplicated.** A duplicated open leaves a channel nobody owns. It holds no SQLite connection and no lock, and it expires at the channel idle limit. Channels are bounded per principal and per database on each pod; at the bound, open is refused with `too_many_channels` and nothing is created.

**The commit identifier** stored in the commit object ([0004 decision 3](/docs/decisions/0004-storage-engine/#3-durable-layout-in-object-storage-per-database)) is composed by the server: the 16 bytes of the channel identifier, then the request's sequence number as 8 bytes, big-endian. Clients never choose it. Nothing reads it in this version. It is stored so that a later version can resolve unknown outcomes across a leader change by reading commit headers, which the engine keeps for the life of the database. Nothing further is designed on it here.

### 3. Replay of the last outcome

The recorded outcome of a channel's last request is its status or error, rows affected, last insert row id, the transaction number, whether the statement was read-only, the transaction state after it, and its result rows if the whole response fits the retained-result bound and the database's budget for retained results ([decision 9](#9-limits-and-defaults)). Over the budget, the summary is kept and the rows are not, and a replay says `rows_omitted`.

A driver that did not receive a complete response resends the same request, with the same sequence number, and gets the recorded outcome. That makes a lost response invisible to the application, for as long as the channel lives, for commits, for writes that return counts, and for small `RETURNING` results, the shape GORM uses for inserts (inference from GORM's documented behaviour; not checked against a pinned source).

When the rows were not retained:

- **A read-only statement** is sent again under a new sequence number, if no row of the first response was handed to the application ([decision 5](#5-reads)).
- **An autocommit write** is reported as committed, result unavailable. The Go driver returns a `*skylite.Error` for which `errors.Is(err, skylite.ErrResultUnavailable)` holds and whose `Committed()` is true, with the transaction number. The JDBC driver raises `SQLException` (the base class) with SQLState `SKR01`, vendor code 0, and the transaction number in the message.
- **A write inside a transaction** is reported as a failed statement, with the transaction still open and the statement's effects still in it. The Go driver returns the same `ErrResultUnavailable` error with `Committed()` false; the JDBC driver raises `SQLException` with SQLState `SKR02`, vendor code 0.

Class `SK` is in none of the SQLState classes that Spring's translator maps ([SQLStateSQLExceptionTranslator.java#L59-L94][spring-state-sets]), and the base class `SQLException` is in none of the subclass translator's branches ([SQLExceptionSubclassTranslator.java#L75-L124][spring-sub]), so Spring reports both as an uncategorized, non-transient exception. Through Hibernate, vendor code 0 matches no case of the community SQLite dialect's converter ([SQLiteDialect.java#L451-L470][hib-sqlite-exc]); that Hibernate then yields a generic JDBC exception that Spring does not class as transient is inference from the translator source ([HibernateExceptionTranslator.java#L150-L200][spring-het]), not run.

### 4. When a driver may report "not executed"

The complete set:

1. The request was never sent.
2. The owner accepted the request and recorded a failure for it. Every other delivery of it gets that same answer. These answers carry `final: true`.
3. The owner refused the body with `sequence_conflict`. That body can never be accepted.
4. The owner's answer to a `close` shows a last accepted sequence number below the request's, or its answer to a `cancel` recorded the request as cancelled before the request arrived ([decision 6](#6-statements-transactions-and-errors)). A close or cancel answer that names the request itself carries its recorded outcome, which is case 2 or a success.

For a transaction, the driver may also report "not committed" when it never sent the commit request, whatever happened to the channel.

Every other answer is about one delivery. That covers a refusal before the owner accepted the request (no leader, a forwarding failure, overload, a validation error from a pod that is not the owner), `in_flight`, `channel_busy`, `stale_sequence`, `sequence_gap`, a response without the `Skylite-Protocol` marker, a body without a terminal frame, and a transport error. The driver may send the same request again, with the same sequence number. Forwarding pods may resend it as well, because resending is always safe. If the driver gives up, it closes the channel; if the close answer satisfies case 4, it reports "not executed", and otherwise "unknown". Every error object therefore carries two flags: `not_executed`, which is about this delivery, and `final`, which says that this answer is the request's settled outcome.

**`channel_lost` is not a proof.** It says that nothing on the channel can execute from now on. It does not say what happened before, because the channel may have ended after executing the request:

1. A driver sends an autocommit insert as sequence 7 to leader L, epoch 3. L commits it at transaction 1301. The response is lost when L's pod is killed.
2. L2 takes office at epoch 4.
3. The driver resends sequence 7. L2 answers `channel_lost`: the channel's owner is gone.

The insert is committed. The driver must report unknown. The same holds when an owner ended the channel by its idle limit after answering a request whose answer was lost, and when a pod of a later epoch refuses the request. A driver that receives `channel_lost` stops resending, reports unknown for a request it had sent and not settled, and opens a new channel for its next request.

### 5. Reads

**Stateless reads.** A statement outside a transaction that the driver expects to be a read goes to `query`, which any pod serves. The server compiles it, and refuses anything that is not a pure read with `needs_channel`, executing nothing; the driver then sends it on its write channel. A wrong guess by the driver costs a round trip and nothing else. A statement is a pure read when SQLite's read-only test is true for it ([`sqlite3_stmt_readonly`][sq-readonly], exposed as `Stmt.ReadOnly` ([stmt.go#L70-L77][nc-ro])), it is not refused below, and the SQL policy permits it on this path. SQLite's test is true for transaction control and for `ATTACH` and `DETACH`, so those are refused explicitly on every path: transaction control is never SQL in this protocol, and `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` are allowed only inside a transaction on a channel.

**The SQL policy hook.** A statement can pass the read-only test and still have effects. SQLite's documentation says application-defined functions can change the database as a side effect and the test still returns true ([sqlite.h.in#L4740-L4768][sq-readonly]), and some pragmas change the connection while the statement is prepared, before any step ([pragma.c#L1158-L1190][sq-pragma-flag]). The SQL policy of the later lockdown record must therefore be consulted before any such effect, on every path, including `describe`:

1. The policy is installed as SQLite's authorizer on every connection, before its first prepare ([config.go#L202][nc-auth]). SQLite consults the authorizer while it prepares a statement, and for a pragma it does so before the pragma takes effect ([pragma.c#L471][sq-pragma-auth]).
2. The server prepares the statement. A denial by the authorizer refuses the statement with `statement_forbidden`.
3. The server checks the uncompiled tail ([conn.go#L193-L203][nc-prepare]), the read-only test where the path requires it, and, for a batch entry, that it returns no columns.
4. Only then does the server bind or step.

The policy itself belongs to that record.

**Read-your-writes.** Every response carries `txn`, the transaction number of the state it reflects. Each driver keeps the highest it has seen in one place shared by a pool, and sends it as `min_txn` with stateless reads and with the begin of a read-only transaction. In the Go driver the owner is the `driver.Connector` behind a `sql.DB`. In the JDBC driver it is a registry keyed by URL and connection properties, which every `DataSource` and `DriverManager` connection with the same key shares. A pod whose state is older than `min_txn` waits up to the follower catch-up limit, then forwards a `query`, a `describe` or a read-only open to the leader. A read-only channel cannot move, so its `begin` fails with `behind`, recorded, and the driver opens another read-only channel. The guarantee covers snapshots opened after the write completed. An already open read-only transaction keeps its snapshot.

**Read-only transactions** use a read-only channel, opened on whichever pod received the open and bound to it. Its connection refuses writes: a write fails with `sql_error` carrying SQLite's `SQLITE_READONLY`.

**Exposed rows.** A driver may retry a read only if no row of it has been handed to the application. Once a row has been exposed, a failed or incomplete response is an error, never a silent restart and never a short result.

**Isolation.** SQLite gives serializable isolation: a write transaction holds the only write lock, and a read-only transaction reads one snapshot. A request for a weaker level is served at serializable. The JDBC 4.3 specification permits a driver to substitute a more restrictive level (section 10.2.1, [JSR 221][jdbc-spec]; the design review checked the text). The JDBC driver will report `TRANSACTION_SERIALIZABLE`. The Go driver will accept every `sql.IsolationLevel` that serializable satisfies and return an error for `LevelLinearizable`, as the driver contract requires for an unsupported level ([driver.go#L279-L294][go-begintx], [sql.go#L126-L141][go-iso]), because a follower may serve a snapshot that is not the newest.

### 6. Statements, transactions and errors

- **One statement per execution.** Text after the first statement other than whitespace and comments is refused with `invalid_request`. No server-side statement handles and no cursors: SQL text is sent with every execution.
- **Autocommit failure commits nothing.** An autocommit request that fails rolls back its implicit transaction, including the changes of a statement that failed under SQLite's `FAIL` conflict action, which SQLite itself would keep ([lang_conflict.html][sq-conflict], read on 2026-11-15). That difference from SQLite is deliberate. An autocommit batch is one transaction: all of it commits or none of it.
- **Inside a transaction, SQLite's semantics apply.** An error makes no claim that the statement had no effect, and carries `not_executed: false`. Every answer on a channel reports `txn_state` (`none`, `open` or `aborted`), read from SQLite after the statement with `sqlite3_get_autocommit` ([sqlite.h.in#L7000-L7013][sq-autocommit], exposed as `Conn.GetAutocommit` ([conn.go#L273-L279][nc-autocommit])), because SQLite rolls the whole transaction back after some errors. A batch inside a transaction stops at its first failing entry; the entries before it keep their effect, and the error carries `batch_index`. There are no internal savepoints.
- **A durable commit stays committed in every answer.** Once a commit is durable, no later event, such as a response write timeout, a cancelled caller or a slow client, may produce a negative or "no effect" answer for it. The statement time limit covers execution up to the commit point. Writing the response has its own deadline, and failing it only cuts the response, which the driver resolves by replay.
- **Cancellation** is an explicit `cancel` request naming the channel and the sequence number. If that request is running and has not reached its commit point, the owner interrupts SQLite through the connection's context ([conn.go#L328-L348][nc-interrupt]). It answers after the request has finished, with what actually happened: cancelled and not committed, or committed. If the named request has not arrived yet and is the next one, the owner records it as cancelled, so that it never executes. A dropped HTTP connection cancels nothing by itself, except that the server stops streaming the rows of a read.
- **Driver deadlines.** Go requires `ExecContext` and `QueryContext` to return when their context is cancelled ([driver.go#L198][go-ctx-exec], [driver.go#L225][go-ctx-query]). JDBC requires a statement that exceeds its query timeout to raise `SQLTimeoutException` ([Statement.java#L224-L232][jdk-timeout]). Both drivers will meet these by sending `cancel` and waiting for its answer up to the driver's cancel wait. Without an answer, the driver returns promptly with "unknown", stops using the channel and sends it a `close` without waiting. `driver.Tx.Commit` takes no context, so the Go driver bounds a commit by its own commit wait.
- **Connections are not reused after a write.** The SQLite connection that served a transaction or an autocommit write is closed when it ends, until a complete reset is specified and proven. Stateless reads may share pooled connections, because the policy hook keeps a pure read from changing connection state.
- **Error text.** Responses to the authenticated caller may carry SQLite's message, which can include application text such as a trigger's `RAISE` message. Server logs carry codes and identifiers only, never SQL text, parameters, tokens or SQLite messages. Deployments must not log request bodies or `Authorization` headers at the mesh or the ingress.

**What each driver raises for an unknown outcome.** The uncertainty must survive `database/sql`, GORM, Hibernate and Spring:

| Driver | Error |
|---|---|
| Go | `*skylite.Error` for which `errors.Is(err, skylite.ErrOutcomeUnknown)` holds, carrying the channel identifier, sequence number and epoch. It does not match `context.Canceled`, `context.DeadlineExceeded` or `driver.ErrBadConn` |
| JDBC | `SQLNonTransientConnectionException`, SQLState `08007` ("transaction resolution unknown"), vendor code 0 |

Every place where the default translation would turn an uncertain outcome into something an application treats as retryable, and which the drivers must therefore avoid:

- **`driver.ErrBadConn`.** `database/sql` retries the operation on it ([driver.go#L151-L163][go-badconn], [sql.go#L1569-L1580][go-retry]). The Go driver returns it only from an operation that sent no request.
- **Bare context errors.** Generic Go retry helpers commonly retry `context.DeadlineExceeded` (inference). The Go driver does not wrap the context error into an unknown outcome. `database/sql` itself returns the context error from `Rows.Err` after a cancellation during iteration ([sql.go#L3133-L3150][go-rows-err]) and from `Tx.Commit` when the context ended before the commit was sent; neither hides an unknown outcome, because rows are iterated only after their request settled, and a commit that was never sent has not committed.
- **`SQLTimeoutException`.** Spring turns it into a transient `QueryTimeoutException` ([SQLExceptionSubclassTranslator.java#L91-L93][spring-sub-timeout]), and so does its Hibernate translator for Hibernate's own query timeout ([HibernateExceptionTranslator.java#L156-L157][spring-het-timeout]). The JDBC driver raises `SQLTimeoutException` only when the cancel answer settled the statement as not executed. For an unknown outcome it raises `08007` and names the timeout in the message, a deliberate departure from the wording of `setQueryTimeout`.
- **`SQLTransientConnectionException`, `SQLTransactionRollbackException`, `SQLRecoverableException` and SQLState class 40.** Spring maps them to transient or recoverable exceptions ([SQLExceptionSubclassTranslator.java#L81-L90][spring-sub-transient], [#L118-L119][spring-sub-recoverable], [SQLStateSQLExceptionTranslator.java#L77-L80][spring-state-40]). Not used for unknown.
- **Vendor codes 5 and 6.** The community SQLite dialect turns `SQLITE_BUSY` and `SQLITE_LOCKED` into `LockAcquisitionException` ([SQLiteDialect.java#L455-L460][hib-sqlite-busy]), which Spring turns into the transient `CannotAcquireLockException` ([HibernateExceptionTranslator.java#L159-L160][spring-het-lock]). Unknown carries vendor code 0, and skylite's dialect must keep code 0 unmapped.

`SQLNonTransientConnectionException` reaches Spring's non-transient `DataAccessResourceFailureException` directly ([SQLExceptionSubclassTranslator.java#L96-L98][spring-sub-nontransient]) and through Hibernate's `JDBCConnectionException` ([SQLStateConversionDelegate.java#L78-L80][hib-state-08], [HibernateExceptionTranslator.java#L150-L151][spring-het-conn]); the design review traced both paths in source. The GORM dialector used with the Go driver must pass `ErrOutcomeUnknown` through untranslated, including when GORM's `TranslateError` option is on (unverified: the dialector is chosen with the Go driver).

### 7. Transport and routing

- Ordinary HTTP requests and responses over HTTP/1.1 or HTTP/2. TLS is terminated by the mesh or by the server. WebSockets, gRPC and long-lived streams are not used.
- **Any pod accepts any request.** Clients are configured with one base URL and never receive pod addresses. A pod serves what it owns (a stateless read, a channel it owns) and forwards everything else: a write-channel open, a read-only open it cannot serve at `min_txn`, `GET` status and `PUT` create to the leader, and a channel request to the pod its token names. A follower that cannot compile a statement forwards it to the leader, because it may not yet have the schema change the statement needs.
- **One hop at most.** The forwarding pod marks the request `Skylite-Forwarded: 1` and never forwards a request that carries it. Forwarding preserves the method, path, end-to-end headers (including the caller's `Authorization`) and body bytes, regenerates hop-by-hop headers and message framing ([RFC 9110 section 7.6.1][rfc9110-conn], [RFC 9113 section 8.2.2][rfc9113-conn]), and relays the owner's frames unchanged. Each hop flushes after every frame. The receiving pod verifies the bearer token again: the hop is never trusted for authentication or authorisation. If the hop fails after the owner started its response, the forwarding pod ends the body without a terminal frame; it never invents one.
- **Time budgets are passed on.** The forwarding pod sends `Skylite-Budget-Ms`: the request's remaining time budget, decremented by the time already spent. The owner uses the smaller of that and its own limits. The header can only lower a limit.
- **Pod-to-pod requests must authenticate the receiving pod**, with mesh mutual TLS and a checked identity or with server TLS and a cluster certificate, and a pod takes forwarding addresses only from the cluster's own records. Otherwise a forged address would collect callers' bearer tokens. Cluster membership is a later record.
- **Keepalive frames** may keep an idle timeout from cutting a response that is waiting for a lock, an upload or a slow statement. Correctness never depends on them: HTTP does not guarantee incremental delivery through intermediaries ([RFC 9110 section 7.6][rfc9110-fwd]), and a cut response is resolved by replay. For availability, a deployment's route or request timeout must exceed the lock wait plus the statement time limit plus the response deadline, and its idle timeout must exceed the keepalive interval, or the same total where a proxy buffers responses.

### 8. Values and shared conventions

Both drivers must agree on these, so that Go and Java services can share a database.

| Storage class | Encoding | Example |
|---|---|---|
| NULL | JSON `null` | `null` |
| INTEGER | JSON number with no fraction and no exponent, parsed as an exact signed 64-bit integer; a value outside that range is `invalid_request` | `-9223372036854775808` |
| REAL | JSON number containing `.`, `e` or `E`, written so that it parses back to the same double; infinities are `{"real":"Infinity"}` and `{"real":"-Infinity"}` | `1.0`, `-0.0` |
| TEXT, valid UTF-8 | JSON string | `"Dune"` |
| TEXT, not valid UTF-8 | `{"text_bytes":"<base64>"}`, standard base64 with padding (RFC 4648 section 4) | `{"text_bytes":"/w=="}` |
| BLOB | `{"blob":"<base64>"}` | `{"blob":""}` |

- **No NaN.** Binding a NaN in SQLite stores NULL ([vdbemem.c#L1092-L1098][sq-nan]); the protocol has no encoding for it, drivers reject a NaN parameter before sending it, and the server rejects one.
- **No JSON booleans** as values. A JSON number is never read through a double. Go's `encoding/json` replaces invalid UTF-8 and lone surrogate escapes with U+FFFD when it decodes ([decode.go#L98-L101][go-json-dec]) and replaces invalid bytes when it encodes ([encode.go#L83-L84][go-json-enc]); neither the server nor the Go driver may use it unmodified for values. A body with invalid UTF-8, a lone surrogate escape or a duplicated key is `invalid_request`.
- **Text and blobs stay apart**, and so do the empty string, the empty blob and NULL. A row reports each value's own storage class.
- **Booleans** are INTEGER 0 and 1. Both drivers read 0 as false and 1 as true, and raise an error for any other value read as a boolean, so that Go's `database/sql`, which accepts only those integers for `bool` ([convert.go#L382-L387][go-scan-bool], [types.go#L49-L58][go-bool]), and the JDBC driver agree.
- **Instants** (Go `time.Time`; Java `Instant`, `OffsetDateTime`, `ZonedDateTime` and `java.sql.Timestamp`) are written as UTC text, `YYYY-MM-DD HH:MM:SS.fffffffff`, nine fraction digits, no zone suffix, and text without a zone is read back as UTC. **Local date-times** (Java `LocalDateTime`, `LocalDate`, `LocalTime`) are written as their fields, in the same form, without conversion. A fixed width makes text order equal time order, and the Hibernate SQLite dialect computes date parts with SQLite's `strftime` ([SQLiteDialect.java#L176-L186][hib-sqlite-strftime]), which reads any number of fraction digits ([date.c#L220-L231][sq-frac]).
- **Declared date types in Go.** The Go driver returns a `time.Time` for a TEXT value in a column declared `DATE`, `DATETIME` or `TIMESTAMP` only when the text parses in one of SQLite's date forms. Otherwise it returns the text unchanged.
- **Decimals** are TEXT in plain notation. Stored in a column with NUMERIC affinity they become INTEGER or REAL and can lose precision ([datatype3.html][sq-affinity], read on 2026-11-15); a decimal that must be exact needs a column declared `TEXT`.
- **Conformance vectors.** Both drivers' value codecs will be tested against one file, `clients/conformance/values.ndjson`, which is not created in this pull request. Each line is one JSON object: `name`; `wire`, the exact bytes of a value as a JSON string; `class`, the storage class; `value`, the canonical value as text (decimal for numbers, hexadecimal for bytes); and, for vectors that must be refused, `error` with the code. A codec passes when it decodes every valid `wire` to `class` and `value`, encodes every valid `value` back to the identical `wire`, and refuses every invalid vector. The set must cover both ends of the 64-bit range, integral REALs, `-0.0`, both infinities, NaN, invalid UTF-8, lone surrogates, embedded NUL, the empty string, the empty blob, NULL and a value at the largest allowed frame size.

### 9. Limits and defaults

Every limit is server configuration. A channel's open answer and `GET` status report them.

| Limit | Default |
|---|---|
| Request body | 16 MiB |
| Request header block | 64 KiB |
| Entries in one batch | 10,000 |
| Result size: bytes of all frames of one response before its terminal frame | 64 MiB |
| Terminal frame, reserved in every response | 4 KiB |
| Statement time: for a write, from arrival to its commit point; for a read, to its last row written | 30 s; a request may lower it |
| Response deadline, after a request's outcome is recorded | 10 s |
| Keepalive interval | 10 s |
| Lock wait | 5 s |
| Writers waiting for the lock, per database | 64 |
| Follower catch-up wait for `min_txn` | 500 ms |
| Channel idle limit | 10 min |
| Write transaction idle limit | 30 s |
| Write transaction lifetime | 5 min |
| Read-only transaction idle limit | 30 s |
| Read-only transaction lifetime | 120 s |
| Channels per pod and database | 4,096 |
| Channels per pod, database and principal | 256 |
| Concurrent stateless requests per pod | 256 |
| Concurrent stateless requests per pod and principal | 64 |
| Retained result, one request | 64 KiB |
| Retained results, per database and pod | 32 MiB |
| Driver wait for a cancel answer (driver configuration) | 2 s |

Every bound has a defined refusal: `too_many_channels` for channels, `busy` for waiting writers (recorded, so final), `overloaded` for stateless requests, `request_too_large` for bodies, `rows_omitted` for retained results, and status 431 from the HTTP layer, possibly without the marker, for an oversized header block.

Idle time runs from the end of one response to the arrival of the next request on the channel; a running request is never idle. A transaction that reaches its idle limit or lifetime is rolled back and the channel enters the aborted state. A commit past its commit point is never interrupted by a timer.

**Why the read limit includes writing.** On the leader, an open read pins a WAL snapshot, checkpoints cannot backfill past it, and the leader refuses writes once the WAL reaches its bound ([0004 decision 10](/docs/decisions/0004-storage-engine/#10-limits-accepted-in-the-first-version)). A slow reader would otherwise stop every writer.

**Workload limits.** A write transaction holds the database's only write lock from begin to commit, including the application's own work between statements, so long work inside a write transaction blocks every other writer, and a competing writer fails with `busy` after the lock wait. A transaction that holds the write lock while it waits for another write transaction of the same application, such as a nested `REQUIRES_NEW` write in Spring, can only time out. A Spring `@Transactional` method that is not read-only is a write transaction even if it only reads.

### 10. Authentication

- Every request carries `Authorization: Bearer <JWT>`, and every pod that handles a request verifies it, the forwarding hop included. Authentication and the principal match are checked before the sequence number, so a refused delivery changes nothing, and a driver can resend with a refreshed token for the same principal.
- A token for another principal on a channel gets `channel_lost` and leaves the channel untouched. A driver whose credential source changes principal therefore sees `channel_lost`, reports unknown for an unsettled request, and opens a new channel.
- `unauthenticated`, `forbidden` and `statement_forbidden` are distinct codes. Authorisation is checked before acceptance; how token claims map to permissions is a later record.

### 11. What the driver records decide

Driver-internal behaviour is specified with each driver, in a record that ships in the same pull request as that driver. Those records must decide:

| Topic | Review finding |
|---|---|
| The `java.sql.Connection` lifecycle: auto-commit transitions, `commit` and `rollback` in auto-commit mode, close, abort, network timeout, pool reset, `isValid` through the channel's `ping`, and the failed-transaction latch; the Go equivalents, `driver.Validator`, `driver.SessionResetter` and transaction-context cancellation | 14, 15 |
| The complete mapping of error codes to Go errors and to JDBC exception classes and SQLStates by meaning, within the constraints of [decision 6](#6-statements-transactions-and-errors) | 16, 28 |
| Generated keys beyond a single row id, including the zero row id, keys requested by column, multi-row inserts, and keeping the skylite Hibernate dialect off a separate `select last_insert_rowid()` outside a transaction | 17 |
| Declared-type to JDBC-type mapping, `ResultSetMetaData`, `ParameterMetaData`, `DatabaseMetaData`, and who fixes the schema-validation failure | 18 |
| Savepoint handles | 28 |
| Batch failure reporting: `BatchUpdateException`, update-count arrays and large counts | 28 |
| Result-set behaviour: forward-only, closing, and draining or buffering a response before the next request on the channel; `getObject`, typed getters, `wasNull` and streams | 13, 28 |
| Credential-provider ownership and refresh, in pools and across a request's resends | 11, 28 |
| Session-level timeouts reaching the server, such as Spring's transaction timeout | 20 |

### 12. Later work

The authorisation model; the SQL lockdown policy; database deletion and listing; compression; observability; cluster membership; resolving unknown outcomes across a leader change from the stored commit identifiers.

## Wire specification

This section is normative.

### Paths

`{db}` is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit.

| Method and path | Request | Served by |
|---|---|---|
| `POST /v1/databases/{db}/query` | One statement outside a transaction; pure reads only | The receiving pod, or the leader by forwarding |
| `POST /v1/databases/{db}/describe` | Parameters and result columns of a statement, outside a transaction | The receiving pod, or the leader by forwarding |
| `POST /v1/databases/{db}/channels` | Open a channel | Write: the leader. Read-only: the receiving pod |
| `POST /v1/databases/{db}/channel` | A sequenced request on a channel | The token's owner |
| `POST /v1/databases/{db}/channel/cancel` | Cancel one request | The token's owner |
| `POST /v1/databases/{db}/channel/close` | Close a channel | The token's owner |
| `GET /v1/databases/{db}` | Status | The leader |
| `PUT /v1/databases/{db}` | Create the database if absent | The leader, or the pod that wins creation |

Requests carry `Authorization: Bearer <JWT>`; `POST` and `PUT` carry `Content-Type: application/json`. Bodies are JSON objects (RFC 8259), UTF-8, with no byte order mark and no content encoding. The server refuses an unknown field, a duplicated key, a value of the wrong type and a field that the request kind does not allow.

### Request fields

All integers are JSON integers without fraction or exponent, in the signed 64-bit range. Absent optional fields take the stated default.

| Field | Type and range | Absent | Meaning |
|---|---|---|---|
| `sql` | string, at least one statement, no U+0000 | required where listed | One SQL statement |
| `params` | array of values | no parameters | Element *i* binds parameter index *i* + 1 |
| `named` | object of values | no parameters | Binds `:key`, `@key` and `$key`; the key is the name without its prefix |
| `min_txn` | integer ≥ 0 | 0 | Lowest transaction number the request may observe |
| `timeout_ms` | integer, 1 to 2,147,483,647, milliseconds | the server limit | Lowers the statement time limit; larger values are capped |
| `mode` | `"write"` or `"read_only"` | required on open | Channel kind |
| `channel` | string, the token, at most 256 characters from `A-Z a-z 0-9 . _ -` | required on channel requests | The channel |
| `seq` | integer, 1 to 2^63 − 1 | required on sequenced requests and `cancel` | Sequence number |
| `op` | `"begin"`, `"execute"`, `"batch"`, `"describe"`, `"commit"`, `"rollback"` or `"ping"` | required on sequenced requests | What to do |
| `execute` | object with `sql` and optionally `params` or `named` | no first statement | The first statement of a `begin` |
| `param_sets` | array, 1 to the batch limit, of arrays or objects | — | One statement executed once per parameter set |
| `statements` | array, 1 to the batch limit, of objects with `sql` and optionally `params` or `named` | — | Several statements |

A request uses `params` or `named`, not both. Every parameter must be bound exactly once: an unbound parameter, a duplicate, an extra positional value or an unmatched name is `invalid_request`.

| Request | Required | Optional |
|---|---|---|
| `query` | `sql` | `params` or `named`, `min_txn`, `timeout_ms` |
| `describe` | `sql` | `min_txn` |
| open | `mode` | `min_txn` (read-only) |
| `op: "begin"` | `channel`, `seq`, `op` | `min_txn` (read-only channel), `execute`, `timeout_ms` |
| `op: "execute"` | `channel`, `seq`, `op`, `sql` | `params` or `named`, `timeout_ms` |
| `op: "batch"` | `channel`, `seq`, `op`, and either `sql` with `param_sets` or `statements` | `timeout_ms` |
| `op: "describe"` | `channel`, `seq`, `op`, `sql` | none |
| `op: "commit"`, `"rollback"`, `"ping"` | `channel`, `seq`, `op` | `timeout_ms` (commit) |
| cancel | `channel`, `seq` | none |
| close | `channel` | none |
| `PUT` | none: the body is `{}` | |

### Responses and frames

Every response skylite produces carries `Skylite-Protocol: 1`, `Skylite-Request-Id` (assigned by the pod that received the request from the client and passed on the hop), `Content-Type: application/x-ndjson` and `Cache-Control: no-store`, and on 401 `WWW-Authenticate: Bearer`. A response without the marker was not produced by skylite and proves nothing. No response uses HTTP trailers.

A body is a sequence of frames, each one JSON value on one line ended by `\n`:

```text
body     = *keepalive [ columns *( row / keepalive ) ] terminal
terminal = done / error
```

- `columns` is `{"columns":[...]}`, with per column `name`, `decltype` (or null) and `database`, `table` and `origin` (or null) ([stmt.go#L403-L452][nc-colmeta], with the column metadata build option [sqlite_opt.h#L27][nc-opt-meta]). A statement without result columns has no `columns` frame, except in a describe answer, which always has one.
- A row is a JSON array with exactly as many values as there are columns.
- `keepalive` is `{"keepalive":{}}`.
- Exactly one terminal frame, last. Nothing follows it.
- An object frame whose only key is unknown is skipped before the terminal frame. An unknown value tag inside a row is an error.
- The result limit counts every byte before the terminal frame. Each response reserves the terminal frame's 4 KiB; error messages are truncated to fit it.
- **A body without a terminal frame is incomplete.** The driver reports it as an error, never as a short result, and treats the request as unsettled.
- An autocommit write's rows are held until its commit has an outcome, so no client reads rows of a write that then fails to commit. Inside a transaction, and for reads, rows stream as they are produced.

**Status codes.** A stateless request that started executing, and every channel request the owner accepted, gets 200, with any error as its terminal frame; so does every replay. Everything refused before that gets the status of its code, with one `error` frame. `PUT` returns 200 on creation. The only 5xx statuses skylite produces are 500 (`internal`) and 503 (`no_leader`, `not_serving`, `behind`). Drivers decide from the marker and the frames, never from the status alone.

**`done` fields.** On a channel, every `done` also carries `seq` and `txn_state`.

| Request | Fields |
|---|---|
| `query`, `op: "execute"`, `begin` with `execute` | `txn`, `read_only`, `rows_affected`, `last_insert_rowid`; `rows_omitted: true` on a replay without rows |
| `op: "batch"` | `txn`, `results`: one `{"rows_affected":n,"last_insert_rowid":n}` per entry |
| `describe`, `op: "describe"` | `txn`, `params`: one `{"name":":id"}` or `{"name":null}` per parameter index |
| `begin` without `execute` | `txn`, the snapshot |
| `op: "commit"` | `txn`: the committed transaction, or the snapshot for a transaction that wrote nothing; `committed: true`; `wrote` |
| `op: "rollback"`, `op: "ping"` | `txn` |
| open | `channel`: `token`, `id`, `mode`, `epoch` (write), `limits` |
| cancel | `outcome`: the named request's recorded outcome |
| close | `last_seq`; `outcome`, the recorded outcome of `last_seq`, absent if `last_seq` is 0 |
| `GET` status | `txn` (the leader's head), `epoch`, `sqlite_version`, `limits` |
| `PUT` | `txn`, `epoch` |

`txn` is the transaction number of the state the answer reflects. `rows_affected` is SQLite's change count for a statement that is not read-only and 0 for one that is. `last_insert_rowid` is SQLite's per-connection value after the statement. A recorded `outcome` object has `seq`, `ok`, and either the `done` fields above or `error`, the error object.

### Errors

```json
{"error":{"code":"busy","not_executed":true,"final":true,"message":"the write lock was not granted within 5000 ms","request_id":"01JDZ3W4Q8M6","channel_id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","seq":4,"txn_state":"none"}}
```

`code`, `not_executed`, `final`, `message` and `request_id` are always present. `channel_id`, `seq` and `txn_state` are present on answers from a channel's owner. `sqlite_code` (primary) and `sqlite_extended_code` are present when SQLite produced the error, `batch_index` for a failed batch entry, and `txn` and `epoch` when known. No error carries a token.

- `not_executed: true`: this delivery had no effect.
- `final: true`: this answer is the request's settled outcome. It is set exactly on a recorded failure replayed or answered by the owner, and on `sequence_conflict`.

The closed list of codes. "Status" applies when the error is the whole response. "Recorded" means the error can be a channel request's recorded outcome; such an error has `final: true`, `not_executed: true` outside a transaction and for a failed commit, and `not_executed: false` for a statement inside a transaction.

| Code | Status | Recorded | Meaning |
|---|---|---|---|
| `invalid_request` | 400 | yes | Malformed body, unknown field, bad value, binding mismatch, more than one statement, transaction-control SQL, a batch entry with columns. Before acceptance only when the body names no channel and sequence number, or on a stateless path |
| `unauthenticated` | 401 | no | Bearer token missing, malformed, expired or unverifiable |
| `forbidden` | 403 | no | The principal may not do this on this database |
| `statement_forbidden` | 403 | yes | The SQL policy refused the statement |
| `database_not_found` | 404 | no | No such database |
| `database_exists` | 409 | no | `PUT` for a database that exists |
| `needs_channel` | 409 | no | `query` for a statement that is not a pure read |
| `channel_busy` | 409 | no | Another request on the channel is running |
| `in_flight` | 409 | no | This request is running |
| `stale_sequence` | 409 | no | Sequence number below the last accepted |
| `sequence_gap` | 409 | no | Sequence number above the last accepted plus one |
| `sequence_conflict` | 409 | no | Sequence number taken by a different body; `final: true` |
| `channel_lost` | 410 | no | Unknown token, another principal, or a channel that has ended |
| `request_too_large` | 413 | no | Body above the limit |
| `too_many_channels` | 429 | no | Channel bound reached; nothing was created |
| `overloaded` | 429 | no | Stateless request bound reached |
| `invalid_state` | final frame | yes | The operation is not allowed in the channel's state, such as `commit` with no transaction or `begin` inside one |
| `transaction_aborted` | final frame | yes | The transaction was rolled back by SQLite or by a limit; only `rollback` is accepted |
| `busy` | final frame | yes | The write lock was not granted within the lock wait, or the waiting-writer bound was reached |
| `timeout` | final frame | yes | The statement time limit or the transaction lifetime was reached before the commit point |
| `cancelled` | final frame | yes | Cancelled by `cancel` or `close` before the commit point |
| `result_too_large` | final frame | yes | The result passed the result limit |
| `sql_error` | 400 or final frame | yes | SQLite reported an error; its codes are in the object |
| `commit_failed` | final frame | yes | The commit definitely did not happen: too large for one commit object or the local WAL bound, a definite upload failure, or the leader was fenced ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)) |
| `commit_outcome_unknown` | final frame | no | The upload's outcome was not definite within the engine's bound; the pod stops serving the database. `not_executed: false` |
| `no_leader` | 503 | no | No leader is reachable |
| `not_serving` | 503 | no | This pod does not serve this database now, or a forwarded request reached a pod that does not own it |
| `behind` | 503 or final frame | yes, for a read-only `begin` | A read-only channel's pod did not reach `min_txn` within the catch-up wait, or the leader's state is below `min_txn`, which only a leader that has been superseded can answer |
| `internal` | 500 or final frame | yes, before the commit point | Unexpected server failure; `not_executed` true only when known |

### Channel state machine

States: **idle** (nothing running, no transaction); **running** (request *s* running outside a transaction, including a `begin` waiting for the lock); **open** (transaction open, nothing running); **running in transaction**; **committing** (commit running before its commit point); **durable wait** (a commit or an autocommit write past its commit point, waiting for storage's definite answer); **aborted**; **ended**.

Before the table applies, every request goes through these checks in order, and a refusal changes nothing: the body parses and names a channel; the bearer token verifies; the token names this pod (otherwise forward, or `not_serving` if already forwarded); the channel exists here, belongs to this principal and has not ended (otherwise `channel_lost`).

| State | Event | Action | Next state | Answer |
|---|---|---|---|---|
| any live | sequenced, `seq` < last | none | unchanged | `stale_sequence` |
| any live | sequenced, `seq` > last + 1 | none | unchanged | `sequence_gap` |
| any live | sequenced, `seq` = last, other digest | none | unchanged | `sequence_conflict` |
| running, running in transaction, committing, durable wait | sequenced, `seq` = last, same digest | none | unchanged | `in_flight` |
| idle, open, aborted | sequenced, `seq` = last, same digest | none | unchanged | Recorded outcome |
| running, running in transaction, committing, durable wait | sequenced, `seq` = last + 1 | none | unchanged | `channel_busy` |
| idle | `begin` | Accept; write: wait for the lock (`BEGIN IMMEDIATE`); read-only: wait for `min_txn`, then `BEGIN` | running | When it finishes |
| idle | `execute`, `batch` | Accept; a write takes the lock and a fresh connection | running | When it finishes |
| idle | `describe`, `ping` | Accept | running | When it finishes |
| idle | `commit`, `rollback` | Accept, record `invalid_state` | idle | `invalid_state` |
| running | `begin` succeeds, with its first statement if any | Record | open, or aborted if the first statement's error ended the transaction | `done`, or the error with `txn_state` |
| running | `begin` not granted the lock in time, or waiting-writer bound | Record `busy` | idle | `busy` |
| running | read-only `begin` does not reach `min_txn` within the catch-up wait | Record `behind` | idle | `behind` |
| running | autocommit write reaches its commit point | none | durable wait | none yet |
| running | autocommit request fails before its commit point | Roll back, close the connection, record | idle | The error, `not_executed: true` |
| running | read-only or describe or ping request finishes | Record | idle | Its result |
| open | `execute`, `batch`, `describe`, `ping`, `rollback` | Accept | running in transaction | When it finishes |
| open | `commit` | Accept | committing | When it finishes |
| open | `begin` | Accept, record `invalid_state` | open | `invalid_state` |
| running in transaction | statement finishes or fails | Record; read `sqlite3_get_autocommit` | open, or aborted if SQLite ended the transaction | The result or error with `txn_state` |
| running in transaction | `rollback` finishes | Roll back, close the connection, record | idle | `done` |
| committing | the transaction wrote nothing, or is read-only | End it, close the connection, record committed at the snapshot | idle | `done`, `wrote: false` |
| committing | reaches its commit point | none | durable wait | none yet |
| committing | fails before its commit point (too large, timeout, cancel, error) | Roll back, close the connection, record | idle | The error, `final`, `not_executed: true` |
| durable wait | storage: committed | Close the connection, record committed | idle | `done` |
| durable wait | storage: definitely not committed | Close the connection, record `commit_failed` | idle | `commit_failed` |
| durable wait | storage: unknown within the engine's bound | The pod stops serving the database | ended | `commit_outcome_unknown` if the response can still be written |
| aborted | `rollback` | Accept, record | idle | `done` |
| aborted | any other sequenced request | Accept, record `transaction_aborted` | aborted | `transaction_aborted` |
| idle, open, aborted | `cancel`, `seq` = last | none | unchanged | `done`, the recorded outcome |
| idle, open, aborted | `cancel`, `seq` = last + 1 | Record *s* as `cancelled`, without a digest; every later delivery of *s* gets that outcome | unchanged | `done`, outcome `cancelled` |
| running, running in transaction, committing | `cancel`, `seq` = last | Interrupt SQLite | as for a failure or success of the request | `done` with the outcome, when it finishes |
| durable wait | `cancel`, `seq` = last | none | as storage decides | `done` with the outcome, when storage decides |
| running states | `cancel`, `seq` = last + 1 | none | unchanged | `channel_busy` |
| any live | `cancel`, `seq` < last or > last + 1 | none | unchanged | `stale_sequence` or `sequence_gap` |
| idle, aborted | `close` | End | ended | `done` with `last_seq` and its outcome |
| open | `close` | Roll back, close the connection, end | ended | `done` with `last_seq` and its outcome |
| running, running in transaction, committing | `close` | Interrupt SQLite; when the request has finished and been recorded, roll back and end | ended | `done` with `last_seq` and its outcome |
| durable wait | `close` | Wait for storage's answer, record, end | ended | `done` with `last_seq` and its outcome |
| ended | `close` or any request | none | ended | `channel_lost` |
| idle, aborted | channel idle limit | End | ended | none |
| open | transaction idle limit or lifetime | Roll back, close the connection | aborted | none; the next request learns it |
| running in transaction | transaction lifetime | Interrupt SQLite, roll back, record `timeout` | aborted | `timeout`, `txn_state: "aborted"` |
| running, running in transaction, committing | statement time limit | Interrupt SQLite, record `timeout`; inside a transaction read `sqlite3_get_autocommit` | idle, open or aborted | `timeout` |
| durable wait | any timer | none | unchanged | none |
| running states | the caller's HTTP connection drops | A read stops producing rows and is recorded with `rows_omitted`; anything else continues | as it finishes | none |
| after recording | response deadline passes | The response is cut | unchanged | none; the driver replays |
| any live | the pod restarts, stops serving the database, or the epoch changes | Interrupt everything before its commit point; answer running requests where a response can still be written; requests in durable wait finish first unless the pod stops on an unknown outcome | ended | per request, or none |

A late duplicate of the commit itself while it is in durable wait gets `in_flight`; a late duplicate of the statement before it gets `stale_sequence`. Neither changes anything.

## Worked examples

Headers that every request and response carries are left out.

### A GORM create whose commit response is lost

GORM wraps a create in a transaction. The Go driver's connection has an open write channel at sequence 11.

```http
POST /v1/databases/library/channel HTTP/1.1

{"channel":"w1.9QvX0aYk3H8...","seq":12,"op":"begin","execute":{"sql":"INSERT INTO books (title) VALUES (?) RETURNING id","params":["Dune"]}}
```

```text
{"columns":[{"name":"id","decltype":"INTEGER","database":"main","table":"books","origin":"id"}]}
[78]
{"done":{"seq":12,"txn_state":"open","txn":1209,"read_only":false,"rows_affected":1,"last_insert_rowid":78}}
```

The driver sends `{"channel":"w1.9QvX0aYk3H8...","seq":13,"op":"commit"}`. The leader commits at 1210, and the connection resets before the answer arrives. The driver resends the identical body. The owner finds sequence 13 finished, with the same digest, and replays:

```text
{"done":{"seq":13,"txn_state":"none","txn":1210,"committed":true,"wrote":true}}
```

`Commit` returns `nil`. Had the resend arrived while the upload was still running, the answer would have been `in_flight`, and the driver would have waited and resent.

### A leader killed during a commit

The same commit, but the leader's pod is killed after the upload starts. The driver resends sequence 13 and gets `no_leader`, then, after L2 takes office, `channel_lost`. Neither settles the request, so `Commit` returns an error matching `skylite.ErrOutcomeUnknown`, carrying the channel identifier and sequence 13. Commit 1210 may or may not be in the history. The driver opens a new channel for the connection's next transaction.

## Attempts to break the channel design

| Sequence | What happens |
|---|---|
| A duplicate of any sequenced request arrives after its answer | `seq` = last: the recorded outcome. `seq` < last: `stale_sequence`. Nothing executes |
| A duplicate arrives after the channel closed | `channel_lost`. Nothing executes. The driver settled the request from the close answer |
| A duplicate arrives after a leader change | The new leader answers `channel_lost` for an older epoch. The old leader, if still running, may accept it on its own channel, but cannot commit: its next commit meets the new leader's epoch bump ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)) and is `commit_failed` |
| A duplicate arrives after the driver gave up and reported unknown | If it is the next sequence number on a live channel, it may execute and commit, once. Unknown was the honest answer |
| A duplicate of `open`, `close` or `cancel` | `open`: an orphan channel, holding nothing, expiring at the idle limit. `close`: `channel_lost`. `cancel` naming an older sequence number: `stale_sequence` |
| A duplicate of the previous statement while the commit is in durable wait | `stale_sequence`; the commit continues. This was finding 9 |
| Two drivers present the same token | The second body under a taken sequence number gets `sequence_conflict`, final for that body; the first driver's state is untouched. If the second takes the next number first, the first driver gets `sequence_conflict` on its next request, closes the channel and reports its transaction failed; the commit it never sent cannot commit |
| Another principal replays a token | `channel_lost`. The channel is untouched |
| The owner pod restarts mid-request | Channel state is in memory only, so the channel has ended. A write channel's leader restarting means a new epoch ([0004 invariant 1](/docs/decisions/0004-storage-engine/#4-invariants)). A commit in durable wait may land; the driver sees `channel_lost` and reports unknown |
| A leader change during a commit | Before the commit point: the commit fails with no effect. After it: storage answers committed, or definitely not if the bump took the name, and the owner answers if it still can; otherwise the driver reports unknown |
| A close races a commit | The close waits for the commit's definite answer and returns it. A close answered before the commit was decided would have let a driver report "not executed" for a commit that then landed, so close never answers early |
| A cancel races a commit | Before the commit point: interrupted, recorded `cancelled`, not committed. After it: the cancel waits and reports the outcome. A cancel that overtakes its request records it as cancelled, and the request's late delivery gets that answer |
| A forwarded request reaches a stale leader | Channel requests are routed to the pod the token names, never to "the leader". A stale leader that owns the channel can execute a statement but not commit it. A write-channel open that reaches a stale leader creates a channel that ends when that leader stops |
| Replay memory under many channels | Each channel keeps one recorded outcome. Rows are retained up to 64 KiB per request and 32 MiB per database and pod; past that, summaries only. Channels are bounded per pod, database and principal |
| A driver that never closes anything | Channels expire at the idle limit. A leaking driver reaches its principal's bound and gets `too_many_channels`, which stops that principal without starving others below the database bound |
| A transaction expires while the driver still believes it open | The channel enters the aborted state. The driver's next statement gets `transaction_aborted`, never autocommit |

What remains open is not a safety violation: every write in transit across a leader change ends unknown, including one the old leader never received, because no answer from a later epoch can exclude a duplicate that reached the old leader before its fence. A leader restart during a rolling deployment will therefore surface unknown outcomes to applications. Resolving them from the stored commit identifiers is later work.

## Response to the design review

| # | Finding | Disposition |
|---|---|---|
| 1 | Commit identifier not bound to its operation | Falls away. Clients choose no identifier. Replayed: driver B cannot select A's identity; it can only share A's token, and a different body under a taken sequence number gets `sequence_conflict`, final for B's request, with A's channel untouched. The eviction sequence has no analogue: deduplication is by sequence number for the channel's whole life, and an ended channel answers `channel_lost` for ever |
| 2 | Parser rejection claims "not committed" | Falls away. Replayed: the malformed second delivery is refused before acceptance, with `final: false`, and says nothing about the request. Only the owner's recorded outcome, a `sequence_conflict` or a close or cancel answer settles a request ([decision 4](#4-when-a-driver-may-report-not-executed)) |
| 3 | No-op commit becomes "not committed" | Falls away. Replayed: the no-op update's outcome is recorded as committed and replayed while the channel lives. After the takeover, the resend gets `channel_lost`, which is never a proof, so the driver reports unknown |
| 4 | Known-committed operations become failures | Closed by decisions [1](#1-what-the-protocol-promises) and [3](#3-replay-of-the-last-outcome). Exactly-once for the application is not promised; replay returns counts and small results for the channel's life; the remaining case raises a committed, result-unavailable error that is not transient. Durable receipts not accepted: beyond what this version promises |
| 5 | Outcome recovery is not result recovery | Closed by [decision 5](#5-reads) (no retry once a row is exposed) and [decision 3](#3-replay-of-the-last-outcome). Durable recovery of results not accepted |
| 6 | Response timeout after a durable commit | Closed by [decision 6](#6-statements-transactions-and-errors): the statement time limit stops at the commit point, and a response deadline only cuts the response |
| 7 | Driver deadlines against recovery | Closed by [decision 6](#6-statements-transactions-and-errors): cancel, bounded wait, unknown, and the exception paths that must be avoided |
| 8 | Statements not atomic on error | Closed by [decision 6](#6-statements-transactions-and-errors): autocommit failure rolls back everything; inside a transaction no no-effect claim, `txn_state` from SQLite, the aborted state, and no internal savepoints |
| 9 | Late delivery rolls back a committing session | Falls away. Replayed: the late duplicate of statement 3 arrives while commit 4 is in durable wait; it gets `stale_sequence`, and a refused sequence number changes nothing |
| 10 | Commit table has no memory bound | Falls away. Replayed: there is no outcome request and no refusal table. A client can create at most its principal's bound of channels, each holding one bounded outcome |
| 11 | Token refresh changes the principal during recovery | Falls away. Replayed: Q's resend on P's channel gets `channel_lost`, which is not a proof, so the driver reports unknown. Nothing searches in Q's name. Credential-provider ownership moves to the driver records |
| 12 | Begin replayable without identity | Falls away. Replayed: `begin` is a sequenced request, so the duplicate gets `in_flight`, the recorded outcome or `stale_sequence`, and never a second transaction. A lost open answer leaves an orphan channel that holds no lock, bounded and expiring |
| 13 | Result-set closure cancels the transaction | Abandoning a response no longer rolls anything back. Draining and buffering move to the driver records |
| 14 | `GET` status does not validate a session | The channel's `ping` validates the channel itself; the lifecycle moves to the driver records |
| 15 | Driver lifecycle unspecified | Moved to the driver records ([decision 11](#11-what-the-driver-records-decide)); the server-side latch is the aborted state |
| 16 | Permanent errors mapped to retryable ones | The uncertainty mappings are fixed here; the full mapping by meaning moves to the driver records |
| 17 | One row id is not a generated-key contract | Moved to the driver records; the protocol carries `last_insert_rowid` and `RETURNING` rows |
| 18 | Affinity is not a JDBC type mapping | Moved to the driver records |
| 19 | Pool-wide consistency needs an owner | Closed by [decision 5](#5-reads): the connector and the registry, scoped to new snapshots |
| 20 | Defaults reject ordinary transactions | Closed by [decision 9](#9-limits-and-defaults): longer transaction limits and the workload limits stated. Transaction timeouts reaching the server move to the driver records |
| 21 | Go and Java timestamps differ | Closed by [decision 8](#8-values-and-shared-conventions): instants as UTC, local date-times as written. Calendar handling moves to the JDBC driver record |
| 22 | Declared-type coercion destroys text | Closed by [decision 8](#8-values-and-shared-conventions): unparseable text returned unchanged; one boolean domain |
| 23 | Describe can have effects | Closed by the policy hook in [decision 5](#5-reads), on every path |
| 24 | Connection cleanup missing | Closed by [decision 6](#6-statements-transactions-and-errors): connections that served a write are closed |
| 25 | Unbounded resources | Closed by [decision 9](#9-limits-and-defaults); outcome scanning no longer exists |
| 26 | Redaction depends on an inference | Closed by [decision 6](#6-statements-transactions-and-errors): the guarantee's boundary is the server's logs; messages to the caller may carry SQLite's text |
| 27 | Forwarding and keepalive contract | Closed by [decision 7](#7-transport-and-routing) |
| 28 | Wire interoperability gaps | Closed by the [wire specification](#wire-specification) (statuses, field types and widths, terminal frame, transaction state, begin failure, commit numbers, cancel and close, empty batches refused, result limits); JDBC result, value, batch, savepoint and credential behaviour moves to the driver records. Sparse `?NNN` parameters bind by index under the existing rule |
| 29 | Rewrite recovery against retired headers | Falls away. Nothing reads commit identifiers in this version; there is no retention rule, window or lookup. Retired headers keep the server-composed identifier for a later version |
| 30 | Value encoding traps | Closed by the conformance vectors in [decision 8](#8-values-and-shared-conventions) |

## Alternatives considered

**Client-chosen commit identifiers with a deduplication window and an outcome lookup.** The previous draft. Rejected for the reasons in [Context](#context).

**An exact log position as the operation identity**, suggested by the review: a write names the transaction number it must commit at, and recovery reads that one header. It makes lookup direct, but stateless writers contend for one slot, every write needs a fresh head, and a definite "not committed" needs a durable cancellation or a fence. Not adopted; the stored commit identifier leaves it open for the later version.

**Starting every transaction deferred.** SQLite's plain `BEGIN` defers the write lock ([sqlite.h.in#L4765-L4768][sq-begin]), and in WAL mode a transaction whose snapshot is no longer the newest cannot take it, returning `SQLITE_BUSY_SNAPSHOT` ([wal.c#L3816-L3823][sq-busysnap]). The failure would come part-way through a transaction; `BEGIN IMMEDIATE` moves the wait to the start, where `busy` is definite. Rejected.

**Server-side prepared statements, cursors and paging.** Each would tie state to a pod and a snapshot across requests. Rejected; the server may cache compiled statements privately.

**Reusing Hrana, the rqlite API or Flight SQL.** Rejected for the reasons in [Context](#context).

## Consequences

- Each driver connection that writes costs one extra request to open its channel, and a new one after a leader change or an idle period.
- GORM's default transaction around each create, update and delete takes two requests: a combined begin and a commit.
- A lost response costs one resend and is invisible to the application while the channel lives, except for large write results.
- Writes in transit across a leader change surface as unknown, with a distinct Go error and SQLState `08007`. Applications decide.
- The drivers must parse JSON integers exactly and must not use a JSON library that replaces invalid text.
- In this pull request: the [architecture page](/docs/architecture/) links this record from its interface section and records the protocol's part of client authentication.

## Requirements on the engine and the format

1. **The commit path takes the commit identifier from the server**: 24 bytes, the 16-byte channel identifier and the 8-byte big-endian sequence number. The format's field holds up to 64 bytes (commit header bytes 68 to 132, `internal/format/doc.go` on the `feat/engine-format` branch); its description of the field as "supplied by the client" must change, and the field is compared as exact bytes. Epoch-bump commits carry none.
2. **The commit point is visible.** The storage layer must tell the server when it begins the create of a commit object, and the server must not interrupt the SQLite connection after that point.
3. **Definite outcomes reach the server before fail-stop.** When the engine resolves a commit definitely (committed, or an epoch bump at the name) and then stops serving, the server must be able to answer the running request first.
4. **The engine reports** the head, the epoch, a follower's applied transaction number and its application of epoch-bump commits, and waits until a follower reaches a given number.
5. **Not required:** retention of commit objects for outcome lookups, an outcome search, or a refusal check on the commit path. The previous draft's requirements on garbage collection are withdrawn.

## Not verified, and what the first implementation must prove

Nothing here was run: there is no server or driver. Unverified or inference, each needing a test:

- That GORM uses `RETURNING` for inserts with the chosen dialector, and that its `TranslateError` path keeps `ErrOutcomeUnknown`.
- That Hibernate's translation of a vendor-code-0 `SQLException` with SQLState class `SK` ends non-transient in Spring.
- That pods and proxies flush keepalive frames promptly, through Istio and through ingress-nginx with buffering on and off.
- That SQLite's authorizer sees every function call and pragma the SQL policy needs to refuse.

The first implementation must prove, by test:

- No request changes the database twice: duplicates of every request kind delivered at every point in the state machine, including after the answer, after close, after a leader change and after the driver gave up.
- Every "not executed" and "not committed" a driver reports is true, for commits killed at each step of 0004's commit sequence, and `channel_lost` alone never produces one.
- A refused sequence number changes no state, and a different body under a taken number gets `sequence_conflict`.
- Close and cancel answers never precede the outcome of a running commit.
- A durable commit is never answered with a negative or no-effect error, including under response deadlines and dropped connections.
- Replay returns counts and retained rows; over budget it returns `rows_omitted`.
- An autocommit `INSERT OR FAIL` that fails leaves nothing; inside a transaction `txn_state` matches SQLite after every error, and an aborted transaction never runs a statement in autocommit.
- The policy hook runs before every prepare, including `describe`, and stateless `query` refuses transaction control, `ATTACH` and `DETACH`.
- Values round-trip through the conformance vectors in both drivers; a body cut at every byte offset is reported as incomplete, never as a short result.
- A follower behind `min_txn` waits and then forwards; a read after a write in the same pool sees the write.
- The unknown-outcome error survives `database/sql` and GORM, and Hibernate and `JdbcTemplate` through Spring, as a non-transient error; and the GORM suite and the Spring Data JPA suite in `clients/java/acceptance/` pass.

[nc-ro]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/stmt.go#L70-L77
[nc-colmeta]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/stmt.go#L403-L452
[nc-prepare]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L193-L203
[nc-autocommit]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L273-L279
[nc-interrupt]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L328-L348
[nc-auth]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L202
[nc-opt-meta]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L27
[sq-readonly]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L4740-L4768
[sq-autocommit]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L7000-L7013
[sq-begin]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L4765-L4768
[sq-pragma-auth]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pragma.c#L471
[sq-pragma-flag]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pragma.c#L1158-L1190
[sq-nan]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbemem.c#L1092-L1098
[sq-busysnap]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L3816-L3823
[sq-frac]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/date.c#L220-L231
[sq-conflict]: https://sqlite.org/lang_conflict.html
[sq-affinity]: https://sqlite.org/datatype3.html
[go-badconn]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L151-L163
[go-ctx-exec]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L198
[go-ctx-query]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L225
[go-begintx]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L279-L294
[go-retry]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/sql.go#L1569-L1580
[go-rows-err]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/sql.go#L3133-L3150
[go-iso]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/sql.go#L126-L141
[go-scan-bool]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/convert.go#L382-L387
[go-json-dec]: https://github.com/golang/go/blob/go1.26.2/src/encoding/json/decode.go#L98-L101
[go-json-enc]: https://github.com/golang/go/blob/go1.26.2/src/encoding/json/encode.go#L83-L84
[jdk-timeout]: https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.sql/share/classes/java/sql/Statement.java#L224-L232
[jdbc-spec]: https://jcp.org/en/jsr/detail?id=221
[hib-sqlite-exc]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470
[hib-sqlite-strftime]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L176-L186
[hib-state-08]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L78-L80
[spring-het]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L150-L200
[spring-het-timeout]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L156-L157
[spring-sub]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L75-L124
[spring-sub-transient]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L81-L90
[spring-sub-timeout]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L91-L93
[spring-sub-nontransient]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L96-L98
[spring-sub-recoverable]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L118-L119
[spring-state-40]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java#L77-L80
[spring-state-sets]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java#L59-L94
[rfc9110-fwd]: https://www.rfc-editor.org/rfc/rfc9110#section-7.6
[rfc9110-conn]: https://www.rfc-editor.org/rfc/rfc9110#section-7.6.1
[rfc9113-conn]: https://www.rfc-editor.org/rfc/rfc9113#section-8.2.2
[spring-het-conn]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L150-L151
[spring-het-lock]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L159-L160
[hib-sqlite-busy]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L455-L460
[go-bool]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/types.go#L49-L58
