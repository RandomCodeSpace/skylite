---
title: "0005: Client protocol"
description: "skylite will define its own protocol between the server and its Go and JDBC drivers: JSON over ordinary HTTP requests, newline-delimited JSON results, sessions only for explicit transactions, and client commit identifiers for recovering the outcome of a lost commit."
weight: 5
---

**Status:** Accepted

**Date:** 2026-11-15

Nothing in this record exists yet. Every statement about skylite, its server and its drivers is a requirement or a plan. The Decision section is a specification: its rules say what the server and the two drivers must do, and the drivers are to be written from it independently. Statements about other software carry a link pinned to a version and a line, or are labelled inference or unverified.

## Context

skylite's server does not exist, and neither do its drivers. Before either is written, the project needs one protocol that both drivers implement and that the server serves. The constraints were fixed by the project owner:

- Services reach skylite over HTTPS only, through Istio or an ingress. There is no raw TCP port and no requirement for gRPC.
- Several pods serve one database. One leader takes writes; followers serve reads and take over on failure. Pods have no persistent volumes.
- A write is acknowledged only after it is confirmed in object storage ([0004](/docs/decisions/0004-storage-engine/)). A commit can end with the answer unknown, and the node that gave it stops serving the database until a takeover.
- Clients are a Go `database/sql` driver, which must pass a GORM test suite, and a JDBC driver with a Hibernate dialect, which must pass the Spring Data JPA suite in `clients/java/acceptance/` ([0003](/docs/decisions/0003-jdbc-transport/)).
- Endpoints authenticate callers with OIDC JWT bearer tokens.

A research report prepared for this record (not in this repository) compared Hrana over HTTP, the rqlite HTTP API and Arrow Flight SQL, and their existing clients. Its conclusion, accepted by the project owner, is that none is reusable: the Go Hrana client is deprecated, the rqlite Go driver's transactions are no-ops, the inspected protocols have no way to learn the outcome of a commit whose response was lost, and Hrana carries stream state this design does not want. [0003](/docs/decisions/0003-jdbc-transport/) separately rejected Arrow Flight SQL for JDBC. So skylite defines its own protocol. The report's list of what such a protocol must cover is walked through under [Coverage of the minimum contract](#coverage-of-the-minimum-contract).

## Decision

The protocol is the one described below, version 1. The server must implement it as written, and both drivers will.

### 1. Transport

- Ordinary HTTP requests and responses, usable over HTTP/1.1 and HTTP/2. TLS is terminated by the mesh or by the server. WebSockets, gRPC and long-lived streams are not used and are not required by any intermediary.
- Every path is under `/v1`. Request bodies are JSON objects (RFC 8259), UTF-8, with no byte order mark and no content encoding. Compression is later work.
- The server must reject a request that contains a field this specification does not define, a duplicated key, or a value of the wrong type. Clients must ignore response fields and frame kinds they do not know. A new optional response field is therefore a compatible change; a new request field is not, and a newer driver can detect an older server by its `invalid_request` answer.
- Responses must not use HTTP trailers, because intermediaries may drop them. Everything a driver needs is in the body.

### 2. Endpoints

`{db}` is a database name: 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit. How a name maps to an object-storage prefix is server configuration.

| Method and path | Purpose | Handled by |
|---|---|---|
| `POST /v1/databases/{db}/execute` | One statement in its own transaction | Read-only statement: the receiving pod. Otherwise the leader |
| `POST /v1/databases/{db}/batch` | Many statements or parameter sets in one transaction | The leader |
| `POST /v1/databases/{db}/describe` | Parameters and result columns of a statement, without executing it | The receiving pod |
| `POST /v1/databases/{db}/begin` | Start a session, optionally with its first statement | Write session: the leader. Read-only session: the receiving pod |
| `POST /v1/databases/{db}/session/execute` | One statement in a session | The session's owner |
| `POST /v1/databases/{db}/session/batch` | A batch in a session | The session's owner |
| `POST /v1/databases/{db}/session/describe` | Describe within a session, which sees its temporary objects | The session's owner |
| `POST /v1/databases/{db}/session/commit` | Commit a session | The session's owner |
| `POST /v1/databases/{db}/session/rollback` | Roll back or cancel a session | The session's owner |
| `POST /v1/databases/{db}/outcome` | Ask what happened to a commit identifier | The leader |
| `GET /v1/databases/{db}` | Status: epoch, transaction number, limits, SQLite version | The leader |
| `PUT /v1/databases/{db}` | Create a database | The leader, or the pod that wins creation |

Every request carries `Authorization: Bearer <JWT>`. `POST` and `PUT` requests carry `Content-Type: application/json`.

### 3. Request bodies

Common fields:

| Field | Type | Meaning |
|---|---|---|
| `sql` | string | Exactly one SQL statement. Trailing whitespace and comments are allowed; any further statement is `invalid_request`. A string that contains no statement, or contains the character U+0000, is `invalid_request`. |
| `params` | array of values | Positional parameters: element *i* binds parameter index *i* + 1. |
| `named` | object of values | Named parameters. The key is the name without its prefix; it binds every parameter written `:key`, `@key` or `$key`. |
| `min_txn` | integer, at least 0 | The lowest transaction number the request may observe. For a request that can commit, it is also the lower bound of the commit's position ([decision 10](#10-commit-identifiers-outcomes-and-retries)). Default 0. |
| `epoch` | integer, at least 0 | The leader epoch the driver knew when it created the commit identifier. 0 means unknown. |
| `commit_id` | string | 32 lowercase hexadecimal characters: 16 bytes from a cryptographically secure random source, created by the driver. |
| `timeout_ms` | integer, at least 1 | Lowers the statement time limit for this request. Values above the server's limit are capped to it. |
| `session` | string | The session token, on every `session/` request. |
| `seq` | integer, at least 1 | The session request's sequence number. |

A request uses `params` or `named`, not both. Every parameter of the statement must be bound exactly once: an unbound parameter, a parameter bound twice, an extra positional value or a name that matches no parameter is `invalid_request`. SQLite would bind a missing parameter as NULL; the protocol does not allow that.

Bodies per endpoint. "Required" fields must be present; the others are optional.

| Endpoint | Required | Optional |
|---|---|---|
| `execute` | `sql`, `commit_id`, `epoch` | `params` or `named`, `min_txn`, `timeout_ms` |
| `batch` | `commit_id`, `epoch`, and either `sql` with `param_sets`, or `statements` | `min_txn`, `timeout_ms` |
| `describe` | `sql` | `min_txn` |
| `begin` | `mode`: `"write"` or `"read_only"` | `min_txn`, `execute`: an object with `sql`, and `params` or `named`, and `timeout_ms` |
| `session/execute` | `session`, `seq`, `sql` | `params` or `named`, `timeout_ms` |
| `session/batch` | `session`, `seq`, and either `sql` with `param_sets`, or `statements` | `timeout_ms` |
| `session/describe` | `session`, `seq`, `sql` | |
| `session/commit` | `session`, `seq`, `commit_id`, `epoch` | `min_txn` |
| `session/rollback` | `session` | |
| `outcome` | `commit_id`, `epoch`, `min_txn` | |
| `PUT` (create) | | none: the body is `{}` |

`param_sets` is an array whose elements are each a positional array or a named object. `statements` is an array of objects with `sql` and `params` or `named`. Drivers send `commit_id` and `epoch` with every `execute` and `batch`, because a driver cannot tell from SQL text whether a statement writes. The `outcome` request must carry the `commit_id`, `epoch` and `min_txn` of the request whose outcome it asks about.

### 4. Responses and frames

Every response that skylite produces must carry these headers:

- `Skylite-Protocol: 1`. This is the marker. A response without it, such as a bare 502, 503 or 504 from a proxy, was not produced by skylite and proves nothing about whether a request was executed.
- `Skylite-Request-Id`, an identifier for logs, assigned by the pod that received the request from the client and passed on the forwarding hop.
- `Content-Type: application/x-ndjson` and `Cache-Control: no-store`.
- On 401, `WWW-Authenticate: Bearer`.

Every body, successful or not, is a sequence of frames. A frame is one JSON object or array on one line, ended by `\n`. The kinds:

| Frame | Form | When |
|---|---|---|
| session | `{"session":{...}}` | First frame of a successful `begin` |
| columns | `{"columns":[...]}` | Before the rows of a statement; an empty array for a statement that returns no rows |
| row | a JSON array of values, one per column | Zero or more, after `columns` |
| keepalive | `{"keepalive":{}}` | At most every keepalive interval while a statement produces nothing; drivers skip it |
| done | `{"done":{...}}` | Final frame of a success |
| error | `{"error":{...}}` | Final frame of a failure, which may come after rows |

The body of `execute`, of `session/execute` and of a `begin` with a statement is: `session` (begin only), `columns`, rows, and then `done` or `error`. The body of every other request is one `done` or `error` frame, preceded by `columns` for `describe` and by `session` for `begin`. Keepalive frames may appear anywhere before the final frame.

**A body without a final frame is incomplete.** The driver must report it as an error, never as a short result set, and must treat the request's outcome as not known: this is the same case as a lost response. That holds whether the body was cut by the server, by a proxy or by the network.

**Status codes.** A response whose request was accepted for processing has status 200, and any error it hits later arrives as its final frame. A request refused before processing gets the status listed for its code in [decision 11](#11-errors), with one `error` frame as its body. Statuses 502, 503 and 504 are never produced by the server, so a retry policy keyed on them never fires on a skylite answer. The only 5xx the server produces is 500, for `internal`. Drivers must decide from the frames and the marker, never from the status alone.

**`done` fields.**

| Request | Fields |
|---|---|
| `execute`, `session/execute`, begin with a statement | `txn`, `epoch`, `read_only`, `rows_affected`, `last_insert_rowid` |
| `batch`, `session/batch` | `txn`, `epoch`, `results`: one `{"rows_affected":n,"last_insert_rowid":n}` per statement or parameter set |
| `describe`, `session/describe` | `txn`, `params`: one `{"name":":id"}` or `{"name":null}` per parameter index |
| `begin` without a statement | `txn`, `epoch` |
| `session/commit` | `txn`, `epoch`, `committed`: `true` |
| `session/rollback` | `txn`, `rolled_back`: `true` |
| `outcome` | `outcome`: `"committed"`, `"not_committed"` or `"unknown"`; `txn` when committed; `epoch` |
| `GET` status | `txn` (the leader's head), `epoch`, `sqlite_version`, `limits` (the values in [decision 14](#14-limits-and-defaults)) |
| `PUT` create | `txn`, `epoch` |

`txn` is the transaction number of the database state the response reflects: the snapshot a read used, the transaction a write committed, or, inside a session, the session's snapshot. `epoch` is the serving leader's epoch or, on a follower, the epoch recorded in the latest commit object it has applied. `rows_affected` is SQLite's change count for a statement that is not read-only and 0 for a read-only statement, because SQLite does not reset its change count after a `SELECT`. `last_insert_rowid` must be reset to 0 before each statement outside a session, so that one client never sees a row id left on a pooled server connection by another; inside a session it keeps SQLite's per-connection meaning. ncruces/go-sqlite3 exposes both the read and the reset ([conn.go#L281-L295][nc-rowid]).

**The session frame** has `token` (the secret session token), `id` (a separate identifier that is not secret, for logs and error reports), `mode`, `epoch`, `txn` (the snapshot), `idle_timeout_ms` and `lifetime_ms`.

**The columns frame** has, per column: `name` (the result column name, `sqlite3_column_name`), `decltype` (declared type, or null for an expression), and `database`, `table` and `origin` (the source schema, table and column, or null). ncruces/go-sqlite3 exposes all five on a compiled statement ([stmt.go#L403-L452][nc-colmeta]), and its build enables the column metadata they need ([sqlite_opt.h#L27][nc-opt-meta]). Nullability, primary key and auto-increment are not carried; see [decision 16](#16-driver-conventions).

### 5. Values

The five SQLite storage classes must survive both directions without loss.

| Class | Encoding | Example |
|---|---|---|
| NULL | JSON `null` | `null` |
| INTEGER | JSON number with no fraction and no exponent. Readers must parse it as an exact signed 64-bit integer; a value outside that range is `invalid_request`. | `-9223372036854775808` |
| REAL | JSON number that contains `.`, `e` or `E`, so that it cannot be read as an integer. Writers must use a representation that parses back to the same double. Infinities are `{"real":"Infinity"}` and `{"real":"-Infinity"}`. | `1.0`, `2.5e-8`, `-0.0` |
| TEXT | JSON string, when the text is valid UTF-8 | `"Dune"` |
| TEXT that is not valid UTF-8 | `{"text_bytes":"<base64>"}`: the exact bytes, standard base64 with padding (RFC 4648 section 4) | `{"text_bytes":"/w=="}` |
| BLOB | `{"blob":"<base64>"}`, standard base64 with padding | `{"blob":""}` is the empty blob |

Rules that follow:

- **No NaN.** SQLite stores no NaN: binding one stores NULL ([vdbemem.c#L1092-L1098][sq-nan]). The protocol has no encoding for NaN; drivers must reject a NaN parameter with an error before sending it, and the server must reject one.
- **No booleans.** JSON `true` and `false` are `invalid_request` as parameter values. Booleans are a driver convention ([decision 16](#16-driver-conventions)).
- **No silent replacement of text.** Go's `encoding/json` replaces invalid UTF-8 and invalid surrogate escapes with U+FFFD when it decodes ([decode.go#L98-L101][go-json-dec]) and replaces invalid bytes when it encodes a string ([encode.go#L83-L84][go-json-enc]). Neither the server nor the Go driver may use it unmodified for values: request bodies that contain invalid UTF-8 or a lone surrogate escape must be rejected as `invalid_request`, and text that is not valid UTF-8 must be written in the `text_bytes` form. The Go driver will return such text as a Go `string` holding the exact bytes and will send a Go `string` that is not valid UTF-8 as `text_bytes`. A Java `String` cannot hold arbitrary bytes, so the JDBC driver will raise `SQLDataException` from `getString` for a `text_bytes` value and return the bytes from `getBytes`; it will reject a `String` parameter containing an unpaired surrogate.
- **Text and blobs stay apart**, and so do the empty string, the empty blob and NULL.
- A row frame reports each value's own storage class, so SQLite's per-value dynamic typing reaches the driver unchanged.

### 6. Statements and batches

- **One statement per execution.** The server compiles the first statement and must refuse anything after it other than whitespace and comments. ncruces/go-sqlite3 returns the uncompiled tail and returns neither a statement nor an error for text that holds only comments ([conn.go#L193-L203][nc-prepare]), which is enough to check this.
- **No server-side statement handles and no cursors.** SQL text is sent with every execution. The server may cache compiled statements privately.
- **Transaction control is not SQL.** `BEGIN`, `COMMIT`, `END` and `ROLLBACK` are refused with `invalid_request` everywhere; transactions are begun, committed and rolled back through the protocol. `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` are allowed only in a session. The server can detect these at compile time through SQLite's authorizer, which ncruces/go-sqlite3 exposes ([config.go#L202][nc-auth]).
- **Read-only, for routing.** A statement is read-only when `sqlite3_stmt_readonly` is true for it, which ncruces/go-sqlite3 exposes as `Stmt.ReadOnly` ([stmt.go#L70-L77][nc-ro]), and it is not refused above. SQLite returns true for transaction control and for `ATTACH` and `DETACH` ([sqlite.h.in#L4757-L4768][sq-readonly]), which is why transaction control is refused and why `ATTACH` and `DETACH` are left to the SQL lockdown record.
- **Stateless writes are buffered.** A statement executed outside a session that is not read-only commits when it finishes. The server must hold its result, up to the result-size limit, and send nothing but keepalive frames until the commit is confirmed in object storage, so that no client reads rows of a write that then fails to commit. Inside a session rows stream as they are produced, because nothing is committed until the session commits.
- **Batches.** A batch is one statement with many parameter sets (`sql` with `param_sets`, for JDBC `PreparedStatement.executeBatch`) or a list of statements (`statements`, for `Statement.executeBatch`). Outside a session it runs in one transaction on the leader and commits once. Inside a session it runs inside a savepoint. It stops at the first error: outside a session nothing is committed, and inside a session the savepoint is rolled back and the session stays active. The error frame carries `batch_index`, the zero-based index of the failing entry. A batch entry that returns columns is refused with `invalid_request` before anything executes, because JDBC batches return update counts only.
- **Describe** compiles the statement and returns the columns frame and the parameter names without executing anything. JDBC's `PreparedStatement.getMetaData` and `getParameterMetaData` need it. It follows the routing of a read.

### 7. Routing

- **Any pod accepts any request.** Clients are configured with one base URL. They never receive pod addresses and never send a token anywhere else.
- **Reads are served where they land.** A pod that receives `execute`, `describe` or a read-only `begin` serves it if the statement compiles as read-only (for `execute`) and the pod's state is at least `min_txn` ([decision 9](#9-reads-on-followers-and-read-your-writes)).
- **Everything else is forwarded to its owner**: writes, batches, write sessions, `outcome` and `GET` status to the leader, session requests to the session's owning pod. A follower that cannot compile a statement also forwards it to the leader, because the follower may not have the schema change that the statement needs yet.
- **One hop at most.** The forwarding pod sends the same method, path and body, with the caller's original `Authorization` header, and adds `Skylite-Forwarded: 1` and the request id. The receiving pod verifies the bearer token again, as if the request came from the client: the hop is never trusted for authentication or authorisation. A pod never forwards a request that carries `Skylite-Forwarded`. If such a request lands on a pod that is not its owner, the pod answers with `no_leader`, `not_serving` or `lost_session`, with `not_executed` true. A client that sets the header itself gains nothing: its request is just never forwarded.
- **The forwarding pod relays the owner's response unchanged**, frames and headers. If the hop fails after the owner started its response, the forwarding pod ends the body without a final frame; it must never invent one.
- **Pod-to-pod requests must authenticate the receiving pod** (mesh mutual TLS with a checked identity, or server TLS with a cluster certificate), and a pod must take forwarding addresses only from the cluster's own records, never from a request. Otherwise a forged address would collect callers' bearer tokens. How pods find the leader and each other is the cluster membership design, a later record.

### 8. Sessions

A session is one SQLite connection on one pod. Sessions exist only for explicit transactions.

**Token.** The token is opaque to the client, at most 256 characters from `A-Z a-z 0-9 . _ -`, and contains at least 128 bits from a cryptographically secure random source. It names the owning pod, by an identifier that is not an address, and the leader epoch at begin. The owner binds the session to the database and to the authenticated principal: the token's issuer (`iss`) and subject (`sub`). A token from a previous epoch, an unknown token, or a token presented by another principal gets `lost_session` and never creates a session. A token presented by another principal also leaves the real session untouched, so that a stolen token cannot be used to roll back someone else's transaction.

**Write sessions** live on the leader and begin with `BEGIN IMMEDIATE`. A write session holds the database's single write lock from begin until it commits, rolls back or expires. While it holds it, every other writer, whether a stateless write, a batch or another write session's begin, waits for the lock. The leader must grant the lock in arrival order. A writer that has waited for the lock-wait limit gets `busy`, with `not_executed` true, and nothing of it ran.

**Read-only sessions** are declared at begin with `"mode":"read_only"` and live on whichever pod received the begin, subject to `min_txn`. Their connection must refuse writes: a write fails with `sql_error` carrying SQLite's `SQLITE_READONLY`.

**Isolation.** SQLite gives serializable isolation: a write session holds the only write lock, and a read-only session reads one snapshot. A request for a weaker level is served at serializable. The JDBC specification permits this: its section 10.2.1 says that a driver may substitute a higher, more restrictive level when the requested one is not supported (quoted from the JDBC 4.3 specification; this record could not fetch the document to check the wording, so the quotation is unverified). The `setTransactionIsolation` Javadoc does not forbid it ([Connection.java#L425-L448][jdk-iso]). The JDBC driver will accept the four standard levels and report `TRANSACTION_SERIALIZABLE` from `getTransactionIsolation`. The Go driver contract requires an error for a level the driver does not support ([driver.go#L279-L294][go-begintx]). The Go driver will treat a level as supported when serializable gives every guarantee that level names: it will accept `LevelDefault`, `LevelReadUncommitted`, `LevelReadCommitted`, `LevelWriteCommitted`, `LevelRepeatableRead`, `LevelSnapshot` and `LevelSerializable` ([sql.go#L126-L141][go-iso]), and return an error for `LevelLinearizable`, because a follower may serve a snapshot that is not the newest. `TxOptions.ReadOnly` selects a read-only session.

**Begin combined with the first statement.** `begin` may carry `execute`, which runs as the session's first statement. The response is the session frame, then that statement's result. If the statement fails with an error after which SQLite keeps the transaction, the session stays active and the error says so.

**Sequence numbers.** One request may be in flight per session. Each request after begin carries `seq`, starting at 1 and increasing by one. The server must consume a sequence number when it accepts an authenticated request for the session, whatever the request's result. A repeated or out-of-order number is refused with `sequence_violation`, and the session is rolled back. Two exceptions:

- `session/rollback` carries no sequence number and may be sent while another request is in flight. It is how a session is cancelled.
- A `session/commit` is first checked against the commit table by its `commit_id`, before its sequence number. A repeated delivery of a commit that already committed is answered `already_committed`, not with a rollback, because a committed transaction cannot be rolled back.

Authentication is checked before the sequence number, so a request refused as `unauthenticated` consumes none, and the driver can resend it with a refreshed token.

**Timeouts.** Each session has an idle timeout and an absolute lifetime, enforced by the server and reported in the session frame. Idle time runs from the end of one response to the arrival of the next request; a request in progress is never idle, and its own time is bounded by the statement time limit. An expired session is rolled back. A commit that has begun its upload is never interrupted by either timer.

**Lost responses inside a session.** If the response to any session request other than commit is lost, the driver will send `session/rollback`, without waiting for its answer to decide anything, and report the transaction as failed and not committed. A session's statements commit nothing until its commit, so nothing more is needed. Only the commit needs outcome recovery.

**Session states.** The server keeps one state per session. Every transition, and what the client is told:

| From | Event | To | The client is told |
|---|---|---|---|
| (none) | Begin succeeds | active | Session frame with token and limits |
| (none) | Write lock not granted within the lock-wait limit | (none) | `busy`, `not_executed` true |
| active | Statement succeeds | active | Its result |
| active | Statement fails and SQLite keeps the transaction (for example a constraint violation) | active | `sql_error`, `session_state` `"active"` |
| active | Statement fails and SQLite rolls the transaction back (for example an interrupted write ([sqlite.h.in#L2934-L2937][sq-interrupt]), or a full disk) | rolled back | The error, `session_state` `"rolled_back"` |
| active | `session/rollback` | rolled back | `done` with `rolled_back` |
| active | Repeated or out-of-order `seq` | rolled back | `sequence_violation` |
| active | The client abandons an in-flight request, or the server's statement time limit ends it | rolled back | `cancelled` or `timeout` if a response can still be written; otherwise the next request gets `lost_session` |
| active | Idle timeout or lifetime reached | expired (rolled back) | The next request gets `lost_session` |
| active | The owning pod stops serving the database, restarts, or sees a new leader epoch | lost | The next request gets `lost_session` |
| active | `session/commit` accepted | committing | Nothing yet |
| committing | Upload confirmed; or a write session that changed nothing; or a read-only session | committed | `done` with `txn` |
| committing | Definite failure: the transaction is too large for one commit object, the upload definitely failed, or the commit window has closed | rolled back | The error, `outcome` `"not_committed"` |
| committing | Upload outcome not definite within the engine's bound ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)) | outcome unknown | `commit_outcome_unknown`; the pod stops serving the database |
| committing | Another delivery of the same commit | committing | `commit_in_progress` |
| committing | `session/rollback` | committed or rolled back, whichever the commit reaches | After the commit resolves: `already_committed` with `txn`, or `done` with `rolled_back` |
| committed | Another delivery of the same commit | committed | `already_committed` with `txn` |
| committed, rolled back, expired or lost | Any other session request | (unchanged) | `lost_session` |
| outcome unknown | `outcome` request, after a takeover | (resolved) | `committed`, `not_committed` or `unknown` ([decision 10](#10-commit-identifiers-outcomes-and-retries)) |

### 9. Reads on followers and read-your-writes

- Every `done` frame reports `txn`, the transaction number of the state it reflects. Error frames report it where known.
- A request may carry `min_txn`. A pod whose state is older waits up to the follower catch-up limit for it, then forwards the request to the leader. A leader whose head is below `min_txn` answers `invalid_request`, because no client can have seen a transaction that is not in the history.
- Each driver keeps, per pool, the highest `txn` and the highest `epoch` it has seen in any response, and sends them as `min_txn` and `epoch` by default. That gives read-your-writes and monotonic reads within one client process, across all connections of its pool. The suite's test of a commit seen by a connection that is already open ([0003](/docs/decisions/0003-jdbc-transport/)) depends on it.
- Followers arrive in a later milestone. Until then non-leader pods forward everything. The protocol defines follower reads from the start, so that drivers need no change when followers arrive.

### 10. Commit identifiers, outcomes and retries

**Which requests carry a commit identifier.** `execute`, `batch` and `session/commit`. The driver creates the identifier once per logical write and keeps the `epoch` and `min_txn` it sent with it.

**What the server stores.** The leader must write into the commit object's commit identifier field 32 bytes: the 16 bytes of `commit_id`, then the first 16 bytes of SHA-256 over the caller's `iss`, a zero byte and `sub`. One principal therefore cannot find, block or collide with another principal's commit. The format's field holds up to 64 bytes (`internal/format/doc.go` on the `feat/engine-format` branch).

**The commit window.** Let *X* be a commit identifier sent with epoch *e* and `min_txn` *s*, and let *W* be the commit window ([decision 14](#14-limits-and-defaults)). The rules:

1. Only the leader whose epoch equals *e* may commit *X*.
2. *X* may commit only at a transaction number at most *s* + *W*. The leader checks this when it holds the write lock and knows the next number, before anything executes for a stateless request and before the upload for a session commit. A write session holds the write lock from begin, so its commit is always inside the window when `min_txn` is at least its snapshot.

Together they make *X*'s possible positions known and bounded: a commit of *X* can only be the commit object at a transaction number in (*s*, *s* + *W*], written in epoch *e*. No clock is involved.

**The commit table.** The leader keeps, in memory, one entry per commit identifier and principal it has seen in its term: in flight, committed at *N*, or refused with an error. An entry may be dropped once the head reaches its *s* + *W*, because rule 2 then stops it from ever committing. The table therefore holds at most about *W* entries.

**What the leader does with a request that carries *X*:**

1. If the table has *X*: in flight gives `commit_in_progress`; committed gives `already_committed` with *N*; refused gives the recorded error again. A repeated delivery never executes.
2. If *e* is greater than the leader's own epoch, the leader reads the manifest. If the manifest's epoch is greater than its own, a takeover has begun: the pod stops serving the database, as [0004](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop) requires once a leader may be fenced, and answers `not_serving`. Otherwise no leader has held epoch *e*, and the answer is `invalid_request`. A client-supplied number never stops a leader on its own.
3. If *e* is lower than the leader's own epoch, or the window is closed (head + 1 > *s* + *W*), the leader looks for *X* in the commit objects at (*s*, min(head, *s* + *W*)]. Found gives `already_committed`. Not found, with every one of those objects still retained, gives `stale_epoch` (for an older epoch) or `stale_request` (for a closed window), final, and the refusal is recorded. If any of them has been deleted, the answer is `commit_outcome_unknown`. The lookup is needed for a closed window too, because the table may already have dropped an entry for *X*.
4. Otherwise the leader records *X* as in flight and executes.

**Keeping `min_txn` and `epoch` fresh.** A driver whose highest transaction number and epoch were last updated more than the freshness bound ago ([decision 14](#14-limits-and-defaults)) must refresh them with `GET` status before it creates a commit identifier. Without that, an idle client's first write after many others' writes would carry a `min_txn` so old that its window is closed and its lookup range no longer retained, and the honest answer would be unknown. The bound is a driver-local liveness rule: no server guarantee depends on it. It works as long as a database cannot make *W* commits within the freshness bound; with one upload per commit ([0004 decision 10](/docs/decisions/0004-storage-engine/#10-limits-accepted-in-the-first-version)) and the defaults, that needs more than 1,000 commits a second, which is an inference about upload latency, not a measurement. A change such as group commit must revisit *W*.

The lookup in step 3 reads 144-byte commit headers by range request. The leader keeps the identifiers of the last *W* commits in memory once it has read them, so the cost is at most *W* range reads once per takeover, and usually a few, because a driver's `min_txn` is usually close to the head.

**The outcome request.** `outcome` asks the leader about *X*. The answers:

- **Committed at *N*:** *X* is in the commit table as committed, or in a commit object in (*s*, min(head, *s* + *W*)].
- **Definitely not committed:** *X* is in none of the commit objects in (*s*, min(head, *s* + *W*)], every one of them is still retained, and one of these holds:
  - the window is closed (head ≥ *s* + *W*), so no commit of *X* can still happen;
  - *e* is lower than the answering leader's epoch. Every leader of epoch *e* is fenced at the epoch-bump commit that the next leader created: its commits all sit at transaction numbers below that bump, which the walk of the log has passed ([0004 decision 8](/docs/decisions/0004-storage-engine/#8-compaction-and-garbage-collection)), and every later leader refuses *X* by step 3;
  - *e* equals the answering leader's epoch, and *X* is not in its table. The leader records *X* as refused before it answers, so no later delivery can commit it.
- **Unknown:** any other case. That covers 0004's indeterminate commit before a takeover has resolved it, a range in which some commit object has been deleted, and a leader that cannot read storage.

The `outcome` answer waits, up to the outcome wait limit, for an *X* that is in flight, and otherwise answers unknown.

**A commit that changed nothing** creates no commit object. The leader records it in its table as committed at the current head. After a leader change its outcome reads as not committed. That is harmless: the state is the same either way.

**The requirement this places on garbage collection.** "Retained" above is the condition for a definite answer. It needs two rules that [0004 decision 8](/docs/decisions/0004-storage-engine/#8-compaction-and-garbage-collection) does not yet have:

- Garbage collection must not delete a commit log object among the newest *W* transaction numbers, and must not delete one younger than the outcome horizon (default one hour). The first rule makes every answer definite for a request whose `min_txn` is within *W* of the head; the second gives a known duration within which an outcome request is answered definitely.
- The engine must know the lowest transaction number whose commit object is still guaranteed present, so that a lookup never mistakes a deleted object for an absent identifier.

Neither rule is needed for safety. Rule 1 and rule 2 above stop *X* from committing twice whatever garbage collection does; retention only decides whether "not committed" can be said, or only "unknown". An alternative that keeps less: compaction could write, for each range it folds, a small object listing each folded commit's transaction number and identifier, retained under the same rules, so that the folded commit objects themselves can go.

**What the `not_executed` flag and the `outcome` field mean.**

- `not_executed` is about one delivery: true means skylite guarantees this request had no effect. It is in every error frame.
- `outcome` is about the commit identifier, and is present on error frames for requests that carry one. `"not_committed"` means *X* has not committed and never will; `"committed"` comes with `txn`; `"unknown"` means this answer does not decide it. `outcome` may be `"not_committed"` only when set by: the leader of epoch *e* after its table check, a later leader after the lookup in step 3, any pod for a statement it compiled as read-only, or any pod for a body it rejects as `invalid_request`.
- A driver must read `outcome` before `not_executed`. A request that landed on a pod that is not its owner is `not_executed` true, and still says nothing about *X*: another delivery of the same request may have reached the leader.

**Driver rules for a request that carries a commit identifier.**

1. `done`: the write committed.
2. `outcome` `"committed"` (`already_committed`): the write committed. Counts and row ids are not known. The Go driver's `Result` returns `skylite.ErrResultUnavailable` from `RowsAffected` and `LastInsertId`; the JDBC driver raises the exception below instead of inventing an update count.
3. `outcome` `"not_committed"`: definite. For `busy`, `stale_epoch` and `stale_request` the driver may retry automatically, with a new commit identifier and the `epoch` and `min_txn` it has just learned. For any other code it reports the error.
4. Anything else, including a response without the marker, a body without a final frame, a transport error, `not_executed` true without an outcome, `commit_in_progress`, `no_leader` and `commit_outcome_unknown`: the driver sends `outcome` requests for *X*, with backoff, until it gets a definite answer or its outcome budget runs out, and then reports unknown. It never sends a new identifier for the same write without a definite "not committed".
5. A response that does not carry the marker is never read as an answer.

Reads carry a commit identifier too, so a stateless read whose response is lost costs an outcome request before it is retried. Lost responses are rare; the alternative would be a driver that guesses from SQL text whether a statement writes.

Drivers retry reads that carry no commit identifier (`describe`, `GET`, a read-only `begin`) on any failure. The Go driver will never return `driver.ErrBadConn` for an operation that may have executed, as the `database/sql` contract requires ([driver.go#L151-L163][go-badconn]), because `database/sql` retries on it ([sql.go#L1569-L1580][go-retry]). In practice it will return it only from an operation that sent no request.

**What each driver surfaces.**

| Outcome | Go driver | JDBC driver |
|---|---|---|
| Committed | `nil` error | Normal return |
| Committed, counts unavailable | `nil` error; `Result` methods return `ErrResultUnavailable` | `SQLException`, SQLState `SK001`, message naming the transaction number. For `commit()`, a normal return, because no count is needed |
| Busy, not executed | `*skylite.Error`, `errors.Is(err, skylite.ErrBusy)` and `errors.Is(err, skylite.ErrNotCommitted)` | `SQLTransactionRollbackException`, SQLState `40001`, error code 5 (`SQLITE_BUSY`) |
| Definitely not committed, other causes, including a lost or rolled-back session | `*skylite.Error`, `errors.Is(err, skylite.ErrNotCommitted)` | `SQLTransactionRollbackException`, SQLState `40000`; constraint violations use the row below |
| Constraint violation | `*skylite.Error` with SQLite's codes, `ErrNotCommitted` | `SQLIntegrityConstraintViolationException`, SQLState `23505` (unique or primary key), `23502` (not null), `23503` (foreign key), `23514` (check), `23000` (other); error code 19 |
| Unknown | `*skylite.Error`, `errors.Is(err, skylite.ErrOutcomeUnknown)`, with the commit identifier, epoch and `min_txn` so that the application can ask later | `SQLNonTransientConnectionException`, SQLState `08007` ("transaction resolution unknown") |

The JDBC error code is SQLite's primary result code, the low eight bits of the extended code, because Hibernate's community SQLite dialect switches on primary codes ([SQLiteDialect.java#L451-L470][hib-sqlite-exc]); the extended code is available from the driver's exception class.

The `busy` mapping reaches a retryable Spring exception by both paths. Through Hibernate, the dialect turns `SQLITE_BUSY` into `LockAcquisitionException` ([SQLiteDialect.java#L458-L460][hib-sqlite-exc]), as does SQLState `40001` ([SQLStateConversionDelegate.java#L51-L52][hib-state]), and Spring turns that into `CannotAcquireLockException` ([HibernateExceptionTranslator.java#L159-L160][spring-het]). Through plain JDBC, Spring turns `SQLTransactionRollbackException` with `40001` into `CannotAcquireLockException` ([SQLExceptionSubclassTranslator.java#L85-L90][spring-sub], [SQLStateSQLExceptionTranslator.java#L207-L209][spring-state-cal]). `CannotAcquireLockException` is a `PessimisticLockingFailureException`, which is a `ConcurrencyFailureException`, which is a `TransientDataAccessException` ([CannotAcquireLockException.java#L31][spring-cale], [PessimisticLockingFailureException.java#L36][spring-plfe], [ConcurrencyFailureException.java#L33][spring-cfe]). The unknown outcome deliberately avoids SQLState class 40, which Spring maps to a pessimistic-locking failure ([SQLStateSQLExceptionTranslator.java#L77-L80][spring-state-40]) that generic retry logic would retry. Class 08 is a connection exception, which Hibernate turns into `JDBCConnectionException` ([SQLStateConversionDelegate.java#L78-L80][hib-state-08]) and Spring into a non-transient resource failure (inference from the translator source; not run).

**Retries that are not the driver's.** A delivery can be repeated below the driver. The design does not depend on any of these behaving well, but their defaults matter for how often outcome recovery runs:

- **Go's HTTP transport** retries a request on a reused connection only if nothing was written, or if the request is replayable, and a `POST` is replayable only with a rewindable body and an `Idempotency-Key` or `X-Idempotency-Key` header ([transport.go#L89-L96][go-transport-doc], [transport.go#L818-L863][go-shouldretry], [request.go#L1534-L1548][go-replayable]). Its HTTP/2 path also retries after a refused stream, a GOAWAY, or a protocol error from the peer, when the body can be rewound ([h2_bundle.go#L7969-L8013][go-h2retry]); a protocol error does not prove the server did not process the request (inference). The Go driver will clear `Request.GetBody` and never send an idempotency header, so the transport has no body to resend.
- **The JDK HTTP client** retries a request the peer reports as unprocessed, and retries after a connection failure; any other failure is retried only for `GET` and `HEAD`, unless the `jdk.httpclient.enableAllMethodRetry` property is set ([MultiExchange.java#L491-L528][jdk-retry-cfg], [MultiExchange.java#L537-L575][jdk-retry]).
- **Istio's default retry policy** is two retries on `connect-failure,refused-stream,unavailable,cancelled,retriable-status-codes`, with no retriable status codes in the default policy, and `reset-before-request` on inbound routes ([retry.go#L34-L54][istio-retry]). In Envoy, `connect-failure` and `refused-stream` cover requests the upstream never processed, `reset-before-request` covers requests whose headers were never sent, and `unavailable` and `cancelled` are gRPC status codes ([router_filter.rst#L92-L128][envoy-retry], [router_filter.rst#L149-L175][envoy-grpc]). With these defaults Istio should not resend a request that skylite processed (inference from the source; not tested through a mesh). An operator who adds `5xx`, `reset` or `gateway-error` gets retries after the upstream may have executed the request ([router_filter.rst#L74-L91][envoy-retry-5xx]).
- **ingress-nginx** defaults `proxy-next-upstream` to `error timeout` ([configmap.md#L179][ing-next]), and nginx does not pass a `POST` to the next upstream after it was sent unless `non_idempotent` is configured ([ngx_http_upstream.c#L4701-L4708][ngx-nonidem]).

Duplicates are harmless by construction. A repeated stateless write or commit meets its own commit table entry, or the window and epoch rules, and never executes twice. A repeated session statement meets the sequence check and rolls the session back, which commits nothing. The [duplicated delivery example](#example-5-a-duplicated-delivery) walks through both.

### 11. Errors

One error object:

```json
{"error":{
  "code": "busy",
  "not_executed": true,
  "outcome": "not_committed",
  "message": "the write lock was not granted within 5000 ms",
  "sqlite_code": 5,
  "sqlite_extended_code": 5,
  "request_id": "01JDZ3W4Q8M6",
  "session_id": "7c1d2e3f40a1b2c3",
  "session_state": "active",
  "txn": 1209,
  "epoch": 3
}}
```

`code`, `not_executed`, `message` and `request_id` are always present. `outcome` is present for requests that carry a commit identifier. `sqlite_code` (primary) and `sqlite_extended_code` are present when SQLite produced the error. `session_id` and `session_state` are present for session requests; `session_state` is `"active"`, `"rolled_back"` or `"committed"`. `txn` and `epoch` are present when known; for `already_committed` `txn` is the commit's, for `stale_request` it is the head, for `stale_epoch` `epoch` is the leader's. `batch_index` is present for a failed batch entry. The session token never appears in an error.

The closed list of codes. "Status" is the HTTP status when the error is the whole response; an error after processing began arrives as the final frame of a 200 response.

| Code | Status | Meaning | `not_executed` |
|---|---|---|---|
| `invalid_request` | 400 | Malformed body, unknown field, bad value, more than one statement, transaction-control SQL, binding mismatch, `min_txn` ahead of the head | true |
| `unauthenticated` | 401 | Bearer token missing, malformed, expired or not verifiable | true |
| `forbidden` | 403 | The principal is not permitted this operation on this database | true |
| `statement_forbidden` | 403 | The SQL lockdown refuses the statement | true |
| `database_not_found` | 404 | No such database | true |
| `database_exists` | 409 | `PUT` for a database that exists | true |
| `request_too_large` | 413 | Body above the request size limit | true |
| `lost_session` | 410 | Unknown, expired or ended session, previous epoch, or another principal | true when the owning pod decided it; false when the owner could not be reached and the request was a commit |
| `sequence_violation` | 409 | Repeated or out-of-order `seq`; the session has been rolled back | true |
| `busy` | 409 | The write lock was not granted within the lock-wait limit, or the pod's session slots are full | true |
| `no_leader` | 409 | No leader is available, or the leader could not be reached | true only if the forwarding pod knows the request was never sent |
| `not_serving` | 409 | This pod stopped serving this database: it was fenced, failed, or is shutting down | true if refused before execution |
| `stale_epoch` | 409 | The commit identifier was created under an older epoch and has not committed | true |
| `stale_request` | 409 | The commit identifier's window has closed | true |
| `commit_in_progress` | 409 | Another delivery of this commit identifier is executing | true |
| `already_committed` | 409 | This commit identifier committed at `txn` | true (this delivery did nothing; `outcome` is `"committed"`) |
| `commit_outcome_unknown` | 200 (final frame) | The commit's upload was indeterminate, or the lookup range is no longer retained | false |
| `timeout` | final frame | The statement time limit was reached. Never reported for a commit past the start of its upload | true |
| `cancelled` | final frame | The client cancelled the request or the session | true |
| `result_too_large` | final frame | The result passed the result-size limit | true |
| `sql_error` | 400 or final frame | SQLite reported an error; its codes are in the object | true |
| `internal` | 500 | Unexpected server failure | false |

`timeout`, `cancelled`, `result_too_large` and `sql_error` mean the statement had no effect. Inside a session `session_state` says whether the transaction survived it. Messages must not contain bearer tokens, session tokens or parameter values; SQLite's own messages name tables, columns and constraints rather than bound values (inference, not checked message by message); a trigger's `RAISE` message is application text and may contain anything.

### 12. Authentication in the protocol

- Every request carries a bearer token, and every pod that handles a request verifies it, the forwarding hop included.
- A session accepts a refreshed token for the same issuer and subject. A token for another principal gets `lost_session`, as in [decision 8](#8-sessions).
- `unauthenticated`, `forbidden` and `statement_forbidden` are distinct codes.
- Bearer tokens, session tokens and SQL parameter values must never appear in logs or error text. SQL text can contain literals; whether it is logged belongs to the observability record.
- Named gaps, for a later record: how token claims map to permissions on a database and on operations, including who may create a database; the SQL lockdown rules, including `ATTACH`, `DETACH`, `PRAGMA`, extension loading, and connection-scoped functions such as `last_insert_rowid()` outside a session, which on a pooled server connection would return another request's value; and the issuer, audience and key configuration for token verification.

### 13. Cancellation

- **A stateless request** is cancelled by abandoning the HTTP request: closing the connection on HTTP/1.1, resetting the stream on HTTP/2. The server must interrupt the SQLite statement when the request's context ends; ncruces/go-sqlite3 ties a context to a connection's interrupt ([conn.go#L328-L348][nc-interrupt]). A write that has not reached its commit is rolled back. A write whose upload has begun runs to its outcome, because the commit point cannot be interrupted; the client, having left, learns it by an outcome request. `timeout_ms` is the cancellation a client can set in advance; the Go driver will send the context's remaining time in it, and the JDBC driver will send `Statement.setQueryTimeout` in it.
- **A session** is cancelled by `session/rollback`, which may be sent while a request is in flight. The server interrupts the statement in flight, rolls back and answers `done` with `rolled_back`. If a commit is in flight, rollback waits for it to resolve and reports the result: `already_committed` with `txn` if the commit crossed its durable point, or `rolled_back`. Abandoning an in-flight session request also ends the session with a rollback, unless it is a commit past the start of its upload.
- JDBC `Statement.cancel()` on a statement in a session therefore rolls the transaction back, not just the statement.

### 14. Limits and defaults

Every limit is server configuration, reported by `GET` status, and the session limits also in the session frame.

| Limit | Default |
|---|---|
| Request body | 16 MiB |
| Entries in one batch | 10,000 |
| Result size: bytes of all frames of one response | 64 MiB |
| Statement time, from arrival to the last byte written, excluding a commit upload in progress | 30 s; a request may lower it |
| Keepalive interval | 10 s |
| Lock wait | 5 s |
| Follower catch-up wait for `min_txn` | 500 ms |
| Write session idle timeout | 10 s |
| Write session lifetime | 60 s |
| Read-only session idle timeout | 30 s |
| Read-only session lifetime | 120 s |
| Read-only sessions per pod and database | 256 |
| Outcome wait for an in-flight commit | 5 s |
| Commit window *W* | 10,000 transactions |
| Outcome horizon, the minimum age a commit object is retained | 1 hour |
| Driver retries of definite, retryable errors | 3 attempts, backoff from 50 ms |
| Driver outcome budget | 30 s |
| Driver freshness bound for `min_txn` and `epoch` | 10 s |

**Why the statement time includes writing the last byte.** On the leader, an open read transaction pins a WAL snapshot. Checkpoints cannot backfill past it and the WAL cannot restart while it is in use, so the local WAL grows, and the leader refuses writes once the WAL reaches its bound ([0004 decision 10](/docs/decisions/0004-storage-engine/#10-limits-accepted-in-the-first-version)). A client that reads a result slowly keeps that snapshot open for as long as it reads. Bounding only execution would let one slow reader stop every writer. The bound ends the result with a `timeout` frame, or ends the connection if even that cannot be written.

**Intermediaries that buffer.** The design is correct either way. ingress-nginx turns response buffering off by default ([configmap.md#L193][ing-buffer]). nginx's own default is on: it reads the upstream response as fast as possible into memory buffers and, if needed, a temporary file ([ngx_http_proxy_module.xml#L172-L200][ngx-buffering]). Buffering changes when bytes arrive, not which bytes: the frames reach the driver intact, later and perhaps all at once. The server finishes writing sooner, into the proxy, which only helps the leader release its snapshot. If the upstream fails after the headers were sent, a proxy can only end the body, and the driver sees a body without a final frame, which is defined as incomplete. ingress-nginx's read timeout applies between two reads, not to the whole response ([configmap.md#L1066-L1068][ing-read]), with a default of 60 s ([configmap.md#L172][ing-read-default]); the keepalive frames keep a long statement from hitting it when the proxy does not buffer. Whether a given buffering proxy forwards keepalive frames before it has a full buffer is unverified.

### 15. Database creation and status

- `PUT /v1/databases/{db}` with body `{}` creates a database: the engine builds the first commit object and the database identity ([0004 decision 5](/docs/decisions/0004-storage-engine/#5-commits-are-captured-from-sqlites-write-ahead-log)). It answers 201 with `done`, or `database_exists`. Creation is create-if-absent, so a repeated delivery cannot create two databases; it answers `database_exists`, and a client that needs to know whether its own request created the database must check by other means.
- `GET /v1/databases/{db}` returns the leader's status. It is safe for any layer to retry, and the drivers will use it for `Ping`, `Connection.isValid` and for refreshing `min_txn` and `epoch` ([decision 10](#10-commit-identifiers-outcomes-and-retries)).
- Deletion and listing are later work.

### 16. Driver conventions

The protocol carries only the five storage classes. Everything else is a driver convention, shared by both drivers so that Go and Java services can share a database.

- **Booleans** are INTEGER 1 and 0. The Go driver will encode `bool` that way; `database/sql` scans an integer 1 or 0 into `bool` ([convert.go#L382-L387][go-scan-bool], [types.go#L49-L58][go-bool]). The JDBC driver will send `setBoolean` as 1 or 0 and read any non-zero integer as true. Hibernate's boolean binder calls `setBoolean` ([BooleanJdbcType.java#L89][hib-bool]); that the SQLite dialect uses that binder for `Boolean` attributes is inference.
- **Timestamps** are TEXT in the form `YYYY-MM-DD HH:MM:SS.fffffffff`, with exactly nine fraction digits and no zone suffix. The Go driver will write `time.Time` converted to UTC. The JDBC driver will write the wall-clock fields of the value in the zone JDBC prescribes: the `Calendar` passed to `setTimestamp`, or the JVM default zone. Dates are `YYYY-MM-DD` and times `HH:MM:SS.fffffffff`. Reasons: the Hibernate SQLite dialect computes date parts with SQLite's `strftime` ([SQLiteDialect.java#L176-L186][hib-sqlite-strftime]), which needs SQLite's text date format; SQLite reads any number of fraction digits ([date.c#L220-L231][sq-frac]), though its date functions keep only milliseconds; and a fixed width makes text order equal time order for values in one zone. The Go driver will return `time.Time` for a TEXT value in a column whose declared type is `DATE`, `DATETIME` or `TIMESTAMP`, because `database/sql` converts to `time.Time` only from a `time.Time` source ([convert.go#L281-L288][go-scan-time]) and otherwise ends in an unsupported-scan error ([convert.go#L488][go-scan-err]) (read from source; the path for a string source was not run). Both drivers will also read SQLite's own forms, with `T` or a space, optional fraction, optional `Z` or offset, and will reject anything else instead of guessing. [0003](/docs/decisions/0003-jdbc-transport/) found the baseline driver kept milliseconds only; this convention keeps nanoseconds.
- **Decimals** are TEXT in plain notation (`BigDecimal.toPlainString()` in Java, the decimal library's string form in Go). SQLite's NUMERIC affinity converts such text to INTEGER or REAL when it is stored in a column declared `DECIMAL` or `NUMERIC`, keeping about 15 significant digits (SQLite's documented affinity rules; not re-checked for this record: unverified). The drivers will read INTEGER, REAL and TEXT back into a decimal; values that need more precision need a column declared `TEXT`. The Hibernate SQLite dialect declares `DECIMAL` columns from version 3 on ([SQLiteDialect.java#L119-L129][hib-sqlite-types]).
- **JDBC metadata.** `ResultSetMetaData` gets label, column name, table, schema and declared type from the columns frame. `getColumnType` will be derived in the driver from the declared type by SQLite's affinity rules. Nullability, auto-increment, precision and scale are not carried: the driver will report `columnNullableUnknown` and `false`, and derive precision and scale from a declared type such as `DECIMAL(12,2)`. `ParameterMetaData` gets the count and names from `describe`; SQLite parameters have no type, so the driver will report `Types.OTHER`. `DatabaseMetaData` (`getTables`, `getColumns`, `getPrimaryKeys`, `getImportedKeys`, `getIndexInfo`) will be implemented in the driver with SQL over `sqlite_schema` and SQLite's pragma table-valued functions, so the protocol needs no metadata endpoints; the lockdown record must allow those read-only queries. The product name `SQLite` and the version from `GET` status are what Hibernate's community dialect resolution reads (from the research report; not re-checked for this record: unverified). Which of the baseline failures in [0003](/docs/decisions/0003-jdbc-transport/) the driver or the dialect takes is the driver's design; this protocol carries what either choice needs, and ncruces's build turns foreign keys on by default ([sqlite_opt.h#L22][nc-opt-fk]).
- **Generated keys.** JDBC `getGeneratedKeys` and Go's `LastInsertId` use `last_insert_rowid` from the final frame. Hibernate's SQLite dialect instead runs `select last_insert_rowid()` as a separate statement (per [0003](/docs/decisions/0003-jdbc-transport/)), which is correct inside a session because the session is one connection, and is why that function is a lockdown question outside one.
- **Transactions in the drivers.** Both drivers will begin a session lazily, with the first statement after `BeginTx` or `setAutoCommit(false)`, using the combined begin. The JDBC driver will choose a read-only session when `Connection.isReadOnly()` is true at that point. Spring calls `setReadOnly(true)` on the connection before it begins a read-only transaction ([HibernateJpaDialect.java#L132-L147][spring-hjd], [DataSourceUtils.java#L195-L206][spring-dsu], [DataSourceUtils.java#L232-L234][spring-dsu-ro]), when the Hibernate connection handling mode keeps the connection for the transaction, which Spring's Hibernate adapter sets by default ([HibernateJpaVendorAdapter.java#L190-L193][spring-hjva]). Spring Data JPA's repository implementation is annotated `@Transactional(readOnly = true)` at class level ([SimpleJpaRepository.java#L110][sdj-ro]), with its writing methods annotated `@Transactional` separately. JDBC requires `setReadOnly` to be called outside a transaction ([Connection.java#L319-L331][jdk-ro]), which matches beginning the session lazily.

### 17. Later work

Named here so that the gaps are explicit: the authorisation model; the SQL lockdown; database deletion and listing; compression; observability, including whether SQL text is logged; cluster membership and how pods resolve each other; and HTTP/2-specific tuning.

## Worked examples

Headers that every request and response carries (`Authorization`, `Content-Type`, `Skylite-Protocol`, `Skylite-Request-Id`, `Cache-Control`) are shown once and then left out. Commit identifiers are shortened to `c1`, `c2` in the prose.

### Example 1: a stateless read on a follower

The pool has seen transaction 1207 and epoch 3. The request lands on a follower that has applied 1205.

```http
POST /v1/databases/orders/execute HTTP/1.1
Host: skylite.example.internal
Authorization: Bearer eyJhbGciOi...
Content-Type: application/json

{"sql":"SELECT id, total FROM orders WHERE customer_id = ?","params":[42],"min_txn":1207,"epoch":3,"commit_id":"3f6c2a1e9d8b47c5a0e1f2d3c4b5a697"}
```

The follower compiles the statement, finds it read-only, waits until it has applied 1207 (well inside 500 ms here, at 1208), and serves it.

```http
HTTP/1.1 200 OK
Content-Type: application/x-ndjson
Skylite-Protocol: 1
Skylite-Request-Id: 01JDZ3W4Q8M6
Cache-Control: no-store

{"columns":[{"name":"id","decltype":"INTEGER","database":"main","table":"orders","origin":"id"},{"name":"total","decltype":"DECIMAL(12,2)","database":"main","table":"orders","origin":"total"}]}
[9001,129.5]
[9002,15.0]
{"done":{"txn":1208,"epoch":3,"read_only":true,"rows_affected":0,"last_insert_rowid":0}}
```

The driver raises its pool's highest transaction to 1208. Had the follower not reached 1207 within 500 ms, it would have forwarded the request to the leader.

### Example 2: a stateless write

```http
POST /v1/databases/orders/execute HTTP/1.1

{"sql":"INSERT INTO orders (customer_id, total) VALUES (?, ?)","params":[42,"18.20"],"min_txn":1208,"epoch":3,"commit_id":"9a0b1c2d3e4f405162738495a6b7c8d9"}
```

1. A follower receives it, compiles it, finds it is not read-only, and forwards it to the leader with the same `Authorization` header and body, plus `Skylite-Forwarded: 1`.
2. The leader verifies the bearer token, finds no entry for `c1` in its commit table, sees epoch 3 is its own, and that the next number, 1209, is within 1208 + *W*. It records `c1` as in flight and waits for the write lock.
3. It executes the insert, holds the result, captures the commit with `c1` in the commit header, and uploads it. Storage confirms. `c1` becomes committed at 1209.
4. The leader sends the result; the follower relays it unchanged.

```text
{"columns":[]}
{"done":{"txn":1209,"epoch":3,"read_only":false,"rows_affected":1,"last_insert_rowid":77}}
```

### Example 3: a transaction with a combined begin

A Spring `@Transactional` method saves a `Book` with an `IDENTITY` id. The JDBC driver has seen `setAutoCommit(false)` and no statement yet, so the insert begins the session.

```http
POST /v1/databases/library/begin HTTP/1.1

{"mode":"write","min_txn":1209,"execute":{"sql":"insert into book (author_id,title) values (?,?)","params":[7,"Dune"]}}
```

The leader takes the write lock, runs `BEGIN IMMEDIATE` and the insert.

```text
{"session":{"token":"s1.qQ5vX0aYk3H8...","id":"7c1d2e3f40a1b2c3","mode":"write","epoch":3,"txn":1209,"idle_timeout_ms":10000,"lifetime_ms":60000}}
{"columns":[]}
{"done":{"txn":1209,"epoch":3,"read_only":false,"rows_affected":1,"last_insert_rowid":78}}
```

Hibernate asks for the identity on the same connection, so the driver sends it in the session:

```http
POST /v1/databases/library/session/execute HTTP/1.1

{"session":"s1.qQ5vX0aYk3H8...","seq":1,"sql":"select last_insert_rowid()"}
```

```text
{"columns":[{"name":"last_insert_rowid()","decltype":null,"database":null,"table":null,"origin":null}]}
[78]
{"done":{"txn":1209,"epoch":3,"read_only":true,"rows_affected":0,"last_insert_rowid":78}}
```

The method returns and Spring commits:

```http
POST /v1/databases/library/session/commit HTTP/1.1

{"session":"s1.qQ5vX0aYk3H8...","seq":2,"commit_id":"0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f50","epoch":3,"min_txn":1209}
```

```text
{"done":{"txn":1210,"epoch":3,"committed":true}}
```

Three requests, one of which waits for one object-storage upload. Until the commit, every other writer waits for the lock.

### Example 4: a lost commit response, recovered after a leader change

A session on leader L (epoch 3) sends its commit as `seq` 5 with `c2`, `epoch` 3 and `min_txn` 1300. L creates commit object 1301, holding `c2`, and storage accepts it. L's pod is killed before the response is written. The driver sees the connection reset: no marker, no final frame. It does not know whether the transaction committed, so it asks:

```http
POST /v1/databases/library/outcome HTTP/1.1

{"commit_id":"5e4d3c2b1a0f4e8d9c7b6a5f4e3d2c1b","epoch":3,"min_txn":1300}
```

No leader exists yet:

```http
HTTP/1.1 409 Conflict
Skylite-Protocol: 1

{"error":{"code":"no_leader","not_executed":true,"outcome":"unknown","message":"no leader is available for this database","request_id":"01JDZ41B2K7T"}}
```

`outcome` is unknown, so the driver waits and asks again. Meanwhile L2 takes office with epoch 4: it walks the log to 1301 and creates its epoch-bump commit at 1302. The second outcome request reaches L2. `c2` is not in its table, and epoch 3 is lower than 4, so L2 reads the commit headers at 1301 and 1302 and finds `c2` at 1301.

```text
{"done":{"outcome":"committed","txn":1301,"epoch":4}}
```

The driver returns normally from `commit()`.

If L had died before its upload landed, 1301 would be L2's bump, and the head would be 1301. L2 would not find `c2` at 1301, which is retained, with epoch 3 below its own, and answer `{"done":{"outcome":"not_committed","epoch":4}}`. The JDBC driver would raise `SQLTransactionRollbackException` with SQLState `40000`. That answer is final: L is fenced, and any later delivery of `c2` meets step 3 at L2 or at any later leader.

### Example 5: a duplicated delivery

An operator has added `retryOn: reset` to the mesh route. A stateless write with `c3` reaches the leader, which commits it at 1400; the connection resets before the response gets back to the sidecar, and the sidecar sends the identical request again. The leader finds `c3` committed at 1400:

```text
{"error":{"code":"already_committed","not_executed":true,"outcome":"committed","message":"this commit identifier committed at transaction 1400","request_id":"01JDZ47S0VJ9","txn":1400,"epoch":4}}
```

The sidecar returns that response to the driver, which reports success. The Go driver's `RowsAffected` returns `ErrResultUnavailable`, because the counts went with the lost response.

The same reset during a session statement sent as `seq` 3: the repeated delivery carries `seq` 3 again, the server answers `sequence_violation` and rolls the session back, and the driver reports the transaction as failed. Nothing of it committed. During a session commit: the repeated delivery's `commit_id` is found first, and the answer is `already_committed` or `commit_in_progress`, never a rollback.

## Coverage of the minimum contract

The research report lists twelve requirements for a project-owned protocol. Where this record meets each:

| # | Requirement | Where |
|---|---|---|
| 1 | Session ownership and identity, stale tokens, one operation in flight | [Decision 8](#8-sessions): token naming pod and epoch, bound to database and principal, `lost_session`, sequence numbers. The server stores the binding; the token is not signed, because only the owner can act on it |
| 2 | Routing through HTTPS, bounded forwarding, no token sent to other origins | [Decision 7](#7-routing): one hop, original token re-verified, session requests always to the owner, no pod addresses to clients, authenticated pod-to-pod hop |
| 3 | Explicit begin modes, same connection, state distinctions, poisoned connections | [Decision 8](#8-sessions): write and read-only modes, isolation, one connection per session, the state table. Pools see `lost_session` and roll back on any lost response |
| 4 | Idle and total lifetimes, lock wait, query deadline | [Decisions 8 and 14](#14-limits-and-defaults): idle timeout, lifetime, lock wait, statement time; activity is defined; there are no heartbeats, so an abandoned writer cannot be kept alive. There are no cursors, so no cursor lifetime |
| 5 | Durable commit receipt and outcome recovery | [Decision 10](#10-commit-identifiers-outcomes-and-retries): the identifier is stored in the commit object, which is the durable decision; outcome answers committed, not committed or unknown, with the exact conditions. "Still deciding" is `commit_in_progress` or the outcome wait |
| 6 | Retry rules at every layer, no `ErrBadConn` | [Decision 10](#10-commit-identifiers-outcomes-and-retries): marker, `not_executed`, `outcome`, driver rules, transport, JDK, Istio and ingress defaults |
| 7 | Proxy deadlines and long-running work, cancellation | [Decisions 13 and 14](#13-cancellation): keepalive frames, statement time, cancellation by abandoning the request or by `session/rollback`, and what is reported after the durable point |
| 8 | Bounded results, final status | [Decisions 4 and 14](#4-responses-and-frames): one response per execution, final frame required, result-size and time limits. No paging, so no snapshot across pages and no re-execution after failover |
| 9 | Lossless values and binding | [Decision 5](#5-values) and [decision 3](#3-request-bodies): five classes, NaN, invalid UTF-8, binding rules; [decision 16](#16-driver-conventions) for booleans, timestamps, decimals and metadata |
| 10 | Error and batch semantics | [Decision 11](#11-errors): one object, closed code list; [decision 6](#6-statements-and-batches): atomic batches that stop at the first error and report per-entry counts |
| 11 | Driver lifecycle and ORM behaviour | [Decision 16](#16-driver-conventions): lazy begin, read-only detection, generated keys, metadata, savepoints through SQL in sessions. Connection validity uses `GET` status |
| 12 | Per-request OIDC, token refresh, verifiable forwarding | [Decision 12](#12-authentication-in-the-protocol) |

## Alternatives considered

**Start every transaction deferred and upgrade on first write.** SQLite's default `BEGIN` only sets flags and touches the database later, unlike `BEGIN IMMEDIATE` ([sqlite.h.in#L4765-L4768][sq-begin]). A transaction that reads, then writes after another writer has committed, fails at that write with a snapshot conflict, part-way through: in WAL mode SQLite refuses the write lock to a connection whose snapshot is no longer the newest and returns `SQLITE_BUSY_SNAPSHOT` ([wal.c#L3816-L3823][sq-busysnap]). ORMs do not retry whole transactions; Hibernate and GORM report the error to the application (inference; not tested). `BEGIN IMMEDIATE` moves the wait to the start, where a `busy` answer is definite and nothing has run. Rejected.

**Server-side prepared statements and cursors.** They would tie a handle to one pod and one connection, outside any session, and would need expiry, routing and loss handling of their own. Sending SQL text every time costs bytes, which the server can offset with a private statement cache. Rejected.

**Paging large results.** A page token would have to pin a snapshot across requests, or re-execute the query and risk a different answer after a failover. Rejected for one bounded response per execution.

**Reusing Hrana, the rqlite API or Flight SQL.** Rejected by the research report and by [0003](/docs/decisions/0003-jdbc-transport/), for the reasons in [Context](#context).

**A durable tombstone for every "not committed" answer.** The leader could write an object recording that an identifier is refused. That costs a storage write per outcome request, and the epoch and window rules give the same guarantee from data the commit log already holds. Rejected.

**An expiry time inside the commit identifier.** The server would refuse identifiers older than a few minutes. It bounds the commit table, but its guarantee would rest on clocks, which 0004 keeps out of every safety argument. The commit window uses transaction numbers instead. Rejected.

**HTTP status codes as the outcome signal.** Proxies produce the same codes, and a status cannot be changed once a streamed body has started. The marker header and the final frame carry the outcome instead.

## Consequences

- Every write session holds the database's only write lock, and a Spring `@Transactional` method that is not marked read-only is a write session even if it only reads. Such methods run one at a time per database, and each holds the lock for its whole duration, including the application's own work between statements. Marking read-only methods `readOnly = true` matters more on skylite than on a database with row locks.
- GORM's default transaction around each create, update and delete makes each such write a session: a combined begin and a commit, two requests at least.
- A client that sends a write after a failover, or after more than *W* transactions it did not see, gets one definite refusal and one retry.
- A lost response costs an outcome request, for reads as well as writes.
- Results are limited to one bounded response. A query whose result passes the limit fails; the application pages with SQL.
- A commit with an unknown outcome surfaces to the application as unknown, with a distinct Go error and a distinct SQLState, and the application has to decide.
- The drivers must parse JSON integers exactly and must not use a JSON library that replaces invalid text.
- Garbage collection gains two retention rules ([below](#requirements-on-the-engine-and-the-format)).
- In this pull request: the [architecture page](/docs/architecture/) links this record from its interface section and settles the client authentication question's protocol part. The roadmap does not change: this record specifies the HTTPS query API and the clients milestones without changing them.

## Requirements on the engine and the format

[0004](/docs/decisions/0004-storage-engine/) and the format package on the `feat/engine-format` branch do not yet meet these:

1. **Retention for outcome lookups.** Garbage collection must keep the newest *W* commit log objects and every commit log object younger than the outcome horizon, and the engine must know the lowest transaction number whose commit object is guaranteed present ([decision 10](#10-commit-identifiers-outcomes-and-retries)). 0004 decision 8 deletes a folded commit object after a grace period chosen to protect readers, and the format's garbage-collection section counts a commit object as garbage as soon as it is at or below the manifest's transaction number and not an epoch bump; neither keeps anything for outcomes. The format already says that an identifier not found after its object was deleted means unknown, which this record relies on. Alternatively, compaction writes an index of folded commit identifiers under the same retention.
2. **The commit path takes the identifier, epoch and `min_txn`.** It must refuse a commit at a transaction number above `min_txn` + *W* and a commit whose epoch is not the leader's, before the upload.
3. **The commit identifier field holds 32 bytes:** the client's 16 and a 16-byte principal hash. The format allows up to 64 (`internal/format/doc.go`, commit header bytes 68 to 132), so the size fits; the format should state that the identifier is compared as exact bytes.
4. **Header range reads by transaction number** for the lookup, which the format already allows: the header is the first 144 bytes, verified by its own checksum, and it carries the database identifier, so a header from another database is rejected.
5. **The engine must report** the head, the epoch, a follower's applied transaction number, and wait until a follower reaches a given number.
6. **Optional:** recording rows affected and last insert row id for a single-statement commit would let `already_committed` carry counts. Not required.

## Not verified, and what the first implementation must prove

Nothing here could be tested through a real Istio mesh, an ingress controller or a cloud in this environment; there is no server yet. The following are unverified or inference, and each needs a test in the first implementation:

- The JDBC 4.3 specification text on substituting a higher isolation level was not re-read.
- That Istio's default policy never resends a request skylite processed. That Envoy forwards keepalive frames promptly with and without buffering. What a buffering proxy does with a body cut after the headers. These need a test through Istio, and through ingress-nginx with buffering on and off.
- That Go's HTTP/2 transport does not resend a body once `GetBody` is cleared, and that a protocol error from the peer can follow processing.
- SQLite's NUMERIC affinity rules as stated in [decision 16](#16-driver-conventions).
- That Spring's exception translation for SQLState `08007` yields a non-transient exception, by a test through Hibernate and through `JdbcTemplate`.

The first implementation must prove, by test:

- No commit identifier commits twice: repeated deliveries of stateless writes, batches and commits, with and without a leader change between them, including a delivery delayed until after an outcome answer of not committed.
- Every "not committed" answer is true, and every answer is one of the three outcomes, for commits killed at each step of 0004's commit sequence and resolved after a takeover. A lookup over a range with a deleted commit object answers unknown.
- The commit window refuses a commit above `min_txn` + *W*, and the commit table never exceeds its bound.
- A session token from a previous epoch, from another principal or for another database never creates or ends a session; a refreshed token for the same principal works.
- Repeated and out-of-order sequence numbers roll back; a repeated commit after it committed does not.
- Stateless writes send no row before their commit is confirmed.
- Values round-trip for all five classes, both extremes of int64, `-0.0`, infinities, invalid UTF-8 in both directions, the empty string, the empty blob and NULL; NaN and booleans are refused.
- A response cut at every byte offset is reported as incomplete by both drivers, never as a short result.
- A follower behind `min_txn` waits and then forwards; a read after a write in the same pool sees the write.
- A driver idle past the freshness bound refreshes `min_txn` and `epoch` before its next write, and a client-supplied epoch above the leader's never stops the leader unless the manifest shows a takeover.
- Cancellation interrupts SQLite, and a cancelled commit past its upload reports committed.
- The statement time limit ends a slow reader on the leader and lets the WAL restart.
- The Go GORM suite and the Spring Data JPA suite in `clients/java/acceptance/` pass against the server, with the exception mappings in decision 10.
- Transaction-control statements are refused in stateless requests, and `last_insert_rowid` never leaks between stateless requests.

[nc-ro]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/stmt.go#L70-L77
[nc-colmeta]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/stmt.go#L403-L452
[nc-prepare]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L193-L203
[nc-rowid]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L281-L295
[nc-interrupt]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L328-L348
[nc-auth]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L202
[nc-opt-meta]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L27
[nc-opt-fk]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L22
[sq-readonly]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L4757-L4768
[sq-begin]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L4765-L4768
[sq-interrupt]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L2934-L2937
[sq-nan]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbemem.c#L1092-L1098
[sq-busysnap]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L3816-L3823
[sq-frac]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/date.c#L220-L231
[go-badconn]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L151-L163
[go-begintx]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/driver.go#L279-L294
[go-retry]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/sql.go#L1569-L1580
[go-iso]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/sql.go#L126-L141
[go-transport-doc]: https://github.com/golang/go/blob/go1.26.2/src/net/http/transport.go#L89-L96
[go-shouldretry]: https://github.com/golang/go/blob/go1.26.2/src/net/http/transport.go#L818-L863
[go-replayable]: https://github.com/golang/go/blob/go1.26.2/src/net/http/request.go#L1534-L1548
[go-h2retry]: https://github.com/golang/go/blob/go1.26.2/src/net/http/h2_bundle.go#L7969-L8013
[go-scan-bool]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/convert.go#L382-L387
[go-scan-time]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/convert.go#L281-L288
[go-scan-err]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/convert.go#L488
[go-bool]: https://github.com/golang/go/blob/go1.26.2/src/database/sql/driver/types.go#L49-L58
[go-json-dec]: https://github.com/golang/go/blob/go1.26.2/src/encoding/json/decode.go#L98-L101
[go-json-enc]: https://github.com/golang/go/blob/go1.26.2/src/encoding/json/encode.go#L83-L84
[jdk-iso]: https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.sql/share/classes/java/sql/Connection.java#L425-L448
[jdk-ro]: https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.sql/share/classes/java/sql/Connection.java#L319-L331
[jdk-retry-cfg]: https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.net.http/share/classes/jdk/internal/net/http/MultiExchange.java#L491-L528
[jdk-retry]: https://github.com/openjdk/jdk/blob/jdk-25%2B36/src/java.net.http/share/classes/jdk/internal/net/http/MultiExchange.java#L537-L575
[hib-sqlite-exc]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470
[hib-sqlite-types]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L119-L129
[hib-sqlite-strftime]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L176-L186
[hib-bool]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BooleanJdbcType.java#L89
[hib-state]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L51-L52
[hib-state-08]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L78-L80
[spring-het]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L159-L160
[spring-sub]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLExceptionSubclassTranslator.java#L85-L90
[spring-state-40]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java#L77-L80
[spring-state-cal]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/support/SQLStateSQLExceptionTranslator.java#L207-L209
[spring-cale]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-tx/src/main/java/org/springframework/dao/CannotAcquireLockException.java#L31
[spring-plfe]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-tx/src/main/java/org/springframework/dao/PessimisticLockingFailureException.java#L36
[spring-cfe]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-tx/src/main/java/org/springframework/dao/ConcurrencyFailureException.java#L33
[spring-hjd]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/vendor/HibernateJpaDialect.java#L132-L147
[spring-dsu]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/DataSourceUtils.java#L195-L206
[spring-dsu-ro]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-jdbc/src/main/java/org/springframework/jdbc/datasource/DataSourceUtils.java#L232-L234
[spring-hjva]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/vendor/HibernateJpaVendorAdapter.java#L190-L193
[sdj-ro]: https://github.com/spring-projects/spring-data-jpa/blob/4.1.2/spring-data-jpa/src/main/java/org/springframework/data/jpa/repository/support/SimpleJpaRepository.java#L110
[istio-retry]: https://github.com/istio/istio/blob/1.32.0/pilot/pkg/networking/core/route/retry/retry.go#L34-L54
[envoy-retry-5xx]: https://github.com/envoyproxy/envoy/blob/v1.40.2/docs/root/configuration/http/http_filters/router_filter.rst#L74-L91
[envoy-retry]: https://github.com/envoyproxy/envoy/blob/v1.40.2/docs/root/configuration/http/http_filters/router_filter.rst#L92-L128
[envoy-grpc]: https://github.com/envoyproxy/envoy/blob/v1.40.2/docs/root/configuration/http/http_filters/router_filter.rst#L149-L175
[ing-read-default]: https://github.com/kubernetes/ingress-nginx/blob/controller-v1.15.1/docs/user-guide/nginx-configuration/configmap.md#L172
[ing-next]: https://github.com/kubernetes/ingress-nginx/blob/controller-v1.15.1/docs/user-guide/nginx-configuration/configmap.md#L179
[ing-buffer]: https://github.com/kubernetes/ingress-nginx/blob/controller-v1.15.1/docs/user-guide/nginx-configuration/configmap.md#L193
[ing-read]: https://github.com/kubernetes/ingress-nginx/blob/controller-v1.15.1/docs/user-guide/nginx-configuration/configmap.md#L1066-L1068
[ngx-nonidem]: https://github.com/nginx/nginx/blob/release-1.31.8/src/http/ngx_http_upstream.c#L4701-L4708
[ngx-buffering]: https://github.com/nginx/nginx.org/blob/8422f139ba3026beb8fea388298a05a4b65c15da/xml/en/docs/http/ngx_http_proxy_module.xml#L172-L200
