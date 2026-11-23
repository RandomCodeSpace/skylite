---
title: Server
description: What `skylite serve` does in this version, which is authenticate requests, create databases, lead the databases it is configured for, answer read-only queries on them, run autocommit writes and transactions on write channels and forward requests between pods, and how to configure it.
weight: 15
status: current
---

`skylite serve -config <file>` runs the server of [0005: Client protocol](/docs/decisions/0005-client-protocol/) with the authentication of [0006: Authentication](/docs/decisions/0006-authentication/). In this version it creates databases, leads the databases its configuration lists, answers the two stateless requests, `query` and `describe`, on leaders and [opt-in followers](#reading-on-a-follower), and runs autocommit writes and transactions on [write channels](#write-channels) on leaders. With a peer listener configured, a pod forwards what it cannot serve to the pod that can ([Forwarding between pods](#forwarding-between-pods)). It runs no batch or `resolve`. The [Go driver](/docs/go-driver/) sends `query`, `describe`, opens, channel requests, `cancel` and `close`; the [JDBC driver](/docs/jdbc-driver/) uses `query`, `describe` and write channels.

## What it serves

| Request | Answer |
|---|---|
| `PUT /v1/databases/{db}` with body `{}` | `200` and `{"done":{"txn":1,"epoch":1}}` when it created the database; `409` `database_exists` when the database exists or a concurrent `PUT` won |
| `POST /v1/databases/{db}/query` with `sql` and optionally `params` or `named`, `min_txn` and `timeout_ms` | One pure read on the leader or on an enabled follower: a `columns` frame, one frame per row, then `done` with `txn`, `read_only`, `rows_affected` and `last_insert_rowid`. See [Queries](#queries) |
| `POST /v1/databases/{db}/describe` with `sql` and optionally `min_txn` | The statement's result columns and parameter names, without running it: a `columns` frame, `params` frames, then `done` with `txn` and `param_count` |
| `POST /v1/databases/{db}/channels` with `{"mode":"write"}` | Opens a write channel: `done` with `txn`, the leader's head, and `channel` with `token`, `id`, `mode`, `epoch` and `limits`. See [Write channels](#write-channels). A read-only open gets `503` `not_serving` |
| `POST /v1/databases/{db}/channel` with `channel`, `seq` and `op`: `begin` (optionally with a first statement in `execute`, `min_txn` and `timeout_ms`), `execute` (with `sql`, optionally `params` or `named`, `min_txn` and `timeout_ms`), `commit` (optionally `timeout_ms`), `rollback` or `ping` | A request on the channel: an autocommit write, or a step of a [transaction](#transactions) |
| `POST /v1/databases/{db}/channel/cancel` with `channel` and `seq` | Cancels that request: `done` with `seq`, `txn_state` and the request's receipt. See [Cancel](#cancel) |
| `POST /v1/databases/{db}/channel/close` with `channel` | Closes the channel: `done` with `last_seq` and the receipt of that request |
| Channel requests on a database this server does not lead, or stateless reads with neither a local leader nor an enabled local follower | With `peer` set, forwarded once to the pod that serves them ([Forwarding between pods](#forwarding-between-pods)); otherwise `503` `not_serving`, the body unread |
| `resolve` and `GET` status | After authentication, and authorisation at the `read` role on the name: with `peer` set, a pod that does not lead the database forwards `resolve` to the leader; the leader, or a pod without `peer`, answers `503` `not_serving` without reading the body. Every pod answers `GET` status with `503` `not_serving` |
| Anything else, or a path with a query string | `400` `invalid_request` |

`{db}` is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or a digit. A `PUT` needs the `admin` role on the name; a write open and every request on a write channel need the `write` role, checked again on each request because a new token's claims can differ; every other request, a close and a cancel included, needs the `read` role. Every answer is a sequence of NDJSON frames ending in one terminal frame, and a refusal is one error frame, with the headers `Skylite-Protocol: 1`, `Skylite-Request-Id`, `Content-Type: application/x-ndjson` and `Cache-Control: no-store`; a `401` also carries `WWW-Authenticate: Bearer`. The error object and its codes are those of [0005's errors](/docs/decisions/0005-client-protocol/#errors).

A new database has 4,096-byte pages and base groups of 256 pages. When the store cannot tell whether a create landed, the answer is `500` `internal`, never `database_exists`: the database may or may not exist, and a `PUT` sent again answers which.

### Checks before acceptance

The server runs [0005's checks](/docs/decisions/0005-client-protocol/#checks-before-acceptance) in this order, with routing before the body as 0005 allows; a channel request's own checks follow in [Write channels](#write-channels):

1. The header block is at most 64 KiB, to which Go's HTTP/1.1 server adds 4 KiB of slack (otherwise `431` from the HTTP layer), and arrives within 10 seconds on HTTP/1.1; the method and path are known, and the path has no query string.
2. *Admission is implemented only for connections:* the server accepts at most 1,024 connections at a time, and one beyond that waits in the operating system's listen queue until another closes. There are no request bounds yet.
3. The bearer token verifies, read from the headers only (`401` `unauthenticated`). The body is never read before this.
4. The principal holds `read` on the name, and `admin` for a `PUT` (`403` `forbidden`). This comes before routing and before anything that depends on whether the database exists, so the answer is the same either way.
5. The server must lead the database for channel requests. For `query` and `describe` it can also use an enabled local follower. If neither route applies and it cannot forward the request, because `peer` is not set or another pod forwarded it, it answers `503` `not_serving` with the body unread. A request it can forward goes through steps 6 and 7 and is then forwarded. A `PUT` can go to any server.
6. The body share of the memory budget has room for 8 × `Content-Length`, and so does the principal's part of it (`429` `overloaded`, the body unread). The share is half the memory budget, 1 GiB; a principal, an issuer and a subject, may hold a quarter of it, 256 MiB, at once. A body is charged from before its read until its request ends, so one that is declared and never sent stays charged until the 30-second body read deadline or the client's close.
7. `Content-Type` is `application/json`, compared without regard to case, with parameters ignored; `Content-Length` is present once, with one value, and at most 16 MiB, or 4 KiB for a close, a cancel or a resolve (`413` `request_too_large` above it); there is no `Transfer-Encoding`. The whole body is read within 30 seconds and must be exactly `Content-Length` bytes: one JSON object of 0005's form with only the fields its request allows, and exactly `{}` for a `PUT`. A cut body, a reset stream or a body that arrives late is `400` `invalid_request`, and nothing is created or run.
8. For a `query` or `describe`, the server reserves a SQLite connection (`429` `overloaded`; see [Connections for queries](#connections-for-queries)), and the leader is not in maintenance (`503` `maintenance`).

The 30-second body read deadline starts when the server has read the header block, so it also bounds a body that a refusal leaves unread. Such a refusal is answered at once. On HTTP/1.1 it carries `Connection: close`, and Go's HTTP server reads and discards what is left of the body, within the same deadline, before it closes the connection.

### Connections

An answer must be written within 10 seconds (0005's response deadline), refusals included; a client that does not read it loses it and the connection, and on HTTP/2 every stream on that connection. Over TLS the connection is released up to 5 seconds later, while Go's TLS layer tries to send its closing alert. A connection with no request running is closed after 90 seconds, on HTTP/1.1 and HTTP/2. 0005 gives no value for this idle timeout; 90 seconds is this server's. On HTTP/2 it also bounds a header block that never ends. An HTTP/2 connection carries at most 16 concurrent streams.

## Queries

### Leading a database

For each name in `databases`, the server runs the office loop of [0004](/docs/decisions/0004-storage-engine/): it takes office when the database's lease is free, renews the lease every 20 seconds (a lease lives 60 seconds), and steps down when it is deposed or its leader stops. A server that does not hold office is a standby: it reads the lease at most every 5 seconds and runs a candidacy once the lease is released or expires. A database in `databases` need not exist: once a `PUT` has created it, the server takes office within its retry backoff, at most 20 seconds.

### Reading on a follower

With `follower_reads` on, the default when `peer` is set, a standby serves `query` and `describe` for its configured databases. A follower polls the durable log about every 250 ms and pins each request to one snapshot before SQLite opens it. An observation of the end of the log is usable for new snapshots for less than two seconds. When the follower has no fresh observation, it refuses with `503` `not_serving`. A request can wait up to 500 ms within its statement budget for a fresh generation at or above `min_txn`. A fresh follower that cannot reach the requested transaction in time answers `503` `behind`.

If compaction overtakes the pin before the response starts, the server can retry once on a newer pin within the request and catch-up budgets. If it cannot obtain a usable replacement, it answers `503` `not_serving`. If compaction overtakes a pin after the response starts, an `internal` error ends the stream. A standby stops polling when it takes office, before it starts leading, and starts polling again after it steps down or fails to start leading.

`follower_reads` defaults to `true` when `peer` is set and to `false` without it; set it to `false` to serve stateless reads only on the leader. With `peer` set, a follower forwards to the leader a read it has no fresh snapshot for, or is still `behind` on, before its answer starts, and every write and channel request. Without `peer`, those get `503` `not_serving`, so more than one pod behind a Service needs `peer` to deliver writes. That is why the default is `false` without `peer`: a standby that serves reads is ready, so a Service would send it writes it refuses.

### How a query runs

A `query` runs on a pooled SQLite connection of the leader or on a SQLite connection opened for one follower snapshot. Both connections have [0007's SQL policy](/docs/decisions/0007-sql-policy/) installed before their first statement. The server begins a read transaction, takes its snapshot's transaction number from the engine, and compiles the statement under the policy. Before it binds or steps anything it checks, in this order:

1. The policy allowed the statement (`403` `statement_forbidden`), which is one statement (`400` `invalid_request`) that compiles (`400` `sql_error`).
2. SQLite's read-only test is true for it (`409` `needs_channel`). Nothing is executed: a write is never stepped, so no commit reaches the log.
3. It is not transaction control by its first keyword, `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT` or `RELEASE`, after white space, comments, semicolons and `EXPLAIN` (`400` `invalid_request`). The policy itself allows `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` inside a transaction, and the query runs inside the server's own.

From there the statement has started executing, and the answer is `200` whatever follows: every parameter is bound exactly once (`invalid_request` otherwise), the rows stream as they are produced, and an error ends the body as its terminal frame. `txn` in `done` is the transaction number of the snapshot the rows were read from. The server then rolls the read transaction back.

| Limit | Value |
|---|---|
| Statement time, from the end of the body read to the last row written, storage page reads included (`timeout`) | 30 seconds; `timeout_ms` lowers it |
| Result size: the bytes of all frames before the terminal one (`result_too_large`) | 64 MiB |
| Frame size; a `columns` frame above it fails with `result_too_large` | 1 MiB |
| `min_txn` above the snapshot's transaction number | `503` `behind` |

A `describe` compiles the statement under the same policy and makes checks 1 and 3, then answers its result columns and parameter names without binding or stepping it; any statement can be described, writes included. A statement that reaches its time limit ends with the `timeout` frame, also after rows were sent. A dropped connection stops a query's rows, and a client that stops reading them holds its connection until a second after the statement time ends: the response's write deadline is that second later, so that the statement ends first and its `timeout` frame can be written.

A pragma's table-valued function, such as `SELECT * FROM pragma_user_version`, is compiled by SQLite when the statement first scans it, not when it is prepared, so the policy refuses it only at the first step: `describe` answers `200` with its columns, and `query` answers `200` with `statement_forbidden` as its terminal frame. Nothing is executed either way.

### Connections for queries

The memory budget is 2 GiB, and it charges exactly three things: request bodies, at 8 × `Content-Length`, within the body share of 1 GiB; channels, within a share of 128 MiB that holds a 2 KiB receipt for each of the 16,384 channels a server may hold (32 MiB), held write results (64 MiB) and retained replay data (32 MiB); and SQLite connections, at a 64 MiB ceiling each, within the remaining 896 MiB. So the server holds at most 14 SQLite connections, less one for each database in `databases`, whose leader keeps a connection of its own; stateless reads and writes share them. A follower read and a write each open a fresh connection and close it when done. With every limit reached, bodies, channels and connections hold the whole 2 GiB and no more. Nothing else is charged: not HTTP connections or their header blocks, not response frames, and not the engine's page cache (64 MiB per server, shared by the databases it leads or follows) or temporary files (256 MiB per led database), which must fit in what the container has beyond the budget. A principal, an issuer and a subject, may use a quarter of them at once, at least one. A query or describe that finds its principal's share in use, or every connection in use with none idle, waits for one, first in, first out, for at most 250 ms and never past its statement time, and gets `429` `overloaded` if none comes; a connection that frees goes to the first waiter it fits, so a waiter held back by its own principal's share does not hold back another principal's. At most 256 requests wait on a server, 64 of them from one principal; one beyond either bound gets `overloaded` at once. A waiter whose client goes away leaves the queue and holds nothing. A write takes its connection only once it holds the write lock, so writers waiting for the lock hold none; a write granted the lock that finds no connection records `overloaded` (`final: true`, `not_executed: true`) and passes the lock on. An idle connection of another database is closed to make room.

A connection is reset before every reuse, [as 0007 requires](/docs/decisions/0007-sql-policy/#session-state): an open transaction is rolled back, temporary objects are dropped, foreign keys are enforced and the last inserted row id is cleared. A connection whose reset fails is closed and replaced. A statement that breaks its connection, by overflowing SQLite's C stack or exhausting the connection's memory, gets `internal`; the engine discards the connection without running SQLite on it again, and the leader serves on.

## Write channels

A write channel is [0005's channel](/docs/decisions/0005-client-protocol/#2-channels) on the leader. In this version it carries the sequenced requests `begin`, `execute`, `commit`, `rollback` and `ping`, each with `channel`, `seq` and `op`: an `execute` with no transaction open is an autocommit write of one statement, and the others run [transactions](#transactions). A channel holds a SQLite connection only while a request runs or a transaction is open.

### Opening

An open with `{"mode":"write"}` needs the `write` role. It is refused with `429` `too_many_channels`, creating nothing, when the server holds 16,384 channels, the database 4,096, or the principal 256 on the database. The token is `w1.<instance>.<database id>.<open head>.<secret>`: the identifier this server process drew at start, the database's 16-byte identifier in hex, the head the open answered as `txn`, and 256 random bits. Requests carry it in the body only. The channel identifier, 32 hex digits, is drawn separately and reveals nothing about the token. The open answer's `limits` are 0005's defaults.

### Checks on a channel request

After the checks above, a request on a channel goes through these, in order:

1. The token names this database (`400` `invalid_request`).
2. It names this server process and a channel of the leader in office, of this principal, that has not ended (`410` `channel_lost`). With `peer` set, a request whose token names another process was already forwarded to that process, before this check, on a leader too ([Forwarding between pods](#forwarding-between-pods)). Without `peer`, a token of another process gets `channel_lost`. A request of another principal leaves the channel untouched.
3. The channel is not closing (`channel_lost`); the operation is not `batch` or `describe`, which get `503` `not_serving` and take no sequence number.
4. The sequence number, by [0005's table](/docs/decisions/0005-client-protocol/#2-channels): the last, recorded as cancelled before it arrived, gets that `cancelled` receipt whatever the body; the next number with nothing running is accepted; the next with a request running gets `409` `channel_busy`; the last, with the same body digest, gets `409` `in_flight` while it runs and its recorded outcome after; the last with another body, `409` `sequence_conflict` (`final: true`); a lower number `409` `stale_sequence`, a higher `409` `sequence_gap`. A refusal changes nothing.
5. For a `begin` or `execute` with no transaction open: `min_txn` is at most the leader's head (`503` `behind`), and the leader is not in maintenance (`503` `maintenance`). Its SQLite connection is reserved and opened fresh after acceptance, once it holds the write lock.

Then the request is accepted: its sequence number and the SHA-256 digest of its exact body are recorded, the answer is `200`, and its outcome is recorded whatever it is. A field the request may not have, or a bad value, is a recorded `invalid_request`. `ping` records `txn`: the snapshot inside a transaction, otherwise the leader's head.

### An autocommit write

An `execute` waits for the database's write lock, then runs on its own fresh connection: `BEGIN IMMEDIATE`, the statement under [0007's SQL policy](/docs/decisions/0007-sql-policy/), then `COMMIT`, or `ROLLBACK` after any failure, so that a failed write leaves nothing, also under SQLite's `FAIL` conflict action. The connection is closed after it. Transaction control is refused as for `query`. Every `execute` takes the write lock, a read included; `read_only` in its answer is SQLite's read-only test. A read's rows are held like a write's, so a read on a channel returns at most the held-result bound, 1 MiB, and larger results need `query`.

The write's commit carries the commit identifier of 0005: the first 16 bytes of SHA-256 of the whole channel token, then the sequence number as 8 bytes, big-endian. The bearer token never enters it. A write that changes no page, such as an update that matches no rows, creates no commit, and its `txn` is the head it ran on.

| Limit | Value |
|---|---|
| Write lock wait, first in, first out (recorded `busy`) | 5 seconds |
| Writers waiting for the lock, per database, and per database and principal (recorded `busy`) | 64 and 16 |
| Statement time, from before check 5 (the watermark) to the commit point, the lock wait and the connection's open included (recorded `timeout`) | 30 seconds; `timeout_ms` lowers it |
| Held result: the columns and rows of a write, kept back until its commit has an outcome, per request and per server (recorded `result_too_large`, nothing committed) | 1 MiB and 64 MiB |

No row of a write reaches the client before its commit has an outcome. A write's outcome maps to its answer like this; every recorded failure has `final: true` and `not_executed: true`:

| Engine outcome | Answer |
|---|---|
| Committed (a nonzero transaction number), whatever failed after | `done` with that `txn` |
| Too large for one commit object or the local WAL bound, or fenced by another leader | Recorded `commit_failed` |
| Maintenance (the local WAL bound), or the log at its tail bound | Recorded `maintenance` |
| No SQLite connection free for it once it holds the lock: its principal's share or every connection in use | Recorded `overloaded`; nothing ran, and the lock passes to the next writer |
| The connection broke (stack overflow, its memory ceiling) | Recorded `internal`; the engine discards the connection |
| Unknown | `commit_outcome_unknown`, `not_executed: false`, not recorded; the server stops serving the database |
| The leader stopped under the write without committing it | `channel_lost`, `not_executed: false`, not recorded; the channel has ended |
| The server failed after acceptance (a defect: a panic in its own code) | `channel_lost`, `not_executed: false`, not recorded; the channel has ended, and its write lock and held rows are released |

A client that drops its connection cancels nothing: the write runs on, and its outcome is recorded for a resend to replay.

### Transactions

A transaction follows [0005's channel state machine](/docs/decisions/0005-client-protocol/#channel-state-machine). Every answer on a channel carries `txn_state`: `none`, `open` or `aborted`.

- **`begin`** waits for the write lock like an autocommit write, then runs `BEGIN IMMEDIATE` on a fresh connection, and the first statement in `execute`, if any. Its `txn` is the transaction's snapshot: the head when `BEGIN IMMEDIATE` returned. The transaction then holds its connection and the database's write lock across requests until it ends, so every other writer waits, and after the lock wait gets `busy`. A `begin` that fails, in its first statement too, rolls back, closes the connection and leaves no transaction, with `not_executed: true` and `txn_state: "none"`: a first statement under SQLite's `FAIL` conflict action leaves nothing either. A `begin` inside a transaction records `invalid_state`.
- **`execute`** inside a transaction runs on the transaction's connection; its rows stream as they are produced, and up to 64 KiB of them are kept for replay. Its recorded failure has `not_executed: false`, whatever the code, because SQLite's semantics apply inside the transaction. After it, the server reads SQLite's autocommit state: when SQLite rolled the whole transaction back, as an `INSERT OR ROLLBACK` does, the answer says `aborted`, and the connection and the lock are released at once. `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` run as statements inside a transaction ([0007](/docs/decisions/0007-sql-policy/)); outside one, and in an autocommit write, they are refused like other transaction control.
- **`commit`** runs `COMMIT` under the commit identifier of its own sequence number. It ends the transaction whatever happens: committed (`committed: true`, `wrote: true`, `txn` the new transaction), committed with nothing to write (`wrote: false`, `txn` the snapshot), or failed, which rolls back and records the error with `not_executed: true`. A deferred foreign key that fails at `COMMIT` is such a failure, although SQLite itself would keep the transaction open. A commit's outcome maps to its answer as an autocommit write's does.
- **`rollback`** rolls back, closes the connection and releases the lock; its `txn` is the leader's head. Without a transaction it records `invalid_state`. A `rollback` that fails its validation after acceptance, one with a `sql` member for example, records `invalid_request` and leaves the state as it was: an open transaction keeps its connection and the lock, an aborted channel holds neither and stays aborted, until a valid `rollback`, a close or a limit ends it. A statement that succeeds after SQLite's transaction has gone underneath it answers with `txn_state: aborted`, and the lock is released ([0007](/docs/decisions/0007-sql-policy/) requirement 8).
- **Aborted**: a channel whose transaction SQLite, a limit or the engine rolled back holds nothing, and records `transaction_aborted` (`not_executed: true`) for every request but `rollback`, which returns it to `none`. When the engine ended the transaction, as maintenance does, the error carries `cause: "maintenance"`.

| Limit | Value |
|---|---|
| Statement time, per request, from before check 5 to the commit point for a `commit` (recorded `timeout`). Inside a transaction an interrupted write statement makes SQLite roll back, and the answer is `aborted`; an interrupted read leaves the transaction open | 30 seconds; `timeout_ms` lowers it |
| Write transaction idle limit, from the end of one response to the arrival of the next request; the transaction is rolled back and the channel aborted, checked every second | 30 seconds |
| Write transaction lifetime, from the `begin`'s acceptance. An idle transaction is aborted, a running statement is interrupted and recorded `timeout` with the transaction aborted, and a running `commit` before its commit point fails with `timeout` | 5 minutes |

No timer acts on a commit past its commit point: a commit in its upload when its lifetime passes completes, and its answer is the commit. While a transaction is open the channel idle limit does not run.

The engine tells the server through the connection when it ends an open transaction on its own, as maintenance rolls one back ([0005, requirement 15](/docs/decisions/0005-client-protocol/#requirements-on-the-engine-and-the-format)); the server then releases the write lock and aborts the channel without waiting for its next request. A statement or commit that maintenance interrupts records `maintenance`, and its transaction is aborted or, for a commit, ended.

### Cancel

A `cancel` names a channel and a sequence number, needs the `read` role and the channel's principal, has a 4 KiB body limit, and takes no sequence number of its own:

| The named request | Answer |
|---|---|
| The last, finished | `done` with its receipt, changing nothing |
| The last, running | Interrupts it through its connection unless it is past its commit point, then answers once it has finished, with its receipt: `cancelled` if the interrupt came first, the commit if it did not. A commit in its upload is never interrupted |
| The next, with nothing running | Records it as cancelled before it arrived (`cancelled`, `not_executed: true`, `final: true`), with no body digest: every later delivery of that number gets that receipt, whatever its body, and nothing runs. An open transaction stays open |
| The next, with a request running | `409` `channel_busy` |
| Lower, or higher than the next | `409` `stale_sequence` or `sequence_gap` |
| Any but the last, on a closing channel | `410` `channel_lost` |

A cancel that joins a closing channel's wait answers with the same receipt and `txn_state: "none"`.

### Receipts and replay

Each channel keeps the receipt of its last request: its terminal frame, with an error's own request id. A resend of that request with an identical body gets the frames its response had before the terminal one, if they were kept, then the receipt. They are kept only for a success, up to 64 KiB, and while the server's and the database's retained data, 32 MiB each, have room (0005 allows 256 MiB per server when the memory budget has room; this server keeps 32 MiB, within its channel share); otherwise the replay is the receipt alone, with `data_omitted: true` when the response had frames. Kept data is released when the channel accepts its next request or ends. A recorded error keeps at most 1,024 bytes of its message, the error message bound.

### Closing and ending

A close needs the `read` role and the channel's principal. A channel with nothing running ends at once, and an open transaction is rolled back before the answer. One with a request running becomes closing: the request is interrupted through its connection, which before its commit point records `cancelled`, and the close is answered once the request has finished, its outcome is recorded, and an open transaction is rolled back and the write lock released, so a close never answers before a running commit's outcome. A second close, or a cancel of the running request, joins the first and answers after that too. The answer carries `last_seq` and that request's receipt.

A channel also ends when it has been idle for 10 minutes (the channel idle limit, checked every second; idle time runs from the end of one request to the arrival of the next, a replay or a refusal included, and never while a request runs or a transaction is open), and when its leader stops or leaves office, which rolls back an open transaction. A request that runs when the leader stops keeps its channel in the table, ended but unsettled, until it has finished. Every request on an ended channel gets `channel_lost`.

### Maintenance, the tail bound and a stopping leader

While the leader is in maintenance, a query or describe gets `503` `maintenance`, and a running one ends with `maintenance` as its terminal frame. The leader compacts the database's log in the background. When the log still reaches the engine's tail bound, 32,768 commits above the manifest, the engine refuses reads and writes until a compaction clears it: a query then gets `maintenance` with a message that names the bound and asks for a retry, and the server stays ready. If the leader cannot compact, because the new manifest would pass its size limit, the refusal lasts: the message says the leader cannot compact, and the server reports itself not ready for as long as it leads that database. A query running when the leader stops, on shutdown, step-down or fail-stop, is interrupted and ends with `not_serving`. A write interrupted that way gets `channel_lost` (see [Write channels](#write-channels)). At the tail bound, a write refused before acceptance gets `maintenance` without naming the bound: the engine refuses its fresh connection as it does in any maintenance.

### Readiness

With `probe_listen` set, the server answers `GET /readyz` on that address, in plain HTTP and without authentication: `200` while it leads at least one configured database or has a fresh follower for one. A led database at a tail bound it cannot compact past vetoes readiness across the pod. The probe answers `503` when neither a leader nor a fresh follower is available, during a tail-bound veto, and from the start of shutdown. A server whose databases do not exist yet is not ready, so a Service that sends traffic only to ready pods never delivers the first `PUT`. Send it through a second Kubernetes Service over the same pods with `publishNotReadyAddresses: true`, or through `kubectl port-forward` to one pod.

Known limits of readiness in this version:

- Readiness is per pod, not per database. One database at a tail bound its leader cannot compact past makes the pod not ready for every database it leads.
- Each database's office loop runs on its own, so the databases of a set of pods can be led by different pods, and readiness cannot steer a request to the pod that leads its database. With `peer` set, the others forward it; without it, they answer `not_serving`, so list each database on one set of active pods, or run one active pod.

## Forwarding between pods

With `peer` set, any pod that lists a database accepts any request for it. A pod that cannot serve a request itself forwards it once to the pod that can, over a second listener with mutual TLS. A client never gets a redirect.

| Request on a pod that does not lead the database | Goes to |
|---|---|
| `query`, `describe` | A fresh local follower, if `follower_reads` is on; otherwise, or when the follower answers `not_serving` or `behind` before its answer starts, the leader |
| A channel open, `resolve` | The leader |
| `channel`, `channel/cancel`, `channel/close` | The pod whose process the token names, which in this version is the leader that opened the channel |
| `PUT`, `GET` status | Served where it arrives; status is `503` `not_serving` on every pod |

A pod that leads the database serves every request itself, except a channel request, cancel or close whose token names another process: it is forwarded as on any other pod, so that a leader deposed without noticing yet, for up to a lease renewal period, sends it to the leader in office rather than answering `channel_lost`.

**Membership.** Once its office loop reports it in office, a leader writes a leader record at `<prefix>/<db>/leader` with its process identifier (the one its channel tokens name), the `host:port` of its peer listener, the database identifier, its epoch and the transaction number of its epoch bump. It replaces only a record of an epoch no higher than its own, and retries a failed write every second. On step-down or shutdown it removes the address with a conditional replace, so it never overwrites a later leader's record. A forwarding pod reads the record for each request it forwards. Nothing beyond object storage and the configuration is needed.

**The hop.** The forwarding pod authenticates and authorises the request as usual, reads and checks the whole body, then sends the same method, path, body bytes, end-to-end headers and `Authorization` to the peer listener, with `Skylite-Forwarded: 1`, `Skylite-Request-Id` (its own request id, which the answer carries) and `Skylite-Budget-Ms`: what is left of the statement time, 30 seconds or `timeout_ms` if lower, after the time already spent. Hop-by-hop headers are dropped. The receiving pod verifies the bearer token and the role again, lowers its statement time to the budget, and never forwards the request again: if it cannot serve it, it answers `503` `not_serving`. The answer is relayed as it arrives, frame by frame, with its status and headers. If the other pod's answer is cut after it started, the forwarding pod cuts its own (HTTP/1.1 closes the connection, HTTP/2 resets the stream) and adds no terminal frame. It waits for the other pod at most the budget plus 30 seconds.

**One attempt.** A forwarding pod sends a request at most once. Only a `POST` with a body is forwarded: Go's HTTP client resends a `GET` or a request without a body by itself after some connection failures, and does not resend a body it cannot rewind. When the hop fails the forwarding pod answers itself:

| Failure | Answer |
|---|---|
| No leader is recorded, or the record has no address | `503` `no_leader`, or `owner_unreachable` for a channel request, cancel or close; `not_executed: true` |
| The hop failed before a connection to the other pod was obtained, such as a refused connection or a failed TLS handshake | The same, `not_executed: true` |
| The hop failed after a connection to the other pod was obtained, or the answer had no `Skylite-Protocol` marker | The same with `not_executed: false`: the other pod may have accepted the request, so its outcome is unsettled. The line is the connection, not the request headers, because Go's HTTP/2 transport can send the headers before it reports them written |
| The token names this pod's own process, which does not lead the database | `410` `channel_lost`, `not_executed: true` |
| The token names another process than the record, the record is of the same database, and its bump is above the token's open head | `410` `channel_lost`, `not_executed: true`: the channel opened under an earlier office, which has ended |
| The token names another process than the record otherwise | `503` `owner_unreachable`, `not_executed: true` |

**Pod authentication.** The peer listener requires TLS 1.3 and a client certificate that chains to `peer.ca_file`. A forwarding pod requires a server certificate that chains to the same CA and carries `peer.server_name`. Every pod presents its one peer certificate as both server and client, so the certificate needs both the `serverAuth` and `clientAuth` extended key usages. Use a CA that issues peer certificates and nothing else: whoever holds a certificate it issued is a pod. Forwarded requests go only to the address in the leader record, and through no HTTP proxy. The pod-to-pod headers are accepted only on the peer listener: a client that sends `Skylite-Forwarded` or `Skylite-Budget-Ms` gets `400` `invalid_request`, and a `Skylite-Request-Id` it sends is ignored.

## Configuration

The configuration is one JSON file. Unknown fields are refused, and the server does not start with a configuration it refuses. The file holds no secret.

```json
{
  "listen": ":8443",
  "tls": {"cert_file": "/etc/skylite/tls.crt", "key_file": "/etc/skylite/tls.key"},
  "auth": {
    "issuers": [
      {"issuer": "https://kubernetes.default.svc.cluster.local", "audience": "skylite", "jwks_file": "/etc/skylite/jwks.json"}
    ],
    "rules": [
      {"issuer": "https://kubernetes.default.svc.cluster.local", "subject": "system:serviceaccount:ops:provisioner", "database": "*", "role": "admin"}
    ]
  },
  "storage": {
    "prefix": "skylite",
    "s3": {"bucket": "my-bucket", "region": "eu-west-1"}
  },
  "databases": ["orders"],
  "peer": {
    "listen": ":8444", "address_env": "SKYLITE_PEER_ADDRESS",
    "cert_file": "/etc/skylite/peer.crt", "key_file": "/etc/skylite/peer.key",
    "ca_file": "/etc/skylite/peer-ca.crt", "server_name": "skylite-peer"
  },
  "probe_listen": ":8081"
}
```

| Field | Meaning |
|---|---|
| `listen` | TCP address to listen on. Required |
| `tls.cert_file`, `tls.key_file` | PEM certificate chain and key. The server serves HTTP/1.1 and HTTP/2 over TLS 1.2 or later. They are read once, at start |
| `plaintext` | `true` serves HTTP/1.1 and HTTP/2 without TLS (prior-knowledge h2c), for a server behind a mesh or proxy that terminates TLS. Exactly one of `tls` and `plaintext` must be set |
| `auth.issuers[]` | `issuer` (the exact `iss` value), `audience` (a value `aud` must contain) and, optionally, `jwks_file`, a JSON Web Key Set file read instead of OIDC discovery. The server reads each `jwks_file` at start and does not start if one is missing, unreadable or holds no usable key; it reads it again by 0006's cache policy, so a rotated file is picked up without a restart. Without `jwks_file`, `issuer` must be an `https` URL. The rules are those of 0006 |
| `auth.rules[]` | `issuer`; exactly one of `subject` or `claim` with `value`; `database`, a name or a pattern with `*` and `?`; `role`, one of `read`, `write`, `admin`. Without a matching rule, access is denied |
| `storage.prefix` | Key prefix, segments separated by `/`. A database's objects live under `<prefix>/<db>/`. Empty means no prefix |
| `storage.localfs.path` | A local directory. For trying the server out; a pod's local disk is not durable |
| `storage.s3` | `bucket` and `region` (required), `endpoint` for an S3-compatible service, `path_style` to address the bucket in the path |
| `storage.gcs` | `bucket` (required) and `endpoint` for an emulator, which then gets no credentials |
| `storage.azure` | `account` and `container` (required), `endpoint` for Azurite or another blob account URL (empty means `https://<account>.blob.core.windows.net`), and exactly one of `shared_key_file` (path to a file holding the base64 shared key), `shared_key_env` (name of an environment variable holding it) and `entra: true` (an Entra ID token from `azidentity`'s default credential chain) |
| `databases` | The databases this server leads and answers queries on: names of the `{db}` form, each once, at most 13. Optional; without it the server serves `PUT` alone. The leaders' local files go in a directory under the system's temporary directory (`TMPDIR`), removed on shutdown |
| `follower_reads` | Serve stateless reads from a fresh local follower while in standby. Optional, defaults to `true` when `peer` is set and to `false` otherwise |
| `peer.listen` | TCP address of the peer listener for [forwarded requests](#forwarding-between-pods). Without `peer`, nothing is forwarded |
| `peer.address_env` | Name of an environment variable holding the `host:port` other pods dial to reach this pod's peer listener, for example set to `$(POD_IP):8444` from the downward API. The server does not start if it is unset or holds no host |
| `peer.cert_file`, `peer.key_file` | PEM certificate and key this pod presents as peer server and client, read once at start |
| `peer.ca_file` | PEM certificates of the CA that issues peer certificates, and nothing else |
| `peer.server_name` | The DNS name every peer certificate carries, checked on the pod a request is forwarded to |
| `probe_listen` | TCP address of the [readiness probe](#readiness). Optional |

An `endpoint` must be an `http` or `https` URL with a host and no user information, query string or fragment, so that it carries no credential; the server does not start with one that breaks this, and the error does not repeat the value.

`storage` holds exactly one of `localfs`, `s3`, `gcs` and `azure`. The [storage backends page](/docs/storage-backends/) says how each one has been tested. S3 credentials come from the AWS SDK's default chain (environment variables, shared files, web identity, the instance or task role), Google Cloud Storage credentials from Application Default Credentials, and Azure Blob Storage either reads an account shared key from the file named by `shared_key_file` or the environment variable named by `shared_key_env`, or, with `entra: true`, uses `azidentity`'s `DefaultAzureCredential` (a service principal in `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `AZURE_CLIENT_SECRET`, workload identity, a managed identity, then the Azure CLI, Azure Developer CLI and Azure PowerShell logins; `AZURE_TOKEN_CREDENTIALS` narrows it).

## Logs and shutdown

The server writes JSON log lines to standard error: one when it starts; one per refused request with its request id and code; one for each authentication or authorisation refusal with the fields of [0006](/docs/decisions/0006-authentication/#principal-and-logging): the reason, the configured issuer, the subject once the signature has verified, the key id and the first 12 hex digits of the token's SHA-256 hash, and for a `403` the database and the role it needed; one for each created database with the principal's issuer and subject; the office loop's lines, with the database's name, when it takes office, steps down or fails a candidacy; one for each query or write that fails with `internal`, ends with an unknown outcome or fails after committing, with the engine's class of the failure; and one for each failed forward, saying whether its request may have been sent, and for each leader record the server could not read, write or withdraw. Logs never hold a bearer token or part of one, a channel token, SQL text, parameters or SQLite's messages.

On SIGTERM or SIGINT the server reports itself not ready, stops accepting connections, waits for running requests (a create, a query or a write runs for at most 30 seconds before its commit point, and the wait is at most a minute), leaves office, releasing each lease, then exits with status 0. If requests are still running after a minute, it closes their connections and exits with status 1. A second SIGTERM or SIGINT during the wait ends the process at once.

## Try it locally

A JWKS file and a token from your own issuer, or from a test signer, are enough:

```sh
cat > skylite.json <<'EOF'
{
  "listen": "127.0.0.1:8080",
  "plaintext": true,
  "auth": {
    "issuers": [{"issuer": "https://issuer.example", "audience": "skylite", "jwks_file": "jwks.json"}],
    "rules": [{"issuer": "https://issuer.example", "subject": "me", "database": "*", "role": "admin"}]
  },
  "storage": {"localfs": {"path": "data"}},
  "databases": ["orders"]
}
EOF
mkdir -p data
skylite serve -config skylite.json &
curl -i -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{}' http://127.0.0.1:8080/v1/databases/orders
curl -i -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT 1 + ? AS two","params":[1]}' http://127.0.0.1:8080/v1/databases/orders/query
```

The query answers `503` `not_serving` until the server has taken office for the new database. Its office loop retries a candidacy that fails, as one does while the database does not exist, with a backoff that doubles up to 20 seconds, so that can take about 20 seconds after the `PUT`. A new database has no tables. To write, open a channel and send each statement with the next sequence number:

```sh
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"mode":"write"}' http://127.0.0.1:8080/v1/databases/orders/channels
# take "token" from the answer's channel object
curl -s -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"channel":"'"$CHANNEL"'","seq":1,"op":"execute","sql":"CREATE TABLE t (a)"}' \
  http://127.0.0.1:8080/v1/databases/orders/channel
```

## Not built yet, and not verified

- Admission control of [0005 decision 9](/docs/decisions/0005-client-protocol/#9-limits-and-defaults): request and per-principal request bounds. The memory budget is fixed at 2 GiB rather than taken from the container's limit, and response frames are not charged to it.
- `batch`, `describe` on a channel and read-only channels; `resolve`, so a write whose channel was lost stays unknown to its driver; `GET` status.
- A follower does not forward a `query` or `describe` it cannot compile, as 0005 asks for a schema change it has not applied yet; it answers the compile error. Forwarded requests are not counted against any admission bound beyond the body share, and the leader record is read from storage for every forwarded request.
- Forwarding is tested between servers in one process on one local store, over loopback TLS, not between pods or through a mesh.
- `database_not_found` for a query: a server that does not lead the database and has no enabled follower for it answers `not_serving`, whether or not it exists.
- Keepalive frames during a long query or a write's lock wait, and the compiled-statement cache: every query and write compiles its statement.
- 0005's commit point is not visible to the server, so a close or a cancel cannot tell a write or commit before it from one in its upload; it interrupts the connection, and the engine reports what happened.
- Maintenance starts only from a commit or a write of the writer that holds the write lock, so it never meets another channel's idle open transaction in this version; the server's handling of the engine ending an idle transaction is tested by closing its engine connection, not by maintenance.
- A tail bound the leader cannot compact past is noticed when a request meets it, so a server that takes office there reports itself ready until its first query.
- The header block deadline on HTTP/2 streams: there, only the idle timeout bounds a header block that never ends. The 431 answer is tested on HTTP/1.1 only.
- Reloading a rotated TLS certificate without a restart.
- Running behind a real mesh or ingress has not been tested.
