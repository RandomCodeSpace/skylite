---
title: "0005: Client protocol"
description: "skylite will define its own protocol between the server and its Go and JDBC drivers: JSON over ordinary HTTP requests, newline-delimited JSON results, and channels with sequence numbers for every request that can write, and resolution from commit headers when a channel is lost, so that no request takes effect twice and no driver reports an outcome it cannot prove."
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

An earlier draft of this record identified stateless writes by a client-chosen commit identifier, deduplicated through a table bounded by a transaction-number window, with an outcome lookup on top. Its design review found that client-chosen identity invites collisions and reuse, that a table of refusals has no memory bound, and that the lookup was being stretched into exactly-once delivery for the application. The project owner replaced the mechanism with channels and narrowed the promises. Channels alone left every write in transit across a leader change unknown, which a rolling restart would surface on every deploy; the project owner then added resolution from commit headers ([decision 4](#resolving-a-request-after-its-channel-is-lost)). The [response to the review](#response-to-the-design-review) goes through every finding. A second design review found no counterexample to channels, replay or resolution by position, and found gaps in the wire schemas, the terminal frame, the state machine and admission; the [response to the second review](#response-to-the-second-design-review) goes through its findings.

## Decision

The protocol is version 1, as described below and in the [wire specification](#wire-specification). The server must implement it as written, and both drivers will.

### 1. What the protocol promises

- **At most once.** No request changes a database more than once, whatever intermediaries do: lose, delay without bound, truncate, reorder or duplicate requests and responses. A stateless read can be executed more than once if an intermediary duplicates it; that has no effect, because the stateless endpoint executes nothing that can change a database ([decision 5](#5-reads)).
- **Honest outcomes.** A driver reports success only for a write that is durably committed. It reports "not executed" or "not committed" only with one of the proofs in [decision 4](#4-when-a-driver-may-report-not-executed). Otherwise it reports "unknown". These promises hold for a driver that is the only holder of its channel's token ([two drivers sharing one](#attempts-to-break-the-channel-design)).
- **Resolution after a channel is lost.** A write whose channel is lost before the driver learns its outcome, by a leader change, a restart, the channel idle limit or anything else, is settled as committed or not committed by [`resolve`](#resolving-a-request-after-its-channel-is-lost) when all of these hold before the driver's resolve wait ends (default 30 s): a leader is in office and serving the database; the channel's open head is at most the resolve bound (default 100,000 transactions) below that leader's head; the leader's header window covers every transaction number from the open head to its head; and the leader has time left to answer. A new leader fills its window in the background after it takes office, so a resolve soon after a takeover can end unknown. Otherwise, and while the channel is live or not yet settled on the leader in office, the driver reports unknown.
- **Not promised:**
  - Exactly-once for the application. An application that retries after an error issues a new operation; that is the application's business, as with any database.
  - Recovery of a result after its response was lost, beyond the replay in [decision 3](#3-replay-of-the-last-outcome).

### 2. Channels

Every request that can write goes through a **channel**: a small piece of server state on one pod, opened by the driver. A driver connection (`driver.Conn`, `java.sql.Connection`) holds at most one write channel and at most one read-only channel.

**Opening.** `open` returns an opaque, unguessable **token** and a **channel identifier** that is not secret, for logs and errors. The channel is bound to the database, the authenticated principal (the JWT's issuer and subject), its owning pod and, for a write channel, the leader epoch. Write channels live on the leader. A read-only channel lives on the pod that received the open. The token carries at least 256 bits from a cryptographically secure random source, names the database identifier and the owning pod by an instance identifier that is drawn each time the pod's process starts and is not an address, carries the channel's **open head**, the head that the open answer reports as `txn` ([decision 4](#resolving-a-request-after-its-channel-is-lost)), and travels only in request bodies, never in a URL or a header. A token is matched by all of its bytes. The channel identifier is drawn independently and reveals nothing about the token.

**Sequence numbers.** Every request on a channel, other than `cancel` and `close`, carries a sequence number. The owner keeps, per channel: the last accepted sequence number, whether that request is still running, its recorded outcome ([decision 3](#3-replay-of-the-last-outcome)) and the SHA-256 digest of its exact request body bytes ([request bodies](#request-bodies)). On arrival, after the [checks before acceptance](#checks-before-acceptance):

| Sequence number | Answer |
|---|---|
| last + 1, nothing running | Accepted and executed |
| last + 1, the previous request still running | `channel_busy` |
| last, recorded as cancelled before it arrived | The recorded `cancelled` outcome, whatever the body |
| last, same body digest, still running | `in_flight` |
| last, same body digest, finished | The recorded outcome again |
| last, different body digest | `sequence_conflict` |
| below last | `stale_sequence` |
| above last + 1 | `sequence_gap` |

The rows apply in this order. A `cancel` that arrives before the request it names records that sequence number as cancelled, with no digest ([decision 6](#6-statements-transactions-and-errors)); that record takes precedence over the digest rule, so every later delivery of that sequence number, with any body, gets `cancelled`.

**Sequence numbers do not wrap.** They run from 1 to 2^63 − 1. A request with a larger number is refused before acceptance and changes nothing. A driver replaces a channel at its next idle point once its next sequence number would pass 2^62, and never begins a transaction on such a channel; at a million requests per second a channel reaches 2^62 after more than 100,000 years.

**A refused sequence number never changes any state.** It never rolls anything back and never ends the channel. A driver resends a request with a byte-identical body. A different body under an accepted sequence number is refused, and that refusal is final for the body that was refused: it can never be accepted, because its sequence number is taken. So two drivers that share a token by mistake are never told each other's outcome for the same sequence number; they still act on one channel ([attempts to break the channel design](#attempts-to-break-the-channel-design)).

**Transactions are a state of the channel.** Begin, statements, commit and rollback are requests on it. While a write transaction is open, the channel holds one SQLite connection and the database's write lock. An autocommit write is a single request on the channel. When SQLite rolls a transaction back by itself after an error, or a transaction limit ends it, the channel enters the **aborted** state: it holds nothing, and refuses every request except `rollback` until it gets one. So a statement is never run outside the transaction the driver believes it is in.

**Ending.** A channel ends when the driver closes it, when it has been idle past the channel idle limit, when its owning pod restarts or stops serving the database, or, for a write channel, when the owner stops holding office at the channel's epoch. A read-only channel's "epoch change" is its pod applying an epoch-bump commit. Requests on a channel that has ended are refused, for ever, with `channel_lost`: tokens are never reused, and channel state never moves between pods. A channel leaves its owner's table only once it has ended and nothing of it can still reach storage; `resolve` depends on that. An open transaction is rolled back, except a commit already past its commit point, which completes or fails on its own. The **commit point** is the moment the storage layer begins the create of the commit object ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)). Nothing interrupts a request after it.

**Closing** is how a driver makes sure that nothing still in transit on that channel can ever execute. The owner applies a close and the completion of a running request one at a time, in the order it applies them. A close of a channel with nothing running ends it at once. A close of a channel with a request running puts it in the **closing** state, which accepts no new sequence number; the running request is interrupted if it is before its commit point, and once it has finished and its outcome is recorded, the channel ends. Either way the answer carries the last accepted sequence number and that request's receipt ([decision 3](#3-replay-of-the-last-outcome)), and no sequence number is accepted after the close was applied. A second close, or a `cancel`, that arrives while the channel is closing waits for the same end and gets the same answer. A close of a channel that has already ended gets `channel_lost`.

**Opening is not deduplicated.** A duplicated open leaves a channel nobody owns. It holds no SQLite connection and no lock, and it expires at the channel idle limit. Channels are bounded per pod, per pod and database, and per pod, database and principal; at a bound, open is refused with `too_many_channels` and nothing is created.

**The commit identifier** stored in the commit object ([0004 decision 3](/docs/decisions/0004-storage-engine/#3-durable-layout-in-object-storage-per-database)) is composed by the server, 24 bytes: the first 16 bytes of the SHA-256 digest of all of the token's bytes, its open head included, then a sequence number as 8 bytes, big-endian. An autocommit write carries its own sequence number; a transaction carries the sequence number of its `commit` request. Clients never choose it. Only a holder of the token can compute it, and the digest does not reveal the token. The channel identifier is not stored in commits. Sixteen bytes make a collision between two channels a 2^−128 event per pair, and the identifier stays well inside the format's 64-byte field.

### 3. Replay of the last outcome

The recorded outcome of a channel's last request has two parts.

- **The receipt**, always kept. It is the request's terminal frame, which holds only fields of fixed size ([`done` fields](#done-fields), [errors](#errors)): the status, or the error object with its code, flags, SQLite codes, a message of at most 1,024 bytes and, for a batch, the first failed index `batch_index`; the total rows affected; the last insert row id; `txn`; `txn_state`; whether the statement was read-only; a batch's entry count; a describe's parameter count; and a commit's `committed` and `wrote`. Its largest encoding is under 1,500 bytes. Every channel reserves 2 KiB for its receipt when it opens, so the channel bounds also bound receipt memory, and recording an outcome never waits for memory. `cancel` and `close` answers embed the receipt.
- **Replay data**, kept when it fits: every frame of the response before its terminal frame, that is result columns and rows, a batch's `results` frames, and a describe's `columns` and `params` frames. It is kept whole or not at all, only if its encoded frames are at most the retained-data bound of 64 KiB, and only while the database's budget on this pod (32 MiB) and the pod's budget (256 MiB) both have room. Every kept byte is charged to both budgets and released when the channel records its next request or ends. Otherwise the data is dropped, and a replay sends the receipt alone with `data_omitted: true`.

Worst case per pod, at the defaults ([decision 9](#9-limits-and-defaults)): 16,384 channels × 2 KiB of receipts is 32 MiB, plus at most 256 MiB of replay data, 288 MiB in all, before allocator and index overhead. The leader's header windows are counted separately ([requirement 10](#requirements-on-the-engine-and-the-format)).

A driver that did not receive a complete response resends the same request, with the same sequence number, and gets the recorded outcome. That makes a lost response invisible to the application, for as long as the channel lives, for commits, for writes that return counts, and for small `RETURNING` results, the shape GORM uses for inserts (inference from GORM's documented behaviour; not checked against a pinned source).

When the replay data was dropped:

- **A read-only statement or a describe** is sent again under a new sequence number, if no row of the first response was handed to the application ([decision 5](#5-reads)).
- **An autocommit write**, a batch included, is reported as committed, result unavailable. The Go driver returns a `*skylite.Error` for which `errors.Is(err, skylite.ErrResultUnavailable)` holds and whose `Committed()` is true, with the transaction number. The JDBC driver raises `SQLException` (the base class) with SQLState `SKR01`, vendor code 0, and the transaction number in the message.
- **A write inside a transaction**, a batch included, is reported as a failed statement, with the transaction still open and the statement's effects still in it. The Go driver returns the same `ErrResultUnavailable` error with `Committed()` false; the JDBC driver raises `SQLException` with SQLState `SKR02`, vendor code 0.

Class `SK` is in none of the SQLState classes that Spring's translator maps ([SQLStateSQLExceptionTranslator.java#L59-L94][spring-state-sets]), and the base class `SQLException` is in none of the subclass translator's branches ([SQLExceptionSubclassTranslator.java#L75-L124][spring-sub]), so Spring reports both as an uncategorized, non-transient exception. Through Hibernate, vendor code 0 matches no case of the community SQLite dialect's converter ([SQLiteDialect.java#L451-L470][hib-sqlite-exc]), so Hibernate's standard converter falls back to `GenericJDBCException` ([StandardSQLExceptionConverter.java#L32-L40][hib-std-conv]). That exception matches none of the JDBC exception cases of Spring's Hibernate translator ([HibernateExceptionTranslator.java#L150-L200][spring-het]), which falls back to `JpaSystemException` ([#L217-L221][spring-het-fallback]), an `UncategorizedDataAccessException` ([JpaSystemException.java#L31][spring-jpa-sys]) and so a `NonTransientDataAccessException` ([UncategorizedDataAccessException.java#L29][spring-uncat]). The second design review traced this path in source; it was not run.

### 4. When a driver may report "not executed"

The complete set:

1. The request was never sent.
2. The owner accepted the request and recorded a failure for it. Every other delivery of it gets that same answer. These answers carry `final: true`.
3. The owner refused the body with `sequence_conflict`. That body can never be accepted.
4. The owner's answer to a `close` shows a last accepted sequence number below the request's, or its answer to a `cancel` recorded the request as cancelled before the request arrived ([decision 6](#6-statements-transactions-and-errors)). A close or cancel answer that names the request itself carries its recorded outcome, which is case 2 or a success.
5. A `resolve` answered `not_committed` ([below](#resolving-a-request-after-its-channel-is-lost)): no commit carries the request, and none ever will, so it has no effect on the database, now or later.

For a transaction, the driver may also report "not committed" when it never sent the commit request, whatever happened to the channel. Only the `commit` request's sequence number is ever stored, so a statement inside a transaction never needs a resolve.

Every other answer is about one delivery. That covers every refusal before the owner accepted the request ([checks before acceptance](#checks-before-acceptance): an incomplete or malformed body, no leader, an unreachable owner, overload, `behind`), `in_flight`, `channel_busy`, `stale_sequence`, `sequence_gap`, a redirect, a response without the `Skylite-Protocol` marker, a body without a terminal frame, and a transport error. The driver may send the same request again, with the same sequence number. Forwarding pods may resend it as well, because resending is always safe. If the driver gives up, it closes the channel; a close answer always settles the request (case 4, or the recorded outcome), and a close answered `channel_lost` sends the driver to `resolve`. Every error object therefore carries two flags: `not_executed`, which is about this delivery, and `final`, which says that this answer is the request's settled outcome.

**`channel_lost` is not a proof.** It says that nothing on the channel can execute from now on. It does not say what happened before, because the channel may have ended after executing the request:

1. A driver opens a write channel on leader L, epoch 3, at head 1300, and sends an autocommit insert as sequence 7. L commits it at transaction 1301. The response is lost when L's pod is killed.
2. L2 takes office at epoch 4; its epoch bump is at 1302.
3. The driver resends sequence 7. L2 answers `channel_lost`: the channel's owner is gone.

The insert is committed. The driver sends `resolve` for sequence 7; L2 finds the identifier at 1301, above the open head 1300 and below its bump, and answers `committed` at 1301.

#### Resolving a request after its channel is lost

**The open head.** The token carries the channel's open head *o*, the owner's head when the channel was opened, which the open answer also reports as `txn` ([decision 2](#2-channels)). The owner creates a commit only at its head plus one, so every commit of the channel is above *o*.

**`resolve`** takes the token and the sequence number, and nothing else. Any pod accepts it and forwards it to the leader. A leader's channel table holds each of its channels as **live**, as **ended but unsettled** (ended, while a commit of it may still reach storage), or not at all (**absent**: never held here, or ended with nothing left that can reach storage). The leader:

1. If it is stopping the database or has stopped serving it, answers `unknown`. It never answers from step 6 then.
2. Takes one **observation**, atomically with respect to every head advance, window change and channel-table change: the channel's status in the table, its head *h*, and whether its header window holds every transaction number from *o* + 1 to *h*.
3. If *o* is above *h*, answers `behind`. The answer is about this delivery.
4. If *o* is more than the resolve bound below *h*, answers `unknown`.
5. If the window does not hold the whole range, fills the missing headers from storage and goes back to step 2; the bound is checked again on the new observation. If the time left to the request runs out first, answers `unknown`.
6. From the observation: a header carrying the identifier, `committed`, at that transaction number. Otherwise, an epoch-bump commit in the range, `not_committed`. Otherwise `pending` if the channel is live or ended but unsettled, and `not_committed` if it is absent.

An ended but unsettled entry is never treated as absent.

**What the answers mean.** `committed` means that the request's changes are durably in the database's history. `not_committed` means that the request has no effect on the database and never will. A request that completed and changed nothing, such as an update that matched no rows or a transaction that only read, creates no commit object, and `resolve` answers `not_committed` for it. That answer is exact: the request has no effect, and an application that runs it again gets a serial execution in which the first never happened. While the channel lives, the owner answers the same commit with `committed: true, wrote: false`, which is also true of it.

Resolve changes nothing and needs no deduplication. The leader answers from a window of the last resolve-bound transaction numbers that it keeps in memory, indexed by identifier and by the positions of its epoch bumps, so step 6 is two lookups rather than a scan. The window is filled from the leader's own commits, the headers its takeover walk read, and the rest read from storage once. A new leader starts filling the rest in the background once it is in office; taking office never waits for it. A resolve that needs a range not yet filled waits for that fill rather than starting its own: the fill runs once per database however many resolves wait, with at most 16 header reads from storage in flight (configuration). A retired commit keeps its header ([0004 decision 3](/docs/decisions/0004-storage-engine/#3-durable-layout-in-object-storage-per-database)), so either form serves. A token that names another database is `invalid_request`, before acceptance. Resolve needs the permission a write-channel open needs, and counts against the control request bounds ([decision 9](#9-limits-and-defaults)).

*Correction marked by this record's author:* the project owner's decision answered `pending` whenever the scan reached the head. Then a channel ended by its idle limit or a close on a leader that stays in office would never settle. Step 6 settles it from the leader's own channel table, and the fourth bullet below shows that it settles every channel of an earlier leader as well. The owner's decision also bounded the number of headers scanned; this record bounds the open head's distance from the head instead, so that the leader never reads a header outside its window.

**Why the answers are final.** Let L be the leader on which the channel was opened. L's head was *o* then, and a leader's head is exactly its own last commit, so L's epoch bump is at or below *o*, and every commit the channel can cause is above *o*. A candidacy creates at most one bump ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)), so every bump above *o* was created by another candidacy, whether that candidacy then took office or failed. Each of them fences L: L creates a commit at *n* + 1 only while it holds *n*, and its names run contiguously from its own bump, so L holds no name at or above a bump of another candidacy that lies above its own. All of L's commits therefore lie below the first bump above *o*, and once that bump exists, the request committed if and only if its identifier is between *o* and the bump.

- **Identifier found.** The first object created at a name is the only object ever there ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)), and its retired form keeps the header. The match is permanent.
- **Bump found, no identifier.** The request's commit, if any, is L's, above *o* and below the first bump above *o*. Every name between is occupied for good, and none carries the identifier, so the commit does not exist and never will.
- **Head reached, channel absent, and L is this leader.** The channel has ended and nothing of it can still reach storage, it never accepts a request again, and the observation is atomic with every head advance, so every commit of it is at or below *h*. The window held the whole range, none was found above *o*, and every commit of it is above *o*.
- **Head reached, and L is another leader.** L's bump is at or below *o*. Names between this leader's bump and *h* are this leader's own commits, so L's bump is below this leader's bump, and this leader's bump fences L. No bump was found above *o*, so this leader's bump is at or below *o*, and so are all of L's commits. The request has no commit.
- **Pending** only when the channel is live or ended but unsettled on this leader: the request may still be running, may arrive, or may still reach storage.

**What the driver supplies.** The token and the sequence number, nothing else; the driver sends both unchanged from the unsettled request. No other value enters the argument, so no driver choice can turn `committed` into `not_committed` or back. Every answer is true of the request that the pair names, and a pair that names a different request gets a true answer about that one: a driver that resolves sequence 6 instead of 7 reports 7 as committed whenever 6 committed. Drivers must resolve the unsettled request's own pair. A token altered in any byte, its open head included, names an identifier that nothing was stored under, because the identifier is a hash of the whole token, and, matched by all its bytes, no channel. Its resolve never answers `committed`, and the only party it misleads is the client that altered it.

A leader that has been deposed and does not know it answers only from names that never change and from its own table, so its answers stay true. A channel of a later leader has an open head at or above that leader's bump, which is above the deposed leader's head: `behind` by step 3. Without step 3 the deposed leader would reach step 6 for a channel it never held and answer a false `not_committed`.

**Without a leader change.** An owner pod that restarts, or stops serving and takes office again, does so as a new candidacy with a new bump ([0004 invariant 1](/docs/decisions/0004-storage-engine/#4-invariants)), and its old channels settle by the second bullet. A channel ended by its idle limit or a close, on a leader that stays in office, settles by the third. The case that cannot be made final is a channel still live, or ended but unsettled, on the leader in office; a driver reaches a live one only after its principal changed ([decision 10](#10-authentication)), and reports unknown unless the channel's idle limit ends it within the resolve wait.

**Reopening.** A driver keeps each write channel's open head from its open answer, and the time of the last response on each channel. Before it uses a write channel that is idle, with no transaction open and no request unsettled, the driver closes it and opens a new one if either holds: its open head is more than half the resolve bound below the highest transaction number the pool has seen, or it has had no response for more than 60 seconds (driver configuration). The driver never replaces a channel with a transaction open or a request unsettled. Both rules are availability measures: they change only how often a resolve answers `unknown`, never what an answer means, and at a high enough commit rate no rule keeps a channel inside the resolve bound. A channel that is not idle cannot be replaced, and if its open head is more than the bound below the head when it resolves, the answer is `unknown`. A write transaction holds the database's only write lock from begin to commit, so no other commit lands while it is open, and the transaction lifetime (5 min) adds nothing. The head moves while the request waits for the lock and between its commit and the resolve answer, the lock wait plus the resolve wait, 35 s by default, plus the driver's resends before it learns `channel_lost`. A channel that was used within the last 60 seconds raised the pool's highest from its own answer then, so at most 60 seconds of commits are missing from the pool's highest when the open-head rule is checked. From the rule's edge, 50,000 behind, `unknown` needs more than 50,000 commits in that time:

| Commits per second | Pool active: 35 s | Channel idle up to 60 s: 95 s |
|---|---|---|
| 50 | 1,750 | 4,750 |
| 100 | 3,500 | 9,500 |
| 1,000 | 35,000 | 95,000: `unknown` possible |

An active pool reaches `unknown` only above about 1,400 commits per second, a channel idle for up to 60 seconds above about 520.

**What the driver does.** A driver that holds an unsettled autocommit `execute` or `batch`, or an unsettled `commit`, and gets `channel_lost`, sends `resolve` with the request's token and sequence number. It sends it again after `no_leader`, `behind`, `pending`, `overloaded` or a transport error, until its resolve wait ends (default 30 s, sooner if the caller's own deadline is sooner).

- `committed`: a commit succeeds. An autocommit write whose result the caller needs gets the committed, result-unavailable error of [decision 3](#3-replay-of-the-last-outcome).
- `not_committed`: an autocommit write may be executed once more, on a new channel, as a new request; otherwise it is reported not executed. A commit is reported as a rolled-back transaction.
- `unknown`, `pending` at the end of the wait, or no answer: unknown.

An unsettled statement inside a transaction settles without a resolve: the commit was never sent, so the transaction is reported rolled back. A `begin`, `describe` or `ping` changed nothing and is sent again on a new channel. After any of these, the driver opens a new channel for its next request.

### 5. Reads

**Stateless reads.** A statement outside a transaction that the driver expects to be a read goes to `query`, which any pod serves. The server compiles it, and refuses anything that is not a pure read with `needs_channel`, executing nothing; the driver then sends it on its write channel. A wrong guess by the driver costs a round trip and nothing else. A statement is a pure read when SQLite's read-only test is true for it ([`sqlite3_stmt_readonly`][sq-readonly], exposed as `Stmt.ReadOnly` ([stmt.go#L70-L77][nc-ro])), it is not refused below, and the SQL policy permits it on this path. SQLite's test is true for transaction control and for `ATTACH` and `DETACH`, so those are refused explicitly on every path: transaction control is never SQL in this protocol, and `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` are allowed only inside a transaction on a channel.

**The SQL policy hook.** A statement can pass the read-only test and still have effects. SQLite's documentation says application-defined functions can change the database as a side effect and the test still returns true ([sqlite.h.in#L4740-L4768][sq-readonly]), and some pragmas change the connection while the statement is prepared, before any step ([pragma.c#L1158-L1190][sq-pragma-flag]). The SQL policy of the later lockdown record must therefore be consulted before any such effect, on every path, including `describe`:

1. The policy is installed as SQLite's authorizer on every connection, before its first prepare ([config.go#L202][nc-auth]). SQLite consults the authorizer while it prepares a statement, and for a pragma it does so before the pragma takes effect ([pragma.c#L471][sq-pragma-auth]).
2. The server prepares the statement. A denial by the authorizer refuses the statement with `statement_forbidden`.
3. The server checks the uncompiled tail ([conn.go#L193-L203][nc-prepare]), the read-only test where the path requires it, and, for a batch entry, that it returns no columns.
4. Only then does the server bind or step.

The policy itself belongs to that record.

**Read-your-writes.** Every successful answer carries `txn`, the transaction number of the state it reflects, directly or in the receipt it embeds ([`done` fields](#done-fields)). Each driver keeps the highest it has seen, its **watermark**, in one place shared by a pool, raises it only from reported `txn` values, and never lowers it. It sends the watermark as `min_txn` with every stateless read, every `begin`, and every `execute` or `batch` on an idle channel, write or read-only. In the Go driver the owner is the `driver.Connector` behind a `sql.DB`. In the JDBC driver it is a registry held by the driver class and living as long as it, keyed by the canonical base URL and the database name and by nothing else: never by credentials or other connection properties, so a credential refresh or a replaced connection keeps the same watermark. The base URL is canonicalised by RFC 3986's syntax-based and scheme-based normalisation ([sections 6.2.2 and 6.2.3][rfc3986-norm]): scheme and host in lower case, percent-encoding normalised, the default port removed, and an empty path or a trailing `/` removed; a base URL with user information, a query or a fragment is refused.

A pod whose state is older than `min_txn` waits up to the follower catch-up limit, then forwards a `query`, a `describe` or a read-only open to the leader. On a channel the check comes before acceptance ([checks before acceptance](#checks-before-acceptance)): the owner of a read-only channel waits up to the catch-up limit, and the owner of a write channel does not wait, because a leader in office has every durable transaction: its takeover walk placed its bump after the last commit in the log ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)). An owner still below `min_txn` refuses with `behind`: not executed, the sequence number unchanged. The driver then closes the channel, which settles the request as not executed ([decision 4](#4-when-a-driver-may-report-not-executed), case 4), and sends it again on a new channel. On a write channel, `behind` means that the owner has been deposed without knowing it; without the check, an autocommit read on that channel, or an update that matched no rows, would succeed against state older than a write the pool has seen, because neither creates a commit object that meets the fence. The guarantee covers snapshots opened, and autocommit statements accepted, after the write completed. An already open transaction keeps its snapshot.

**Snapshots.** A `begin` establishes its transaction's snapshot and reports its transaction number in one step. A plain SQLite `BEGIN` only sets flags and does not touch the database ([sqlite.h.in#L4765-L4768][sq-begin]), so the server starts the read itself, and takes the snapshot's transaction number from the engine at that moment ([requirement 12](#requirements-on-the-engine-and-the-format)), before it answers. A write `begin` uses `BEGIN IMMEDIATE`, which takes the write lock and starts the read at once.

**Read-only transactions** use a read-only channel, opened on whichever pod received the open and bound to it. Its connection refuses writes: a write fails with `sql_error` carrying SQLite's `SQLITE_READONLY`. A statement on any channel that its pod cannot compile, for example because the pod has not yet applied a schema change, fails on that channel with its recorded error; only stateless requests are forwarded for that reason ([decision 7](#7-transport-and-routing)).

**Exposed rows.** A driver may retry a read only if no row of it has been handed to the application. Once a row has been exposed, a failed or incomplete response is an error, never a silent restart and never a short result.

**Isolation.** SQLite gives serializable isolation: a write transaction holds the only write lock, and a read-only transaction reads one snapshot. A request for a weaker level is served at serializable. The JDBC 4.3 specification permits a driver to substitute a more restrictive level (section 10.2.1, [JSR 221][jdbc-spec]; the design review checked the text). The JDBC driver will report `TRANSACTION_SERIALIZABLE`. The Go driver will accept every `sql.IsolationLevel` that serializable satisfies and return an error for `LevelLinearizable`, as the driver contract requires for an unsupported level ([driver.go#L279-L294][go-begintx], [sql.go#L126-L141][go-iso]), because a follower may serve a snapshot that is not the newest.

### 6. Statements, transactions and errors

- **One statement per execution.** Text after the first statement other than whitespace and comments is `invalid_request`: refused on a stateless path, recorded on a channel. No server-side statement handles and no cursors: SQL text is sent with every execution.
- **Autocommit failure commits nothing.** An autocommit request that fails rolls back its implicit transaction, including the changes of a statement that failed under SQLite's `FAIL` conflict action, which SQLite itself would keep ([lang_conflict.html][sq-conflict], read on 2026-11-15). That difference from SQLite is deliberate. An autocommit batch is one transaction: all of it commits or none of it.
- **Inside a transaction, SQLite's semantics apply.** An error makes no claim that the statement had no effect, and carries `not_executed: false`. Every answer on a channel reports `txn_state` (`none`, `open` or `aborted`), read from SQLite after the statement with `sqlite3_get_autocommit` ([sqlite.h.in#L7000-L7013][sq-autocommit], exposed as `Conn.GetAutocommit` ([conn.go#L273-L279][nc-autocommit])), because SQLite rolls the whole transaction back after some errors. A batch inside a transaction stops at its first failing entry; the entries before it keep their effect, and the error carries `batch_index`, counted from 0. There are no internal savepoints.
- **A failed `begin` leaves no transaction.** A `begin` that fails after it started the SQLite transaction, while establishing its snapshot or in its first statement, rolls the transaction back, closes the connection and leaves the channel idle, with the error recorded, `not_executed: true` and `txn_state: "none"`. For a first statement under SQLite's `FAIL` conflict action that discards changes SQLite would keep inside a transaction, the same deliberate difference as for autocommit. The driver sends the application's next statement in that transaction as a new `begin`.
- **A durable commit stays committed in every answer.** Once a commit is durable, no later event, such as a response write timeout, a cancelled caller or a slow client, may produce a negative or "no effect" answer for it. The statement time limit covers execution up to the commit point. Writing the response has its own deadline, and failing it only cuts the response, which the driver resolves by replay.
- **Cancellation** is an explicit `cancel` request naming the channel and the sequence number. If that request is running and has not reached its commit point, the owner interrupts SQLite through the connection's context ([conn.go#L328-L348][nc-interrupt]). It answers after the request has finished, with what actually happened: cancelled and not committed, or committed. If the named request has not arrived yet and is the next one, the owner records it as cancelled, so that it never executes, and every later delivery of it gets that outcome, whatever its body. A dropped HTTP connection cancels nothing by itself, except that the server stops streaming the rows of a read.
- **Go deadlines.** Go requires `ExecContext` and `QueryContext` to return when their context is cancelled ([driver.go#L198][go-ctx-exec], [driver.go#L225][go-ctx-query]). The Go driver returns as soon as the context ends. If the request was never sent, it returns the context's error. If it was sent and is not settled, it returns an error that satisfies `errors.Is` for both `skylite.ErrOutcomeUnknown` and the context's error, and stops using the channel; sending `cancel`, waiting for its answer up to the cancel wait and closing the channel continue in the background, and nothing waits for them. Every request on a write channel is settled before any of its rows reach `database/sql`: the driver reads the whole response, up to the result limit, before it returns `driver.Rows`, so an autocommit `RETURNING` never shows rows of an unsettled write. A pure read cannot leave an outcome unknown, so its rows may stream. `driver.Tx.Commit` takes no context, so the Go driver bounds a commit by its own commit wait.
- **JDBC query timeouts.** JDBC requires a statement that exceeds its query timeout to raise `SQLTimeoutException` ([Statement.java#L224-L232][jdk-timeout]). The JDBC driver sends `cancel` when the timeout passes and waits for its answer up to the cancel wait. It raises `SQLTimeoutException` only when the answer's receipt is `cancelled`, which the server records only for a statement it interrupted before its commit point. If the receipt shows that the statement finished, the driver returns its result. Otherwise the outcome is unknown, and the driver raises `SQLNonTransientConnectionException` with SQLState `08007`, names the timeout in the message, stops using the channel and closes it without waiting. That is a deliberate departure from the `setQueryTimeout` contract: Spring classes `SQLTimeoutException` as transient (below), and a transaction template or retry policy that trusts that class may run again a write that committed. The JDBC driver's own conformance tests must expect `08007` for this case.
- **Connections are not reused after a write.** The SQLite connection that served a transaction or an autocommit write is closed when it ends, until a complete reset is specified and proven. Stateless reads may share pooled connections, because the policy hook keeps a pure read from changing connection state.
- **Compiled statements are not shared across authorisation contexts.** SQLite consults the authorizer while it compiles a statement ([sqlite.h.in#L3396-L3401][sq-authorizer]), so a cached compiled statement skips the policy hook. A compiled statement is never reused under a different principal, different token claims or a different policy generation. Until the authorisation record defines safe reuse, compiled statements are cached only per channel, and dropped when a request on the channel presents different claims or the policy generation changes, or, on the stateless read path, keyed by the SQL text and the full authorisation context: principal, claims and policy generation.
- **Error text.** Responses to the authenticated caller may carry SQLite's message, which can include application text such as a trigger's `RAISE` message. Server logs carry codes and identifiers only, never SQL text, parameters, tokens or SQLite messages. Deployments must not log request bodies or `Authorization` headers at the mesh or the ingress.

**What each driver raises for an unknown outcome.** The uncertainty must survive `database/sql`, GORM, Hibernate and Spring:

| Driver | Error |
|---|---|
| Go | `*skylite.Error` for which `errors.Is(err, skylite.ErrOutcomeUnknown)` holds, carrying the channel identifier, sequence number and epoch. It never matches `driver.ErrBadConn`. It also matches the context's error when the caller's context ended, and only then |
| JDBC | `SQLNonTransientConnectionException`, SQLState `08007` ("transaction resolution unknown"), vendor code 0 |

Every place where the default translation would turn an uncertain outcome into something an application treats as retryable, and which the drivers must therefore avoid:

- **`driver.ErrBadConn`.** `database/sql` retries the operation on it ([driver.go#L151-L163][go-badconn], [sql.go#L1569-L1580][go-retry]). The Go driver returns it only from an operation that sent no request.
- **Context errors.** Generic Go retry helpers commonly retry `context.DeadlineExceeded` (inference). An unknown outcome caused by the caller's deadline matches both `ErrOutcomeUnknown` and the context's error, so that code testing for its own cancellation sees it; a helper that retries on the context error without testing `ErrOutcomeUnknown` first can run again a write whose outcome is unknown, which is the application's retry and outside the promises of [decision 1](#1-what-the-protocol-promises). `database/sql` itself returns the context error from `Rows.Err` after a cancellation during iteration ([sql.go#L3133-L3150][go-rows-err]) and from `Tx.Commit` when the context ended before the commit was sent; neither hides an unknown outcome, because rows of a write-channel request reach `database/sql` only after the request settled, and a commit that was never sent has not committed.
- **`SQLTimeoutException`.** Spring turns it into a transient `QueryTimeoutException` ([SQLExceptionSubclassTranslator.java#L91-L93][spring-sub-timeout]), and so does its Hibernate translator for Hibernate's own query timeout ([HibernateExceptionTranslator.java#L156-L157][spring-het-timeout]). The JDBC driver raises `SQLTimeoutException` only when the cancel answer's receipt is `cancelled`. For an unknown outcome it raises `08007` and names the timeout in the message, a deliberate departure from the `setQueryTimeout` contract ([JDBC query timeouts](#6-statements-transactions-and-errors)).
- **`SQLTransientConnectionException`, `SQLTransactionRollbackException`, `SQLRecoverableException` and SQLState class 40.** Spring maps them to transient or recoverable exceptions ([SQLExceptionSubclassTranslator.java#L81-L90][spring-sub-transient], [#L118-L119][spring-sub-recoverable], [SQLStateSQLExceptionTranslator.java#L77-L80][spring-state-40]). Not used for unknown.
- **Vendor codes 5 and 6.** The community SQLite dialect turns `SQLITE_BUSY` and `SQLITE_LOCKED` into `LockAcquisitionException` ([SQLiteDialect.java#L455-L460][hib-sqlite-busy]), which Spring turns into the transient `CannotAcquireLockException` ([HibernateExceptionTranslator.java#L159-L160][spring-het-lock]). Unknown carries vendor code 0, and skylite's dialect must keep code 0 unmapped.

`SQLNonTransientConnectionException` reaches Spring's non-transient `DataAccessResourceFailureException` directly ([SQLExceptionSubclassTranslator.java#L96-L98][spring-sub-nontransient]) and through Hibernate's `JDBCConnectionException` ([SQLStateConversionDelegate.java#L78-L80][hib-state-08], [HibernateExceptionTranslator.java#L150-L151][spring-het-conn]); the design review traced both paths in source. The GORM dialector used with the Go driver must pass `ErrOutcomeUnknown` through untranslated, including when GORM's `TranslateError` option is on (unverified: the dialector is chosen with the Go driver).

### 7. Transport and routing

- Ordinary HTTP requests and responses over HTTP/1.1 or HTTP/2. TLS is terminated by the mesh or by the server. WebSockets, gRPC and long-lived streams are not used.
- **Any pod accepts any request.** Clients are configured with one base URL and never receive pod addresses. A pod serves what it owns (a stateless read, a channel it owns) and forwards everything else: a write-channel open, a read-only open it cannot serve at `min_txn`, `GET` status and `PUT` create to the leader, and a channel request to the pod its token names. A follower that cannot compile the statement of a stateless `query` or `describe` forwards it to the leader, because it may not yet have the schema change the statement needs. A channel request is never forwarded for that reason: it runs on its channel's connection and snapshot, and a compilation failure is that sequence number's recorded error.
- **Forwarding failures.** A forwarding pod that cannot reach the pod a token names answers `owner_unreachable`: not executed, not final, and no evidence that the channel has ended. It answers `channel_lost` only when the cluster's records show that the named pod instance no longer exists or no longer serves the database, and `no_leader` only for a request that goes to the leader. An unreachable owner is not an unreachable leader, and the reverse.
- **One hop at most.** The forwarding pod marks the request `Skylite-Forwarded: 1` and never forwards a request that carries it. Forwarding preserves the method, path, end-to-end headers (including the caller's `Authorization`) and body bytes, regenerates hop-by-hop headers and message framing ([RFC 9110 section 7.6.1][rfc9110-conn], [RFC 9113 section 8.2.2][rfc9113-conn]), and relays the owner's frames unchanged. Each hop flushes after every frame. The receiving pod verifies the bearer token again: the hop is never trusted for authentication or authorisation. The forwarding pod sends the request body with the caller's `Content-Length`; if it cannot pass on the complete body, it aborts the upstream request (resetting the stream, or closing the connection) and never ends a prefix normally. If the hop fails after the owner started its response, the forwarding pod ends the body without a terminal frame; it never invents one.
- **Time budgets are passed on.** The forwarding pod sends `Skylite-Budget-Ms`: the request's remaining time budget, decremented by the time already spent. The owner uses the smaller of that and its own limits. The header can only lower a limit.
- **Pod-to-pod requests must authenticate the receiving pod**, with mesh mutual TLS and a checked identity or with server TLS and a cluster certificate, and a pod takes forwarding addresses only from the cluster's own records. Otherwise a forged address would collect callers' bearer tokens. Cluster membership is a later record.
- **Keepalive frames** may keep an idle timeout from cutting a response that is waiting for a lock, an upload or a slow statement. Correctness never depends on them: HTTP does not guarantee incremental delivery through intermediaries ([RFC 9110 section 7.6][rfc9110-fwd]), and a cut response is resolved by replay.
- **Redirects.** Both drivers refuse every redirect: a 3xx answer is a transport error for that delivery, and the request stays unsettled. A driver never follows a redirect, never changes the base URL's scheme, host or port, and never sends a request or its `Authorization` header anywhere but the configured base URL. Go's default client follows up to 10 redirects ([client.go#L833-L835][go-redirect-max]) and resends the method and body on 307 and 308 ([client.go#L512-L540][go-redirect]), so the Go driver must install a redirect policy that refuses them.

**What a deployment must provide.** This record pins no version of Istio, Envoy or an ingress controller, and nothing here was checked against one. A deployment is supported when its mesh and ingress meet these requirements; validating them through a real mesh and ingress is [unverified](#not-verified-and-what-the-first-implementation-must-prove). Let *T* be the statement time limit (which runs from arrival and so includes the lock wait), plus the engine's bound on resolving a commit after its commit point ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop), set by the engine), plus the response deadline: 30 s + *R* + 10 s at the defaults, where *R* is the engine's bound.

- **Overall route or request timeout** at least *T*, on every hop.
- **Per-try timeout**, where the proxy retries, at least *T*: a shorter one cuts every long commit and every retry with it. Retries by a proxy are safe, because resending is always safe, but nothing depends on them.
- **Idle timeout** above the keepalive interval (10 s) where the proxy passes response frames on as they arrive, and at least *T* where it buffers responses.
- **Buffering.** Request buffering is allowed, but the request must reach the server with its `Content-Length` and complete body. Response buffering is allowed; keepalive frames then do not help, and only the idle timeout above keeps a long answer.

A deployment that misses one of these costs availability (more replays and resolves), never correctness.

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
- **Booleans** are written as INTEGER 0 and 1. Both drivers read INTEGER 0 as false and 1 as true, and raise an error for any other INTEGER read as a boolean. That is the shared rule, and it covers INTEGER only. Go's `database/sql` applies its own conversions after the driver has returned a value: scanning into a `bool` also accepts a string or byte slice that `strconv.ParseBool` accepts, such as TEXT `"true"` ([convert.go#L382-L387][go-scan-bool], [types.go#L49-L58][go-bool]), and the driver cannot refuse that. How the JDBC driver reads TEXT, REAL or BLOB as a boolean is for its record.
- **Temporal values** are TEXT, in four canonical forms, written with a fixed width so that text order equals time order within each form:

  | Kind | Go | Java | Canonical form |
  |---|---|---|---|
  | Date | none | `LocalDate` | `YYYY-MM-DD` |
  | Time of day | none | `LocalTime` | `HH:MM:SS.fffffffff` |
  | Local timestamp | none | `LocalDateTime` | `YYYY-MM-DD HH:MM:SS.fffffffff` |
  | Instant | `time.Time` | `Instant`, `OffsetDateTime`, `ZonedDateTime` | `YYYY-MM-DD HH:MM:SS.fffffffff`, converted to UTC, no zone suffix |

  Fractions always have nine digits. Local values are written as their fields, without conversion; instants are converted to UTC. Years run from 0001 to 9999; a driver refuses to write a value outside that range, before sending anything. The Hibernate SQLite dialect computes date parts with SQLite's `strftime` ([SQLiteDialect.java#L176-L186][hib-sqlite-strftime]), which reads any number of fraction digits ([date.c#L220-L231][sq-frac]).
- **Reading existing text.** Text in a database need not be canonical. A driver reads, as a date or timestamp, text in these forms only: `YYYY-MM-DD`; then optionally a space or `T` and `HH:MM`, `HH:MM:SS` or `HH:MM:SS` with 1 to 9 fraction digits; then, after a time, optionally `Z` or an offset `+HH:MM` or `-HH:MM`. A time of day is read from `HH:MM`, `HH:MM:SS` or `HH:MM:SS` with 1 to 9 fraction digits. Read as an instant, text with an offset is converted by it, and text without one is UTC. Read as a local value, text with an offset is an error. Any other text, more than nine fraction digits, and years outside the range are not read as temporal values.
- **Declared date types in Go.** The Go driver returns a `time.Time`, in UTC, for a TEXT value in a column declared `DATE`, `DATETIME` or `TIMESTAMP` only when the text is in one of the date or timestamp forms above; a date alone becomes midnight UTC. Otherwise, and for INTEGER and REAL values, it returns the value unchanged.
- **Decimals** are TEXT in plain notation. Stored in a column with NUMERIC affinity they become INTEGER or REAL and can lose precision ([datatype3.html][sq-affinity], read on 2026-11-15); a decimal that must be exact needs a column declared `TEXT`.
- **Conformance vectors.** Both drivers' value codecs will be tested against one file, `clients/conformance/values.ndjson`, which is not created in this pull request. Each line is one JSON object: `name`; `wire`, the exact bytes of a value as a JSON string; `class`, the storage class; `value`, the canonical value as text (decimal for numbers, hexadecimal for bytes); and, for vectors that must be refused, `error` with the code. A codec passes when it decodes every valid `wire` to `class` and `value`, encodes every valid `value` back to the identical `wire`, and refuses every invalid vector. The set must cover both ends of the 64-bit range, integral REALs, `-0.0`, both infinities, NaN, invalid UTF-8, lone surrogates, embedded NUL, the empty string, the empty blob, NULL and a value at the largest allowed frame size.

### 9. Limits and defaults

Every limit is server configuration unless marked as driver configuration. The [`limits` object](#the-limits-object) in a channel's open answer and in `GET` status reports those that bound what a driver may send or wait for.

| Limit | Default |
|---|---|
| Request body | 16 MiB |
| Request header block | 64 KiB |
| Header block read deadline, from the connection's or stream's first byte of the request | 10 s |
| Body read deadline, from the end of the header block | 30 s |
| Entries in one batch | 10,000 |
| Entries in one `results` or `params` frame | 1,024 |
| Result size: bytes of all frames of one response before its terminal frame | 64 MiB |
| Terminal frame, reserved in every response | 4 KiB |
| Error message, as encoded in the error object | 1,024 bytes |
| Statement time: for a write, from arrival to its commit point; for a read, to its last row written | 30 s; a request may lower it |
| Response deadline, after a request's outcome is recorded | 10 s |
| Keepalive interval | 10 s |
| Lock wait | 5 s |
| Writers waiting for the lock, per database | 64 |
| Writers waiting for the lock, per database and principal | 16 |
| Follower catch-up wait for `min_txn` | 500 ms |
| Channel idle limit | 10 min |
| Write transaction idle limit | 30 s |
| Write transaction lifetime | 5 min |
| Read-only transaction idle limit | 30 s |
| Read-only transaction lifetime | 120 s |
| Channels per pod | 16,384 |
| Channels per pod and database | 4,096 |
| Channels per pod, database and principal | 256 |
| Receipt reserved per channel | 2 KiB |
| Concurrent requests per pod, other than control requests | 1,024 |
| Concurrent requests per pod and principal, other than control requests | 256 |
| Concurrent stateless requests per pod | 256 |
| Concurrent stateless requests per pod and principal | 64 |
| Concurrent control requests (`cancel`, `close`, `resolve`) per pod | 64 |
| Concurrent control requests per pod and principal | 16 |
| Retained replay data, one request | 64 KiB |
| Retained replay data, per database and pod | 32 MiB |
| Retained replay data, per pod | 256 MiB |
| Header reads from storage in flight while a leader fills its window, per database | 16 |
| Resolve bound: how far below the leader's head a channel's open head may be for a resolve, and the leader's window of commit headers | 100,000 transactions |
| Driver wait for a cancel answer (driver configuration) | 2 s |
| Driver wait for a resolution (driver configuration) | 30 s, or the caller's deadline if sooner |
| Driver replacement of a write channel with no response for (driver configuration) | 60 s |

**Admission.** A request counts against the pod's request bounds from the end of its header block until its response ends, so slow bodies, duplicate deliveries and waiting `close` and `cancel` calls are all counted. `cancel`, `close` and `resolve` have their own capacity, so they are never starved by the work they are meant to end, and they count against no other request bound. The server authenticates the bearer token from the headers before it reads the body, and applies the per-principal bounds before it reads the body. A request above a concurrency bound is refused with `overloaded` (not executed, not final). Resolves that wait for a window fill hold control capacity, and the fill itself is bounded per database, not per resolve ([decision 4](#resolving-a-request-after-its-channel-is-lost)).

Every bound has a defined answer: `too_many_channels` for channel bounds, `overloaded` for concurrency bounds, `busy` for waiting writers (recorded, so final), `request_too_large` for bodies, `invalid_request` before acceptance for a body that misses its read deadline, `data_omitted` for replay data over its bounds, `unknown` for a resolve past the resolve bound, and, from the HTTP layer, status 431 for an oversized header block and a closed connection for one that misses its deadline, possibly without the marker.

**Waiting writers** are served first in, first out. No principal has more than 16 of a database's 64 waiting slots. This version promises no fairness between principals beyond that: a principal can hold the write lock for a whole transaction lifetime by sending requests before each idle limit.

Idle time runs from the end of one response to the arrival of the next request on the channel; a running request is never idle. The channel idle limit runs only while the channel is idle or aborted, never while a transaction is open; the transaction's own limits apply then. A transaction that reaches its idle limit or lifetime before its commit point is rolled back and the channel enters the aborted state, except that a running `commit` fails with `timeout` and leaves the channel idle. A commit past its commit point is never interrupted by a timer; only the engine's bound on resolving it applies.

**Why the read limit includes writing.** On the leader, an open read pins a WAL snapshot, checkpoints cannot backfill past it, and the leader refuses writes once the WAL reaches its bound ([0004 decision 10](/docs/decisions/0004-storage-engine/#10-limits-accepted-in-the-first-version)). A slow reader would otherwise stop every writer.

**Workload limits.** A write transaction holds the database's only write lock from begin to commit, including the application's own work between statements, so long work inside a write transaction blocks every other writer, and a competing writer fails with `busy` after the lock wait. A transaction that holds the write lock while it waits for another write transaction of the same application, such as a nested `REQUIRES_NEW` write in Spring, can only time out. A Spring `@Transactional` method that is not read-only is a write transaction even if it only reads.

### 10. Authentication

- Every request carries `Authorization: Bearer <JWT>`, and every pod that handles a request verifies it, from the headers and before it reads the body, the forwarding hop included. Authentication and the principal match are checked before the sequence number, so a refused delivery changes nothing, and a driver can resend with a refreshed token for the same principal.
- A token for another principal on a channel gets `channel_lost` and leaves the channel untouched. A driver whose credential source changes principal therefore sees `channel_lost`, resolves an unsettled request, which stays `pending` while the channel is live, and opens a new channel.
- `unauthenticated`, `forbidden` and `statement_forbidden` are distinct codes. Authorisation is checked before acceptance; how token claims map to permissions is a later record.

### 11. What the driver records decide

Driver-internal behaviour is specified with each driver, in a record that ships in the same pull request as that driver. Those records must decide:

| Topic | Review finding (first review, or second review marked "2:") |
|---|---|
| The `java.sql.Connection` lifecycle: auto-commit transitions, `commit` and `rollback` in auto-commit mode, close, abort, network timeout, pool reset, `isValid` through the channel's `ping`, and the failed-transaction latch; the Go equivalents, `driver.Validator`, `driver.SessionResetter` and transaction-context cancellation | 14, 15 |
| The complete mapping of error codes to Go errors and to JDBC exception classes and SQLStates by meaning, within the constraints of [decision 6](#6-statements-transactions-and-errors) | 16, 28 |
| Generated keys beyond a single row id, including the zero row id, keys requested by column, multi-row inserts, and keeping the skylite Hibernate dialect off a separate `select last_insert_rowid()` outside a transaction | 17 |
| Declared-type to JDBC-type mapping, `ResultSetMetaData`, `ParameterMetaData`, `DatabaseMetaData`, and who fixes the schema-validation failure | 18 |
| Savepoint handles | 28 |
| Batch failure reporting: `BatchUpdateException`, update-count arrays and large counts, and what a replayed batch without its `results` frames reports | 28 |
| Result-set behaviour: forward-only, closing, and draining or buffering a response before the next request on the channel; `getObject`, typed getters, `wasNull` and streams | 13, 28 |
| Credential-provider ownership and refresh, in pools and across a request's resends | 11, 28 |
| Session-level timeouts reaching the server, such as Spring's transaction timeout | 20 |
| Input binding and coercion: Go `driver.Valuer` and `driver.NamedValueChecker`, JDBC `setObject` and `setNull`, numeric range and precision, temporal calendar overloads and the legacy `java.sql.Date`, `Time` and `Timestamp` types, and limits on stream and LOB parameters | 2: 13, 15 |
| Statement API behaviour: the distinctions between execute, query and update, update-count and result transitions, JDBC escape processing, statement closing and concurrent use, and refreshing prepared statements and metadata after a channel or schema change | 2: 15 |
| Capabilities and unsupported operations: how `DatabaseMetaData` agrees with the methods supported, holdability, SQL types and connection features this protocol does not carry | 2: 15 |
| Hibernate dialect SQL generation: DDL and type generation, identity retrieval, functions, pagination and locking | 2: 15 |
| HTTP client lifecycle: the redirect refusal, keeping request bodies for resends, connection reuse and draining, retry and backoff limits, the commit wait, and bounded cleanup after a caller has returned | 2: 9, 15, 16 |
| The watermark owner's identity and lifetime, within [decision 5](#5-reads), including credential refresh | 2: 8, 15 |

### 12. Later work

The authorisation model; the SQL lockdown policy; database deletion and listing; compression; observability; cluster membership.

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
| `POST /v1/databases/{db}/resolve` | Settle a request of a lost channel | The leader |
| `GET /v1/databases/{db}` | Status | The leader |
| `PUT /v1/databases/{db}` | Create the database if absent | The leader, or the pod that wins creation |

Requests carry `Authorization: Bearer <JWT>`; `POST` and `PUT` carry `Content-Type: application/json`. `GET` carries no body.

**Integers.** Every integer in a request or a response is a JSON number with no sign other than a leading `-`, no leading zeros, no fraction and no exponent, in the range its field states. Every stated range lies within the signed 64-bit range, so Go's `int64` and Java's `long` hold every value exactly. Receivers parse integers exactly, never through a double, as for SQL INTEGER values ([decision 8](#8-values-and-shared-conventions)). A value outside its field's range is a bad value.

### Request bodies

- Every `POST` and `PUT` carries `Content-Length`, and its body is exactly that many bytes. A request without `Content-Length`, or with `Transfer-Encoding`, is refused. Under HTTP/2 the body must end with the stream and have exactly the declared length.
- The body is exactly one JSON object (RFC 8259), in UTF-8 with no byte order mark and no content encoding. Its first byte is `{`, and its last byte is the `}` that closes that object: no whitespace, newline or other byte follows.
- The server reads the complete body, within the body read deadline, before it accepts anything. A body shorter or longer than declared, a stream that ends early or is reset, a byte after the closing brace, invalid UTF-8, a lone surrogate escape, a duplicated key, or text that is not one JSON object is refused with `invalid_request`: not executed, not final, nothing recorded.
- The digest of a channel request is SHA-256 over the exact body bytes. A driver resends a request with a byte-identical body.

No proper prefix of a valid body is a valid body: it is shorter than its `Content-Length`, and it does not end with the brace that closes its root object. So a cut request is never accepted as a different request whose digest would later make the complete one a `sequence_conflict`.

### Checks before acceptance

Every request goes through these checks in this order. A request refused by any of them was not executed; the answer is about this delivery, is not final, and nothing is recorded or changed.

1. **Header block** within its size limit and read deadline (otherwise status 431, or a closed connection, from the HTTP layer); a known method and path (`invalid_request`).
2. **Admission**: a place in the pod's request bounds, or, for `cancel`, `close` and `resolve`, in the control capacity (`overloaded`).
3. **Authentication** of the bearer token, from the headers (`unauthenticated`), then the per-principal bounds (`overloaded`).
4. **Database**: `{db}` exists, except for `PUT` (`database_not_found`), and the principal may make this kind of request on it (`forbidden`). An open's permission depends on its `mode` and is checked in step 6.
5. **Body**: `Content-Length` present and at most the body limit (`request_too_large` above it), then the complete body read and framed as [above](#request-bodies) (`invalid_request`).
6. **Identification and validation before acceptance.** A channel request, `cancel`, `close` and `resolve` must carry `channel` as a string of the token's form and, where the request kind has one, `seq` as an integer from 1 to 2^63 − 1 (`invalid_request`). A stateless request and an open are validated completely here, because nothing is recorded for them (`invalid_request`, or `forbidden` for an open's mode).
7. **Database binding**: the token names this database's identifier (`invalid_request`). This applies to every channel request, `cancel`, `close` and `resolve`.
8. **Routing**: for a channel request, `cancel` and `close`, the token names this pod instance; otherwise the pod forwards it, or answers `not_serving` if it was already forwarded, or `owner_unreachable` or `channel_lost` as [decision 7](#7-transport-and-routing) says. For a write open, `resolve`, `GET` and `PUT`, this pod is the leader; otherwise it forwards, or answers `no_leader`, or `not_serving` if already forwarded.
9. **Channel**: it exists on this pod, belongs to this principal and has not ended (`channel_lost`). A sequenced request on a closing channel is refused with `channel_lost` too.
10. **Sequence number**, by the table in [decision 2](#2-channels): `channel_busy`, `in_flight`, `stale_sequence`, `sequence_gap`, `sequence_conflict`, or the recorded outcome again, which is not a refusal.
11. **Watermark**: a `begin`, `execute` or `batch` that would be accepted on an idle channel and carries `min_txn` needs the owner's head (write channel) or applied transaction number (read-only channel, after waiting up to the catch-up limit) to be at least `min_txn` (`behind`).

Then the owner accepts the request: it records the sequence number and the digest, and leaves the idle, open or aborted state, in one step. From then on the request's outcome is recorded ([decision 3](#3-replay-of-the-last-outcome)), and that includes the rest of its validation. An unknown field, a value of the wrong type or range, a field the operation does not allow, a binding mismatch, more than one statement, transaction-control SQL and a batch entry with columns are recorded as `invalid_request`, with `final: true`, at that sequence number. The split is the same everywhere: a body that is not a complete, well-formed JSON object, or that does not identify its channel and sequence number, is refused and records nothing; a channel request that does is classified by its sequence number like any other and, if accepted, records its own failure.

### Requests

| Field | Type and range | Absent | Meaning |
|---|---|---|---|
| `sql` | string, at least one statement, no U+0000 | required where listed | One SQL statement |
| `params` | array of values ([decision 8](#8-values-and-shared-conventions)) | no parameters | Element *i* binds parameter index *i* + 1 |
| `named` | object of values | no parameters | Binds `:key`, `@key` and `$key`; the key is the name without its prefix |
| `min_txn` | integer, 0 to 2^63 − 1 | 0 | Lowest transaction number the request may observe |
| `timeout_ms` | integer, 1 to 2^31 − 1, milliseconds | the server limit | Lowers the statement time limit; larger values are capped |
| `mode` | `"write"` or `"read_only"` | required on open | Channel kind |
| `channel` | string, the token, 1 to 256 characters from `A-Z a-z 0-9 . _ -` | required on channel requests, `cancel`, `close` and `resolve` | The channel |
| `seq` | integer, 1 to 2^63 − 1 | required on sequenced requests, `cancel` and `resolve` | Sequence number |
| `op` | `"begin"`, `"execute"`, `"batch"`, `"describe"`, `"commit"`, `"rollback"` or `"ping"` | required on sequenced requests | What to do |
| `execute` | object with `sql` and optionally `params` or `named` | no first statement | The first statement of a `begin` |
| `param_sets` | array of 1 to the batch limit elements, all arrays or all objects of values | — | One statement executed once per parameter set |
| `statements` | array of 1 to the batch limit objects, each with `sql` and optionally `params` or `named` | — | Several statements |

A request uses `params` or `named`, not both. Every parameter must be bound exactly once: an unbound parameter, a duplicate, an extra positional value or an unmatched name is `invalid_request`.

| Request | Required | Optional |
|---|---|---|
| `query` | `sql` | `params` or `named`, `min_txn`, `timeout_ms` |
| `describe` | `sql` | `min_txn` |
| open | `mode` | `min_txn` (read-only) |
| `op: "begin"` | `channel`, `seq`, `op` | `min_txn`, `execute`, `timeout_ms` |
| `op: "execute"` | `channel`, `seq`, `op`, `sql` | `params` or `named`, `min_txn`, `timeout_ms` |
| `op: "batch"` | `channel`, `seq`, `op`, and either `sql` with `param_sets` or `statements` | `min_txn`, `timeout_ms` |
| `op: "describe"` | `channel`, `seq`, `op`, `sql` | none |
| `op: "commit"`, `"rollback"`, `"ping"` | `channel`, `seq`, `op` | `timeout_ms` (commit) |
| cancel | `channel`, `seq` | none |
| close | `channel` | none |
| resolve | `channel`, `seq` | none |
| `PUT` | none: the body is `{}` | |

`min_txn` on `execute` and `batch` is checked only on an idle channel; inside a transaction, or in the aborted state, the transaction's snapshot or the aborted latch decides what the request observes, and `min_txn` is ignored.

Example bodies, with tokens shortened:

| Request | Body |
|---|---|
| `query` | `{"sql":"SELECT id, title FROM books WHERE id = ?","params":[78],"min_txn":1210}` |
| `describe` | `{"sql":"SELECT id FROM books WHERE title = :t","min_txn":1210}` |
| open | `{"mode":"read_only","min_txn":1210}` |
| `begin` | `{"channel":"w1.9QvX0aYk3H8...","seq":12,"op":"begin","min_txn":1209,"execute":{"sql":"INSERT INTO books (title) VALUES (?) RETURNING id","params":["Dune"]}}` |
| `execute` | `{"channel":"w1.9QvX0aYk3H8...","seq":14,"op":"execute","sql":"UPDATE books SET title = :t WHERE id = :id","named":{"t":"Dune Messiah","id":78},"min_txn":1210}` |
| `batch`, one statement | `{"channel":"w1.9QvX0aYk3H8...","seq":15,"op":"batch","sql":"INSERT INTO tags (book, tag) VALUES (?, ?)","param_sets":[[78,"sf"],[78,"classic"]]}` |
| `batch`, several statements | `{"channel":"w1.9QvX0aYk3H8...","seq":16,"op":"batch","statements":[{"sql":"DELETE FROM tags WHERE book = ?","params":[78]},{"sql":"DELETE FROM books WHERE id = ?","params":[78]}]}` |
| `op: "describe"` | `{"channel":"r1.Xk2pQ9...","seq":3,"op":"describe","sql":"SELECT * FROM books"}` |
| `commit` | `{"channel":"w1.9QvX0aYk3H8...","seq":13,"op":"commit"}` |
| cancel | `{"channel":"w1.9QvX0aYk3H8...","seq":13}` |
| close | `{"channel":"w1.9QvX0aYk3H8..."}` |
| resolve | `{"channel":"w1.9QvX0aYk3H8...","seq":13}` |
| `PUT` | `{}` |

### Responses and frames

Every response skylite produces carries `Skylite-Protocol: 1`, `Skylite-Request-Id` (assigned by the pod that received the request from the client and passed on the hop), `Content-Type: application/x-ndjson` and `Cache-Control: no-store`, and on 401 `WWW-Authenticate: Bearer`. A response without the marker was not produced by skylite and proves nothing. No response uses HTTP trailers.

A body is a sequence of frames, each one JSON value on one line ended by `\n`:

```text
body     = *keepalive [ columns *( row / keepalive ) ] *( results / params / keepalive ) terminal
terminal = done / error
```

- `columns` is `{"columns":[...]}`, with per column `name` (string), `decltype` (string or null) and `database`, `table` and `origin` (each a string or null) ([stmt.go#L403-L452][nc-colmeta], with the column metadata build option [sqlite_opt.h#L27][nc-opt-meta]). A statement without result columns has no `columns` frame, except in a describe answer, which always has one, possibly with an empty array.
- A row is a JSON array with exactly as many values as there are columns.
- `results` appears only in the answer to a batch: `{"results":{"from":0,"entries":[{"rows_affected":1,"last_insert_rowid":91},...]}}`. `from` is the zero-based index of its first entry; each frame holds 1 to 1,024 entries, and the frames run in order from 0 without gaps. A batch that succeeded has one entry per batch entry. A batch that failed inside a transaction has one entry per entry before `batch_index`. An autocommit batch that failed has none, because nothing of it remains.
- `params` appears only in a describe answer, after `columns`: `{"params":{"from":0,"names":[":t",null]}}`, one name per parameter index, with its prefix, or null for a parameter without a name; 1 to 1,024 names per frame, in order from 0 without gaps.
- `keepalive` is `{"keepalive":{}}`.
- Exactly one terminal frame, last. Nothing follows it. Every field of a terminal frame has a bound that does not depend on the request's SQL, parameters or results: the largest terminal frame this version defines, a `cancel` answer that embeds an error receipt with every field at its largest, is 1,528 bytes with its newline, within the 4 KiB reserved for it.
- An object frame whose only key is unknown is skipped before the terminal frame. An unknown value tag inside a row is an error.
- The result limit counts every byte before the terminal frame.
- **A body without a terminal frame is incomplete**, and so is one whose `results` entries do not number `entries` or whose `params` names do not number `param_count`. The driver reports it as an error, never as a short result, and treats the request as unsettled.
- An autocommit write's rows and `results` frames are held until its commit has an outcome, so no client reads rows of a write that then fails to commit. Inside a transaction, and for reads, they stream as they are produced.
- A replay sends the request's retained replay data, if any, then its receipt as the terminal frame. Without the data it sends the receipt alone, with `data_omitted: true` added when the original answer had frames before its terminal frame.

**Status codes.** A stateless request that started executing, and every channel request the owner accepted, gets 200, with any error as its terminal frame; so does every replay and every `resolve` answered with a `resolution`. Everything refused before that gets the status of its code, with one `error` frame. `PUT` returns 200 on creation. The only 5xx statuses skylite produces are 500 (`internal`) and 503 (`no_leader`, `not_serving`, `owner_unreachable`, `behind`). Drivers decide from the marker and the frames, never from the status alone.

### `done` fields

| Field | Type and range | Meaning |
|---|---|---|
| `seq` | integer, 1 to 2^63 − 1 | The request's sequence number; in a cancel answer, the number named |
| `txn` | integer, 1 to 2^63 − 1 | The transaction number of the state the answer reflects; always durable in the log. A driver raises its watermark from it |
| `txn_state` | `"none"`, `"open"` or `"aborted"` | The channel's transaction state after the request |
| `read_only` | boolean | Whether SQLite's read-only test was true for the statement |
| `rows_affected` | integer, 0 to 2^63 − 1 | SQLite's change count for a statement that is not read-only, 0 for one that is; for a batch, the sum over its entries |
| `last_insert_rowid` | integer, −2^63 to 2^63 − 1 | SQLite's per-connection value after the statement, or after the last batch entry that ran |
| `entries` | integer, 1 to the batch limit | The number of entries a batch ran |
| `param_count` | integer, 0 to 2^31 − 1 | The number of parameter indexes of a described statement |
| `committed` | boolean, always true | The commit succeeded |
| `wrote` | boolean | Whether the committed transaction created a commit object |
| `data_omitted` | boolean, absent means false | This replay leaves out data the original answer had |
| `channel` | object | The opened channel: `token` (string, 1 to 256 characters from `A-Z a-z 0-9 . _ -`), `id` (32 lowercase hexadecimal characters), `mode`, `epoch` (write channels only) and `limits` |
| `last_seq` | integer, 0 to 2^63 − 1 | The last sequence number the channel accepted; 0 if none |
| `outcome` | receipt | The receipt of `seq` (cancel) or of `last_seq` (close) |
| `resolution` | `"committed"`, `"not_committed"`, `"pending"` or `"unknown"` | The meanings in [decision 4](#resolving-a-request-after-its-channel-is-lost) |
| `epoch` | integer, 1 to 2^63 − 1 | The leader's epoch |
| `sqlite_version` | string, at most 32 bytes | The SQLite library version |
| `limits` | object | [The `limits` object](#the-limits-object) |

| Request | `done` fields |
|---|---|
| `query` | `txn`, `read_only`, `rows_affected`, `last_insert_rowid` |
| `describe` | `txn`, `param_count` |
| open | `txn`, the pod's head or applied transaction, for a write channel the open head its token carries; `channel` |
| `op: "begin"` without `execute` | `seq`, `txn_state`, `txn`: the snapshot |
| `op: "begin"` with `execute`, `op: "execute"` | `seq`, `txn_state`, `txn`, `read_only`, `rows_affected`, `last_insert_rowid` |
| `op: "batch"` | `seq`, `txn_state`, `txn`, `rows_affected`, `last_insert_rowid`, `entries` |
| `op: "describe"` | `seq`, `txn_state`, `txn`, `param_count` |
| `op: "commit"` | `seq`, `txn_state`, `txn`: the committed transaction, or the snapshot for a transaction that wrote nothing; `committed`; `wrote` |
| `op: "rollback"`, `op: "ping"` | `seq`, `txn_state`, `txn` |
| cancel | `seq`, `txn_state` (the channel's state now), `outcome` |
| close | `last_seq`; `outcome`, absent if `last_seq` is 0 |
| resolve | `resolution`; `txn`: for `committed` the commit's transaction number, otherwise the leader's head |
| `GET` status | `txn` (the leader's head), `epoch`, `sqlite_version`, `limits` |
| `PUT` | `txn`, `epoch` |

Every field listed for a request is present. A close answer has no `seq` and no `txn_state` of its own: the channel has ended, and the state after its last request is in `outcome`.

**The receipt** of a request is its terminal frame, recorded: `{"seq":13,"ok":true,` followed by the request's other `done` fields, or `{"seq":13,"ok":false,"error":{...}}` with the error object. A request cancelled before it arrived has the receipt `{"seq":n,"ok":false,"error":{...}}` with code `cancelled`, `not_executed: true` and `final: true`. Examples:

```text
{"done":{"txn":1200,"channel":{"token":"w1.9QvX0aYk3H8...","id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","mode":"write","epoch":3,"limits":{"request_body_bytes":16777216,"...":0}}}}
{"results":{"from":0,"entries":[{"rows_affected":1,"last_insert_rowid":91},{"rows_affected":1,"last_insert_rowid":92}]}}
{"done":{"seq":15,"txn_state":"open","txn":1210,"rows_affected":2,"last_insert_rowid":92,"entries":2}}
{"done":{"seq":15,"txn_state":"open","txn":1210,"rows_affected":2,"last_insert_rowid":92,"entries":2,"data_omitted":true}}
{"done":{"last_seq":13,"outcome":{"seq":13,"ok":true,"txn_state":"none","txn":1210,"committed":true,"wrote":true}}}
{"done":{"seq":14,"txn_state":"aborted","outcome":{"seq":14,"ok":false,"error":{"code":"cancelled","not_executed":false,"final":true,"message":"cancelled","request_id":"01JDZ3W4Q8M7","channel_id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","seq":14,"txn_state":"aborted"}}}}
```

The first line is an open answer, with its `limits` shortened; the second and third a batch answer; the fourth its replay without data; the fifth a close answer; the sixth a cancel answer for a statement inside a transaction.

### The `limits` object

Every member is always present, and every value is an integer from 1 to 2^63 − 1, or to 2^31 − 1 for members in milliseconds. A driver ignores a member it does not know. The values are those in force when the answer was produced. A configuration change applies to channels opened after it; an open channel keeps, for its life, the limits its open answer reported.

| Member | Limit in [decision 9](#9-limits-and-defaults) |
|---|---|
| `request_body_bytes` | Request body |
| `header_block_bytes` | Request header block |
| `batch_entries` | Entries in one batch |
| `frame_entries` | Entries in one `results` or `params` frame |
| `result_bytes` | Result size |
| `terminal_frame_bytes` | Terminal frame |
| `error_message_bytes` | Error message |
| `retained_request_bytes` | Retained replay data, one request |
| `resolve_bound` | Resolve bound, in transactions |
| `statement_ms` | Statement time |
| `response_ms` | Response deadline |
| `keepalive_ms` | Keepalive interval |
| `lock_wait_ms` | Lock wait |
| `catch_up_ms` | Follower catch-up wait |
| `channel_idle_ms` | Channel idle limit |
| `write_txn_idle_ms` | Write transaction idle limit |
| `write_txn_lifetime_ms` | Write transaction lifetime |
| `read_txn_idle_ms` | Read-only transaction idle limit |
| `read_txn_lifetime_ms` | Read-only transaction lifetime |

Admission and memory bounds are not reported: they change nothing that a driver may send.

### Errors

```json
{"error":{"code":"busy","not_executed":true,"final":true,"message":"the write lock was not granted within 5000 ms","request_id":"01JDZ3W4Q8M6","channel_id":"7c1d2e3f40a1b2c37c1d2e3f40a1b2c3","seq":4,"txn_state":"none"}}
```

| Field | Type and range | Present |
|---|---|---|
| `code` | string, one of the codes below | Always |
| `not_executed` | boolean | Always |
| `final` | boolean | Always |
| `message` | string, at most 1,024 bytes as encoded, quotes included, cut at a character boundary | Always |
| `request_id` | string, 1 to 64 characters from `A-Z a-z 0-9 _ -` | Always; in a receipt, the request's own |
| `channel_id` | string, 32 lowercase hexadecimal characters | Answers from a channel's owner |
| `seq` | integer, 1 to 2^63 − 1 | Answers from a channel's owner to a sequenced request or a `cancel` |
| `txn_state` | `"none"`, `"open"` or `"aborted"` | Answers from a channel's owner |
| `sqlite_code` | integer, 0 to 255, SQLite's primary code | When SQLite produced the error |
| `sqlite_extended_code` | integer, 0 to 2^31 − 1 | When SQLite produced the error |
| `batch_index` | integer, 0 to the batch limit − 1, counted from 0 | A failed batch entry |
| `txn` | integer, 1 to 2^63 − 1 | When the error reflects a known, durable state; never for a commit that did not land |
| `epoch` | integer, 1 to 2^63 − 1 | When known |

No error carries a token. Every field has a fixed bound, so an error frame is at most 1,422 bytes with its newline.

- `not_executed: true`: this delivery had no effect.
- `final: true`: this answer is the request's settled outcome. It is set exactly on a recorded failure, whether answered, replayed or embedded in a cancel or close answer, and on `sequence_conflict`.

The closed list of codes. "Status" applies when the error is the whole response. "Recorded" means the error can be a channel request's recorded outcome; such an error has `final: true`, `not_executed: true` outside a transaction, for a failed `begin`, for a failed commit and for a request cancelled before it arrived, and `not_executed: false` for a statement inside a transaction.

| Code | Status | Recorded | Meaning |
|---|---|---|---|
| `invalid_request` | 400 | after acceptance | Before acceptance, not recorded: a body that is incomplete, late, framed wrongly or not one JSON object; a channel request that does not identify its channel and sequence number; any invalid field of a stateless request or an open; a token for another database; an unknown method or path. After acceptance, recorded: an unknown field, a bad value, a field the operation does not allow, a binding mismatch, more than one statement, transaction-control SQL, a batch entry with columns |
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
| `channel_lost` | 410 | no | Unknown token, another principal, a channel that has ended, or a sequenced request on a closing channel |
| `request_too_large` | 413 | no | Body above the limit |
| `too_many_channels` | 429 | no | A channel bound was reached; nothing was created |
| `overloaded` | 429 | no | A concurrency bound was reached ([admission](#9-limits-and-defaults)) |
| `invalid_state` | 409 or final frame | yes, in the cases the state machine records | The operation is not allowed in the channel's state, such as `commit` with no transaction or `begin` inside one. An event the state machine does not list is refused with this code before acceptance, not recorded |
| `transaction_aborted` | final frame | yes | The transaction was rolled back by SQLite or by a limit; only `rollback` is accepted |
| `busy` | final frame | yes | The write lock was not granted within the lock wait, or a waiting-writer bound was reached |
| `timeout` | final frame | yes | The statement time limit or the transaction lifetime was reached before the commit point |
| `cancelled` | final frame | yes | Cancelled by `cancel` or `close` before the commit point, or recorded as cancelled before it arrived |
| `result_too_large` | final frame | yes | The result passed the result limit |
| `sql_error` | 400 or final frame | yes | SQLite reported an error; its codes are in the object |
| `commit_failed` | final frame | yes | The commit definitely did not happen: too large for one commit object or the local WAL bound, a definite upload failure, or the leader was fenced ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)) |
| `commit_outcome_unknown` | final frame | no | The upload's outcome was not definite within the engine's bound; the pod stops serving the database. `not_executed: false`. A close or cancel waiting on that request gets it too |
| `no_leader` | 503 | no | No leader is reachable, for a request that goes to the leader |
| `not_serving` | 503 | no | This pod does not serve this database now, or a forwarded request reached a pod that does not own it |
| `owner_unreachable` | 503 | no | The pod the token names did not answer the forwarding pod; the channel may still be live |
| `behind` | 503 | no | The owner is below `min_txn` (for a stateless request, after forwarding to the leader), or a resolving leader is below the channel's open head, which only a leader that has been superseded can be |
| `internal` | 500 or final frame | yes, before the commit point | Unexpected server failure; `not_executed` true only when known |

### Channel state machine

States: **idle** (nothing running, no transaction); **running** (request *s* running outside a transaction, including a `begin` waiting for the lock); **open** (transaction open, nothing running); **running in transaction**; **committing** (commit running before its commit point); **durable wait** (a commit or an autocommit write past its commit point, waiting for storage's definite answer); **closing** (a close is waiting for the running request; no new sequence number is accepted); **aborted**; **ended**.

The [checks before acceptance](#checks-before-acceptance) come first. The owner then applies one event at a time per channel, in the order it applies them: a request's completion and a close are never applied together, and whichever is applied first takes effect first. An event the table does not list for a state is, if it is a request, refused with `invalid_state` before acceptance, changing nothing; any other unlisted event, such as a timer, has no effect.

| State | Event | Action | Next state | Answer |
|---|---|---|---|---|
| idle, running, open, running in transaction, committing, durable wait, aborted | sequenced, `seq` < last | none | unchanged | `stale_sequence` |
| the same | sequenced, `seq` > last + 1 | none | unchanged | `sequence_gap` |
| the same | sequenced, `seq` = last, recorded as cancelled before it arrived | none | unchanged | The `cancelled` receipt |
| the same | sequenced, `seq` = last, other digest | none | unchanged | `sequence_conflict` |
| running, running in transaction, committing, durable wait | sequenced, `seq` = last, same digest | none | unchanged | `in_flight` |
| idle, open, aborted | sequenced, `seq` = last, same digest | none | unchanged | Replay: retained data, if any, and the receipt |
| running, running in transaction, committing, durable wait | sequenced, `seq` = last + 1 | none | unchanged | `channel_busy` |
| idle | `begin` | Accept; write: wait for the lock (`BEGIN IMMEDIATE`); read-only: `BEGIN` and start the read; take the snapshot's transaction number in the same step | running | When it finishes |
| idle | `execute`, `batch` | Accept; a write takes the lock and a fresh connection | running | When it finishes |
| idle | `describe`, `ping` | Accept | running | When it finishes |
| idle | `commit`, `rollback` | Accept, record `invalid_state` | idle | `invalid_state` |
| running | `begin` succeeds, with its first statement if any | Record | open | `done` |
| running | `begin` fails after starting the SQLite transaction: its snapshot or its first statement | Roll back, close the connection, record the error | idle | The error, `not_executed: true`, `txn_state: "none"` |
| running | `begin` not granted the lock in time, or a waiting-writer bound reached | Record `busy` | idle | `busy` |
| running | autocommit write reaches its commit point | none | durable wait | none yet |
| running | autocommit request fails before its commit point | Roll back, close the connection, record | idle | The error, `not_executed: true` |
| running | read-only or describe or ping request finishes, or an autocommit write that changed no page | Record; after the write, close the connection | idle | Its result |
| open | `execute`, `batch`, `describe`, `ping`, `rollback` | Accept | running in transaction | When it finishes |
| open | `commit` | Accept | committing | When it finishes |
| open | `begin` | Accept, record `invalid_state` | open | `invalid_state` |
| running in transaction | statement finishes or fails | Record; read `sqlite3_get_autocommit` | open, or aborted if SQLite ended the transaction | The result or error with `txn_state` |
| running in transaction | `rollback` finishes | Roll back, close the connection, record | idle | `done` |
| committing | the transaction wrote nothing, or is read-only | End it, close the connection, record committed at the snapshot | idle | `done`, `wrote: false` |
| committing | reaches its commit point | none | durable wait | none yet |
| committing | fails before its commit point (too large, statement time, transaction lifetime, cancel, error) | Roll back, close the connection, record | idle | The error, `final`, `not_executed: true` |
| durable wait | storage: committed | Close the connection, record committed | idle | `done` |
| durable wait | storage: definitely not committed | Close the connection, record `commit_failed` | idle | `commit_failed` |
| durable wait | storage: unknown within the engine's bound | The pod stops serving the database | ended | `commit_outcome_unknown` if the response can still be written |
| aborted | `rollback` | Accept, record | idle | `done` |
| aborted | any other sequenced request | Accept, record `transaction_aborted` | aborted | `transaction_aborted` |
| idle, open, aborted | `cancel`, `seq` = last | none | unchanged | `done`, the receipt |
| idle, open, aborted | `cancel`, `seq` = last + 1 | Record *s* as cancelled, without a digest; *s* becomes last | unchanged | `done`, the `cancelled` receipt |
| running, running in transaction, committing | `cancel`, `seq` = last | Interrupt SQLite, unless a cancel for it is already waiting | as for a failure or success of the request | `done` with the receipt, when it finishes |
| durable wait | `cancel`, `seq` = last | none | as storage decides | `done` with the receipt, when storage decides |
| running, running in transaction, committing, durable wait | `cancel`, `seq` = last + 1 | none | unchanged | `channel_busy` |
| idle, running, open, running in transaction, committing, durable wait, aborted | `cancel`, `seq` < last or > last + 1 | none | unchanged | `stale_sequence` or `sequence_gap` |
| idle, aborted | `close` | End | ended | `done` with `last_seq` and its receipt |
| open | `close` | Roll back, close the connection, end | ended | `done` with `last_seq` and its receipt |
| running, running in transaction, committing | `close` | Interrupt SQLite | closing | When the channel ends |
| durable wait | `close` | none | closing | When the channel ends |
| closing | the running request finishes, or storage answers committed or definitely not | Record; roll back an open transaction; close the connection; end | ended | The request its answer, if it can still be written; every waiting close `done` with `last_seq` and the receipt; every waiting cancel `done` with the receipt and `txn_state: "none"` |
| closing | storage: unknown within the engine's bound | The pod stops serving the database | ended | `commit_outcome_unknown` to the request and to every waiting close and cancel, where a response can still be written |
| closing | `close`, or `cancel` with `seq` = last | Join the waiting close | closing | The same answer, when the channel ends |
| closing | sequenced request, or `cancel` naming another number | none | closing | `channel_lost` |
| ended | `close` or any request | none | ended | `channel_lost` |
| idle, aborted | channel idle limit | End | ended | none |
| open | transaction idle limit or lifetime | Roll back, close the connection | aborted | none; the next request learns it |
| running in transaction | transaction lifetime | Interrupt SQLite, roll back, record `timeout` | aborted | `timeout`, `txn_state: "aborted"` |
| committing | transaction lifetime | Interrupt, roll back, close the connection, record `timeout` | idle | `timeout`, `not_executed: true` |
| running, running in transaction, committing | statement time limit | Interrupt SQLite, record `timeout`; inside a transaction read `sqlite3_get_autocommit` | idle, open or aborted; idle after a commit | `timeout` |
| durable wait, closing | any timer | none | unchanged | none |
| running states | the caller's HTTP connection drops | A read stops producing rows and is recorded with its replay data dropped; anything else continues | as it finishes | none |
| after recording | response deadline passes | The response is cut | unchanged | none; the driver replays |
| any live | the pod restarts, stops serving the database, or the epoch changes | Interrupt everything before its commit point; answer running requests where a response can still be written; a request in durable wait keeps its channel's entry, ended but unsettled, until storage answers, unless the pod stops on an unknown outcome | ended | per request, or none |

A late duplicate of the commit itself while it is in durable wait gets `in_flight`; a late duplicate of the statement before it gets `stale_sequence`. Neither changes anything. A repeated `close` or `cancel` while one is waiting joins it and gets the same answer. `resolve` is not a channel request: it reads the owner's table and changes nothing.

## Worked examples

Headers that every request and response carries are left out.

### A GORM create whose commit response is lost

GORM wraps a create in a transaction. The Go driver's connection has an open write channel at sequence 11.

```http
POST /v1/databases/library/channel HTTP/1.1

{"channel":"w1.9QvX0aYk3H8...","seq":12,"op":"begin","min_txn":1208,"execute":{"sql":"INSERT INTO books (title) VALUES (?) RETURNING id","params":["Dune"]}}
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

### A commit in transit during a rolling restart

The same GORM create, on a channel opened at head 1200, which its token carries. Leader L, epoch 3, leaves office for a rolling restart while the commit is in transit, and L2 takes office at epoch 4. The driver resends sequence 13 and gets `no_leader`, then `channel_lost`. It resolves:

```http
POST /v1/databases/library/resolve HTTP/1.1

{"channel":"w1.9QvX0aYk3H8...","seq":13}
```

**L received the commit.** L committed at 1210 before it stopped, and L2's bump is at 1211. L2 finds the identifier at 1210, above the open head, and answers:

```text
{"done":{"resolution":"committed","txn":1210}}
```

`Commit` returns `nil`.

**L never received it.** The request was still in the mesh when L stopped, and L2's bump is at 1210. L2 finds no identifier between 1200 and its bump at 1210, the first bump above the open head, and answers:

```text
{"done":{"resolution":"not_committed","txn":1210}}
```

`Commit` returns an error that reports the transaction as rolled back; the application may run it again. If the delayed commit then reaches L's pod, it finds the channel gone; a leader that had not yet stopped would execute it and meet the bump at 1210, and the commit would fail. It cannot reach L2's channels: the token names L's channel only.

**L was killed during the upload.** L's create for 1210 may land late or never. L2's walk ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)) places its bump after the commit if the commit landed first, and at 1210 otherwise. The resolve then answers as in one of the two cases above. A commit whose outcome L itself could not resolve is settled the same way once L2 is in office.

## Attempts to break the channel design

| Sequence | What happens |
|---|---|
| A duplicate of any sequenced request arrives after its answer | `seq` = last: the recorded outcome. `seq` < last: `stale_sequence`. Nothing executes |
| A duplicate arrives after the channel closed | `channel_lost`. Nothing executes. The driver settled the request from the close answer |
| A duplicate arrives after a leader change | The new leader answers `channel_lost` for an older epoch. The old leader, if still running, may accept it on its own channel, but cannot commit: its next commit meets the new leader's epoch bump ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)) and is `commit_failed` |
| A duplicate arrives after the driver gave up and reported unknown | If it is the next sequence number on a live channel, it may execute and commit, once. Unknown was the honest answer |
| A duplicate of `open`, `close` or `cancel` | `open`: an orphan channel, holding nothing, expiring at the idle limit. `close` while the channel is closing: it joins the waiting close and gets the same answer; after the channel ended: `channel_lost`. `cancel` naming an older sequence number: `stale_sequence` |
| A duplicate of the previous statement while the commit is in durable wait | `stale_sequence`; the commit continues. This was finding 9 |
| Two drivers present the same token | A token shared by two drivers is one channel, and holding the token is the authority to act on it. The request digest stops them receiving each other's answers for the same sequence number: the second body under a taken number gets `sequence_conflict`, final for that body. It does not stop one from continuing the other's transaction: the second can send a commit under the next number, which commits the first driver's statements, and the first, refused its own commit with `sequence_conflict`, can report as rolled back a transaction that committed. Sharing a token is a driver bug, and the protocol does not defend against it beyond the digest |
| Another principal replays a token | `channel_lost`. The channel is untouched |
| The owner pod restarts mid-request | Channel state is in memory only, so the channel has ended. A write channel's leader restarting means a new epoch ([0004 invariant 1](/docs/decisions/0004-storage-engine/#4-invariants)). A commit in durable wait may land; the driver sees `channel_lost` and resolves |
| A leader change during a commit | Before the commit point: the commit fails with no effect. After it: storage answers committed, or definitely not if the bump took the name, and the owner answers if it still can; otherwise the driver resolves |
| A close races a commit | The close puts the channel in the closing state, waits for the commit's definite answer and returns it. A close answered before the commit was decided would have let a driver report "not executed" for a commit that then landed, so close never answers early |
| The next request arrives between a completion and a close | The owner applies the completion and the close one at a time. If the close comes first, the channel is closing and the request gets `channel_lost`; if the request is accepted first, it is the last accepted request, and the close interrupts it or waits for it and returns its receipt. Either way the close answer's `last_seq` is exact |
| A cancel races a commit | Before the commit point: interrupted, recorded `cancelled`, not committed. After it: the cancel waits and reports the outcome. A cancel that overtakes its request records it as cancelled, and the request's late delivery gets that answer, whatever its body |
| A forwarded request reaches a stale leader | Channel requests are routed to the pod the token names, never to "the leader". A stale leader that owns the channel can execute a statement but not commit it. A write-channel open that reaches a stale leader creates a channel that ends when that leader stops |
| Replay memory under many channels | Each channel keeps one receipt, in 2 KiB reserved when it opened, and replay data only within 64 KiB per request, 32 MiB per database and pod and 256 MiB per pod; past that, the receipt alone with `data_omitted`. Channels are bounded per pod, per pod and database, and per principal, so receipts take at most 32 MiB per pod |
| A batch of 10,000 entries | Its per-entry results travel in `results` frames before the terminal frame, which stays fixed in size. Their 790 KB or so is over the retained-data bound, so a replay returns the receipt with `entries` and the totals, and `data_omitted` |
| A request body cut after its closing brace, or its HTTP message cut short | The body is shorter than its `Content-Length`, or the stream ends early: refused, nothing recorded. Driver bodies end at the closing brace, so no prefix of a body is a complete one. A forwarding pod aborts its upstream request rather than end a prefix normally |
| An autocommit read on a write channel of a deposed leader | The driver sends its watermark as `min_txn`. If the pool has seen a write that the deposed leader does not have, the leader's head is below the watermark and it refuses with `behind`, before acceptance; the driver closes the channel and sends the read on a new one |
| A driver that never closes anything | Channels expire at the idle limit. A leaking driver reaches its principal's bound and gets `too_many_channels`, which stops that principal without starving others below the database and pod bounds |
| A transaction expires while the driver still believes it open | The channel enters the aborted state. The driver's next statement gets `transaction_aborted`, never autocommit |

## Attempts to break resolution

L is the leader on which the channel was opened, *o* the channel's open head.

| Sequence | What happens |
|---|---|
| A resolve races L's commit that is still landing | The resolve reaches a final answer only at an identifier or at a bump above *o*. Every name below that bump is already occupied, so a commit of L's that landed is found. L's create can land only at its head plus one; if the bump holds that name, the create can never land |
| `not_committed`, then a delayed duplicate of the request reaches L | If L is still running and the channel live there, L may execute it, but its commit meets the bump that the resolve found and fails with `commit_failed`. If the answer came from an absent channel in step 6, the channel has ended: `channel_lost` |
| The same duplicate reaches the new leader, or a follower | The token names L's pod. A follower forwards it there. A pod that is not the owner, or the restarted pod under the same name, does not know the token: `channel_lost`. Nothing executes |
| A resolve reaches a deposed leader that does not know it | Its answers come from names that never change and from its own table, so they hold. A channel of a later leader has an open head above its head: `behind`, and the driver tries again |
| *o* far below the head | More than the resolve bound below the head: `unknown`, at once, with no storage reads. Within the bound the leader reads only its window. Drivers replace idle channels at half the bound, so this needs the commit rates in [reopening](#resolving-a-request-after-its-channel-is-lost) |
| *o* above the leader's head | Every reported `txn` is durable, so only a leader that has been superseded is behind a channel's open head: `behind`, not final. The driver tries again; its resolve wait bounds the attempts |
| A token guessed by another principal | 256 random bits. A guessed token matches no identifier, and the answer for it is `not_committed`, which is true |
| A token replayed by another principal with write access | The holder learns whether that channel's requests committed, and where. It changes nothing: resolve has no effect, and channel requests from another principal get `channel_lost`. The token's secrecy is the protection, as for the channel itself |
| Two channels' identifiers collide in the stored prefix | A 2^−128 event per pair, and needs the same sequence number too. A party cannot choose its token, so it cannot aim for a collision. The consequence would be a false `committed` |
| A retired header in the scan range | The retired form keeps the identifier and the bump flag; a header read is the same in either form |
| A candidacy whose bump landed and which then failed its final check | Its bump fences L like any other ([0004 decision 9](/docs/decisions/0004-storage-engine/#9-takeover)). The scan stops at it: `not_committed` for anything not found below it |
| No writes since *o* | L still in office: nothing above *o*; `pending` if the channel is live or ended but unsettled on L, `not_committed` if it is absent. After a takeover, the bump is at *o* + 1: `not_committed` |
| The channel ended by its idle limit, L still in office | A running request is never idle, and a channel leaves the table only when nothing of it can reach storage. L finds the identifier or answers `not_committed` |
| L's pod restarts and takes office again | A new candidacy at a new epoch, with a new bump above L's last commit; the old channel settles as after any takeover |
| L stopped on a commit whose outcome it could not resolve | The create may land later, at L's head plus one, until a candidate's bump takes that name. Once a new leader is in office, the resolve finds the commit or the bump |
| A token of another database | The token names its database identifier: `invalid_request`, before acceptance |
| A header leaves the window while a resolve runs | The resolve answers only from one observation in which the window held the whole range from *o* + 1 to the head. If filling or eviction changes the window, it observes again and checks the bound again. A header missing from the window is never read as absence |
| A resolve soon after a takeover | The new leader fills its window in the background. Until the range is filled, the resolve waits for the one fill in progress, and answers `unknown` if its time runs out first |
| The database stops while a resolve runs | A leader that is stopping or has stopped the database answers `unknown`, never `not_committed` from an absent entry |
| A client alters the open head inside its token | The stored identifier is a hash of the whole token, so the altered token names an identifier nothing was stored under, and no channel. Its resolve never answers `committed`; only the client that altered it is misled |
| A driver resolves another sequence number of the same channel | The answer is true of the request named, not of the unsettled one: a driver bug, which its tests must exclude |
| The principal changes while a channel is live | `channel_lost` for the new principal while the channel lives; the resolve answers `pending` until the channel's idle limit ends it, so the driver usually reports unknown |

What remains open is not a safety violation: a resolve past the bound, past the driver's resolve wait or before the window is filled, a resolve on a leader that is stopping, and a channel still live or ended but unsettled on the leader in office, as after the driver's principal changed, end unknown.

## Response to the design review

| # | Finding | Disposition |
|---|---|---|
| 1 | Commit identifier not bound to its operation | Falls away. Clients choose no identifier. Replayed: driver B cannot select A's identity; it can only share A's token, and a different body under a taken sequence number gets `sequence_conflict`, final for B's request, with A's channel untouched. The eviction sequence has no analogue: deduplication is by sequence number for the channel's whole life, and an ended channel answers `channel_lost` for ever |
| 2 | Parser rejection claims "not committed" | Falls away. Replayed: the malformed second delivery is refused before acceptance, with `final: false`, and says nothing about the request. Only the owner's recorded outcome, a `sequence_conflict` or a close or cancel answer settles a request ([decision 4](#4-when-a-driver-may-report-not-executed)) |
| 3 | No-op commit becomes "not committed" | Reopened by resolution, and answered by the meaning of the outcomes ([decision 4](#resolving-a-request-after-its-channel-is-lost)). `not_committed` means that the request has no effect on the database and never will, and a commit that changed nothing has none: it creates no commit object, and `resolve` answers `not_committed` exactly. An application that runs it again gets a serial execution in which the first never happened. While the channel lives, the outcome is replayed as `committed: true, wrote: false`, which is also true |
| 4 | Known-committed operations become failures | Closed by decisions [1](#1-what-the-protocol-promises) and [3](#3-replay-of-the-last-outcome). Exactly-once for the application is not promised; replay returns counts and small results for the channel's life; the remaining case raises a committed, result-unavailable error that is not transient. Durable receipts not accepted: beyond what this version promises |
| 5 | Outcome recovery is not result recovery | Closed by [decision 5](#5-reads) (no retry once a row is exposed) and [decision 3](#3-replay-of-the-last-outcome). Durable recovery of results not accepted |
| 6 | Response timeout after a durable commit | Closed by [decision 6](#6-statements-transactions-and-errors): the statement time limit stops at the commit point, and a response deadline only cuts the response |
| 7 | Driver deadlines against recovery | Closed by [decision 6](#6-statements-transactions-and-errors): cancel, unknown, and the exception paths that must be avoided; revised after the second review (its finding 9) |
| 8 | Statements not atomic on error | Closed by [decision 6](#6-statements-transactions-and-errors): autocommit failure rolls back everything; inside a transaction no no-effect claim, `txn_state` from SQLite, the aborted state, and no internal savepoints |
| 9 | Late delivery rolls back a committing session | Falls away. Replayed: the late duplicate of statement 3 arrives while commit 4 is in durable wait; it gets `stale_sequence`, and a refused sequence number changes nothing |
| 10 | Commit table has no memory bound | Falls away. Replayed: there is no refusal table. A client can create at most its principal's bound of channels, each holding one bounded outcome. `resolve` keeps no state; the leader's header window is bounded by the resolve bound |
| 11 | Token refresh changes the principal during recovery | Falls away. Replayed: Q's resend on P's channel gets `channel_lost`, which is not a proof. Q's resolve with the token answers `pending` while P's channel lives and is final once it has ended, so the driver reports unknown unless the channel ends within the resolve wait. Credential-provider ownership moves to the driver records |
| 12 | Begin replayable without identity | Falls away. Replayed: `begin` is a sequenced request, so the duplicate gets `in_flight`, the recorded outcome or `stale_sequence`, and never a second transaction. A lost open answer leaves an orphan channel that holds no lock, bounded and expiring |
| 13 | Result-set closure cancels the transaction | Abandoning a response no longer rolls anything back. Draining and buffering move to the driver records |
| 14 | `GET` status does not validate a session | The channel's `ping` validates the channel itself; the lifecycle moves to the driver records |
| 15 | Driver lifecycle unspecified | Moved to the driver records ([decision 11](#11-what-the-driver-records-decide)); the server-side latch is the aborted state |
| 16 | Permanent errors mapped to retryable ones | The uncertainty mappings are fixed here; the full mapping by meaning moves to the driver records |
| 17 | One row id is not a generated-key contract | Moved to the driver records; the protocol carries `last_insert_rowid` and `RETURNING` rows |
| 18 | Affinity is not a JDBC type mapping | Moved to the driver records |
| 19 | Pool-wide consistency needs an owner | Closed by [decision 5](#5-reads): the connector and the registry, scoped to new snapshots |
| 20 | Defaults reject ordinary transactions | Closed by [decision 9](#9-limits-and-defaults): longer transaction limits and the workload limits stated. Transaction timeouts reaching the server move to the driver records |
| 21 | Go and Java timestamps differ | Closed by [decision 8](#8-values-and-shared-conventions): instants as UTC, local values as written, in four canonical forms after the second review (its finding 13). Calendar handling moves to the JDBC driver record |
| 22 | Declared-type coercion destroys text | Closed by [decision 8](#8-values-and-shared-conventions): unparseable text returned unchanged; one boolean rule, narrowed to INTEGER values after the second review (its finding 12) |
| 23 | Describe can have effects | Closed by the policy hook in [decision 5](#5-reads), on every path |
| 24 | Connection cleanup missing | Closed by [decision 6](#6-statements-transactions-and-errors): connections that served a write are closed |
| 25 | Unbounded resources | Closed by [decision 9](#9-limits-and-defaults). A resolve reads at most the leader's window of the resolve bound, from memory, and answers `unknown` past it |
| 26 | Redaction depends on an inference | Closed by [decision 6](#6-statements-transactions-and-errors): the guarantee's boundary is the server's logs; messages to the caller may carry SQLite's text |
| 27 | Forwarding and keepalive contract | Closed by [decision 7](#7-transport-and-routing) |
| 28 | Wire interoperability gaps | Closed by the [wire specification](#wire-specification) (statuses, field types and widths, terminal frame, transaction state, begin failure, commit numbers, cancel and close, empty batches refused, result limits); JDBC result, value, batch, savepoint and credential behaviour moves to the driver records. Sparse `?NNN` parameters bind by index under the existing rule |
| 29 | Rewrite recovery against retired headers | Closed. The identifier is composed by the server and never rewritten; a retired form keeps the header, identifier and bump flag included ([0004 decision 3](/docs/decisions/0004-storage-engine/#3-durable-layout-in-object-storage-per-database)), and `resolve` reads either form. There is no retention rule or window on storage: names are never freed |
| 30 | Value encoding traps | Closed by the conformance vectors in [decision 8](#8-values-and-shared-conventions) |

## Response to the second design review

The second review reproduced every attack in [attempts to break the channel design](#attempts-to-break-the-channel-design) and [resolution](#attempts-to-break-resolution) and found no counterexample to channels, replay or resolution by position. The design stands; these changes answer its findings.

| # | Finding | Disposition |
|---|---|---|
| 1 | Successful responses cannot fit the terminal frame | Closed. The terminal frame holds only fields of fixed size, at most 1,528 bytes. Per-entry batch results move to `results` frames and describe's parameter names to `params` frames, both before the terminal frame, in chunks of 1,024 ([responses and frames](#responses-and-frames)). `cancel` and `close` embed only the receipt ([decision 3](#3-replay-of-the-last-outcome)) |
| 2 | `min_txn` does not cover every read on a write channel | Closed. Every `begin`, and every `execute` or `batch` on an idle channel, carries the watermark; an owner below it refuses with `behind` before acceptance ([decision 5](#5-reads)). Drivers also replace a write channel with no response for 60 seconds, as an availability measure ([reopening](#resolving-a-request-after-its-channel-is-lost)) |
| 3 | The wire specification leaves implementations incompatible | Closed by the [wire specification](#wire-specification): integer ranges, [request bodies](#request-bodies), [checks before acceptance](#checks-before-acceptance) with one split between refused and recorded requests, request and response schemas with examples, [the `limits` object](#the-limits-object), the receipt, the close answer's own fields, `batch_index` from 0, sequence numbers that do not wrap, database binding on every channel operation, and `owner_unreachable` for forwarding failures |
| 4 | Request truncation can fake a `sequence_conflict` | Closed by [request bodies](#request-bodies): `Content-Length` required, the body read and checked whole before acceptance, nothing after the closing brace, the digest over the exact bytes, and a forwarding pod that aborts an incomplete upstream request. Test added |
| 5 | Resolve needs a consistent window view | Closed by the resolve steps in [decision 4](#resolving-a-request-after-its-channel-is-lost): one observation of the channel's status, the head and the window's coverage; a new observation after filling; ended but unsettled is never absent; a stopping database answers `unknown`. Pause-point tests added |
| 6 | Close and cancellation need precedence | Closed by the [state machine](#channel-state-machine): the closing state, one event at a time per channel, cancellation records before the digest rule, the lifetime during `committing`, no channel idle timer while a transaction is open, a failed `begin` that leaves the channel idle, joining for repeated controls, and refusal of unlisted events |
| 7 | Compilation fallback conflicts with channel ownership | Closed by [decision 7](#7-transport-and-routing): only stateless requests are forwarded for a compilation failure |
| 8 | Snapshot establishment and the JDBC watermark's identity | Closed by [decision 5](#5-reads) and [requirement 12](#requirements-on-the-engine-and-the-format): a `begin` takes its snapshot and its number in one step; the JDBC watermark is keyed by canonical base URL and database name only, lives as long as the driver class and only moves forward |
| 9 | Deadlines do not meet the cited contracts | Closed by [decision 6](#6-statements-transactions-and-errors): Go returns as soon as the context ends, with an error matching both `ErrOutcomeUnknown` and the context's error, cleanup in the background, and rows of write-channel requests only after settlement. JDBC raises `SQLTimeoutException` only for a `cancelled` receipt, and otherwise `08007`, a stated departure from `setQueryTimeout` |
| 10 | The retained-result budget excludes mandatory summaries | Closed by [decision 3](#3-replay-of-the-last-outcome): a receipt of fixed size reserved at open, all replay data charged to the per-request, per-database and per-pod budgets, and the worst case per pod stated |
| 11 | Admission and time bounds do not cover all resources | Closed by [decision 9](#9-limits-and-defaults): pod-wide channel and replay-data bounds, header and body read deadlines, authentication before the body, separate capacity for `cancel`, `close` and `resolve`, and one window fill per database with bounded reads |
| 12 | The boolean rule cannot be enforced through `database/sql` | Closed by [decision 8](#8-values-and-shared-conventions): the shared rule covers INTEGER only, and Go's own conversions are stated |
| 13 | The temporal convention is incomplete | Closed by [decision 8](#8-values-and-shared-conventions): four canonical forms, years 0001 to 9999, the forms read from existing text, offsets and fraction widths. Calendar overloads and legacy types move to the driver records |
| 14 | Cached statements can bypass the policy | Closed by [decision 6](#6-statements-transactions-and-errors): no reuse across principals, claims or policy generations |
| 15 | The driver checklist omits decision families | Closed by [decision 11](#11-what-the-driver-records-decide) |
| 16 | The deployment contract is not pinned | Closed by [decision 7](#7-transport-and-routing): requirements on overall, per-try and idle timeouts and buffering, with the engine's bound in the timing; validation through a real mesh and ingress listed as unverified; both drivers refuse redirects |
| 17 | The resolve promise needs a cold-window qualification | Closed by [decision 1](#1-what-the-protocol-promises) and [decision 4](#resolving-a-request-after-its-channel-is-lost): an answer needs complete coverage and time left; the window fills in the background after takeover |
| — | No fairness between principals | Waiting writers are served first in, first out, and no principal holds more than 16 of a database's 64 waiting slots. Nothing more is promised ([decision 9](#9-limits-and-defaults)) |

## Alternatives considered

**Client-chosen commit identifiers with a deduplication window and an outcome lookup.** The previous draft. Rejected for the reasons in [Context](#context).

**An exact log position as the operation identity**, suggested by the review: a write names the transaction number it must commit at, and recovery reads that one header. It makes lookup direct, but stateless writers contend for one slot, every write needs a fresh head, and a definite "not committed" needs a durable cancellation or a fence. Not adopted: the channel's open head and the first epoch bump above it give a final answer without fixing a slot in advance.

**A commit object for a transaction that changed nothing**, so that `resolve` would answer `committed` for it. Rejected: it would cost a storage write and a permanent header for every read-only transaction on a write channel, such as every Spring `@Transactional` method that is not marked read-only and only reads, to change an answer that is already exact.

**Starting every transaction deferred.** SQLite's plain `BEGIN` defers the write lock ([sqlite.h.in#L4765-L4768][sq-begin]), and in WAL mode a transaction whose snapshot is no longer the newest cannot take it, returning `SQLITE_BUSY_SNAPSHOT` ([wal.c#L3816-L3823][sq-busysnap]). The failure would come part-way through a transaction; `BEGIN IMMEDIATE` moves the wait to the start, where `busy` is definite. Rejected.

**Server-side prepared statements, cursors and paging.** Each would tie state to a pod and a snapshot across requests. Rejected; the server may cache compiled statements privately, within the rule in [decision 6](#6-statements-transactions-and-errors).

**Reusing Hrana, the rqlite API or Flight SQL.** Rejected for the reasons in [Context](#context).

## Consequences

- Each driver connection that writes costs one extra request to open its channel, and a new one after a leader change, an idle period, or once the channel's open head is half the resolve bound behind.
- GORM's default transaction around each create, update and delete takes two requests: a combined begin and a commit.
- A lost response costs one resend and is invisible to the application while the channel lives, except for large write results.
- A write in transit when its channel is lost, by a leader change, a rolling restart or an idle limit, costs one `resolve` and settles as committed or not committed. Unknown, with a distinct Go error and SQLState `08007`, is left for a resolve past its bound or its wait, and for a channel still live on the leader in office.
- Every leader keeps a window of commit headers in memory, and every commit carries a 24-byte identifier for the life of the database.
- The drivers must parse JSON integers exactly and must not use a JSON library that replaces invalid text.
- In this pull request: the [architecture page](/docs/architecture/) links this record from its interface section and records the protocol's part of client authentication.

## Requirements on the engine and the format

1. **The commit path takes the commit identifier from the server**: 24 bytes, the first 16 bytes of SHA-256 of the channel token and the 8-byte big-endian sequence number ([decision 2](#2-channels)). The format's field holds up to 64 bytes (commit header bytes 68 to 132, `internal/format/doc.go` on the `feat/engine-format` branch); its description of the field as "supplied by the client" must change to server-composed, and the field is compared as exact bytes. Epoch-bump commits carry none. The retired form keeps it.
2. **The commit point is visible.** The storage layer must tell the server when it begins the create of a commit object, and the server must not interrupt the SQLite connection after that point.
3. **Definite outcomes reach the server before fail-stop.** When the engine resolves a commit definitely (committed, or an epoch bump at the name) and then stops serving, the server must be able to answer the running request first.
4. **The engine reports** the head, the epoch, a follower's applied transaction number and its application of epoch-bump commits, and waits until a follower reaches a given number.
5. **The header at any transaction number, live or retired, can be read directly** and gives the commit identifier, the epoch and whether it is an epoch bump.
6. **A transaction that changes no page creates no commit object.**
7. **A candidacy creates at most one epoch-bump commit.**
8. **A leader's head is exactly its own last commit**, and it creates commits only at its head plus one. The open head depends on it (every commit of a channel is above it), and so does the argument for a resolve answered by a leader other than the channel's.
9. **Every transaction number the server reports to a client is durable in the log.**
10. **The leader keeps a window of headers in memory**: the identifier and the bump flag of each of the last resolve-bound transaction numbers below its head (100,000 by default, about 2.5 MB per database), indexed by identifier and by bump position, filled from its own commits, from its takeover walk and, for the rest, by reading each missing header from storage once. A header never changes once read, because a name's first object is its only object. A new leader starts filling the window in the background once it is in office, and taking office never waits for it; the fill runs once per database, with at most 16 header reads in flight by default. Every head advance and every change to the window can be ordered against a resolve's observation, together with the server's channel table ([decision 4](#resolving-a-request-after-its-channel-is-lost)).
11. **No outcome index.** The log need not be indexed by identifier or listed, and the commit path needs no refusal check. The previous draft's requirements on garbage collection are withdrawn.
12. **A read transaction reports its snapshot.** The engine starts the read on a connection and reports the transaction number of the snapshot that read holds, in one step, so that a `begin` answers with the number its statements will see ([decision 5](#5-reads)).
13. **Transaction numbers and epochs stay from 1 to 2^63 − 1.** The format stores them in 8 bytes; the engine never creates a commit or an epoch past 2^63 − 1, so every number fits the protocol's signed 64-bit integers. At a million commits per second the bound is more than 290,000 years away.
14. **The bound on resolving a create is stated.** The engine's bound on resolving a commit after its commit point ([0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop)) is a documented configuration value, because the deployment timing in [decision 7](#7-transport-and-routing) depends on it.

0004 decision 6 currently says that a lookup which finds no match means unknown. From a channel's open head, a match or an epoch bump above it is final, as argued in [decision 4](#resolving-a-request-after-its-channel-is-lost); 0004 must carry that rule, or link here for it.

## Not verified, and what the first implementation must prove

Nothing here was run: there is no server or driver. Unverified or inference, each needing a test:

- That GORM uses `RETURNING` for inserts with the chosen dialector, once that dialector is pinned, and that its translation keeps `ErrOutcomeUnknown` and the committed, result-unavailable error with `TranslateError` both on and off.
- That Hibernate's translation of a vendor-code-0 `SQLException` with SQLState class `SK` ends non-transient in Spring: traced in source, not run.
- That a real deployment meets the requirements in [decision 7](#7-transport-and-routing): route, per-try and idle timeouts against *T*, `Content-Length` and complete bodies reaching the server, and keepalive frames passed on promptly, through Istio and through ingress-nginx with buffering on and off. No version of either was pinned or checked.
- That SQLite's authorizer sees every function call and pragma the SQL policy needs to refuse.
- That no write path other than an epoch bump produces a commit object without the identifier: batches, DDL, triggers and statements that change no page included.

The first implementation must prove, by test:

- No request changes the database twice: duplicates of every request kind delivered at every point in the state machine, including after the answer, after close, after a leader change and after the driver gave up.
- Every "not executed" and "not committed" a driver reports is true, for commits killed at each step of 0004's commit sequence, and `channel_lost` alone never produces one.
- A refused sequence number changes no state, and a different body under a taken number gets `sequence_conflict`.
- Close and cancel answers never precede the outcome of a running commit.
- `resolve` never answers `not_committed` for a commit in the history and never `committed` for one outside it: for a leader killed, deposed or fenced by a failed candidacy at each step of 0004's commit sequence, for channels ended by close, idle limit and restart, for a resolve that reaches a deposed leader, with the open head at and past the resolve bound, with an altered token, and with retired headers in range.
- A channel leaves the owner's table only after nothing of it can reach storage, and a resolve answers only from one consistent observation: paused between each of its steps while the window advances and evicts headers, the channel ends, an upload completes and the database fail-stops, it never gives a false answer, never reads an ended but unsettled entry as absent, and answers `unknown` once the database is stopping.
- Each driver resolves with the unsettled request's own token and sequence number; it replaces an idle write channel whose open head is more than half the resolve bound below the pool's highest seen transaction number, or that has had no response for more than 60 seconds, and never one with a transaction open or a request unsettled; it raises the watermark only from reported `txn` values; and the JDBC watermark survives a credential refresh and a replaced connection, and is shared by URLs that differ only in canonicalisation.
- A durable commit is never answered with a negative or no-effect error, including under response deadlines and dropped connections.
- Replay returns the receipt and any retained data; over a bound it returns the receipt with `data_omitted`. A batch at the batch limit and a describe with many parameters keep their terminal frame within its reservation, and their `results` and `params` frames are counted against `entries` and `param_count`.
- No cut request is ever accepted: bodies cut at every byte offset, after the closing brace and before the end of the HTTP message included, over HTTP/1.1, HTTP/2 and a forwarding hop; a body with bytes after its closing brace is refused.
- The state machine's races: a request arriving between a completion and a close; repeated and concurrent closes and cancels; a cancel before arrival, then deliveries of that number with the same and with different bodies; the transaction lifetime firing during `committing`; the channel idle timer with a transaction open; a `begin` whose snapshot or first statement fails.
- A read-only `begin` reports the transaction number of the snapshot its statements read, with commits landing between the `begin` and its first statement.
- An autocommit read and an update that matches no rows, on a write channel of a deposed leader, get `behind` when the pool has seen a later write.
- Go: `ExecContext` and `QueryContext` return as soon as the context ends, and an unsettled request's error matches both `ErrOutcomeUnknown` and the context's error; an autocommit `RETURNING` hands no row to `database/sql` before its request settles. JDBC: a query timeout raises `SQLTimeoutException` only for a `cancelled` receipt and `08007` when the outcome is unknown, directly and through JPA, with timeout races and nested causes.
- No compiled statement is reused across principals, claims or policy generations.
- Admission: `cancel`, `close` and `resolve` are served while ordinary capacity is full; no principal holds more than its share of waiting-writer slots; slow headers and bodies are cut at their deadlines; the body of an unauthenticated request is never read; a resolve burst after a takeover runs one window fill.
- Both drivers refuse every redirect.
- An autocommit `INSERT OR FAIL` that fails leaves nothing; inside a transaction `txn_state` matches SQLite after every error, and an aborted transaction never runs a statement in autocommit.
- The policy hook runs before every prepare, including `describe`, and stateless `query` refuses transaction control, `ATTACH` and `DETACH`.
- Values round-trip through the conformance vectors in both drivers, the temporal forms and the INTEGER boolean rule included; a body cut at every byte offset is reported as incomplete, never as a short result.
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
[rfc3986-norm]: https://www.rfc-editor.org/rfc/rfc3986#section-6.2.2
[sq-authorizer]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/sqlite.h.in#L3396-L3401
[go-redirect]: https://github.com/golang/go/blob/go1.26.2/src/net/http/client.go#L512-L540
[go-redirect-max]: https://github.com/golang/go/blob/go1.26.2/src/net/http/client.go#L833-L835
[hib-std-conv]: https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/StandardSQLExceptionConverter.java#L32-L40
[spring-het-fallback]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L217-L221
[spring-jpa-sys]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/JpaSystemException.java#L31
[spring-uncat]: https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-tx/src/main/java/org/springframework/dao/UncategorizedDataAccessException.java#L29
