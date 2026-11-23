---
title: Go driver
description: Connect a Go application to skylite with database/sql or GORM. Install, connection settings, tokens, transactions, and errors and retries.
weight: 40
navgroup: guides
status: current
---

The Go driver is the package `github.com/RandomCodeSpace/skylite/driver` (package name `skylite`), a `database/sql` driver that talks to the server over HTTPS. It is tested against an in-process fake server and, through a GORM acceptance suite, against a real `skylite serve` on every pull request.

## Install

The driver is part of the main module and needs Go 1.26 or later:

```sh
go get github.com/RandomCodeSpace/skylite@v0.0.2
```

## Connect

A DSN cannot carry a token source, so open the database with a connector:

```go
import (
	"context"
	"database/sql"
	"os"
	"strings"

	skylite "github.com/RandomCodeSpace/skylite/driver"
)

func open() (*sql.DB, error) {
	c, err := skylite.NewConnector(skylite.Config{
		Endpoint: "https://skylite.example.internal",
		Database: "orders",
		Token: skylite.TokenFunc(func(ctx context.Context) (string, error) {
			// A projected service account token, read again for every request.
			b, err := os.ReadFile("/var/run/secrets/tokens/skylite")
			return strings.TrimSpace(string(b)), err
		}),
	})
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}
```

`Endpoint` must be `https://` unless you set `AllowPlaintext: true`, which is meant for a local trial or a server behind a mesh that encrypts traffic. For a private CA, pass its roots in `TLSConfig`. All fields are listed under [Configuration](#configuration).

`sql.Open("skylite", dsn)` is registered but fails on first use with `skylite.ErrNoDSN`.

## Tokens

The driver calls `Token` before every HTTP request and sends the result as a bearer token. Read the token fresh each time, as above, so that a rotated token (such as a Kubernetes projected service account token) is picked up without a restart. The token must come from an issuer in the server's [`auth` configuration](/docs/configuration/#auth), and a rule there must grant the role your application needs: `read` for queries, `write` for writes and transactions.

The driver never logs, and its errors never contain the token or the request URL.

## GORM

GORM's sqlite dialector accepts an existing `*sql.DB`, so GORM runs on this driver unchanged:

```go
import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

db, err := gorm.Open(sqlite.Dialector{Conn: sqlDB}, &gorm.Config{})
```

The acceptance suite runs `gorm.io/gorm` v1.31.2 with `gorm.io/driver/sqlite` v1.6.0, built without cgo, so the dialector's own SQLite driver is never used. It covers `AutoMigrate`, create, read, update, soft and hard delete, associations (has one, has many, belongs to, many to many, preloading and joins), `Transaction` commit and rollback, nested transactions with savepoints, `Begin` and `Rollback`, `CreateInBatches`, and errors.

Two things to know:

- GORM's error translation does not know skylite's errors. With `TranslateError` on, a unique violation stays a `*skylite.Error` (code `sql_error`, SQLite code 19) rather than `gorm.ErrDuplicatedKey`.
- GORM wraps each `Create` in a transaction of its own unless `SkipDefaultTransaction` is set. If the context is cancelled after the insert was sent, GORM can turn the driver's unknown-outcome error into one that `errors.Is` matches only as `sql.ErrTxDone`. Set `SkipDefaultTransaction: true` to keep `errors.Is(err, skylite.ErrOutcomeUnknown)` working; a single-row `Create` is then one autocommit write. The acceptance test `TestCancelledCreate` checks both behaviours, so a GORM upgrade that changes them fails the test.

## Queries, writes and transactions

Use `database/sql` as with any driver:

- `QueryContext` and `QueryRowContext` outside a transaction are stateless reads, which any server in a deployment can answer.
- `ExecContext` outside a transaction is an autocommit write of one statement, run on the leader.
- `BeginTx` starts a transaction on the leader. Every statement in it, reads included, runs on the leader and holds the database's write lock until `Commit` or `Rollback`, so other writers wait. Keep transactions short: the server aborts one that is idle for 30 seconds or older than 5 minutes.
- Read-only transactions (`sql.TxOptions{ReadOnly: true}`) are refused with `skylite.ErrReadOnlyUnsupported`. Run reads outside a transaction instead.
- Parameters are positional (`?`) or named (`sql.Named("t", v)` for `:t`, `@t` or `$t`), not both in one statement.

Each `sql.DB` remembers the newest transaction it has seen and sends it with later requests, so a read after your own write sees that write, even when a different server answers it.

## Errors and retries

An error the server answered is a `*skylite.Error`:

```go
var e *skylite.Error
switch {
case errors.Is(err, skylite.ErrOutcomeUnknown):
	// The write may or may not have committed. Check before repeating it.
case errors.As(err, &e) && e.NotExecuted &&
	(e.Code == skylite.CodeOverloaded || e.Code == skylite.CodeBusy ||
		e.Code == skylite.CodeNotServing || e.Code == skylite.CodeNoLeader):
	// Nothing ran. Retry with backoff.
case errors.As(err, &e) && e.Retryable():
	// Maintenance aborted the transaction. Run it again from the start.
}
```

- **What the driver retries itself.** On a write channel, a request that got no settled answer (a lost response, or a refusal such as `overloaded` before the server accepted it) is sent again with the same sequence number, for up to 60 seconds; the server runs it at most once. A `begin` or autocommit write refused with `behind` is moved to a new channel when the server proves it never ran. Nothing else is retried.
- **What you retry.** Stateless reads refused with `overloaded`, `not_serving`, `no_leader` or `behind` are returned to you; so are writes that end with `busy`, and channel opens refused with `no_leader` or `too_many_channels`. They had no effect (`NotExecuted` is true): retry with backoff.
- **Unknown outcomes.** An error matching `skylite.ErrOutcomeUnknown` means a write or commit was sent and its result could not be learnt, for example because the leader stopped. It is never reported as success or as `driver.ErrBadConn`, so `database/sql` never resends it. Inside a transaction, every later statement and the `Commit` then fail with `skylite.ErrTransactionLost`, and the transaction never commits.
- **`database/sql` and `driver.ErrBadConn`.** The driver returns `driver.ErrBadConn` only before anything was sent, so `database/sql`'s own retry never runs a request twice.

[Running in production](/docs/operations/#errors-to-handle) lists every error code and what to do about it.

## Reference

The rest of this page is the driver's exact behaviour.

### Configuration

| Field | Meaning |
|---|---|
| `Endpoint` | The server's base URL, `https://host[:port][/path]`. User information, a query, a fragment, an empty host and `.` or `..` path segments are refused. |
| `Database` | The database name: 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or digit. |
| `Token` | A `TokenSource` that returns the bearer token (a JWT) of each request. It is called before every request. Required. |
| `TLSConfig` | TLS settings, such as private roots, for the driver's own HTTP client. Without it the system roots are used. |
| `AllowPlaintext` | Permits an `http://` endpoint. Without it only `https://` is accepted. |
| `StatementTimeout` | Lowers the server's statement time limit for each query (1 ms to 2^31-1 ms). The caller's context bounds the request on the client side. |
| `ResponseHeaderTimeout` | Bounds the wait for a response's headers on the driver's own HTTP client. Zero means 60 s, ADR 0005's request time *T* at the server's default limits. Raise it with the server's statement time limit, or through a proxy that holds back headers. |
| `HTTPClient` | Replaces the driver's HTTP client. Cannot be combined with `TLSConfig` or `ResponseHeaderTimeout`, which configure the driver's own client. |

No secret goes in the configuration's strings. The driver never logs, and its errors never hold the token or the request URL; an error from the network or TLS can still name the host and port it could not reach. The driver uses HTTP/2 when the server offers it, and refuses every redirect, so a request and its token go only to the configured endpoint.

The driver is registered for `sql.Open` as `"skylite"`, but a DSN cannot carry a token source, so `sql.Open("skylite", dsn)` fails on first use with `skylite.ErrNoDSN`. Use `skylite.NewConnector` and `sql.OpenDB`, as above.

### What works

- **Queries.** `QueryContext` and `QueryRowContext` outside a transaction send the statement to the stateless `query` endpoint. Rows are decoded as their frames arrive, never buffered whole. `Rows.Close` before the end releases the response, and a cancelled context aborts the request. A statement the server answers with `needs_channel`, because it is not a pure read, such as an `INSERT ... RETURNING`, is sent on the write channel instead; the server executed nothing the first time.
- **Writes.** `ExecContext` runs the statement on the connection's write channel: outside a transaction, an autocommit write of one statement. `RowsAffected` and `LastInsertId` are the server's `rows_affected` and `last_insert_rowid`. An `ExecContext` of a read also goes on the channel, and takes the database's write lock there.
- **Transactions.** `BeginTx` sends `begin` on the write channel; every statement of the transaction, queries included, then goes on the channel, and `Commit` and `Rollback` send `commit` and `rollback`. Every isolation level that SQLite's serializable isolation satisfies is accepted and served at serializable; `sql.LevelLinearizable` and values outside `database/sql`'s levels are refused, and so is a read-only transaction, with `skylite.ErrReadOnlyUnsupported`, because the server has no read-only channels yet. Nothing is sent for a refused `BeginTx`. `Commit` and `Rollback` take no context, so the driver bounds each by 60 seconds. A statement that fails inside a transaction leaves it as the server reports it: open, or aborted, in which case every request but `rollback` fails with `transaction_aborted`. When `Commit` or `Rollback` ends with the server's transaction possibly still open or aborted, because the request failed before it was sent (a token source error, say) or the commit was answered `transaction_aborted`, the driver closes the channel, waiting up to 5 seconds, which rolls back what the server holds, and discards the connection, so that the next borrower never runs inside that transaction. `Commit` and `Rollback` return the original error.
- **Channel results.** A request on a channel is settled before any of its rows reach `database/sql`: the driver reads the whole answer before it returns, up to the `result_bytes` limit the channel's open answer advertised; an answer above it is treated as malformed. An autocommit `execute` returns at most the server's held-result bound, 1 MiB, of rows. `ExecContext` reads and drops the rows of its answer. A request body above the channel's advertised `request_body_bytes` is refused before it is sent or given a sequence number.
- **Prepared statements.** `PrepareContext` outside a transaction sends `describe`: the server compiles the statement, executes nothing and reports its parameter count, which `database/sql` then checks. Inside a transaction it sends nothing and the count is unknown, because a stateless describe cannot see the transaction's schema changes. Each execution of the statement sends its SQL again; there are no server-side handles.
- **Ping.** `PingContext` describes `SELECT 1`, the cheapest request any pod serves without a channel. It checks the endpoint, the token and the database. Because a describe is refused during maintenance, a ping then fails with `maintenance`, which is not `driver.ErrBadConn`.
- **Parameters.** Positional (`?`) or named (`sql.Named("t", v)` for `:t`, `@t` or `$t`), not both in one statement.
- **Read-your-writes.** Each `sql.DB` keeps the highest transaction number its answers have reported and sends it as `min_txn` with every stateless request, every `begin` and every autocommit `execute`.

### Write channels

Each connection holds at most one write channel. It opens it with its first write, `begin`, or query that the server sends on, and closes it in `Close`, which waits up to 5 seconds for the close's answer. An open answer must carry a write channel: its identifier, mode, epoch and limits; otherwise the request fails before anything is sent on it. A channel that has had no answer for more than 60 seconds, or whose next sequence number would pass 2^62, is replaced before its next use outside a transaction, as [0005's reopening rule](/docs/decisions/0005-client-protocol/#resolving-a-request-after-its-channel-is-lost) and its sequence bound ask; the old channel is closed in the background, so its close never holds up the request. The server ends a channel idle for 10 minutes.

Every request on the channel carries the next sequence number. When a delivery gets no settled answer, because of a transport error, a cut or malformed response, a response without the protocol marker, an answer for another sequence number, or a refusal before acceptance such as `in_flight` or `overloaded`, the driver sends the same body again with the same sequence number, after 100 ms, then doubling up to 2 s. That is the protocol's replay: the server runs a sequence number at most once and answers a resend with the recorded outcome. One bound of 60 seconds covers the whole request, even with a context that has no deadline: the token source, every delivery and the reading of every response body. A caller's context that ends before the first delivery returns the context's error, and nothing is sent.

A `begin` or autocommit `execute` refused with `behind` is the one request the driver moves: it closes the channel, and when the close's answer shows a last accepted sequence number below the request's, which proves it was never accepted, it opens a new channel and sends the request there, within the same 60 seconds. A close that proves nothing leaves the request on its channel, resent as above. A caller's context that ends before the move returns the context's error. The driver retries nothing else.

| Answer | What the driver returns |
|---|---|
| A `done` frame | The result |
| An error with `final: true`, the request's recorded outcome, naming the request's channel and sequence number | The `*skylite.Error`. `NotExecuted` says whether it had no effect; inside a transaction a failed statement has `NotExecuted` false, whatever its code |
| `channel_lost` or `commit_outcome_unknown`, even marked final; the caller's context ending after the request may have been sent; 60 seconds without a settled answer; or a receipt that contradicts the request: a `done` whose `txn_state` is not the one the operation leaves (`open` after `begin` and a statement in a transaction, `none` otherwise), or a final error without the request's sequence number and channel identifier or with a `txn_state` the operation cannot leave; or a read-only `done` with rows affected | A `*skylite.Error` matching `skylite.ErrOutcomeUnknown` |
| A replay whose rows the server could not keep, for a read-only statement of a transaction with no rows affected | Nothing: the driver sends the statement again under a new sequence number, after 100 ms, then 200 ms and 400 ms, at most 3 times within the same 60 seconds, and returns that answer. When the tries run out, a pause would pass the 60 seconds or the caller's context ends, a `*skylite.Error` matching `skylite.ErrResultUnavailable`, and the context's error when it ended; the transaction stays open |
| A replay whose rows the server could not keep, for a write, or for a statement outside a transaction (one the `query` endpoint refused with `needs_channel`) | A `*skylite.Error` matching `skylite.ErrResultUnavailable`; nothing is sent again. `Committed()` is true for an autocommit write and false otherwise |

An unknown outcome is never reported as a success and never as `driver.ErrBadConn`. Its error carries the code of the last answer, if any, the channel's identifier (never its token), the sequence number and the epoch, and also matches the context's error when the caller's context ended, and `skylite.ErrTransactionLost` when the request was a statement inside a transaction. The driver then abandons the channel: in the background it sends `cancel` for the request, within 2.5 seconds, and `close` for the channel, within the rest of 5 seconds, and nothing but `db.Close()` waits for them. At most 8 such abandons run at once per connector; with none free the driver sends nothing, and the server ends the channel, rolling back what it holds, at its idle limit. The connection is discarded: `IsValid` returns false and `ResetSession` returns `driver.ErrBadConn`, so `database/sql` closes it instead of reusing it, and any later use of it through a `*sql.Conn` fails before anything is sent. The server's `resolve` does not exist yet, so the driver cannot settle a lost write after the fact.

**A lost transaction.** After an unknown outcome inside a transaction, every later statement of it and its `Commit` fail at once with an error matching `skylite.ErrTransactionLost`, and nothing is sent; the transaction never commits, because its `commit` is never sent and the server rolls it back when the channel closes or the transaction's idle limit, 30 seconds, passes. Its `Rollback` returns nil, as does a `Rollback` whose own outcome is unknown, for the same reason. A `Commit` whose outcome is unknown matches `skylite.ErrOutcomeUnknown`: it may have committed.

### Values

| Go value bound | Sent as | Read back as |
|---|---|---|
| `nil` | NULL | `nil` |
| integers | INTEGER | `int64` |
| `float64`, `float32` | REAL; infinities allowed, NaN refused before sending | `float64` |
| `string` | TEXT | `string` |
| `[]byte` | BLOB | `[]byte` |
| `bool` | INTEGER 0 or 1 | `int64`; `database/sql` scans 0 and 1 into a `bool` |
| `time.Time` | TEXT `YYYY-MM-DD HH:MM:SS.fffffffff`, in UTC; years outside 0001 to 9999 refused before sending | `time.Time` in UTC, for TEXT in a date or timestamp form whose year in UTC is 0001 to 9999, in a column whose type name is `DATE`, `DATETIME` or `TIMESTAMP`; otherwise `string` |

A column's type name is its declared type as SQLite reads it: without the arguments in parentheses, which SQLite ignores, trimmed, in upper case. So `datetime(6)` is `DATETIME` and reads as a date column, while `DATETIMEX` does not. `Rows.ColumnTypes` reports that same name as `DatabaseTypeName`. The protocol carries no nullability or scan type, so those are not reported.

### Errors

An error the server answered is a `*skylite.Error`, read with `errors.As`. It carries the protocol's `Code` (constants `skylite.CodeBusy` and so on), the `NotExecuted` and `Final` flags, the message, the request ID, the `Cause` and, when SQLite produced the error, its codes. `Retryable()` is true for a `transaction_aborted` with cause `maintenance`: maintenance ended the transaction, nothing of it committed, and it can be run again from its start.

| Error | When |
|---|---|
| `skylite.ErrOutcomeUnknown`, `skylite.ErrTransactionLost`, `skylite.ErrResultUnavailable` | See [Write channels](#write-channels). Each is matched by a `*skylite.Error` |
| `skylite.ErrReadOnlyUnsupported` | `BeginTx` of a read-only transaction. Nothing is sent. It also matches `errors.ErrUnsupported`. |
| `driver.ErrBadConn` | Only a connection that was already closed or discarded, before anything is sent. Every failure after a request may have been sent returns the real error, so `database/sql` never runs a request again on its own. A failed channel open returns the real error too. |
| A body that ends early | An error wrapping `io.ErrUnexpectedEOF`, never a short result. |
| A frame above the driver's bound | An error. The driver reads a frame of at most 64 MiB plus its newline, the default result limit, and a terminal frame of at most 4 KiB. A server configured with a larger result limit can send a row the driver refuses. |

### Not yet

- `resolve`. The server does not serve it, so a write or commit whose channel was lost stays unknown. The driver does not settle a request it gave up on through the `close` answer either: it reports it unknown. Only a `behind` refusal is settled through a close.
- Batches, read-only transactions and describe inside a transaction: the server does not serve them on a channel yet.
- Retries of stateless requests. The driver sends each `query` and `describe` once; one refused before acceptance, such as `overloaded` or `not_serving`, is returned to the caller.
- The reopening rule on the open head, which replaces a channel opened too far below the pool's watermark: it matters only to `resolve`.
