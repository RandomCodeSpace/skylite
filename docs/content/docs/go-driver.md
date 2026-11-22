---
title: Go driver
description: The Go database/sql driver, which reads through the stateless query endpoint and writes and runs transactions on write channels, how to configure it, how it reports outcomes it cannot learn, and what it does not do yet.
weight: 20
status: current
---

Package `github.com/RandomCodeSpace/skylite/driver`, whose package name is `skylite`, is a Go `database/sql` driver that speaks the protocol of [0005: Client protocol](/docs/decisions/0005-client-protocol/) over HTTPS. Queries outside a transaction go to the stateless `query` endpoint; writes and transactions go on a [write channel](/docs/server/#write-channels) that each connection opens on first use.

The driver's unit tests run against an in-process fake server built from the protocol's codec. An acceptance test, the module `driver/acceptance`, runs GORM through its sqlite dialector on this driver against `skylite serve` built from the same tree; CI runs it on every pull request.

## Usage

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
		Database: "library",
		Token: skylite.TokenFunc(func(ctx context.Context) (string, error) {
			// A projected service account token, read again on every request.
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

`db.Close()` closes the connector, which closes the idle connections of the driver's own HTTP client. It never closes those of a client passed as `HTTPClient`.

`db.QueryContext(ctx, "SELECT id, title FROM books WHERE id > ?", 10)`, `db.ExecContext` and `db.BeginTx` then work as with any driver.

### GORM

GORM's sqlite dialector accepts an existing `*sql.DB`, so GORM runs on this driver unchanged:

```go
db, err := gorm.Open(sqlite.Dialector{Conn: sqlDB}, &gorm.Config{})
```

The acceptance test runs `gorm.io/gorm` v1.31.2 with `gorm.io/driver/sqlite` v1.6.0, built without cgo, so the dialector's own SQLite driver is never used. It covers `AutoMigrate`, create, read, update, soft and hard delete, associations (has one, has many, belongs to, many to many, preloading and joins), `Transaction` commit and rollback, `Begin` and `Rollback`, `CreateInBatches` and errors: a unique violation, a missing record, a missing table, a statement the SQL policy forbids. Nested transactions do not work: GORM runs them as `SAVEPOINT`, which the server refuses on a channel, and the dialector ignores that error, so the inner work a nested `Transaction` rolls back is committed with the outer transaction. Do not nest `Transaction` calls.

GORM's error translation does not know skylite's errors: with `TranslateError` on, a unique violation stays a `*skylite.Error` (code `sql_error`, SQLite code 19) rather than `gorm.ErrDuplicatedKey`, and an unknown outcome passes through untranslated.

## Configuration

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

## What works

- **Queries.** `QueryContext` and `QueryRowContext` outside a transaction send the statement to the stateless `query` endpoint. Rows are decoded as their frames arrive, never buffered whole. `Rows.Close` before the end releases the response, and a cancelled context aborts the request. A statement the server answers with `needs_channel`, because it is not a pure read, such as an `INSERT ... RETURNING`, is sent on the write channel instead; the server executed nothing the first time.
- **Writes.** `ExecContext` runs the statement on the connection's write channel: outside a transaction, an autocommit write of one statement. `RowsAffected` and `LastInsertId` are the server's `rows_affected` and `last_insert_rowid`. An `ExecContext` of a read also goes on the channel, and takes the database's write lock there.
- **Transactions.** `BeginTx` sends `begin` on the write channel; every statement of the transaction, queries included, then goes on the channel, and `Commit` and `Rollback` send `commit` and `rollback`. Every isolation level that SQLite's serializable isolation satisfies is accepted and served at serializable; `sql.LevelLinearizable` is refused, and so is a read-only transaction, with `skylite.ErrReadOnlyUnsupported`, because the server has no read-only channels yet. Nothing is sent for a refused `BeginTx`. `Commit` and `Rollback` take no context, so the driver bounds each by 60 seconds. A statement that fails inside a transaction leaves it as the server reports it: open, or aborted, in which case every request but `rollback` fails with `transaction_aborted`.
- **Channel results.** A request on a channel is settled before any of its rows reach `database/sql`: the driver reads the whole answer, up to the server's result limit, before it returns. An autocommit `execute` returns at most the server's held-result bound, 1 MiB, of rows.
- **Prepared statements.** `PrepareContext` outside a transaction sends `describe`: the server compiles the statement, executes nothing and reports its parameter count, which `database/sql` then checks. Inside a transaction it sends nothing and the count is unknown, because a stateless describe cannot see the transaction's schema changes. Each execution of the statement sends its SQL again; there are no server-side handles.
- **Ping.** `PingContext` describes `SELECT 1`, the cheapest request any pod serves without a channel. It checks the endpoint, the token and the database. Because a describe is refused during maintenance, a ping then fails with `maintenance`, which is not `driver.ErrBadConn`.
- **Parameters.** Positional (`?`) or named (`sql.Named("t", v)` for `:t`, `@t` or `$t`), not both in one statement.
- **Read-your-writes.** Each `sql.DB` keeps the highest transaction number its answers have reported and sends it as `min_txn` with every stateless request, every `begin` and every autocommit `execute`.

## Write channels

Each connection holds at most one write channel. It opens it with its first write, `begin`, or query that the server sends on, and closes it in `Close`, which waits up to 5 seconds for the close's answer. A channel that has had no answer for more than 60 seconds is closed and replaced before its next use outside a transaction, as [0005's reopening rule](/docs/decisions/0005-client-protocol/#resolving-a-request-after-its-channel-is-lost) asks; the server ends a channel idle for 10 minutes.

Every request on the channel carries the next sequence number. When a delivery gets no settled answer, because of a transport error, a cut or malformed response, a response without the protocol marker, or a refusal before acceptance such as `in_flight` or `overloaded`, the driver sends the same body again with the same sequence number, after 100 ms, then doubling up to 2 s, for at most 60 seconds. That is the protocol's replay: the server runs a sequence number at most once and answers a resend with the recorded outcome. The driver retries nothing else.

| Answer | What the driver returns |
|---|---|
| A `done` frame | The result |
| An error with `final: true`, the request's recorded outcome | The `*skylite.Error`. `NotExecuted` says whether it had no effect; inside a transaction a failed statement has `NotExecuted` false, whatever its code |
| `channel_lost`, `commit_outcome_unknown`, the caller's context ending after the request may have been sent, or 60 seconds of resends without a settled answer | A `*skylite.Error` matching `skylite.ErrOutcomeUnknown` |
| A replay whose rows the server could not keep, for a query | A `*skylite.Error` matching `skylite.ErrResultUnavailable`; `Committed()` is true for an autocommit write |

An unknown outcome is never reported as a success and never as `driver.ErrBadConn`. Its error carries the code of the last answer, if any, the channel's identifier (never its token), the sequence number and the epoch, and also matches the context's error when the caller's context ended, and `skylite.ErrTransactionLost` when the request was a statement inside a transaction. The driver then abandons the channel: in the background it sends `cancel` for the request and `close` for the channel, and nothing waits for them. The connection is discarded: `IsValid` returns false and `ResetSession` returns `driver.ErrBadConn`, so `database/sql` closes it instead of reusing it, and any later use of it through a `*sql.Conn` fails before anything is sent. The server's `resolve` does not exist yet, so the driver cannot settle a lost write after the fact.

**A lost transaction.** After an unknown outcome inside a transaction, every later statement of it and its `Commit` fail at once with an error matching `skylite.ErrTransactionLost`, and nothing is sent; the transaction never commits, because its `commit` is never sent and the server rolls it back when the channel closes or the transaction's idle limit, 30 seconds, passes. Its `Rollback` returns nil, as does a `Rollback` whose own outcome is unknown, for the same reason. A `Commit` whose outcome is unknown matches `skylite.ErrOutcomeUnknown`: it may have committed.

## Values

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

## Errors

An error the server answered is a `*skylite.Error`, read with `errors.As`. It carries the protocol's `Code` (constants `skylite.CodeBusy` and so on), the `NotExecuted` and `Final` flags, the message, the request ID and, when SQLite produced the error, its codes.

| Error | When |
|---|---|
| `skylite.ErrOutcomeUnknown`, `skylite.ErrTransactionLost`, `skylite.ErrResultUnavailable` | See [Write channels](#write-channels). Each is matched by a `*skylite.Error` |
| `skylite.ErrReadOnlyUnsupported` | `BeginTx` of a read-only transaction. Nothing is sent. It also matches `errors.ErrUnsupported`. |
| `driver.ErrBadConn` | Only a connection that was already closed or discarded, before anything is sent. Every failure after a request may have been sent returns the real error, so `database/sql` never runs a request again on its own. A failed channel open returns the real error too. |
| A body that ends early | An error wrapping `io.ErrUnexpectedEOF`, never a short result. |
| A frame above the driver's bound | An error. The driver reads a frame of at most 64 MiB plus its newline, the default result limit, and a terminal frame of at most 4 KiB. A server configured with a larger result limit can send a row the driver refuses. |

## Not yet

- `resolve`. The server does not serve it, so a write or commit whose channel was lost stays unknown. The driver does not settle a request it gave up on through the `close` answer either: it reports it unknown.
- Batches, savepoints, read-only transactions and describe inside a transaction: the server does not serve them on a channel yet.
- Retries of stateless requests. The driver sends each `query` and `describe` once; one refused before acceptance, such as `overloaded` or `not_serving`, is returned to the caller.
- The reopening rule on the open head, which replaces a channel opened too far below the pool's watermark: it matters only to `resolve`.
