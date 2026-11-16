---
title: Go driver
description: The Go database/sql driver's read path, which is all it has in this version, how to configure it, and what it does not do yet.
weight: 20
status: current
---

Package `github.com/RandomCodeSpace/skylite/driver`, whose package name is `skylite`, is a Go `database/sql` driver that speaks the protocol of [0005: Client protocol](/docs/decisions/0005-client-protocol/) over HTTPS. This version has the read path only: statements outside a transaction that the server accepts as pure reads.

The [server](/docs/server/) answers `query` and `describe` on the databases it leads; the driver's own tests run against an in-process fake server built from the protocol's codec, and no test yet runs the driver against the real server.

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

`db.QueryContext(ctx, "SELECT id, title FROM books WHERE id > ?", 10)` then works as with any driver.

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

- **Queries.** `QueryContext`, `QueryRowContext` and `ExecContext` send the statement to the stateless `query` endpoint. Rows are decoded as their frames arrive, never buffered whole. `Rows.Close` before the end releases the response, and a cancelled context aborts the request. `ExecContext` of a read returns the server's row count and last row id.
- **Prepared statements.** `PrepareContext` sends `describe`: the server compiles the statement, executes nothing and reports its parameter count, which `database/sql` then checks. Each execution of the statement sends its SQL again; there are no server-side handles.
- **Ping.** `PingContext` describes `SELECT 1`, the cheapest request any pod serves without a channel. It checks the endpoint, the token and the database. Because a describe is refused during maintenance, a ping then fails with `maintenance`, which is not `driver.ErrBadConn`.
- **Parameters.** Positional (`?`) or named (`sql.Named("t", v)` for `:t`, `@t` or `$t`), not both in one statement.
- **Read-your-writes.** Each `sql.DB` keeps the highest transaction number its answers have reported and sends it as `min_txn` with every request.

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
| `skylite.ErrWritesUnsupported` | The server answered `needs_channel`: the statement is not a pure read. The error also matches the `*skylite.Error` and `errors.ErrUnsupported`. |
| `skylite.ErrTransactionsUnsupported` | `BeginTx`, read-only transactions included. Nothing is sent. |
| `driver.ErrBadConn` | Only a connection that was already closed, before anything is sent. Every failure after a request may have been sent returns the real error, so `database/sql` never runs a request again on its own. |
| A body that ends early | An error wrapping `io.ErrUnexpectedEOF`, never a short result. |
| A frame above the driver's bound | An error. The driver reads a frame of at most 64 MiB plus its newline, the default result limit, and a terminal frame of at most 4 KiB. A server configured with a larger result limit can send a row the driver refuses. |

## Not yet

- Writes and transactions. They need the server's write channels, replay and `resolve`, which do not exist yet.
- Retries. The driver sends each request once; a read refused before acceptance, such as `overloaded` or `not_serving`, is returned to the caller.
- GORM. No GORM test has been run.
