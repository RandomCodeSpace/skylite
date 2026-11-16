---
title: Server
description: What `skylite serve` does in this version, which is authenticate requests, create databases, lead the databases it is configured for and answer read-only queries on them, and how to configure it.
weight: 15
status: current
---

`skylite serve -config <file>` runs the server of [0005: Client protocol](/docs/decisions/0005-client-protocol/) with the authentication of [0006: Authentication](/docs/decisions/0006-authentication/). In this version it creates databases, leads the databases its configuration lists, and answers the two stateless requests, `query` and `describe`, on the databases it leads. It runs pure reads only: it opens no channels and runs no write or transaction. Drivers do not exist yet.

## What it serves

| Request | Answer |
|---|---|
| `PUT /v1/databases/{db}` with body `{}` | `200` and `{"done":{"txn":1,"epoch":1}}` when it created the database; `409` `database_exists` when the database exists or a concurrent `PUT` won |
| `POST /v1/databases/{db}/query` with `sql` and optionally `params` or `named`, `min_txn` and `timeout_ms` | One pure read on the leader: a `columns` frame, one frame per row, then `done` with `txn`, `read_only`, `rows_affected` and `last_insert_rowid`. See [Queries](#queries) |
| `POST /v1/databases/{db}/describe` with `sql` and optionally `min_txn` | The statement's result columns and parameter names, without running it: a `columns` frame, `params` frames, then `done` with `txn` and `param_count` |
| `query` or `describe` on a database this server does not lead | `503` `not_serving`. The body is not read |
| Every other path of 0005 (`channels`, `channel`, `channel/cancel`, `channel/close`, `resolve`, `GET` status) | After authentication, and authorisation at the `read` role on the name, `503` `not_serving`. The body is not read |
| Anything else, or a path with a query string | `400` `invalid_request` |

`{db}` is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or a digit. A `PUT` needs the `admin` role on the name, and every other request the `read` role. Every answer is a sequence of NDJSON frames ending in one terminal frame, and a refusal is one error frame, with the headers `Skylite-Protocol: 1`, `Skylite-Request-Id`, `Content-Type: application/x-ndjson` and `Cache-Control: no-store`; a `401` also carries `WWW-Authenticate: Bearer`. The error object and its codes are those of [0005's errors](/docs/decisions/0005-client-protocol/#errors).

A new database has 4,096-byte pages and base groups of 256 pages. When the store cannot tell whether a create landed, the answer is `500` `internal`, never `database_exists`: the database may or may not exist, and a `PUT` sent again answers which.

### Checks before acceptance

The server runs [0005's checks](/docs/decisions/0005-client-protocol/#checks-before-acceptance) that apply to a `PUT`, a `query` and a `describe`, in their order, except that routing comes before the body:

1. The header block is at most 64 KiB, to which Go's HTTP/1.1 server adds 4 KiB of slack (otherwise `431` from the HTTP layer), and arrives within 10 seconds on HTTP/1.1; the method and path are known, and the path has no query string.
2. *Admission is implemented only for connections:* the server accepts at most 1,024 connections at a time, and one beyond that waits in the operating system's listen queue until another closes. There are no request bounds yet.
3. The bearer token verifies, read from the headers only (`401` `unauthenticated`). The body is never read before this.
4. The principal holds `read` on the name, and `admin` for a `PUT` (`403` `forbidden`). This comes before routing and before anything that depends on whether the database exists, so the answer is the same either way.
5. For a `query` or `describe`, this server leads the database (`503` `not_serving`, the body unread). 0005 routes after the body; a server that does not lead the database cannot serve it whatever the body holds, so it does not read it.
6. The body share of the memory budget has room for 8 × `Content-Length` (`429` `overloaded`, the body unread). The share is half the memory budget, 1 GiB, and a body is charged from before its read until its request ends.
7. `Content-Type` is `application/json`, compared without regard to case, with parameters ignored; `Content-Length` is present once, with one value, and at most 16 MiB (`413` `request_too_large` above it); there is no `Transfer-Encoding`. The whole body is read within 30 seconds and must be exactly `Content-Length` bytes: one JSON object of 0005's form with only the fields its request allows, and exactly `{}` for a `PUT`. A cut body, a reset stream or a body that arrives late is `400` `invalid_request`, and nothing is created or run.
8. For a `query` or `describe`, the server reserves a SQLite connection (`429` `overloaded`; see [Connections for queries](#connections-for-queries)), and the leader is not in maintenance (`503` `maintenance`).

The 30-second body read deadline starts when the server has read the header block, so it also bounds a body that a refusal leaves unread. Such a refusal is answered at once. On HTTP/1.1 it carries `Connection: close`, and Go's HTTP server reads and discards what is left of the body, within the same deadline, before it closes the connection.

### Connections

An answer must be written within 10 seconds (0005's response deadline), refusals included; a client that does not read it loses it and the connection, and on HTTP/2 every stream on that connection. Over TLS the connection is released up to 5 seconds later, while Go's TLS layer tries to send its closing alert. A connection with no request running is closed after 90 seconds, on HTTP/1.1 and HTTP/2. 0005 gives no value for this idle timeout; 90 seconds is this server's. On HTTP/2 it also bounds a header block that never ends. An HTTP/2 connection carries at most 16 concurrent streams.

## Queries

### Leading a database

For each name in `databases`, the server runs the office loop of [0004](/docs/decisions/0004-storage-engine/): it takes office when the database's lease is free, renews the lease every 20 seconds (a lease lives 60 seconds), and steps down when it is deposed or its leader stops. A server that does not hold office is a standby: it reads the lease at most every 5 seconds and runs a candidacy once the lease is released or expires. Only the server in office serves the database; every other server answers `query` and `describe` with `503` `not_serving`. There are no followers and no forwarding, so run one active server and let the [readiness probe](#readiness) steer traffic to it. A database in `databases` need not exist: once a `PUT` has created it, the server takes office within its retry backoff, at most 20 seconds.

### How a query runs

A `query` runs on a pooled SQLite connection of the leader, which has [0007's SQL policy](/docs/decisions/0007-sql-policy/) installed before its first statement. The server begins a read transaction, takes its snapshot's transaction number from the engine, and compiles the statement under the policy. Before it binds or steps anything it checks, in this order:

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

A `describe` compiles the statement under the same policy and makes checks 1 and 3, then answers its result columns and parameter names without binding or stepping it; any statement can be described, writes included. A dropped connection stops a query's rows.

### Connections for queries

Each of the server's SQLite connections is charged a 64 MiB ceiling against a memory budget of 2 GiB, so the server holds at most 32, less one for each database in `databases`, whose leader keeps a connection of its own. A principal, an issuer and a subject, may use a quarter of them at once, at least one. A query that finds its principal's share in use, or every connection in use with none idle, gets `429` `overloaded`. An idle connection of another database is closed to make room.

A connection is reset before every reuse, [as 0007 requires](/docs/decisions/0007-sql-policy/#session-state): an open transaction is rolled back, temporary objects are dropped, foreign keys are enforced and the last inserted row id is cleared. A connection whose reset fails is closed and replaced. A statement that breaks its connection, by overflowing SQLite's C stack or exhausting the connection's memory, gets `internal`; the engine discards the connection without running SQLite on it again, and the leader serves on.

### Maintenance, the tail bound and a stopping leader

While the leader is in maintenance, a query or describe gets `503` `maintenance`, and a running one ends with `maintenance` as its terminal frame. When the database's log reaches the engine's tail bound, 32,768 commits above the manifest, the engine refuses every read and write for good, because this version has no compaction: a query then gets `maintenance` with a message that names the bound, and the server reports itself not ready for as long as it leads that database. A query running when the leader stops, on shutdown, step-down or fail-stop, is interrupted and ends with `not_serving`.

### Readiness

With `probe_listen` set, the server answers `GET /readyz` on that address, in plain HTTP and without authentication: `200` while it leads at least one database of `databases` and none it leads is at its tail bound, and `503` otherwise and from the start of shutdown. A server whose databases do not exist yet is not ready, so the first `PUT` must reach a server directly, for example through a port forward.

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
| `databases` | The databases this server leads and answers queries on: names of the `{db}` form, each once, at most 31. Optional; without it the server serves `PUT` alone. The leaders' local files go in a directory under the system's temporary directory (`TMPDIR`), removed on shutdown |
| `probe_listen` | TCP address of the [readiness probe](#readiness). Optional |

An `endpoint` must be an `http` or `https` URL with a host and no user information, query string or fragment, so that it carries no credential; the server does not start with one that breaks this, and the error does not repeat the value.

`storage` holds exactly one of `localfs`, `s3` and `gcs`. The [storage backends page](/docs/storage-backends/) says how each one has been tested. S3 credentials come from the AWS SDK's default chain (environment variables, shared files, web identity, the instance or task role), and Google Cloud Storage credentials from Application Default Credentials. Azure Blob Storage cannot be configured yet.

## Logs and shutdown

The server writes JSON log lines to standard error: one when it starts; one per refused request with its request id and code; one for each authentication or authorisation refusal with the fields of [0006](/docs/decisions/0006-authentication/#principal-and-logging): the reason, the configured issuer, the subject once the signature has verified, the key id and the first 12 hex digits of the token's SHA-256 hash, and for a `403` the database and the role it needed; one for each created database with the principal's issuer and subject; the office loop's lines, with the database's name, when it takes office, steps down or fails a candidacy; and one for each query that fails with `internal`, with the engine's class of the failure. Logs never hold a bearer token or part of one, SQL text, parameters or SQLite's messages.

On SIGTERM or SIGINT the server reports itself not ready, stops accepting connections, waits for running requests (a create or a query runs for at most 30 seconds, and the wait is at most a minute), leaves office, releasing each lease, then exits with status 0. If requests are still running after a minute, it closes their connections and exits with status 1. A second SIGTERM or SIGINT during the wait ends the process at once.

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

The query answers `503` `not_serving` until the server has taken office for the new database, a second or so after the `PUT`. A new database has no tables, and there is no write path yet to create one.

## Not built yet, and not verified

- Admission control of [0005 decision 9](/docs/decisions/0005-client-protocol/#9-limits-and-defaults): request and per-principal request bounds. The memory budget is fixed at 2 GiB rather than taken from the container's limit, and response frames are not charged to it.
- Writes, channels, transactions, `batch`, `resolve` and `GET` status; followers, forwarding between pods and cluster membership. Until then, run one active server per database.
- `database_not_found` for a query: a server that does not lead the database answers `not_serving`, whether or not it exists.
- Keepalive frames during a long query, and the compiled-statement cache: every query compiles its statement.
- The tail bound is noticed when a request meets it, so a server that takes office at the bound reports itself ready until its first query.
- The header block deadline on HTTP/2 streams: there, only the idle timeout bounds a header block that never ends. The 431 answer is tested on HTTP/1.1 only.
- Reloading a rotated TLS certificate without a restart.
- Running behind a real mesh or ingress has not been tested.
