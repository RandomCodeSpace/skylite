---
title: Server
description: What `skylite serve` does in this version, which is authenticate requests and create databases, and how to configure it.
weight: 15
status: current
---

`skylite serve -config <file>` runs the server of [0005: Client protocol](/docs/decisions/0005-client-protocol/) with the authentication of [0006: Authentication](/docs/decisions/0006-authentication/). In this version it does one thing: it creates a database. It runs no SQL, opens no channels and leads no database. Drivers do not exist yet.

## What it serves

| Request | Answer |
|---|---|
| `PUT /v1/databases/{db}` with body `{}` | `200` and `{"done":{"txn":1,"epoch":1}}` when it created the database; `409` `database_exists` when the database exists or a concurrent `PUT` won |
| Every other path of 0005 (`query`, `describe`, `channels`, `channel`, `channel/cancel`, `channel/close`, `resolve`, `GET` status) | After authentication, and authorisation at the `read` role on the name, `503` `not_serving`. The body is not read |
| Anything else, or a path with a query string | `400` `invalid_request` |

`{db}` is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or a digit. A `PUT` needs the `admin` role on the name, and every other request the `read` role. Every answer is one NDJSON frame with the headers `Skylite-Protocol: 1`, `Skylite-Request-Id`, `Content-Type: application/x-ndjson` and `Cache-Control: no-store`; a `401` also carries `WWW-Authenticate: Bearer`. The error object and its codes are those of [0005's errors](/docs/decisions/0005-client-protocol/#errors).

A new database has 4,096-byte pages and base groups of 256 pages. When the store cannot tell whether a create landed, the answer is `500` `internal`, never `database_exists`: the database may or may not exist, and a `PUT` sent again answers which.

### Checks before acceptance

The server runs [0005's checks](/docs/decisions/0005-client-protocol/#checks-before-acceptance) that apply to a `PUT`, in their order:

1. The header block is at most 64 KiB, to which Go's HTTP/1.1 server adds 4 KiB of slack (otherwise `431` from the HTTP layer), and arrives within 10 seconds on HTTP/1.1; the method and path are known, and the path has no query string.
2. *Admission is implemented only for connections:* the server accepts at most 1,024 connections at a time, and one beyond that waits in the operating system's listen queue until another closes. There are no request or memory bounds yet.
3. The bearer token verifies, read from the headers only (`401` `unauthenticated`). The body is never read before this.
4. The principal holds `read` on the name, and `admin` for a `PUT` (`403` `forbidden`). This comes before routing and before anything that depends on whether the database exists, so the answer is the same either way.
5. `Content-Type` is `application/json`, compared without regard to case, with parameters ignored; `Content-Length` is present once, with one value, and at most 16 MiB (`413` `request_too_large` above it); there is no `Transfer-Encoding`. The whole body is read within 30 seconds and must be exactly `Content-Length` bytes and exactly `{}`. A cut body, a reset stream or a body that arrives late is `400` `invalid_request`, and nothing is created.

The 30-second body read deadline starts when the server has read the header block, so it also bounds a body that a refusal leaves unread. Such a refusal is answered at once. On HTTP/1.1 it carries `Connection: close`, and Go's HTTP server reads and discards what is left of the body, within the same deadline, before it closes the connection.

### Connections

An answer must be written within 10 seconds (0005's response deadline), refusals included; a client that does not read it loses it and the connection, and on HTTP/2 every stream on that connection. Over TLS the connection is released up to 5 seconds later, while Go's TLS layer tries to send its closing alert. A connection with no request running is closed after 90 seconds, on HTTP/1.1 and HTTP/2. 0005 gives no value for this idle timeout; 90 seconds is this server's. On HTTP/2 it also bounds a header block that never ends. An HTTP/2 connection carries at most 16 concurrent streams.

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
  }
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

An `endpoint` must be an `http` or `https` URL with a host and no user information, query string or fragment, so that it carries no credential; the server does not start with one that breaks this, and the error does not repeat the value.

`storage` holds exactly one of `localfs`, `s3` and `gcs`. The [storage backends page](/docs/storage-backends/) says how each one has been tested. S3 credentials come from the AWS SDK's default chain (environment variables, shared files, web identity, the instance or task role), and Google Cloud Storage credentials from Application Default Credentials. Azure Blob Storage cannot be configured yet.

## Logs and shutdown

The server writes JSON log lines to standard error: one when it starts; one per refused request with its request id and code; one for each authentication or authorisation refusal with the fields of [0006](/docs/decisions/0006-authentication/#principal-and-logging): the reason, the configured issuer, the subject once the signature has verified, the key id and the first 12 hex digits of the token's SHA-256 hash, and for a `403` the database and the role it needed; and one for each created database with the principal's issuer and subject. Logs never hold a bearer token or part of one.

On SIGTERM or SIGINT the server stops accepting connections, waits for running requests (a create runs for at most 30 seconds, and the wait is at most a minute), then exits with status 0. If requests are still running after a minute, it closes their connections and exits with status 1. A second SIGTERM or SIGINT during the wait ends the process at once.

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
  "storage": {"localfs": {"path": "data"}}
}
EOF
mkdir -p data
skylite serve -config skylite.json &
curl -i -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{}' http://127.0.0.1:8080/v1/databases/orders
```

## Not built yet, and not verified

- Admission control and the memory budget of [0005 decision 9](/docs/decisions/0005-client-protocol/#9-limits-and-defaults): request and per-principal bounds, the body share, and the memory budget's limit on connections. Only the count of connections is bounded.
- Leading databases, queries, channels, `resolve` and `GET` status; forwarding between pods and cluster membership. Until then, run one pod.
- The header block deadline on HTTP/2 streams: there, only the idle timeout bounds a header block that never ends. The 431 answer is tested on HTTP/1.1 only.
- Reloading a rotated TLS certificate without a restart.
- Running behind a real mesh or ingress has not been tested.
