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
| Every other path of 0005 (`query`, `describe`, `channels`, `channel`, `channel/cancel`, `channel/close`, `resolve`, `GET` status) | After authentication, `503` `not_serving`. The body is not read |
| Anything else | `400` `invalid_request` |

`{db}` is 1 to 63 characters from `a-z`, `0-9` and `-`, starting with a letter or a digit. A `PUT` needs the `admin` role on the name. Every answer is one NDJSON frame with the headers `Skylite-Protocol: 1`, `Skylite-Request-Id`, `Content-Type: application/x-ndjson` and `Cache-Control: no-store`; a `401` also carries `WWW-Authenticate: Bearer`. The error object and its codes are those of [0005's errors](/docs/decisions/0005-client-protocol/#errors).

A new database has 4,096-byte pages and base groups of 256 pages. When the store cannot tell whether a create landed, the answer is `500` `internal`, never `database_exists`: the database may or may not exist, and a `PUT` sent again answers which.

### Checks before acceptance

The server runs [0005's checks](/docs/decisions/0005-client-protocol/#checks-before-acceptance) that apply to a `PUT`, in their order:

1. The header block is at most 64 KiB, to which Go's HTTP/1.1 server adds 4 KiB of slack (otherwise `431` from the HTTP layer), and arrives within 10 seconds on HTTP/1.1; the method and path are known.
2. *Admission is not implemented.* There are no request, connection or memory bounds yet.
3. The bearer token verifies, read from the headers only (`401` `unauthenticated`). The body is never read before this.
4. The principal holds `admin` on the name (`403` `forbidden`). This comes before anything that depends on whether the database exists, so the answer is the same either way.
5. `Content-Type` is `application/json`, compared without regard to case, with parameters ignored; `Content-Length` is present and at most 16 MiB (`413` `request_too_large` above it); there is no `Transfer-Encoding`. The whole body is read within 30 seconds and must be exactly `Content-Length` bytes and exactly `{}`. A cut body, a reset stream or a body that arrives late is `400` `invalid_request`, and nothing is created.

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
| `auth.issuers[]` | `issuer` (the exact `iss` value), `audience` (a value `aud` must contain) and, optionally, `jwks_file`, a JSON Web Key Set file read instead of OIDC discovery. Without `jwks_file`, `issuer` must be an `https` URL. The rules are those of 0006 |
| `auth.rules[]` | `issuer`; exactly one of `subject` or `claim` with `value`; `database`, a name or a pattern with `*` and `?`; `role`, one of `read`, `write`, `admin`. Without a matching rule, access is denied |
| `storage.prefix` | Key prefix, segments separated by `/`. A database's objects live under `<prefix>/<db>/`. Empty means no prefix |
| `storage.localfs.path` | A local directory. For trying the server out; a pod's local disk is not durable |
| `storage.s3` | `bucket` and `region` (required), `endpoint` for an S3-compatible service, `path_style` to address the bucket in the path |
| `storage.gcs` | `bucket` (required) and `endpoint` for an emulator, which then gets no credentials |

`storage` holds exactly one of `localfs`, `s3` and `gcs`. The [storage backends page](/docs/storage-backends/) says how each one has been tested. S3 credentials come from the AWS SDK's default chain (environment variables, shared files, web identity, the instance or task role), and Google Cloud Storage credentials from Application Default Credentials. Azure Blob Storage cannot be configured yet.

## Logs and shutdown

The server writes JSON log lines to standard error: one when it starts, one per refused request with its request id and code, one for each authentication refusal with the reason, and one for each created database. Logs never hold a bearer token or part of one.

On SIGTERM or SIGINT the server stops accepting connections, waits for running requests (a create runs for at most 30 seconds, and the wait is at most a minute), then exits with status 0.

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

- Admission control and the memory budget of [0005 decision 9](/docs/decisions/0005-client-protocol/#9-limits-and-defaults): connection, request and per-principal bounds, and the body share.
- Leading databases, queries, channels, `resolve` and `GET` status; forwarding between pods and cluster membership. Until then, run one pod.
- The header block deadline on HTTP/2 streams. The 431 answer is tested on HTTP/1.1 only.
- Reloading a rotated TLS certificate without a restart.
- Running behind a real mesh or ingress has not been tested.
