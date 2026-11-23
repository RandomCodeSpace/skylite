---
title: Running in production
description: Run several servers for failover and follower reads, what clients see when the leader changes, shutdown and readiness, and the errors applications must handle.
weight: 35
navgroup: guides
status: current
---

skylite has not yet been run on a real Kubernetes cluster, behind a real service mesh or ingress, or with driver failover across several pods. What follows describes how the server behaves, as built and tested in one process, and as measured with three server processes on one machine against a real Azure storage account; check it in your own environment before relying on it.

## One server or several

Each database has exactly one leader at a time, chosen through a lease object in storage. Only the leader writes. Running one server is the simplest setup: it leads every database it lists, and a restart makes the databases unavailable until it is back in office.

Running several servers with the same `databases` list gives failover: one leads each database, the others stand by, and a standby takes over when the leader stops. With a [peer listener](#running-several-pods), standbys also answer reads and forward everything else to the leader, so a client can send any request to any server.

## Running several pods

Give every pod the same configuration, with a `peer` section:

```json
"peer": {
  "listen": ":8444",
  "address_env": "SKYLITE_PEER_ADDRESS",
  "cert_file": "/etc/skylite/peer.crt",
  "key_file": "/etc/skylite/peer.key",
  "ca_file": "/etc/skylite/peer-ca.crt",
  "server_name": "skylite-peer"
}
```

- **Address.** Each pod publishes its peer address in the database's `leader` object when it takes office. Set the variable named by `address_env` to an address other pods can dial, such as the pod IP from the Kubernetes downward API: `SKYLITE_PEER_ADDRESS=$(POD_IP):8444`.
- **Certificates.** The peer listener requires TLS 1.3 and a client certificate. Every pod presents one certificate as both server and client, so it needs the `serverAuth` and `clientAuth` extended key usages, and the DNS name in `server_name`. Issue peer certificates from a CA that issues nothing else, because any holder of a certificate from that CA is accepted as a pod. The certificates are read at start; rotating them needs a restart.
- **Forwarding.** A pod that cannot serve a request forwards it once, over the peer listener, to the pod that can, and relays the answer. The client never sees a redirect. Requests on a write channel go to the pod that opened the channel.
- **Follower reads.** With `peer` set, `follower_reads` defaults to `true`: a standby answers stateless reads from its own follower, which polls storage about every 250 ms. A follower serves a read only while its view of the log is less than two seconds old; with `min_txn`, which the drivers send so that you read your own writes, it waits briefly to catch up and otherwise forwards the read to the leader.

Without `peer`, a pod that does not lead a database answers `503` `not_serving` for it. A load balancer would then send writes to pods that refuse them, so run a single active pod in that case.

## Failover

The leader holds a 60-second lease on each database and renews it every 20 seconds. A standby reads the lease at most every 5 seconds, and with it the manifest's version, so that once it takes office it can retire commits that older manifests have covered for the five-minute grace period without waiting that period again.

| Event | What happens |
|---|---|
| The leader shuts down cleanly (SIGTERM) | It stops accepting connections, finishes running requests for up to a minute, then releases its leases. A standby sees the released lease within about 5 seconds and takes office. |
| The leader crashes or is cut off | Its lease expires within 60 seconds of its last renewal; then a standby takes office. A deposed leader cannot commit: every commit and manifest update is a conditional write that fails once another leader has taken office. |

Taking office does not download the database: the new leader reads pages from storage as it needs them. It does read the commits written since the last compaction, up to 32 at a time, and reads them once. Measured on Azure Blob Storage, a log of 2,000 uncompacted commits took about 2.3 seconds from the start of the takeover to a serving leader (94 seconds in v0.0.2, which read each commit twice, one at a time). A standby with a follower keeps serving reads from it until its leader serves.

Measured in a 13-minute soak of three servers on one machine against a real Azure storage account, at about 26 commits a second with follower reads on. The time is the write outage: from the event to the first successful write.

| Event | Write outage |
|---|---|
| SIGTERM to the leader | 5.6 and 8.8 seconds |
| SIGKILL of the leader | 47.8 seconds |
| SIGSTOP of the leader | 51.7 seconds |

A killed or paused leader's outage is bounded by its lease: the lease expires 60 seconds after its last renewal, which came up to 20 seconds before the event, and a standby notices within its next read of the lease, at most 5 seconds later. No acknowledged write was lost, and a write that committed without being acknowledged was reported to its client as an unknown outcome, not as a failure. The lease time is fixed in this version and not configurable.

### What clients see

- **Reads** keep working on standbys with fresh followers, during the takeover too. Reads that must go to the leader fail with `503` (`no_leader`, `not_serving`, `owner_unreachable` or `behind`) until a new leader is in office.
- **A deposed leader**, such as one that was paused and resumes after another has taken office, stops when it finds the new leader's writes in storage, and from then answers `503` `not_serving`, not executed. It cannot commit: its conditional writes fail.
- **Writes and transactions** run on write channels, which belong to the leader that opened them. When that leader stops, its channels end: an open transaction is rolled back, and a write that was running when it stopped has an **unknown outcome** (`channel_lost`). It may or may not have committed. New writes fail with `503` until a leader is in office, then succeed on a new channel.
- The [Go driver](/docs/go-driver/#errors-and-retries) sends refused requests again with backoff, and asks the new leader with `resolve` whether a lost write committed; it reports an unknown outcome only when that does not settle it. The [JDBC driver's guide](/docs/jdbc-driver/#errors-and-retries) says what it does. For an outcome that stays unknown, your application decides: retry operations that are safe to repeat, and check the data before repeating one that is not.

## Shutdown

On SIGTERM or SIGINT the server reports itself not ready, stops accepting connections, waits up to one minute for running requests, releases its leases and exits with status 0. If requests are still running after a minute, it closes their connections and exits with status 1. A second signal ends it at once. Give the process more than 60 seconds between SIGTERM and SIGKILL (in Kubernetes, `terminationGracePeriodSeconds` above 60).

## Readiness

With `probe_listen` set, `GET /readyz` on that address answers `200` while the server leads at least one configured database or has a fresh follower for one, and `503` otherwise and from the start of shutdown. Use it as the readiness probe. It needs no token and serves plain HTTP, so do not expose it outside the cluster.

Two consequences:

- A server whose databases do not exist yet is not ready, so a Service that sends traffic only to ready pods never delivers the first `PUT`. Create databases through a second Service over the same pods with `publishNotReadyAddresses: true`, or with `kubectl port-forward` to one pod.
- Readiness is per pod, not per database. One database stuck at a commit-log limit its leader cannot compact past makes the pod not ready for every database it leads.

## Errors to handle

Every error carries a `code`, a `message`, a `request_id` (log it; the server logs the same id), and `not_executed`, which is `true` when the request had no effect. The drivers expose these as a Go `*skylite.Error` or a Java `SQLException`; see the [Go](/docs/go-driver/#errors-and-retries) and [JDBC](/docs/jdbc-driver/#errors-and-retries) guides for the mapping.

**Retry with backoff.** Nothing happened; the same request can succeed later.

| Code | HTTP | Meaning |
|---|---|---|
| `overloaded` | 429 | The server or the principal is at a bound: concurrent requests ([request bounds](/docs/server/#request-bounds)), the request body share of the memory budget, SQLite connections, the share of connections read-only transactions may hold, or the requests waiting for an issuer's signing keys (below). Retry with backoff. |
| `too_many_channels` | 429 | A write channel limit was reached. Close idle connections, or retry later. |
| `busy` | final frame | The database's write lock was not granted within 5 seconds, or too many writers were waiting (64 per database, 16 per principal). Another transaction is holding it; keep transactions short. Recorded on the channel as not executed; the Go driver sends a `begin` or autocommit write refused this way again under a new sequence number. |
| `no_leader` | 503 | No leader is in office for the database, as during failover. |
| `not_serving` | 503 | This server does not serve the database now: it does not lead it and cannot forward the request, it is stopping, or it has been deposed. Also the answer until the server takes office for a new database. |
| `owner_unreachable` | 503 | The pod that owns the channel did not answer the pod that forwarded the request. |
| `behind` | 503 | The server has not caught up to a transaction this client already saw (`min_txn`), or a `resolve` reached a leader that has not yet learned it was deposed. |
| `maintenance` | 503 | The leader is in maintenance, for example compacting a commit log that reached its limit. |
| `transaction_aborted` with `cause: "maintenance"` | final frame | Maintenance rolled the transaction back; rerun the whole transaction. |

An issuer whose keys come through OIDC discovery has them fetched on the first request that needs them, and again when a token names a key id the server does not have, as after the issuer rotates its key. While one request fetches them, up to the 10-second fetch timeout, the others for that issuer wait, at most 64 per server; beyond that they get `429` `overloaded`. A burst of requests at start or right after a rotation can therefore see a few `429`s. With `jwks_file` the keys come from a local file, so the wait is short.

**Outcome unknown.** The request may have taken effect. Check before you repeat it.

| Code | HTTP | Meaning |
|---|---|---|
| `channel_lost` | 410 | The write channel ended, for example because its leader stopped. A write that was running may have committed. |
| `commit_outcome_unknown` | final frame | The commit's upload had no definite answer from storage. The server stops serving the database. |

To settle a write whose channel was lost, send `resolve` with the channel's token and the write's sequence number to the leader, or with a peer listener to any pod; the [Go driver](/docs/go-driver/#errors-and-retries) does this itself. The leader answers one of:

| `resolution` | Meaning |
|---|---|
| `committed` | The write committed; `txn` is its commit. |
| `not_committed` | The write had no effect. It is safe to send it again on a new channel. |
| `pending` | The channel is still live or its last request is still running. Ask again. |
| `unknown` | The leader cannot tell, for example because the channel was opened more than 100,000 transactions before its head or the leader stopped. A resolve that stopped at its limit of header reads continues where it left off when sent again; otherwise check the data. |

A `resolve` refused with `503` `behind`, or with any code in the first table, can be sent again. The [server reference](/docs/server/#resolve) gives the exact rules.

**Fix the request.** Retrying the same request fails the same way.

| Code | HTTP | Meaning |
|---|---|---|
| `unauthenticated` | 401 | The token is missing, expired or not from a configured issuer. |
| `forbidden` | 403 | No rule grants the needed role on this database. |
| `statement_forbidden` | 403 | The server's [SQL policy](/docs/decisions/0007-sql-policy/) refuses the statement. |
| `sql_error` | 400 or final frame | SQLite reported an error, such as a constraint violation; its codes are in `sqlite_code` and `sqlite_extended_code`. |
| `commit_failed` | final frame | The commit definitely did not happen, for example because it was too large for one commit. |
| `timeout` | final frame | The statement reached its time limit (30 seconds) before committing. |
| `request_too_large`, `result_too_large` | 413, final frame | The request or result passed its size limit. |
| `database_exists` | 409 | `PUT` for a database that already exists. |

The full list of codes and their exact semantics is in [0005: Client protocol](/docs/decisions/0005-client-protocol/#errors), and the server's own checks, in order, are in the [server reference](/docs/server/#checks-before-acceptance).

## Logs

The server writes JSON lines to standard error: start and stop, each refused request with its request id and code, authentication refusals with the issuer and subject, database creation, leadership changes, and failures. There is no metrics endpoint. Logs never hold a token, SQL text or parameters.
