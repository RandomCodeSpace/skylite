# skylite

[![CI](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml)
[![Docs](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml)

A SQL database server where object storage is the source of truth.

skylite is a beta Go project: a SQL server on SQLite for Kubernetes pods with
no persistent volumes. All durable state lives in object storage (Azure Blob,
AWS S3, Google Cloud Storage or the local filesystem), and clients talk to it
over HTTPS.

## Status: beta

`skylite serve` authenticates requests with OIDC JWT bearer tokens, creates
databases in object storage, leads the databases it is configured for with a
lease and fencing by conditional writes, compacts their commit logs, answers
stateless reads on leaders and opt-in followers, and runs autocommit writes and
transactions on write channels. With a peer listener, a pod forwards a request
it cannot serve one hop, over mutual TLS, to the pod that serves it. The Go
driver passes a GORM acceptance test, and the JDBC driver and Hibernate dialect
pass a Spring Data JPA acceptance suite, both against the real server.

Not yet verified: the real Azure, S3 and Google Cloud Storage services (the
cloud backends are tested against emulators only), a real Kubernetes cluster,
a real Entra ID tenant as token issuer, driver failover across several pods,
and a 24-hour soak. Batch requests, read-only channels, resolve, admission
control and a container image are still planned; see the roadmap below.

## Why

Running a SQL database in Kubernetes usually means persistent volumes, and
persistent volumes tie a pod to a disk. skylite aims to remove that coupling by
making object storage the only durable store, so a pod can be replaced or
rescheduled without moving a disk.

## Quickstart

Install with Go:

```sh
go install github.com/RandomCodeSpace/skylite/cmd/skylite@latest
skylite version
```

Or build from source:

```sh
git clone https://github.com/RandomCodeSpace/skylite.git
cd skylite
make build
./bin/skylite version
```

`skylite help` lists the available commands.

## Design goals

The roadmap below says which goals are met. The storage design is recorded in
[decision record 0004](https://randomcodespace.github.io/skylite/docs/decisions/0004-storage-engine/).

- A write is acknowledged only after it is confirmed in object storage.
- Pod disk is a cache only.
- One leader per database, chosen by a lease in object storage, with stale
  leaders fenced by conditional writes.
- Readers fetch pages on demand with a bounded cache.
- SQL over HTTPS, so it works through Istio and ingress.
- A Go `database/sql` driver, and a JDBC driver with a Hibernate dialect.
- Authentication, authorization and SQL lockdown on the query endpoint.

## Roadmap

Done:

1. Evaluate Litebase as a base and run a durability spike against Azurite.
2. Storage interface and four backends (Azure Blob, AWS S3, Google Cloud
   Storage, local filesystem) with an emulator test suite.
3. Storage engine: commit log, paged reads, compaction.
4. Leader lease and failover.
5. Read replicas: stateless reads on leaders and opt-in followers.
6. HTTPS query API with write channels and transactions, and one-hop
   forwarding between pods.
7. Clients: Go driver proven with GORM, JDBC driver proven with Spring Data JPA.
8. Endpoint authentication, authorization and SQL lockdown.
9. Signed release binaries.

Planned:

1. Batch requests, describe on a channel, read-only channels and resolve.
2. The rest of admission control.
3. Follower-owned read channels.
4. A container image.
5. Verification against the real cloud storage services, on a real Kubernetes
   cluster and with a real Entra ID tenant, driver failover across several
   pods, and a 24-hour soak.

The [roadmap page](https://randomcodespace.github.io/skylite/docs/roadmap/)
has the details.

## Links

- Documentation: https://randomcodespace.github.io/skylite/
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [License](LICENSE) (Apache-2.0)
