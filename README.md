# skylite

[![CI](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml)
[![Docs](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml)

A SQL database server where object storage is the source of truth.

That sentence is the goal, not the current state. skylite is a pre-alpha Go
project that aims to build a SQL server on SQLite for Kubernetes pods with no
persistent volumes: all durable state will live in object storage (Azure Blob
first, then AWS S3, Google Cloud Storage and the local filesystem), and clients
will talk to it over HTTPS.

## Status: pre-alpha

skylite does not work yet. Today the repository contains project scaffolding
and a binary that prints its version and help text. Nothing else exists.
Everything below the quickstart is a roadmap, not a feature list.

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

These are goals, not features. The architecture is design direction and
subject to change after the first roadmap step.

- A write is acknowledged only after it is confirmed in object storage.
- Pod disk is a cache only.
- One leader per database, chosen by a lease in object storage, with stale
  leaders fenced by conditional writes.
- Readers fetch pages on demand with a bounded cache.
- SQL over HTTPS, so it works through Istio and ingress.
- A Go `database/sql` driver, and a JDBC driver with a Hibernate dialect.
- Authentication, authorization and SQL lockdown on the query endpoint.

## Roadmap

1. Evaluate Litebase as a base and run a durability spike against Azurite.
   The architecture is decided after this step.
2. Storage interface and four backends (Azure Blob, AWS S3, Google Cloud
   Storage, local filesystem) with an emulator test suite.
3. HTTPS query API.
4. Leader lease and failover.
5. On-demand readers.
6. Clients: Go driver proven with GORM, JDBC driver proven with Spring Data JPA.
7. Endpoint authentication, authorization and SQL lockdown.

## Links

- Documentation: https://randomcodespace.github.io/skylite/
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [License](LICENSE) (Apache-2.0)
