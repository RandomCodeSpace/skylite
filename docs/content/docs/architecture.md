---
title: Architecture
description: The design direction for skylite, and the questions that are still open.
weight: 20
status: design
---

Nothing on this page is implemented. It records the direction the project intends to take. The architecture is decided after the first milestone, an evaluation of Litebase and a durability spike (see the [roadmap](/docs/roadmap/)), and any part of this page can change as a result.

## The shape of the system

{{< diagram >}}

Services send SQL over HTTPS, through ingress or Istio, so no raw TCP port has to be exposed. A group of skylite pods serves one database. The pods have no persistent volumes. One pod is the leader and takes all writes; the others are followers. Object storage holds everything that has to survive: change files, snapshots and the lease object.

## Durability

Object storage is the source of truth. A write is acknowledged to the client only after object storage has confirmed the upload. If a pod dies after acknowledging a write, that write must already be in object storage.

Pod disk and memory are a cache. They make skylite faster and are never required for capacity: a pod that starts on an empty disk must be able to rebuild what it needs from object storage.

The cost is write latency. Every acknowledged write includes an object storage round trip, and the project accepts that cost.

## One leader per database

Each database has exactly one leader. The leader is chosen by a lease object in object storage. Other pods forward writes to the leader. The leader keeps its working copy on ephemeral disk.

A lease on its own cannot stop a leader that has paused, lost its network or missed its expiry from writing again. skylite is designed to fence a stale leader with conditional writes to object storage, so a write from a leader that has lost the lease fails at the storage layer instead of relying on the leader noticing.

## Reads

Followers serve reads by fetching database pages from object storage on demand and keeping them in a bounded cache. Read capacity grows by adding pods; it is not limited by the size of any one pod's disk.

## Interface and clients

The query interface is planned as full relational SQL, the SQL that SQLite provides, over HTTPS. The plan is to build on SQLite rather than a new engine.

Two client libraries are planned:

- A Go `database/sql` driver. The target is that it works with GORM.
- A JDBC driver and a Hibernate dialect. The target is that they work with Spring Data JPA.

Both drivers are planned to speak one protocol, specified in [0005: Client protocol](/docs/decisions/0005-client-protocol/): ordinary HTTP requests with JSON bodies under `/v1`, results as newline-delimited JSON with a required final frame, sessions only for explicit transactions, and a commit identifier on every write so that a driver can find out whether a commit whose response was lost happened.

## Storage backends

All storage access goes through one storage interface. Azure Blob Storage comes first, then AWS S3, Google Cloud Storage and the local filesystem. The backends are meant to share one test suite that runs against storage emulators.

## Cloud authentication

The goal is to support every authentication method each cloud offers. For Azure that means Entra ID managed identity, workload identity, service principal, CLI login, shared key, SAS token and connection string.

## Security

- Authentication and authorisation on the query endpoint.
- SQL lockdown: no extension loading, no attaching arbitrary files, and limits on query time and size.
- No secrets in logs.

## Open questions

These are undecided. Each one will be settled by an entry in [Decisions](/docs/decisions/).

### Database size and restore on start

A pod that starts on an empty disk has to get what it needs from object storage. What database sizes should skylite expect, and how long can restoring a working copy take before it blocks failover or start-up?

### Ephemeral disk

Is ephemeral `emptyDir` disk acceptable for the leader's working copy and the read cache, or must skylite also work where pods get little or no local disk?

### Wire protocol for JDBC

Should the JDBC driver use the same HTTP API as everything else, or a protocol such as gRPC or Arrow Flight SQL?

### Client authentication

Partly settled by [0005: Client protocol](/docs/decisions/0005-client-protocol/): every request is planned to carry an OIDC JWT bearer token, verified by every pod that handles the request, including after a pod forwards it. How token claims map to permissions, and the SQL lockdown rules, are still open.
