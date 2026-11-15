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

## Storage backends

All storage access goes through one storage interface. Azure Blob Storage comes first, then AWS S3, Google Cloud Storage and the local filesystem. The backends are meant to share one test suite that runs against storage emulators.

## Cloud authentication

The goal is to support every authentication method each cloud offers. For Azure that means Entra ID managed identity, workload identity, service principal, CLI login, shared key, SAS token and connection string.

## Security

- Authentication and authorisation on the query endpoint.
- SQL lockdown: no extension loading, no attaching arbitrary files, and limits on query time and size.
- No secrets in logs.

## Open questions

Each question is settled by an entry in [Decisions](/docs/decisions/). A settled question says so and links its record; the others are undecided.

### Database size and restore on start

A pod that starts on an empty disk has to get what it needs from object storage. What database sizes should skylite expect, and how long can restoring a working copy take before it blocks failover or start-up?

### Ephemeral disk

Is ephemeral `emptyDir` disk acceptable for the leader's working copy and the read cache, or must skylite also work where pods get little or no local disk?

### Wire protocol for JDBC

Settled by [0003: JDBC transport](/docs/decisions/0003-jdbc-transport/). skylite plans to ship its own JDBC driver, and a Hibernate dialect, over the same HTTPS API as everything else, rather than serve Arrow Flight SQL for the Apache Arrow Flight SQL JDBC driver, which did not pass the project's Spring Data JPA acceptance suite. That suite, in `clients/java/acceptance/`, is the requirement the planned driver and dialect have to meet.

### Client authentication

How do clients authenticate to the query endpoint?
