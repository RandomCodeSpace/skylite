---
title: Architecture
description: The planned design for skylite, and the questions that are still open.
weight: 20
status: design
---

Nothing on this page is implemented. It records the design the project has decided on and plans to build. The storage design follows [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/), which rests on the [durability spike](/docs/decisions/0002-durability-spike/) and on exploratory probes. All of that evidence comes from emulators, and the record lists what the engine's own tests must prove before any of it is relied on.

## The shape of the system

{{< diagram >}}

Services send SQL over HTTPS, through ingress or Istio, so no raw TCP port has to be exposed. A group of skylite pods serves one database. The pods have no persistent volumes. One pod is the leader and takes all writes; the others are followers. Object storage holds everything that has to survive: the commit log, the base, the manifest and the lease object.

## Durability

Object storage is the source of truth. A write is acknowledged to the client only after object storage has confirmed the upload. If a pod dies after acknowledging a write, that write must already be in object storage.

Pod disk and memory are a cache. They may make skylite faster and are never required for capacity. Local files are wiped when a pod starts and are never a source of truth.

The cost is write latency. Every acknowledged write includes an object storage round trip, and the project accepts that cost.

## Storage engine

skylite plans its own storage engine under SQLite, using the pure-Go engine [`ncruces/go-sqlite3`](https://github.com/ncruces/go-sqlite3). Litestream is not used in the product. Each database is planned to have four kinds of object in storage:

- A **commit log**: one object per committed transaction, named by its transaction number and created only if absent. That create is the commit point and the acknowledgement point.
- A **base**: the database's pages in fixed-size groups of consecutive page numbers, each an immutable object under a name that is never reused.
- A **manifest**: one small object, changed only by compare-and-swap, that names the base objects and the transaction number they are current to.
- A **lease** object, used so that a takeover makes progress.

On the leader, SQLite is planned to run in WAL mode. The plan is for the storage engine to turn each commit in the write-ahead log into one commit object and upload it before `COMMIT` returns, and to fail the transaction if the upload fails. The leader is to fold the commit log into new base objects and then swap the manifest.

The first version accepts these limits: one upload per commit, a maximum transaction size, a bound on how long the leader's local log may grow while readers hold it back, and followers that lag by their polling interval. The [decision record](/docs/decisions/0004-storage-engine/#8-limits-accepted-in-the-first-version) states each one.

## One leader per database

Each database is to have exactly one leader, and other pods forward writes to it. A lease object in object storage lets a new leader take over, but the lease is not what keeps a stale leader out. A lease on its own cannot stop a leader that has paused, lost its network or missed its expiry from writing again.

The fence is the storage layout. Commits are created only if absent, so a stale leader's next commit loses its name to the new leader's first commit and the stale leader stops. Everything other than a commit changes the manifest by compare-and-swap, which a takeover changes first. A takeover does not download the database, so the intent is that its time depends on the length of the commit log tail, not on database size.

## Reads

The leader and the followers read pages from object storage on demand and keep them in a bounded local cache. Followers pin each read transaction to one version of the database and move to the newest version between transactions. A client that needs to read its own write presents the transaction number it was given. Read capacity is planned to grow by adding pods; it is not limited by the size of any one pod's disk.

## Interface and clients

The query interface is planned as full relational SQL, the SQL that SQLite provides, over HTTPS. SQLite remains the SQL engine; only the storage underneath it is skylite's own.

Two client libraries are planned:

- A Go `database/sql` driver. The target is that it works with GORM.
- A JDBC driver and a Hibernate dialect. The target is that they work with Spring Data JPA.

## Storage backends

All storage access goes through one storage interface whose only writes are create-if-absent and compare-and-swap. Azure Blob Storage comes first, then AWS S3, Google Cloud Storage and the local filesystem. The backends are meant to share one test suite that runs against storage emulators.

## Cloud authentication

The goal is to support every authentication method each cloud offers. For Azure that means Entra ID managed identity, workload identity, service principal, CLI login, shared key, SAS token and connection string.

## Security

- Authentication and authorisation on the query endpoint.
- SQL lockdown: no extension loading, no attaching arbitrary files, and limits on query time and size.
- No secrets in logs.

## Open questions

These are undecided. Each one will be settled by an entry in [Decisions](/docs/decisions/).

### Database size and restore on start

Settled by [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/). skylite plans for databases of 10 to 100 GB, and no pod restores a working copy: the leader and the followers read pages from object storage on demand.

### Ephemeral disk

Settled by [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/). Local disk holds only a cache and the leader's local write-ahead log with its index, all wiped at start, so ephemeral `emptyDir` disk is enough. More disk may make skylite faster but is never needed for capacity.

### Wire protocol for JDBC

Should the JDBC driver use the same HTTP API as everything else, or a protocol such as gRPC or Arrow Flight SQL?

### Client authentication

How do clients authenticate to the query endpoint?
