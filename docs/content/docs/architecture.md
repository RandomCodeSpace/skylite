---
title: Architecture
description: The planned design for skylite, and the questions that are still open.
weight: 20
status: design
---

Nothing on this page is implemented. It records the design the project has decided on and plans to build. The storage design follows [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/), which rests on the [durability spike](/docs/decisions/0002-durability-spike/) and on exploratory probes. The spike ran against an object-store emulator. The probes ran against in-memory models of storage, which is weaker evidence. The record is accepted on condition: the SQLite-side mechanisms must pass the engine's own tests at the pinned versions, or the design falls back to a single-connection mode or returns to the record. The record lists what those tests must prove before any of it is relied on.

## The shape of the system

{{< diagram >}}

Services send SQL over HTTPS, through ingress or Istio, so no raw TCP port has to be exposed. A group of skylite pods serves one database. The pods have no persistent volumes. One pod is the leader and takes all writes; the others are followers. Object storage holds everything that has to survive: the commit log, the base, the manifest and the lease object.

## Durability

Object storage is the source of truth. A write is acknowledged to the client only after object storage has confirmed the upload. If a pod dies after acknowledging a write, that write must already be in object storage.

Pod disk and memory are a cache. They may make skylite faster and are never required for capacity. Local files are wiped when a pod starts and are never a source of truth.

The cost is write latency. Every acknowledged write includes an object storage round trip, and the project accepts that cost.

## Storage engine

skylite plans its own storage engine under SQLite, using the pure-Go engine [`ncruces/go-sqlite3`](https://github.com/ncruces/go-sqlite3). Litestream is not used in the product. Each database is planned to have four kinds of object in storage:

- A **commit log**: one object per transaction, named by its transaction number and created only if absent. That create is the commit point and the acknowledgement point. A name in the log is never freed: once the base covers a commit, garbage collection replaces the object with a small retired form that keeps its header and drops its pages.
- A **base**: the database's pages in fixed-size groups of consecutive page numbers, each an object whose content never changes once it is created. A compaction attempt references a base object only if it created it or read back exactly the bytes it would have written.
- A **manifest**: one object, changed only by compare-and-swap, that names the database identifier, the base objects, the transaction number they are current to, how far the commit log has been retired, the epoch and the current leader's writer nonce.
- A **lease** object, used so that a takeover makes progress.

The database identifier is part of the key of every log and base object, so a delayed request from an old process lands in the namespace of the database it was working on.

On the leader, SQLite is planned to run in WAL mode. The plan is for the storage engine to turn each commit in the write-ahead log into one commit object and upload it before `COMMIT` returns. A client is told one of three outcomes: committed, not committed, or unknown when the upload's result cannot be established. An upload known to have had no effect, followed by a successful truncation of the local log, rolls the transaction back and the pod keeps serving. After an unknown outcome, a failed truncation, or any other failure in the commit path once an upload may have started, the pod stops serving that database until a takeover reads the durable history. The leader is to fold the commit log into new base objects and then swap the manifest.

The first version accepts these limits: one upload per commit; a maximum transaction size, which makes `VACUUM` and large index builds unsupported on large databases; a maximum manifest size; bounds on the leader's local log and on its uncompacted commit log; one small retired object kept per transaction for the life of the database; takeover latency that is not bounded; and followers whose lag is not bounded by their polling interval. The [decision record](/docs/decisions/0004-storage-engine/#10-limits-accepted-in-the-first-version) states each one.

## One leader per database

Each database is to have exactly one leader, and other pods forward writes to it. A lease object in object storage lets a new leader take over, but the lease is not what keeps a stale leader out. A lease on its own cannot stop a leader that has paused, lost its network or missed its expiry from writing again.

The fence is the storage layout. Commits are created only if absent, so a stale leader's next commit meets the new leader's epoch-bump commit at its name, and the stale leader stops. No commit-log object is ever deleted, so this holds even after old commits have been folded into the base. Compaction and the start of a takeover change the manifest by compare-and-swap, and a takeover changes it first, so a stale leader's compaction cannot win. Lease updates and garbage collection do not go through the manifest: the lease carries no safety, and garbage collection only retires commits the base already covers and deletes base objects that no current or future manifest can reference. Two pods may briefly both believe they lead; the design prevents conflicting history, not that belief. A takeover downloads no database content, but it reads the manifest, which grows with the number of base groups.

## Reads

The leader and the followers read pages from object storage on demand and keep them in a bounded local cache. Followers pin each read transaction to one version of the database and move to the newest version between transactions. A client that needs to read its own write presents the transaction number it was given. Read capacity is planned to grow by adding pods; it is not limited by the size of any one pod's disk.

## Interface and clients

The query interface is planned as full relational SQL, the SQL that SQLite provides, over HTTPS. SQLite remains the SQL engine; only the storage underneath it is skylite's own.

Two client libraries are planned:

- A Go `database/sql` driver. The target is that it works with GORM.
- A JDBC driver and a Hibernate dialect. The target is that they work with Spring Data JPA.

## Storage backends

All storage access goes through one storage interface whose writes are create-if-absent, compare-and-swap and an unconditional delete; there is no unconditional put. Azure Blob Storage comes first, then AWS S3, Google Cloud Storage and the local filesystem. The backends are meant to share one test suite that runs against storage emulators.

## Cloud authentication

The goal is to support every authentication method each cloud offers. For Azure that means Entra ID managed identity, workload identity, service principal, CLI login, shared key, SAS token and connection string.

## Security

- Authentication and authorisation on the query endpoint.
- SQL lockdown: no extension loading, no attaching arbitrary files, and limits on query time and size.
- No secrets in logs.

## Open questions

Each question is settled by an entry in [Decisions](/docs/decisions/). A settled question says so and links its record; the others are undecided.

### Database size and restore on start

Settled by [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/). skylite plans for databases of 10 to 100 GB, and no pod restores a working copy: the leader and the followers read pages from object storage on demand.

### Ephemeral disk

Settled by [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/). Local disk holds only a cache and the leader's local write-ahead log with its index, all wiped at start, so ephemeral `emptyDir` disk is enough. More disk may make skylite faster but is never needed for capacity.

### Wire protocol for JDBC

Settled by [0003: JDBC transport](/docs/decisions/0003-jdbc-transport/). skylite plans to ship its own JDBC driver, and a Hibernate dialect, over the same HTTPS API as everything else, rather than serve Arrow Flight SQL for the Apache Arrow Flight SQL JDBC driver, which did not pass the project's Spring Data JPA acceptance suite. That suite, in `clients/java/acceptance/`, is the requirement the planned driver and dialect have to meet.

### Client authentication

Settled by [0006: Authentication](/docs/decisions/0006-authentication/). Clients authenticate to the query endpoint with OIDC JWT bearer tokens only, from issuers in the server's configuration, such as a Kubernetes cluster's service-account issuer or Microsoft Entra ID. Access is denied by default and granted by rules that give a `read`, `write` or `admin` role on a database. No server uses this yet.
