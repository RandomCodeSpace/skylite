---
title: "0001: Build new instead of reusing Litebase"
description: skylite does not reuse or fork Litebase; it is built as a new server that embeds Litestream and adds its own fencing and leader election.
weight: 1
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

[Litebase](https://github.com/litebase/litebase) is an MIT-licensed database server written in Go that runs SQLite with object storage as a tier. Before writing skylite's storage and cluster layers, the project had to answer one question: should skylite reuse Litebase as it is, fork it, contribute the missing parts upstream, or not build on it?

Litebase describes itself as being in [early alpha](https://litebase.com/docs/overview/). It targets a different deployment model from skylite: its [architecture documentation](https://litebase.com/docs/architecture/) says that all nodes must have access to the same distributed storage, with strong read-after-write consistency and POSIX file locking, and its README describes data tiered across instance storage, a distributed file system such as Amazon EFS, and object storage such as Amazon S3. skylite's requirements assume pods with no persistent volume and object storage as the only durable state. Most of the gaps below follow from that difference in goals.

### What was evaluated and how

- Repository: `github.com/litebase/litebase`, branch `main`, commit [`2eb763bc`](https://github.com/litebase/litebase/tree/2eb763bced6d3766caebac72a915463f1da5f3a9) (committed 2026-02-12). Every code link on this page is pinned to that commit.
- Evaluated on 2026-11-14.
- Method: reading the source and documentation, one scoped build (`CGO_ENABLED=0` and `CGO_ENABLED=1`), and one probe test against the storage layer, written for this evaluation. Repository metadata came from the GitHub API on the same day.
- Not done: no cluster was started, no crash or failover test was run, and no end-to-end test against S3 or a shared file system was run. See [Not verified](#not-verified).

The yardstick is skylite's nine requirements: a pure-Go static binary; object storage as the only durable state, with a write acknowledged only after object storage confirms it; Azure Blob, S3, GCS and local backends behind one interface; every authentication method each cloud supports; full relational SQL over HTTPS; several pods per database with one leader chosen by a lease object and a stale leader fenced by conditional writes; minimal pod storage for 10 to 100 GB databases; a Go `database/sql` driver and a JDBC driver with a Hibernate dialect; and OIDC JWT authentication, authorisation and SQL lockdown on the query endpoint.

## Evaluation

| # | Requirement | Verdict | Strongest evidence |
|---|---|---|---|
| 1 | Pure Go, static binary | Not met | SQLite is built through cgo: [sqlite3.go#L26][r1] |
| 2 | Ack after object storage | Not met | The WAL is written to the shared or local file system: [database_resources.go#L127][r2] |
| 3 | Four backends, one interface | Partly | Object tier is local disk or S3 only: [node_file_system.go#L48][r3] |
| 4 | All cloud auth methods | Not met | S3 uses static keys only: [object_file_system_driver.go#L46][r4] |
| 5 | Relational SQL over HTTPS | Partly | TLS only if certificate paths are set: [server.go#L173][r5] |
| 6 | Leader per database, fencing | Not met | Lease validity is checked against the local clock: [lease.go#L38][r6] |
| 7 | Minimal pod storage | Not met | Local cache bounded by file count and age, not bytes: [tiered_file_system_driver.go#L35][r7] |
| 8 | Go and JDBC clients | Partly | A Go driver exists; no JDBC driver: [litebase-go][r8] |
| 9 | Auth, authorisation, lockdown | Partly | Deny-by-default authoriser: [database_connection.go#L743][r9] |

[r1]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/sqlite3/sqlite3.go#L26
[r2]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_resources.go#L127
[r3]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system.go#L48
[r4]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file_system_driver.go#L46
[r5]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/server/server.go#L173
[r6]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/lease.go#L38
[r7]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L35
[r8]: https://github.com/litebase/litebase-go
[r9]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L743

### 1. Pure Go, static binary

SQLite 3.51.1 is a vendored amalgamation compiled through cgo, and the custom VFS is written in C with Go callbacks ([vfs.go#L4](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L4)). `CGO_ENABLED=0 go build ./...` fails with `undefined: SQLITE_INTEGER`. With cgo the build succeeds and produces a dynamically linked binary that needs `libc.so.6` and `libm.so.6`. The release workflow builds with `CGO_ENABLED: 1` ([action.yml#L72](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/.github/actions/build-binary/action.yml#L72)). Whether a static musl build is possible was not tried.

Replacing SQLite with a pure-Go build would mean rewriting the `pkg/sqlite3` bindings and the C VFS, and redesigning how replicas update SQLite's WAL index in shared memory ([vfs.go#L641](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L641)).

### 2. Acknowledgement after object storage

This is the deciding gap. The commit path traced in the code is:

1. SQLite WAL frames are written to the network file system, a plain POSIX directory set by `LITEBASE_STORAGE_NETWORK_PATH` ([database_resources.go#L127][r2]). If no path is set, it falls back to a directory on local disk ([node_file_system.go#L77](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system.go#L77)).
2. Connections use `PRAGMA synchronous=NORMAL` ([database_connection.go#L49](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L49)), under which SQLite does not fsync the WAL at commit. Litebase starts a background fsync at most every 100 ms ([database_wal.go#L30](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L30), [database_wal.go#L582](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L582)). The client's response does not wait for that fsync.
3. Checkpointed pages go to page logs on the same network file system. Compaction into range files of 4,096 pages ([range.go#L23](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/range.go#L23)) is skipped if the last one ran less than an hour ago, the default interval ([page_logger.go#L453](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/page_logger.go#L453)).
4. Range files reach S3 through a tiered file system whose upload runs on a 10 s timer, on the primary only ([tiered_file_system_driver.go#L1145](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L1145)). The upload function returns no error and logs failures at debug level ([tiered_file_system_driver.go#L482](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L482)), so `WriteFile`, which forces an upload, returns nil even when the upload fails ([tiered_file_system_driver.go#L1278](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L1278)).

A client is therefore acknowledged before its write reaches object storage. On a real shared file system, durability comes from that file system. On a pod whose network path is local or `emptyDir` disk, reading the timings above suggests that writes since the last compaction and upload, by default up to about an hour of them, exist only on that pod's disk. That window was inferred from the code, not measured.

**Storage-layer probe.** A test written for this evaluation, kept outside the Litebase tree, used the tiered file system driver with two local directories as its high and low tiers. It wrote `AAAA` to a file, flushed it, wrote `BBBB` through the same descriptor and closed it. After the close the low tier held `AAAA`, the high-tier copy had been removed, and a read through the driver returned `AAAA`: the second write was lost. In the code, a modified file counts as releasable if it was flushed less than 10 s ago ([tiered_file.go#L379](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file.go#L379)), and release flushes only after 10 s have passed ([tiered_file_system_driver.go#L770](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L770)) before removing the high-tier copy ([tiered_file_system_driver.go#L783](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L783)). This shows lost data at the storage driver level only. It was not shown to lose a committed SQL row; that would need a compaction, a second write to the same range file and a release within 10 s, and no such end-to-end test was run.

### 3. Backends

Storage sits behind a POSIX-like `FileSystemDriver` interface of 17 methods ([file_system.go#L12](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/file_system.go#L12)). The object tier is either a local directory or S3 through aws-sdk-go-v2 ([node_file_system.go#L48][r3]). There is no Azure or GCS driver and no Azure or Google SDK in `go.mod`. The S3 driver uses unconditional `PutObject` of whole objects, emulates rename as copy then delete, and downloads a whole object when it is opened ([object_file.go#L86](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file.go#L86)).

### 4. Cloud authentication

The S3 client is always given a static access key and secret, which overrides the AWS default credential chain ([object_file_system_driver.go#L46][r4]). Workload identity on EKS, instance profiles and SSO are therefore not available. No Azure or GCS authentication exists.

### 5. SQL over HTTPS

The engine is SQLite with FTS5, R-Tree and JSON, and extension loading compiled out ([sqlite3.go#L15](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/sqlite3/sqlite3.go#L15)). Queries are posted as JSON to `/v1/databases/{db}/branches/{branch}/query` ([public_routes.go#L326](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/public_routes.go#L326)), and a binary streaming protocol is also offered. Transactions can span requests, with a five-minute limit ([transaction.go#L57](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/transaction.go#L57)). `VACUUM` is rejected on the query path ([resolver.go#L107](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/resolver.go#L107)). The public server uses TLS only when both certificate paths are set ([server.go#L173][r5]); the internal cluster port is plain HTTP by design ([server.go#L194](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/server/server.go#L194)).

### 6. Leader election and fencing

There is one primary per cluster, not per database: the lease and primary files are cluster-wide ([cluster.go#L301](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/cluster.go#L301)). Both live on the shared file system. Election takes a file lock there and collects votes from other nodes over HTTP ([cluster_election.go#L149](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/cluster_election.go#L149)). The 15 s lease is renewed by reading the primary file and then writing the lease file, as separate steps without a lock ([lease.go#L175](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/lease.go#L175)). A node treats its lease as valid while its own clock is before the expiry time ([lease.go#L38][r6]). No epoch or term is attached to writes, and no conditional object writes are used, so a primary that stalls after checking its lease can still write to the shared files.

Replicas read the primary's WAL and page logs from the shared file system and patch SQLite's WAL index from messages the primary broadcasts ([database_wal_synchronizer.go#L78](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal_synchronizer.go#L78)). Without a shared file system they have no data path. The WAL read path panics on a checkpointed WAL, and a comment there describes the replica case as a bug ([database_wal.go#L421](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L421)). The only node address provider is for AWS ECS ([node_address_providers.go#L15](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_address_providers.go#L15)); [Kubernetes support](https://github.com/litebase/litebase/issues/7) is an open issue.

### 7. Pod storage

Memory is bounded by a global limit, 1 GiB by default, and SQLite's page cache is set to zero. Disk is not bounded in bytes: the WAL and all page logs since the last compaction stay on the network or local file system, range files of up to 16 MiB are fetched whole into the local tier, and the local tier is evicted by a count of 10,000 files and a one-hour age ([tiered_file_system_driver.go#L35][r7]).

### 8. Clients

[`litebase-go`][r8] provides a `database/sql` driver that authenticates with an access key. The GitHub API reported no licence file for it. PHP and Laravel SDKs exist. There is no JDBC driver in the code, the documentation or the organisation's repositories.

### 9. Security

The query endpoint accepts signed access keys, bearer tokens and basic authentication. Authorisation is fine-grained: a SQLite authoriser callback maps every action code to the credential's privileges and denies anything unlisted ([database_connection.go#L743][r9]). A connection with no credential attached is allowed every action ([database_connection.go#L665](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L665)). `ATTACH` is always denied ([statements.go#L221](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/auth/statements.go#L221)) and extension loading is compiled out. No `sqlite3_limit` calls were found. Internal requests carry a header encrypted with the cluster key over plain HTTP, and the `POST /v1/cluster/members` route is registered without the internal middleware ([private_routes.go#L24](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/private_routes.go#L24)); its controller checks the encrypted header but not the timestamp.

### Project health

Observed on 2026-11-14 from the git history and the GitHub API.

| Item | Value |
|---|---|
| Licence | MIT |
| Commits on `main` | 2022-10-07 to 2026-02-12 |
| Other branch | `vector`, last commit 2026-03-27 |
| Commits since 2026-05-14 | 0, on any branch |
| Human contributors | 1 (1,395 of 1,414 commits; the rest are bots) |
| Releases | 11 on GitHub, v0.5.0 to v0.5.11, all prereleases, last 2026-01-13 |
| Open issues and PRs | 2 issues, 0 PRs |
| Outside contributions | 2 of 80 issue and PR numbers, both issues |
| Stars and forks | 450 and 11 |
| Tests | 267 test files; no crash-recovery tests found |
| CI | Last 5 runs on `main` passed |

The most recent issue, [#80](https://github.com/litebase/litebase/issues/80) (opened 2026-10-12), had no replies. On [#7](https://github.com/litebase/litebase/issues/7), the request for Kubernetes support open since 2025-08-15, the maintainer replied that there is no timeline.

## Decision

skylite does not reuse or fork Litebase and does not depend on it. skylite is built as a new server that embeds [Litestream](https://github.com/benbjohnson/litestream) as a Go library for its change-file format (LTX), compaction, restore and storage clients. skylite adds its own fencing and its own leader election.

Litestream v0.5.18 ([`057d3956`](https://github.com/benbjohnson/litestream/tree/057d39564c75ebd67ba042410a7ffa05ad4c242d)) was read for this decision. What it provides and what skylite has to add:

- **Pure Go.** Litestream's library packages use `modernc.org/sqlite` ([litestream.go#L16](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/litestream.go#L16)), and the library with its Azure, S3, GCS and file clients builds with `CGO_ENABLED=0`.
- **Acknowledgement after upload.** A caller can turn off Litestream's background sync ([replica.go#L61](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L61)) and call `DB.SyncAndWait`, which returns after the pending change files have been uploaded, as far as the storage client reports success ([db.go#L798](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L798)). skylite plans to acknowledge a write only after that call succeeds. From the code, one Litestream transaction ID covers one sync, not one SQLite transaction, so writes are acknowledged per sync batch.
- **Fencing is skylite's.** Litestream's `WriteLTXFile` takes no precondition ([replica_client.go#L41](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica_client.go#L41)), and the Azure client uploads without access conditions ([abs/replica_client.go#L241](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L241)). skylite plans to wrap the replica client, which every change-file write and delete passes through, so that each change file is created only if absent. A conflict on a name that holds different content means another leader has taken over: the wrapper stops the database and fails unacknowledged writes, rather than returning an error Litestream would retry. Litestream resets its position after a failed upload ([replica.go#L174](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L174)) and would otherwise continue at the next free name.
- **Leader election is skylite's.** Litestream's [lease implementation](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/leaser.go#L24) is S3-only and is not wired into replication. skylite plans a lease object per database in object storage, used for liveness and placement; the create-if-absent write is the fence.
- **Reads are skylite's.** Litestream's on-demand reader needs cgo ([vfs.go#L28](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/vfs.go#L28)). A [pure-Go fork](https://github.com/ncruces/litestream) of it is read-only and 280 commits behind v0.5.18.
- **Azure authentication.** Litestream's Azure client supports SAS tokens, shared keys and the default Azure credential chain ([abs/replica_client.go#L193](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L193)), but not connection strings, and does not accept an injected client. The fencing wrapper builds its own Azure client, so skylite owns credential handling.

## Alternatives considered

**Reuse Litebase as it is.** Little work, but it does not meet requirements 2, 4, 6 and 7. Its durability and clustering depend on a shared POSIX file system, which a pod without a persistent volume does not have, and it is cgo-only. On local disk an acknowledged write can be lost with the pod. There have been no commits since March 2026. Rejected.

**Fork Litebase.** The fork would need: a change-file writer that acknowledges after a successful object-storage write instead of the network file system; a per-database lease with conditional writes and an epoch on every write; replicas that read from object storage instead of shared files; a byte-bounded page cache with ranged reads; Azure and GCS drivers with full credential chains; fixes to the tiered driver's upload path; and, for requirement 1, a move off cgo. That replaces the storage, cluster and database packages, about 27,600 of roughly 72,100 lines of non-test Go source (10.4k, 4.6k and 12.6k respectively). The authentication, HTTP and CLI layers would remain. The fork would carry an alpha codebase, including a panic path that its own comments describe as a bug, with no active upstream to share the work, and every upstream change would conflict with the rewritten core. Rejected.

**Contribute an Azure backend upstream.** An Azure driver for the 17-method interface is small, about 500 lines by estimate, because the S3 driver uses no conditional writes. But it would only make Azure Blob the cold tier: the shared file system, acknowledgement before upload and clock-based lease would all remain, so requirements 2, 6 and 7 would still not be met. The repository has no merged pull requests from outside contributors and no commits since March 2026, so acceptance is uncertain. Rejected.

**Build new on Litestream.** Chosen, as described in [Decision](#decision). skylite owns the core correctness work: fencing, election, the read path and per-commit acknowledgement. A fork would need the same work, plus the cost of removing what Litebase already does differently.

## What skylite may borrow from Litebase

Litebase is MIT-licensed, so skylite may copy parts of it with attribution and the licence notice kept. Parts worth studying:

- The deny-by-default authoriser that maps every SQLite action code to a privilege ([database_connection.go#L663](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L663)) and the privilege checks in [statements.go](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/auth/statements.go#L221). skylite would not copy the rule that a connection without a credential may do anything.
- Its SQL lockdown choices: extension loading compiled out, `ATTACH` always denied.
- Keeping the SQLite amalgamation unpatched and redirecting WAL I/O through the VFS, so SQLite upgrades are scripted.
- Compression and encryption layered over storage ([durable_database_file_system.go#L31](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/durable_database_file_system.go#L31)), and the page-log and range-file layout as a design reference.
- Transactions that span HTTP requests with a hard timeout, as a model for the query API.

## Prior art

Four other projects store SQLite data in object storage. None of them acknowledges a commit only after object storage confirms it by default, none supports Azure Blob today, and all are written in Rust, so skylite can take ideas and formats from them but not code. Observed on 2026-11-14.

- **Rivet's storage engine** ([blog post](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/), [source](https://github.com/rivet-dev/rivet)). Commits are acknowledged after a replicated hot tier on storage-node disks; S3 holds cold chunks and is not on the write path. Writers are fenced by client epochs and a guard against dual writes. skylite plans to borrow the chunk as the unit of upload, which keeps the number of writes down, and takes Rivet's reason for keeping S3 off the write path, its quoted 30 to 100 ms per S3 Standard write, as the latency skylite accepts and has to amortise with group commit.
- **turbolite** ([source](https://github.com/russellromney/turbolite)). Commits go to a local WAL; durability comes at checkpoint, when page groups are uploaded. On its own it has no fencing; the README warns that two writers on one prefix corrupt the manifest. skylite plans to borrow the manifest written as one atomic commit point over immutable data objects, and range reads of seekable compressed frames. Its README describes it as experimental.
- **haqlite and hadb** ([haqlite](https://github.com/russellromney/haqlite), [hadb](https://github.com/russellromney/hadb)). The lease is a JSON object in S3, created with `If-None-Match` and renewed with `If-Match`. A strictly increasing epoch is checked on every manifest write, and a demoted node reinstalls an authoriser that blocks writes. The default mode ships the WAL in the background; an opt-in mode uploads every commit before returning. skylite plans to borrow fencing at the storage write plus an in-process write block on demotion. It plans to avoid a non-atomic HEAD-then-PUT fallback for stores without conditional writes, and plans to test conditional-write atomicity per backend, since haqlite's documentation reports that one S3-compatible store let two racing conditional writes both succeed.
- **Graft** ([source](https://github.com/orbitinghail/graft), [internals](https://graft.rs/docs/internals/)). Clients commit locally and push to object storage asynchronously. A push writes a commit object for the next log position with put-if-absent, so a conflicting writer fails. skylite plans to borrow create-if-absent on the next log position as its fence, and the split between small commit objects and large immutable segments. Graft describes itself as alpha.

## Consequences

- skylite takes an object-storage round trip on every acknowledged write. Group commit is needed to keep throughput acceptable; the cost has not been measured.
- skylite owns fencing, election, the read path and their tests. These are the parts where mistakes lose data, and none of the libraries surveyed provides them in Go.
- From reading the code, Litestream's restore does not chain checksums between change files, so a well-formed change file from a stale leader at the expected name would be applied without complaint. This was not tested. Fencing therefore has to stop such a file being written at all.
- The next step is the durability spike on the [roadmap](/docs/roadmap/), against Azurite: acknowledge only after upload, kill the process mid-write, restore on an empty disk, and show that a stale leader cannot commit. Its results are expected in the next decision records.
- The roadmap's first milestone stays open until the spike is done.

## Not verified

From the Litebase evaluation:

- End-to-end loss of a committed SQL row was not tested. No cluster, crash test, container stack or S3 service was used. The upload-path defect was reproduced at the storage layer only, with local directories as both tiers.
- Behaviour on a real NFS or EFS share, including file-lock semantics and close-to-open caching, and failover timing were not run.
- Whether a static musl build is feasible was not attempted.
- `go vet` and the full test suite were not run; only the scoped probe. CI on `main` showed the last runs passing.
- What happens to an open multi-request transaction when the primary fails over is inferred from the code (it is held in memory, so it is presumably lost), not tested.
- Whether `litebase-go` is published or licensed was not checked beyond the GitHub API, which reported no licence file.
- Litebase's documentation pages on security and the CLI reference were not read in full, and the repository's `docs/` directory was read selectively.
- Why development stopped after March 2026 is not known.

From the Litestream reading behind the decision:

- No test was run against Azure or Azurite. The responses to conditional writes that fail, and how the Azure SDK's retries interact with them, were not observed.
- Per-commit latency under load was not measured.
