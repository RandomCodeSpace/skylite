---
title: "0001: Build new instead of reusing Litebase"
description: skylite will not reuse or fork Litebase; it will be built as a new server embedding Litestream, with fencing and leader election implemented by skylite.
weight: 1
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

[Litebase](https://github.com/litebase/litebase) is an MIT-licensed database server written in Go that runs SQLite with object storage as a tier. Before writing skylite's storage and cluster layers, the project had to answer one question: should skylite reuse Litebase as it is, fork it, contribute the missing parts upstream, or not build on it?

Litebase describes itself as being in [early alpha](https://litebase.com/docs/overview/). It targets a different deployment model from skylite: its [architecture documentation](https://litebase.com/docs/architecture/) says that all nodes must have access to the same distributed storage, with strong read-after-write consistency and POSIX file locking, and its README describes data tiered across instance storage, a distributed file system such as Amazon EFS, and object storage such as Amazon S3. skylite's requirements assume pods with no persistent volume and object storage as the only durable state. Most of the gaps below follow from that difference in goals.

### What was evaluated and how

- Repository: `github.com/litebase/litebase`, branch `main`, commit [`2eb763bc`](https://github.com/litebase/litebase/tree/2eb763bced6d3766caebac72a915463f1da5f3a9) (committed 2026-02-12). Every Litebase code link on this page is pinned to that commit.
- Evaluated on 2026-11-14.
- Method: reading the source and documentation, one scoped build (`CGO_ENABLED=0` and `CGO_ENABLED=1`), and one probe test against the storage layer, written for this evaluation. Repository metadata came from the GitHub API on the same day.
- Not done: no multi-node cluster or cluster behaviour was tested; the probe used the project's single-node test harness with two local-directory tiers. No crash or failover test was run for this evaluation, and no end-to-end test against S3 or a shared file system was run. See [Not verified](#not-verified).

The yardstick is skylite's nine requirements: a pure-Go static binary; object storage as the only durable state, with a write acknowledged only after object storage confirms it; Azure Blob, S3, GCS and local backends behind one interface; every authentication method each cloud supports; full relational SQL over HTTPS; several pods per database with one leader chosen by a lease object and a stale leader fenced by conditional writes; minimal pod storage for 10 to 100 GB databases; a Go `database/sql` driver and a JDBC driver with a Hibernate dialect; and OIDC JWT authentication, authorisation and SQL lockdown on the query endpoint.

## Evaluation

| # | Requirement | Verdict | Strongest evidence |
|---|---|---|---|
| 1 | Pure Go, static binary | Not met | SQLite is built through cgo: [sqlite3.go#L26][r1] |
| 2 | Ack after object storage | Not met | The WAL is written to the shared or local file system: [database_resources.go#L127][r2] |
| 3 | Four backends, one interface | Partly | Object tier is local disk or S3 only: [node_file_system.go#L48][r3] |
| 4 | All cloud auth methods | Not met | S3 uses static keys only: [object_file_system_driver.go#L46][r4] |
| 5 | Relational SQL over HTTPS | Partly | HTTPS is supported when certificate paths are configured ([server.go#L173][r5]); `VACUUM` is rejected ([resolver.go#L107][r10]), `ATTACH` is denied and transactions are capped at five minutes |
| 6 | Leader per database, fencing | Not met | Lease validity is checked against the local clock: [lease.go#L38][r6] |
| 7 | Minimal pod storage | Not met | POSIX high-tier cache evicted by file count and age, not bytes: [tiered_file_system_driver.go#L35][r7] |
| 8 | Go and JDBC clients | Partly | A Go driver exists; no JDBC driver found: [litebase-go][r8] |
| 9 | Auth, authorisation, lockdown | Partly | Fine-grained authorisation exists ([database_connection.go#L743][r9]); no OIDC JWT validation was found in the inspected query authentication path: [authentication_middleware.go#L38][r11] |

[r1]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/sqlite3/sqlite3.go#L26
[r2]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_resources.go#L127
[r3]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system.go#L48
[r4]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file_system_driver.go#L46
[r5]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/server/server.go#L173
[r6]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/lease.go#L38
[r7]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L35
[r8]: https://github.com/litebase/litebase-go
[r9]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L743
[r10]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/resolver.go#L107
[r11]: https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/authentication_middleware.go#L38-L57

### 1. Pure Go, static binary

SQLite 3.51.1 is a vendored amalgamation compiled through cgo, and the custom VFS is written in C with Go callbacks ([vfs.go#L4](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L4)). `CGO_ENABLED=0 go build ./...` fails with `undefined: SQLITE_INTEGER`. With cgo the build succeeds and produces a dynamically linked binary that needs `libc.so.6` and `libm.so.6`. The release workflow builds with `CGO_ENABLED: 1` ([action.yml#L72](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/.github/actions/build-binary/action.yml#L72)). Whether a static musl build is possible was not tried.

Replacing SQLite with a pure-Go build would mean rewriting the `pkg/sqlite3` bindings and the C VFS, and redesigning how replicas update SQLite's WAL index in shared memory ([vfs.go#L641](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L641)).

### 2. Acknowledgement after object storage

This is the deciding gap. The commit path traced in the code is:

1. SQLite WAL frames are written to the network file system, a plain POSIX directory set by `LITEBASE_STORAGE_NETWORK_PATH` ([database_resources.go#L127][r2]). If no path is set, it falls back to a directory on local disk ([node_file_system.go#L77](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system.go#L77)).
2. Connections use `PRAGMA synchronous=NORMAL` ([database_connection.go#L49](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L49)), under which SQLite does not fsync the WAL at commit. Litebase starts a background fsync at most every 100 ms ([database_wal.go#L30](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L30), [database_wal.go#L582](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L582)). The client's response does not wait for that fsync.
3. Checkpointed pages go to page logs on the same network file system. Compaction into range files of 4,096 pages ([range.go#L23](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/range.go#L23)) is skipped if the last one ran less than an hour ago, the default interval ([page_logger.go#L453](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/page_logger.go#L453)).
4. Range files reach S3 through a tiered file system whose upload runs on a 10 s timer, on the primary only ([tiered_file_system_driver.go#L1145](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L1145)). The upload function returns no error and logs failures at debug level ([tiered_file_system_driver.go#L482](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L482)), so `WriteFile`, which forces an upload, returns nil even when the upload fails ([tiered_file_system_driver.go#L1278](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L1278)).

A client is therefore acknowledged before its write reaches object storage. On a real shared file system, durability comes from that file system. On a pod whose network path is local or `emptyDir` disk, the code suggests that writes since the last compaction and upload exist only on that pod's disk. These intervals suggest roughly an hour between ordinary compactions, plus upload delay; they do not bound the amount of data absent from object storage. This was inferred from the code, not measured.

**Storage-layer probe.** A test written for this evaluation, kept outside the Litebase tree and reproduced in full in the [appendix](#appendix-the-storage-layer-probe), ran in the project's single-node test harness and used the tiered file system driver with two local directories as its high and low tiers. It wrote `AAAA` to a file, flushed it, wrote `BBBB` through the same descriptor and closed it. After the close the low tier held `AAAA`, the high-tier copy had been removed, and a read through the driver returned `AAAA`: the second write was lost. In the code, a modified file counts as releasable if it was flushed less than 10 s ago ([tiered_file.go#L379](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file.go#L379)), and release flushes only after 10 s have passed ([tiered_file_system_driver.go#L770](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L770)) before removing the high-tier copy ([tiered_file_system_driver.go#L783](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L783)). This shows lost data at the storage driver level only. It was not shown to lose a committed SQL row; that would need a compaction, a second write to the same range file and a release within 10 s, and no such end-to-end test was run.

### 3. Backends

Storage sits behind a POSIX-like `FileSystemDriver` interface of 17 methods ([file_system.go#L12](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/file_system.go#L12)). The object tier is either a local directory or S3 through aws-sdk-go-v2 ([node_file_system.go#L48][r3]). No Azure or GCS driver was found, and `go.mod` lists no Azure or Google Cloud SDK. The S3 driver uses unconditional `PutObject` of whole objects ([object_file.go#L160](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file.go#L160)), emulates rename as copy then delete ([object_file_system_driver.go#L377](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file_system_driver.go#L377)), and reads with whole-object GETs rather than ranged reads ([object_file.go#L86](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/object_file.go#L86)).

### 4. Cloud authentication

The S3 client is always given a static access key and secret, which overrides the AWS default credential chain ([object_file_system_driver.go#L46][r4]). From this code, workload identity on EKS, instance profiles and SSO would not be used. No Azure or GCS authentication code was found.

### 5. SQL over HTTPS

The engine is SQLite with FTS5, R-Tree and JSON, and extension loading compiled out ([sqlite3.go#L15](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/sqlite3/sqlite3.go#L15)). Queries are posted as JSON to `/v1/databases/{db}/branches/{branch}/query` ([public_routes.go#L326](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/public_routes.go#L326)), and a binary streaming protocol is also offered. Transactions can span requests, with a five-minute limit ([transaction.go#L57](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/transaction.go#L57)). `VACUUM` is rejected on the query path ([resolver.go#L107](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/resolver.go#L107)). The public server uses TLS only when both certificate paths are set ([server.go#L173][r5]); the internal cluster port is plain HTTP by design ([server.go#L194](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/server/server.go#L194)).

### 6. Leader election and fencing

There is one primary per cluster, not per database: the lease and primary files are cluster-wide ([cluster.go#L301](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/cluster.go#L301)). Both live on the shared file system. Election takes a file lock there and collects votes from other nodes over HTTP ([cluster_election.go#L149](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/cluster_election.go#L149)). The 15 s lease is renewed by reading the primary file and then writing the lease file, as separate steps without a shared-file-system lock ([lease.go#L175](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/lease.go#L175)). A node treats its lease as valid while its own clock is before the expiry time ([lease.go#L38][r6]). No epoch or term attached to writes and no conditional object writes were found, so a primary that stalls after checking its lease could still write to the shared files. This paused-primary race is inferred from the code and was not tested.

Replicas read the primary's WAL and page logs from the shared file system and patch SQLite's WAL index from messages the primary broadcasts ([database_wal_synchronizer.go#L78](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal_synchronizer.go#L78)). No other data path for replicas was found. The WAL read path panics on a checkpointed WAL, and a comment there describes the replica case as a bug ([database_wal.go#L421](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_wal.go#L421)). The only node address provider is for AWS ECS ([node_address_providers.go#L15](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_address_providers.go#L15)); [Kubernetes support](https://github.com/litebase/litebase/issues/7) is an open issue.

### 7. Pod storage

Managed caches and buffers share a memory-manager budget of 1 GiB by default ([cluster.go#L113](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/cluster.go#L113-L123)), and SQLite's page cache is set to zero ([database_connection.go#L58](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L58)). Total process memory was not measured; SQLite's shared-memory regions, for example, are allocated directly with `C.malloc` ([vfs.go#L391](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L391-L403)). Disk is not bounded in bytes. The WAL and all page logs since the last compaction stay on the network file system, which is local disk under the fallback. Range files, which hold up to 16 MiB of page payload at the default 4 KiB page size ([config.go#L91](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/config/config.go#L91)) plus an encryption header when encryption is enabled ([stream_encrypter.go#L20](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/stream_encrypter.go#L20)), are fetched whole ([tiered_file_system_driver.go#L738](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/tiered_file_system_driver.go#L738)) into the shared POSIX high tier ([node_file_system.go#L183](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system.go#L183-L189)), or local disk when using the fallback. Its eviction policy uses file count and age rather than a byte budget: 10,000 files and one hour ([tiered_file_system_driver.go#L35][r7]).

### 8. Clients

[`litebase-go`][r8] provides a `database/sql` driver that authenticates with an access key. The GitHub API reported no licence file for it. PHP and Laravel SDKs exist. No JDBC driver was found in the code, the documentation or the organisation's repositories.

### 9. Security

The query endpoint accepts basic authentication, Litebase-issued tokens and signed access keys ([authentication_middleware.go#L38][r11]); no OIDC JWT validation was found on that path. Authorisation is fine-grained: a SQLite authoriser callback checks the action codes it lists against the credential's privileges and denies any action it does not list ([database_connection.go#L743][r9]). A connection with no credential attached is allowed every action ([database_connection.go#L665](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L665)). For connections with a credential, `ATTACH` is denied ([statements.go#L221](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/auth/statements.go#L221)) and extension loading is compiled out. No application-level calls overriding SQLite's runtime limits were found in the inspected bindings. Internal requests carry a header encrypted with the cluster key over plain HTTP, and the `POST /v1/cluster/members` route is registered without the internal middleware ([private_routes.go#L24](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/private_routes.go#L24)); its controller checks the encrypted header but not the timestamp ([cluster_members.controller.go#L70](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/cluster_members.controller.go#L70)) that the internal middleware checks on other routes ([internal_middleware.go#L51](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/http/internal_middleware.go#L51)).

### Project health

Observed on 2026-11-14 from the git history and the GitHub API.

| Item | Value |
|---|---|
| Licence | MIT |
| Commits on `main` | 2022-10-07 to 2026-02-12 ([commits API](https://api.github.com/repos/litebase/litebase/commits?sha=main&per_page=1)) |
| Other branch | `vector`, last commit 2026-03-27 ([branches API](https://api.github.com/repos/litebase/litebase/branches)) |
| Commits since 2026-05-14 | 0, on any branch |
| Human contributors | 1 (1,395 of 1,414 commits; the rest are bots; [contributors API](https://api.github.com/repos/litebase/litebase/contributors)) |
| Latest release | v0.5.11, a prerelease published 2026-01-13; every release listed by the [releases API](https://api.github.com/repos/litebase/litebase/releases?per_page=100) was a prerelease |
| Open issues and PRs | 2 issues, 0 PRs ([issues API](https://api.github.com/repos/litebase/litebase/issues?state=open)) |
| Outside contributions | Two outside-authored issues and no outside-authored pull requests were found among the records visible through the [issues API](https://api.github.com/repos/litebase/litebase/issues?state=all&per_page=100) |
| Stars and forks | 450 and 11 ([repository API](https://api.github.com/repos/litebase/litebase)) |
| Tests | 267 test files, including storage recovery ([page_log_recovery_test.go#L19](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/page_log_recovery_test.go#L19)) and primary-crash recovery ([node_file_system_test.go#L129](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/cluster/node_file_system_test.go#L129)) tests; their coverage and runtime behaviour were not evaluated |
| CI | Last 5 runs on `main` passed ([Actions API](https://api.github.com/repos/litebase/litebase/actions/runs?branch=main&per_page=5)) |

The most recent issue, [#80](https://github.com/litebase/litebase/issues/80) (opened 2026-10-12), had no replies. On [#7](https://github.com/litebase/litebase/issues/7), the request for Kubernetes support open since 2025-08-15, the maintainer replied that there is no timeline.

## Decision

skylite will not reuse or fork Litebase and will not depend on it. skylite will be built as a new server embedding [Litestream](https://github.com/benbjohnson/litestream) as a Go library for its change-file format (LTX), compaction, restore and storage clients, with fencing and leader election implemented by skylite. None of this server functionality exists yet.

Litestream v0.5.18 ([`057d3956`](https://github.com/benbjohnson/litestream/tree/057d39564c75ebd67ba042410a7ffa05ad4c242d)) was read for this decision. What it provides and what skylite has to add:

- **Pure Go.** Litestream's library packages use `modernc.org/sqlite` ([litestream.go#L16](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/litestream.go#L16)), and the library with its Azure, S3, GCS and file clients builds with `CGO_ENABLED=0`.
- **Acknowledgement after upload.** A caller can turn off Litestream's background sync ([replica.go#L61](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L61)) and call `DB.SyncAndWait`, which returns after the pending change files have been uploaded, as far as the storage client reports success ([db.go#L798](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L798)). skylite plans to acknowledge a write only after that call succeeds. From the code, a Litestream transaction ID identifies a change-file capture batch, which may contain several SQLite transactions, and one `SyncAndWait` call can create and upload more than one such batch ([db.go#L1315](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L1315)). Writes would therefore be acknowledged per sync, not per transaction ID.
- **Fencing is skylite's.** Litestream's `WriteLTXFile` takes no precondition ([replica_client.go#L41](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica_client.go#L41)), and the Azure client uploads without access conditions ([abs/replica_client.go#L241](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L241)). skylite plans to wrap the replica client, which change-file writes and deletes, including the compactor's, pass through ([db.go#L884](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L884)). The proposed fence, pending the durability spike, is that each change file is created only if absent. A conflict on a name that holds different content would mean another leader has taken over: the wrapper would stop the database and fail unacknowledged writes, rather than return an error Litestream would retry. Litestream resets its position after a failed upload ([replica.go#L174](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L174)) and would otherwise continue at the next free name.
- **Leader election is skylite's.** Litestream's [lease implementation](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/leaser.go#L24) is S3-only and is not wired into replication. skylite plans a lease object per database in object storage, used for liveness and placement; the create-if-absent write is the proposed fence.
- **Reads are skylite's.** Litestream's on-demand reader needs cgo ([vfs.go#L28](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/vfs.go#L28)). At [`c09d3c3`](https://github.com/ncruces/litestream/tree/c09d3c3dfe6c8353c36316f2e0f755e557e9e7f7), a pure-Go fork's on-demand VFS is read-only; that revision is 280 commits behind v0.5.18. The normal Litestream database and restore APIs work on local database and WAL files ([db.go#L1135](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L1135), [replica.go#L901](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L901)), so a bounded reader does not by itself bound the writer's local footprint.
- **Azure authentication.** Litestream's Azure client supports SAS tokens, shared keys and the default Azure credential chain ([abs/replica_client.go#L193](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L193)), but not connection strings, and does not accept an injected client. The fencing wrapper would build its own Azure client, so skylite would own credential handling.

## Alternatives considered

**Reuse Litebase as it is.** Little work, but it does not meet requirements 2, 4, 6 and 7. Its durability and clustering depend on a shared POSIX file system, which a pod without a persistent volume does not have, and it is cgo-only. On local disk an acknowledged write can be lost with the pod. No commits were observed on the surveyed branches after March 2026. Rejected.

**Fork Litebase.** The fork would need: a change-file writer that acknowledges after a successful object-storage write instead of the network file system; a per-database lease with conditional writes and an epoch on every write; replicas that read from object storage instead of shared files; a byte-bounded page cache with ranged reads; Azure and GCS drivers with full credential chains; fixes to the tiered driver's upload path; and, for requirement 1, a move off cgo. The affected storage, cluster and database packages contain about 27,600 of roughly 72,100 lines of non-test Go source (10.4k, 4.6k and 12.6k respectively). The authentication, HTTP and CLI layers could remain. The fork would carry an alpha codebase, including a panic path that its own comments describe as a bug. No commits were observed on the surveyed branches after March 2026, so upstream maintenance support is uncertain. Core upstream changes could require substantial manual integration. Rejected.

**Contribute an Azure backend upstream.** An Azure driver for the 17-method interface is small, about 500 lines by estimate, because the S3 driver uses no conditional writes. But it would only make Azure Blob the cold tier: the shared file system, acknowledgement before upload and clock-based lease would all remain, so requirements 2, 6 and 7 would still not be met. No pull requests from outside contributors were found, and no commits were observed after March 2026, so acceptance is uncertain. Rejected.

**Build new on Litestream.** Chosen, as described in [Decision](#decision). skylite will own the core correctness work: fencing, election, the read path and per-commit acknowledgement. A fork would need comparable work, plus the cost of removing what Litebase already does differently.

## What skylite may borrow from Litebase

Litebase is MIT-licensed, so skylite may copy parts of it with attribution and the licence notice kept. Parts worth studying:

- The deny-by-default authoriser, which checks listed SQLite action codes against privileges and denies unlisted ones ([database_connection.go#L663](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/database/database_connection.go#L663)) and the privilege checks in [statements.go](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/auth/statements.go#L221). skylite would not copy the rule that a connection without a credential may do anything.
- Its SQL lockdown choices: extension loading compiled out, `ATTACH` denied for connections with a credential.
- Redirecting WAL I/O through the VFS ([vfs.go#L580](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/vfs/vfs.go#L580)) and updating SQLite with a script that copies the upstream amalgamation's `sqlite3.c` and `sqlite3.h` over the vendored files ([update_sqlite.sh#L21-L24](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/scripts/update_sqlite.sh#L21-L24)). Whether the vendored files match the upstream amalgamation was not checked.
- Compression and encryption layered over storage ([durable_database_file_system.go#L31](https://github.com/litebase/litebase/blob/2eb763bced6d3766caebac72a915463f1da5f3a9/pkg/storage/durable_database_file_system.go#L31)), and the page-log and range-file layout as a design reference.
- Transactions that span HTTP requests with a hard timeout, as a model for the query API.

## Prior art

Four other projects store SQLite data in object storage. None of them acknowledges a commit only after object storage confirms it by default, and all are written in Rust, so skylite can take ideas and formats from them but not code. Azure support was not confirmed for Rivet; the surveyed interfaces of the other three projects did not expose Azure Blob support. Observed on 2026-11-14.

- **Rivet's storage engine** ([blog post](https://rivet.dev/blog/2026-07-31-how-we-built-the-first-zero-disk-s3-tiered-storage-engine-for-sqlite/), [source](https://github.com/rivet-dev/rivet)). Commits are acknowledged after a replicated hot tier on storage-node disks; S3 holds cold chunks and is not on the write path. Writers are fenced by client epochs and a guard against dual writes. skylite plans to borrow the chunk as the unit of upload, which keeps the number of writes down, and takes Rivet's reason for keeping S3 off the write path, its quoted 30 to 100 ms per S3 Standard write, as the latency skylite will accept and will have to amortise with group commit.
- **turbolite** ([source](https://github.com/russellromney/turbolite)). Commits go to a local WAL; durability comes at checkpoint, when page groups are uploaded. On its own it has no fencing; the README warns that two writers on one prefix corrupt the manifest. skylite plans to borrow the manifest written as one atomic commit point over immutable data objects, and range reads of seekable compressed frames. Its README describes it as experimental.
- **haqlite and hadb** ([haqlite](https://github.com/russellromney/haqlite), [hadb](https://github.com/russellromney/hadb)). The lease is a JSON object in S3, created with `If-None-Match` and renewed with `If-Match`. A strictly increasing epoch is checked on every manifest write, and a demoted node reinstalls an authoriser that blocks writes. The default mode ships the WAL in the background; an opt-in mode uploads every commit before returning. skylite plans to borrow fencing at the storage write plus an in-process write block on demotion. It plans to avoid a non-atomic HEAD-then-PUT fallback for stores without conditional writes, and plans to test conditional-write atomicity per backend, since haqlite's documentation reports that one S3-compatible store let two racing conditional writes both succeed.
- **Graft** ([source](https://github.com/orbitinghail/graft), [internals](https://graft.rs/docs/internals/)). Clients commit locally and push to object storage asynchronously. A push writes a commit object for the next log position with put-if-absent, so a conflicting writer fails. skylite plans to borrow create-if-absent on the next log position as its fence, and the split between small commit objects and large immutable segments. Graft describes itself as alpha.

## Consequences

- skylite will take an object-storage round trip on every acknowledged write. Group commit is expected to be needed to keep throughput acceptable; the cost has not been measured.
- skylite will own fencing, election, the read path and their tests. These are the parts where mistakes lose data, and no surveyed Go library supplies the complete combination required here.
- From reading the code, Litestream writes change files with checksum tracking turned off ([db.go#L2259](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L2259)), restore keeps that flag ([replica.go#L845-L847](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L845-L847)), the LTX decoder then checks only each file's own checksum ([decoder.go#L107-L116](https://github.com/superfly/ltx/blob/434a99d36f0311033915820a5a53d2eafd32a559/decoder.go#L107-L116), ltx v0.5.2), and the LTX compactor checks transaction IDs but does not compare one file's pre-apply checksum with the previous file's post-apply checksum ([compactor.go#L90-L119](https://github.com/superfly/ltx/blob/434a99d36f0311033915820a5a53d2eafd32a559/compactor.go#L90-L119)). So a well-formed change file from a stale leader at the expected name would be applied without complaint. This was not tested. Fencing therefore has to stop such a file being written at all.
- The next step is the durability spike on the [roadmap](/docs/roadmap/), against Azurite: acknowledge only after upload, kill the process mid-write, restore on an empty disk, and show that a stale leader cannot commit. Its results are expected in the next decision records.
- The roadmap's first milestone stays open until the spike is done.

## Not verified

From the Litebase evaluation:

- End-to-end loss of a committed SQL row was not tested. No multi-node cluster or cluster behaviour was tested; the probe used the project's single-node test harness with two local-directory tiers. No crash test, container stack or S3 service was used. The lost update was reproduced at the storage layer only.
- Behaviour on a real NFS or EFS share, including file-lock semantics and close-to-open caching, and failover timing were not run.
- Whether a static musl build is feasible was not attempted.
- `go vet` and the full test suite were not run; only the scoped probe. CI on `main` showed the last runs passing.
- What happens to an open multi-request transaction when the primary fails over is inferred from the code (it is held in memory, so it is presumably lost), not tested.
- Whether `litebase-go` is published or licensed was not checked beyond the GitHub API, which reported no licence file.
- Litebase's documentation pages on security and the CLI reference were not read in full, and the repository's `docs/` directory was read selectively.
- The reason for the observed absence of commits after March 2026 is not known.

From the Litestream reading behind the decision:

- No test was run against Azure or Azurite. The responses to conditional writes that fail, and how the Azure SDK's retries interact with them, were not observed.
- Per-commit latency under load was not measured.
- Create-if-absent is the proposed fence, pending the durability spike. It stops a writer overwriting an occupied change-file name; it does not by itself make a vacant name conditional on holding the lease. The spike must therefore also pause the old leader, transfer leadership, and resume the old leader before the new leader's first change-file write. Fencing in that interval has not been established.
- Meeting the minimal-pod-storage requirement for a writable 10 to 100 GB database is unverified. It may need integration beyond a read-only on-demand reader, because the writer path above uses local files.
- After fencing, compaction, snapshots and deletes must also be gated, not only ordinary L0 uploads, since the compactor deletes through the same client ([compactor.go#L272](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/compactor.go#L272), [compactor.go#L408](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/compactor.go#L408)). Whether the wrapper can gate all of them was not verified.
- A new leader must restore and establish its position before its first write; Litestream seeds the position from the latest remote L0 file ([db.go#L1698-L1725](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L1698-L1725)). Recovery when L0 retention has removed the latest L0 file was not verified.

## Appendix: the storage-layer probe

The probe below was placed in Litebase's `pkg/storage` directory at the evaluated commit, run once, and then removed. It forces the driver's sync eligibility to true and uses two local directories as tiers, so it exercises the tiered driver only: no object storage, no SQL, and one node.

```go
package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

// Evaluation probe: does a second update made within WriteInterval of the last
// durable flush survive closing the last descriptor?
func TestEvalReleaseDropsRecentUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		high := storage.NewFileSystem(storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/eval_high"))
		low := storage.NewFileSystem(storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/eval_low"))
		fsd := storage.NewTieredFileSystemDriver(context.Background(), high, low, app.Cluster.MemoryManager,
			func(ctx context.Context, d *storage.TieredFileSystemDriver) { d.CanSyncDirtyFiles = func() bool { return true } })
		defer fsd.Shutdown()

		if err := fsd.MkdirAll("eval", 0750); err != nil {
			t.Fatal(err)
		}
		f, err := fsd.OpenFile("eval/file", os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("AAAA"), 0); err != nil {
			t.Fatal(err)
		}
		// Durable flush of "AAAA" while the descriptor stays open.
		if err := fsd.Flush(); err != nil {
			t.Fatal(err)
		}
		afterFlush, _ := low.ReadFile("eval/file")
		t.Logf("low tier after flush=%q", afterFlush)
		if _, err := f.WriteAt([]byte("BBBB"), 0); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		lowData, _ := low.ReadFile("eval/file")
		_, highErr := high.Stat("eval/file")
		viaTiered, _ := fsd.ReadFile("eval/file")
		t.Logf("low tier=%q highTierStatErr=%v tieredRead=%q", lowData, highErr, viaTiered)
		if string(viaTiered) != "BBBB" {
			t.Errorf("acknowledged update lost: got %q want %q", viaTiered, "BBBB")
		}
	})
}
```

Command and recorded result, on 2026-11-14:

```text
go test ./pkg/storage -run TestEvalReleaseDropsRecentUpdate
FAIL: low tier="AAAA", high-tier file removed, tiered read="AAAA"
```

The result line is a summary of the logged values and the failed assertion, not the verbatim test output. The run was not repeated.
