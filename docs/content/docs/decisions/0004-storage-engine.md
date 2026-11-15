---
title: "0004: Build skylite's own storage engine"
description: skylite will build its own storage engine on ncruces/go-sqlite3, with a commit log, a paged base and a manifest in object storage, and will not use Litestream in the product.
weight: 4
---

**Status:** Accepted. It supersedes the choice of Litestream in [0001](/docs/decisions/0001-litebase-evaluation/).

**Date:** 2026-11-15

Nothing in this record exists yet. Every sentence about skylite is a decision, a plan or a requirement. The evidence comes from the [durability spike](/docs/decisions/0002-durability-spike/), which ran against the Azurite emulator only, and from exploratory probes run while preparing this record. Those probes are not in this repository. Where a statement rests only on a probe, it says so, and the behaviour is listed under [What the engine's tests must prove](#what-the-engines-tests-must-prove).

## Context

### Requirements

These were fixed by the project owner before this decision:

- Go, one static binary, built with `CGO_ENABLED=0`.
- Object storage holds all durable state. Pod disk is a cache. More pod disk or memory may make skylite faster, but it must never be needed for capacity.
- A write is acknowledged only after object storage has confirmed it.
- Databases of 10 to 100 GB. A new leader must serve without first downloading the database, so the leader reads pages from object storage on demand, as readers do.
- One leader per database. Conditional writes must stop a stale leader. The lease alone is not enough.
- Azure Blob, S3, GCS and the local filesystem sit behind one storage interface whose only writes are create-if-absent and compare-and-swap.
- All testing is on emulators for now. Nothing in this record has been checked against a real cloud.

### What the spike established

[0002](/docs/decisions/0002-durability-spike/) embedded Litestream as a library, with `modernc.org/sqlite`, against Azurite. Its decision section adopts three rules, and this record keeps all three:

1. A commit is acknowledged only after skylite's own storage layer has confirmed the commit object in object storage.
2. Every object that becomes part of the durable history is created with create-if-absent. A leader that loses a create to different content stops writing for good. A new leader takes office by winning a create-if-absent for an epoch-bump commit.
3. The lease exists so that a takeover makes progress. No safety argument may rest on it.

The spike also showed what it cost to keep Litestream in the write path. The spike needed a guard of its own for each of three Litestream behaviours (0002 findings 2, 3 and 4). It still left a window in which a stale leader, paused between its checks and its storage request, got a snapshot, a compaction file and a delete accepted by storage (finding 7). Its client could not publish a file above the single-upload limit atomically, so compacting a 257.8 MiB database failed (finding 6). And it kept a full local copy of the database.

### What the research established

The probes behind this record looked at two questions: which pure-Go SQLite engine can host a storage layer written in Go, and how such a layer can see each commit before `COMMIT` returns. The second question was probed three ways: through SQLite's batch-atomic write interface, by watching the rollback journal, and by reading SQLite's write-ahead log (WAL). The findings are cited where they are used below.

## Decision

### 1. skylite builds its own storage engine; Litestream is not used in the product

Four reasons, all from the evidence:

- **Litestream's writer needs a complete local copy of the database.** Its `DB` opens a SQLite database at a local path ([db.go#L1135-L1138][ls-dbopen]), and restore decodes a whole database into a local file ([replica.go#L901][ls-restore]). That conflicts with the requirement that pod disk is never needed for capacity.
- **Its on-demand reader indexes every page.** The reader builds a map with one entry per page number ([vfs.go#L1362-L1364][ls-index]), so its memory grows with the number of pages in the database. The reader also needs cgo: the file carries the `vfs` build tag and imports `psanford/sqlite3vfs` ([vfs.go#L1-L28][ls-vfs]).
- **The spike needed a guard of its own for each of three behaviours, and still left a window open** (0002 findings 2, 3, 4 and 7).
- **Files above the single-upload limit cannot be published atomically** by the spike's client (0002 finding 6).

The fencing rules proven in the spike carry over unchanged. The spike code under `internal/spike/durability/` stays in the repository as evidence until the engine's own tests replace it.

### 2. SQLite engine: `github.com/ncruces/go-sqlite3`

`ncruces/go-sqlite3` is the one pure-Go engine found that can host a writable storage layer written in Go. It is a cgo-free wrapper that translates a Wasm build of SQLite to Go ([README.md#L1-L12][nc-readme]). It exposes SQLite's file interface as public Go interfaces that a storage layer implements ([vfs/api.go#L16-L51][nc-vfsapi]). The version read for this record is v0.37.0 (tag commit `2d9b687b`), which builds SQLite 3.54.1 through `go-sqlite3-wasm/v6` v6.5.35401.

The risks are accepted, and stated plainly:

- **One maintainer.** GitHub's [contributor list](https://github.com/ncruces/go-sqlite3/graphs/contributors) shows one person with nearly all commits.
- **It panics on out-of-memory** instead of returning an error ([conn.go#L493-L495][nc-nomem]).
- **Each connection has its own memory ceiling**, 256 MiB by default, which `WithMaxMemory` can change ([wrap.go#L16-L35][nc-mem]).

These mitigations become requirements:

- Pin the engine version, and upgrade only through a pull request that runs the engine's tests.
- Recover from a panic at the connection boundary and discard that connection.
- Configure the per-connection memory ceiling explicitly.
- Keep the engine behind skylite's own storage layer, so it can be replaced.

### 3. Durable layout in object storage, per database

The names below are illustrative. The format package decides the exact keys and encodings.

- **Commit log.** One object per committed transaction, named by a fixed-width transaction number and written with create-if-absent. It holds the changed pages, the database size after the commit, the leader's epoch and a checksum. That single create is the commit point, the acknowledgement point and the fence for commits.
- **Base.** The database's pages, in fixed-size groups of consecutive page numbers. Each group is an immutable object under a name that is never reused. A page's group follows from arithmetic on its page number, so the index needs memory in proportion to the number of groups, not the number of pages.
- **Manifest.** One small object, updated only by compare-and-swap. It names the base objects, the transaction number the base is current to, and the epoch. It is the atomic fence for everything that is not a commit.
- **Lease.** An object updated by compare-and-swap, used only so that a takeover makes progress.

Every object must fit in one upload request on every backend, because 0002 finding 6 rules out multi-part uploads for objects that fence. The format package sets the group size and the maximum commit size with that in mind.

### 4. Commits are captured from SQLite's write-ahead log

On the leader, SQLite runs in WAL mode with a file-backed shared-memory index. The WAL and that index are ordinary local files. The main database file is virtual: the storage layer serves it from the local cache and from object storage.

- **Capture point.** With `synchronous=FULL`, the probes saw SQLite sync the WAL exactly once per commit, after the commit frame and before `COMMIT` returned. The storage layer builds the commit object from the WAL frames at that sync and uploads it. In SQLite's source the sync comes before the new frames are added to the shared index and before the index header is published ([wal.c#L4281-L4346][sq-walframes]). `synchronous=FULL` must be set explicitly, because ncruces compiles SQLite with the WAL default set to `NORMAL` ([sqlite_opt.h#L5][nc-opt]). Under `NORMAL`, the probes saw no sync at commit.
- **Failure.** If the upload fails or loses its create to another writer, the storage layer truncates the WAL back to the end of the last uploaded commit, inside the failing call, and returns an error. SQLite then rolls back. In the probes, IOERR, IOERR_FSYNC, a plain Go error and FULL from the sync all produced an error to the caller and a rollback, and none made SQLite report success.
- **Why the truncation is required.** After a failed sync, the frames and the commit marker are still in the WAL with valid checksums. SQLite's recovery accepts every valid frame up to the last commit marker ([wal.c#L1534-L1556][sq-recover]). In the probes, a failed transaction reappeared after a reopen whenever the WAL was kept. With the truncation it did not.
- **Visibility.** Other connections see a transaction only after its upload succeeded. In the probes, readers on other connections saw only the old value while an upload was blocked for 300 ms, and never saw a failed commit.
- **Checkpoints.** A local WAL checkpoint writes only to the local cache, because its pages are already in object storage. In the probes every page a checkpoint wrote was byte-identical to a page already uploaded. The one exception is listed below.
- **Start-up.** Local files are wiped when a node starts. They are never a source of truth.

Two behaviours seen in the probes qualify the description above. The engine must handle both:

- **`BUSY` does not roll back an explicit `COMMIT`.** If the commit point returns `BUSY` inside an explicit transaction, `COMMIT` fails with "database is locked" and the transaction stays open with the write lock held. A retried `COMMIT` writes the frames again ([vdbeaux.c#L3438-L3446][sq-busy1], [vdbe.c#L4197-L4202][sq-busy2]). The storage layer must therefore never return `BUSY` from the commit point, or must handle the open transaction explicitly.
- **Entering WAL mode writes page 1 outside the WAL.** `PRAGMA journal_mode=wal` changes page 1 through a rollback-journal transaction, whatever the journal mode ([vdbe.c#L8302-L8307][sq-jm], [btree.c#L11572-L11600][sq-ver]). That write never passes through WAL capture. On a new database the probes saw the first WAL commit send page 1 again, so storage ended up correct. The engine must not rely on that by accident.

WAL mode with exclusive locking, which allows one connection and needs no shared-memory index ([vfs/README.md#L69-L74][nc-walreadme]), is the recorded fallback if the shared-memory route fails the engine's tests.

### 5. Reads

- **Lookup.** A page is looked up first in the commit log written since the manifest's transaction number, through a small in-memory index that compaction keeps bounded, and then in its base group. Fetched data goes into a bounded local cache.
- **Leader reads of the main file.** On the leader, reads of the virtual main file return each page as of the last local checkpoint, not as of the newest upload. SQLite expects the main file to hold only checkpointed state. In a probe, serving the newest uploaded version let a reader that began before a commit see that commit's data, which breaks snapshot isolation.
- **Followers.** Followers use a read-only storage layer. It pins each read transaction to one version and moves to the newest version between transactions. In a probe, a follower that did not pin mixed two versions inside one scan. SQLite decides whether to drop its page cache by comparing bytes 24 to 39 of the file header when it takes a shared lock ([pager.c#L5452-L5482][sq-cache]). In WAL mode the change counter there moves only when page 1 is part of a commit ([pager.c#L3272][sq-cc]). The follower's storage layer must therefore write its own version number into those bytes. In a probe that skipped this, the follower returned stale data.

### 6. Compaction and garbage collection

- **Compaction.** The leader folds the commit log into new base group objects, under new names, and then swaps the manifest by compare-and-swap. A stale leader's compaction output stays unreachable unless its swap wins. A takeover changes the manifest first, so the stale leader's swap cannot win.
- **Deletes.** An object is deleted only when a manifest read from storage no longer references it and a grace period has passed. Every node re-reads the manifest more often than the grace period.
- **Why a stale leader's deletes cannot remove live data.** Names are never reused and manifests only move forward. So an object that is garbage under one committed manifest is garbage under every later one, and a stale leader working from an old manifest only deletes objects that are already garbage. This also requires that a manifest never references an object that an earlier manifest dropped.
- **The grace period protects readers, not safety.** It gives a reader still using an older manifest time to finish. It rests on timing, so it must not be part of any safety argument.

This closes, by construction, the window that [0002](/docs/decisions/0002-durability-spike/) measured in finding 7. That is a design argument. No test has shown it yet.

**A gap the design must close.** The commit fence relies on the name of the next transaction number being taken. Garbage collection removes commit log objects once the base covers them, which frees their names again. A stale leader paused for longer than the grace period could then win a create-if-absent at a transaction number that is already folded into the base, and acknowledge a write that no reader will ever see. The engine must make this impossible without relying on time. One candidate is to re-read the manifest after the create and before acknowledging, and to stop if the manifest has moved past that transaction number or to a higher epoch. This is sound only because manifests move forward. The candidate is not decided or tested.

Objects from a stale leader's lost compaction are never referenced by any manifest, so the rule above never deletes them. Reclaiming them needs a separate rule, not yet decided.

### 7. Takeover

1. A candidate takes the lease.
2. It swaps the manifest to a higher epoch.
3. It reads the tail of the commit log.
4. It wins a create-if-absent for an epoch-bump commit at the next transaction number.

The old leader's next commit loses that name, and it stops for good. If the old leader commits first, the candidate loses the create, reads the tail again and retries. The takeover does not download the database, so the intent is that takeover time depends on the length of the log tail, not on database size. No safety argument rests on clocks or on the lease. An old leader that ignores the lease can delay a takeover, as 0002 finding 8 observed, but cannot break the history.

### 8. Limits accepted in the first version

- **One upload per commit.** A leader can commit at most one transaction per storage round trip. Group commit is a possible later change, not a promise.
- **Maximum transaction size.** A transaction must fit in one commit object. A larger one fails with a clear error.
- **Long readers hold back checkpoints.** In the probes, an open read transaction on the leader stopped checkpoints from backfilling the WAL, and the WAL kept growing. SQLite restarts the WAL only once it is fully backfilled and no reader is using it ([wal.c#L3961-L3995][sq-restart]). Readers on the leader therefore need time limits, and the leader refuses writes once its local WAL grows past a bound.
- **Follower lag.** Followers lag by their polling interval. A client that needs to read its own write presents the transaction number it was given.

## Alternatives considered

**Litestream as a library.** This is what the spike ran. Rejected for the four reasons in [decision 1](#1-skylite-builds-its-own-storage-engine-litestream-is-not-used-in-the-product).

**`modernc.org/sqlite`.** Its public VFS package exposes an `fs.FS` read-only: "it has no write path and refuses to create a journal" ([vfs/patches.go#L5-L25][mc-ro]). A writable Go storage layer would have to wire SQLite's method table by hand through `unsafe` pointer writes, the way that package does for its own read-only methods ([vfs/patches.go#L115-L123][mc-unsafe]). That is not a supported interface. In a probe, modernc's compile options did not include batch-atomic writes. A third-party wrapper that claims a writable Go VFS on modernc was found and not evaluated. Rejected.

**Capture through SQLite's batch-atomic write interface.** ncruces compiles this in ([sqlite_opt.h#L25-L26][nc-opt]) and exposes it to a Go storage layer ([vfs/api.go#L143-L153][nc-batch]). Rejected for four reasons:

- **A failed upload can be reported as success.** An IOERR from the commit makes SQLite create a rollback journal and write the same pages directly ([pager.c#L6698-L6705][sq-ba-ioerr]). A plain Go error from the storage layer maps to an IOERR code ([vfs/vfs.go#L415-L437][nc-errcode]). In a probe, the caller saw success and the change was applied outside the upload.
- **It is skipped for three kinds of transaction.** The first write to an empty database does not use it ([pager.c#L1207-L1210][sq-ba-empty]). In a probe, a transaction larger than the page cache fell back to a journal and direct writes. Truncation, as in `VACUUM` or `auto_vacuum`, happens after the commit, outside the batch ([pager.c#L2193-L2202][sq-trunc]).
- **Other connections read stale data without a workaround.** With both atomic-write options compiled in, as ncruces does, SQLite skips the change-counter update for a batch commit ([pager.c#L6595-L6653][sq-ba-cc]). In a probe, a second connection kept returning old data until the storage layer synthesised the counter.
- **Readers wait out every upload.** Under rollback-journal locking, in a probe, a reader on another connection got "database is locked", or waited for the whole 300 ms upload.

The WAL route showed none of these four in the probes.

**Watching the rollback journal.** The storage layer would treat direct writes to the main file as private until the journal is deleted, then publish them. A probe recorded the call sequences only. What SQLite does when that final step fails was not probed. Pages spilled during a large transaction would need a local buffer whose size is bounded only by the transaction. Readers wait out uploads as with batch-atomic. Rejected.

**WAL with exclusive locking.** It needs no shared-memory index, but it allows one connection per database ([vfs/README.md#L69-L74][nc-walreadme]), so leader reads cannot run during a commit. Kept as the fallback in [decision 4](#4-commits-are-captured-from-sqlites-write-ahead-log).

**A manifest swap on every commit, with no log.** Each commit would be a compare-and-swap on one object. The manifest would have to name every page changed since the last compaction, or point to a second object that does, and a writer that loses the swap must re-read before trying again. haqlite and hadb work this way: an epoch check, then a version compare-and-swap on each manifest write ([store.rs#L14-L37][hadb-epoch]). A create-if-absent on a new name per commit gives the same fence with a smaller object and no read before the write. This is a design argument, not a measurement. Rejected.

**A storage-side lease as the fence.** Azure Blob can lease a blob, but the storage interface allows only create-if-absent and compare-and-swap, so a fence built on one backend's lease cannot be carried to the other three. Rejected.

**Prior-art systems.** None can be used as a library from pure Go. All are written in Rust. Each one contributed an idea:

- **Graft** writes one commit object per log position with an atomic create that fails if the object exists ([remote.rs#L190-L205][graft-put]). By default its clients commit locally first and push asynchronously ([consistency.mdx#L18][graft-async]), so a commit is acknowledged before object storage has it. skylite takes the create-if-absent commit log from Graft.
- **turbolite** keeps immutable page groups and makes the manifest write the atomic commit point ([README.md#L129][tl-manifest]). Its README warns that two writers on one prefix corrupt the manifest ([README.md#L212][tl-single]). Durability comes at checkpoint, when page groups are uploaded ([README.md#L351][tl-sync]). skylite takes the immutable groups and the manifest, not the commit model.
- **haqlite and hadb** create the lease with `If-None-Match: *` and renew it with `If-Match` ([lease_store.rs#L85-L93][hadb-create], [lease_store.rs#L147-L156][hadb-renew]), and check an epoch on each manifest write ([store.rs#L14-L37][hadb-epoch]). Only their opt-in Cloud mode uploads every commit before returning ([README.md#L81-L83][haq-modes]). skylite takes the lease protocol and the epoch on the manifest.
- **Rivet's storage engine** acknowledges a write once a quorum of storage replicas has written it to disk ([README.md#L1-L6][rivet-quorum]). It fences with client epochs, and a handle without quorum progress for thirty seconds is fenced ([README.md#L19-L22][rivet-epoch], [README.md#L38][rivet-fence]). An acknowledgement that waits for storage-node disks does not meet skylite's rule that only object storage holds durable state, so skylite takes only the epoch fence from Rivet.

## Consequences

- skylite owns the storage engine: the WAL parser, the commit format, the paged reader, compaction, garbage collection and takeover. These are the parts where a mistake loses data. The engine's own tests carry the weight that the spike's tests carried for Litestream.
- Every acknowledged write costs at least one object-storage round trip. This was already accepted in 0001. It has still not been measured against a real cloud.
- The engine depends on a project with one maintainer. The pinned version, the panic recovery and the storage layer boundary are what make replacing it possible.
- The gap in [decision 6](#6-compaction-and-garbage-collection), a commit name freed by garbage collection, has to be closed before garbage collection of the commit log is enabled.
- Orphaned objects from a lost compaction need a reclamation rule. None is decided.
- 0001's choice to embed Litestream is superseded. Its rejection of Litebase stands.
- In this pull request: the [architecture page](/docs/architecture/) and the [roadmap](/docs/roadmap/) now follow this decision.

## What the engine's tests must prove

The probes behind this record are exploratory code that is not in this repository. Nothing below may be relied on until a test in the engine proves it. Each test runs on emulators until real-cloud testing is funded, and says so.

**Commit capture and failure**

- A failed commit never reappears, after a crash or after every connection closes and the database reopens. This must cover every error the commit point can return, a failed create-if-absent, and a crash at each step of the truncation.
- No error from the commit point is turned into success. Every error code the storage layer can return must surface to the caller as an error.
- No connection sees a transaction before its upload has succeeded.
- `BUSY` inside an explicit `COMMIT` is either never returned or handled. A retried `COMMIT` must not upload the transaction twice, or under a second transaction number.
- The page-1 write made when a database first enters WAL mode reaches object storage, for a new database and for an existing one.
- The WAL header sync after each WAL restart is not taken for a commit.
- The commit-frame layout is checked on the storage layer the engine actually uses. The probes saw no sector padding because the WAL reported powersafe overwrite. That was not probed without it.
- The parser handles frames spilled before `COMMIT`, frame numbers reused after a rollback, dead frames past the commit frame, pages overwritten in place, and checksum-only rewrites after the commit frame.
- A process is killed at every step of a commit: before the upload, during it, after storage has accepted it but before the response, and after the response but before `COMMIT` returns. Every acknowledged write survives. A write that was not acknowledged either is absent or, if storage accepted it, appears whole.

**Reads**

- Main-file reads on the leader return the checkpointed version of each page, with a local cache that evicts pages, under concurrent commits and checkpoints.
- The size reported for the main file is the size of the checkpointed image.
- Followers pin each read transaction to one version, see the next version in their next transaction, and write their version number into the header's change counter, so that no follower returns stale cached pages.
- A reader whose base objects were deleted gets an error, never wrong data.

**Limits and size**

- The WAL grows under a long reader on the leader. The leader refuses writes at the configured bound and recovers once the reader ends.
- A transaction above the maximum size fails with a clear error and leaves nothing in storage.
- Databases above 1 GiB work. SQLite reserves the page that holds its lock bytes at the 1 GiB offset ([os.h#L147-L160][sq-pending]), and the reader, the commit log, compaction and garbage collection must all handle that page.
- Each connection's memory ceiling is set, and an out-of-memory panic discards only that connection.

**Fencing, takeover and garbage collection**

- Takeover time does not depend on database size. It is measured at more than one database size with the same log tail.
- After a takeover, a stale leader cannot get a commit acknowledged, cannot make its compaction reachable, and cannot delete an object that any later manifest references. This is tested with the stale leader paused before and after each of its storage requests.
- A stale leader paused for longer than the grace period cannot win a commit name that garbage collection has freed, or cannot acknowledge it if it does.
- An old leader that ignores the lease never breaks the history. Only the takeover's progress may suffer.

**Shared-memory index**

- If the shared-memory index is ever used across processes, cross-process use is tested on its own, including a process that dies holding its locks. The probes used several connections in one process only, on Linux only.

[ls-dbopen]: https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L1135-L1138
[ls-restore]: https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L901
[ls-index]: https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/vfs.go#L1362-L1364
[ls-vfs]: https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/vfs.go#L1-L28
[nc-readme]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/README.md#L1-L12
[nc-vfsapi]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/api.go#L16-L51
[nc-batch]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/api.go#L143-L153
[nc-nomem]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L493-L495
[nc-mem]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/wrap.go#L16-L35
[nc-errcode]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/vfs.go#L415-L437
[nc-walreadme]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/README.md#L69-L74
[nc-opt]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L1-L26
[sq-walframes]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L4281-L4346
[sq-recover]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L1534-L1556
[sq-restart]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L3961-L3995
[sq-busy1]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbeaux.c#L3438-L3446
[sq-busy2]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbe.c#L4197-L4202
[sq-jm]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbe.c#L8302-L8307
[sq-ver]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/btree.c#L11572-L11600
[sq-cache]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L5452-L5482
[sq-cc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L3272
[sq-ba-ioerr]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L6698-L6705
[sq-ba-empty]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L1207-L1210
[sq-trunc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L2193-L2202
[sq-ba-cc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L6595-L6653
[sq-pending]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/os.h#L147-L160
[mc-ro]: https://gitlab.com/cznic/sqlite/-/blob/v1.62.0/vfs/patches.go#L5-L25
[mc-unsafe]: https://gitlab.com/cznic/sqlite/-/blob/v1.62.0/vfs/patches.go#L115-L123
[graft-put]: https://github.com/orbitinghail/graft/blob/b07d9312dfdccc0c2de3c49fab890381d3436e0e/crates/graft/src/remote.rs#L190-L205
[graft-async]: https://github.com/orbitinghail/graft/blob/b07d9312dfdccc0c2de3c49fab890381d3436e0e/docs/src/content/docs/docs/concepts/consistency.mdx#L18
[tl-manifest]: https://github.com/russellromney/turbolite/blob/6a4f60741bbfd4243331bd837227fb264bdaa349/README.md#L129
[tl-single]: https://github.com/russellromney/turbolite/blob/6a4f60741bbfd4243331bd837227fb264bdaa349/README.md#L212
[tl-sync]: https://github.com/russellromney/turbolite/blob/6a4f60741bbfd4243331bd837227fb264bdaa349/README.md#L351
[hadb-create]: https://github.com/russellromney/hadb/blob/3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b/hadb-lease-s3/src/lease_store.rs#L85-L93
[hadb-renew]: https://github.com/russellromney/hadb/blob/3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b/hadb-lease-s3/src/lease_store.rs#L147-L156
[hadb-epoch]: https://github.com/russellromney/hadb/blob/3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b/turbodb/src/store.rs#L14-L37
[haq-modes]: https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/README.md#L81-L83
[rivet-quorum]: https://github.com/rivet-dev/rivet/blob/319e4d2940089bc6db42d2f99c6d83dcd33aaa3a/engine/packages/depot-volume/README.md#L1-L6
[rivet-epoch]: https://github.com/rivet-dev/rivet/blob/319e4d2940089bc6db42d2f99c6d83dcd33aaa3a/engine/packages/depot-volume/README.md#L19-L22
[rivet-fence]: https://github.com/rivet-dev/rivet/blob/319e4d2940089bc6db42d2f99c6d83dcd33aaa3a/engine/packages/depot-volume/README.md#L38
