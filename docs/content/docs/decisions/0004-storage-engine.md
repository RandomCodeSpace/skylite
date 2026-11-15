---
title: "0004: Build skylite's own storage engine"
description: skylite will build its own storage engine on ncruces/go-sqlite3, with a commit log, a paged base and a manifest in object storage, and will not use Litestream in the product.
weight: 4
---

**Status:** Accepted. It supersedes the choice of Litestream in [0001](/docs/decisions/0001-litebase-evaluation/).

**Date:** 2026-11-15

Nothing in this record exists yet. Every sentence about skylite is a decision, a plan or a requirement. The evidence comes from the [durability spike](/docs/decisions/0002-durability-spike/), which ran against the Azurite emulator only, and from exploratory probes run while preparing this record. The probes ran against in-memory models of object storage, not against an emulator or a cloud, and they are not in this repository. Where a statement rests only on a probe, it says so, and the behaviour is listed under [What the engine's tests must prove](#what-the-engines-tests-must-prove). Where a statement is an argument from the design that no test has checked, it is labelled a design argument.

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

An independent design review of an earlier draft of this record found three sequences in which a stale leader could get a write acknowledged that no reader would see. Decisions [4](#4-invariants), [6](#6-commit-outcomes-and-fail-stop) and [8](#8-compaction-and-garbage-collection) close them by design argument.

## Decision

### 1. skylite builds its own storage engine; Litestream is not used in the product

Four reasons, all from the evidence:

- **Litestream's writer needs a complete local copy of the database.** Its `DB` opens a SQLite database at a local path ([db.go#L1135-L1138][ls-dbopen]), and restore decodes a whole database into a local file ([replica.go#L901][ls-restore]). That conflicts with the requirement that pod disk is never needed for capacity.
- **Its on-demand reader indexes every page.** The reader builds a map with one entry per page number ([vfs.go#L1362-L1364][ls-index]), so its memory grows with the number of pages in the database. The reader also needs cgo: the file carries the `vfs` build tag and imports `psanford/sqlite3vfs` ([vfs.go#L1-L28][ls-vfs]).
- **The spike needed a guard of its own for each of three behaviours, and still left a window open** (0002 findings 2, 3, 4 and 7).
- **Files above the single-upload limit cannot be published atomically** by the spike's client (0002 finding 6).

The fencing rules are adopted from the spike. The spike code under `internal/spike/durability/` stays in the repository as evidence until the engine's own tests replace it.

### 2. SQLite engine: `github.com/ncruces/go-sqlite3`

`ncruces/go-sqlite3` is the one pure-Go engine found that can host a writable storage layer written in Go. It is a cgo-free wrapper that translates a Wasm build of SQLite to Go ([README.md#L1-L12][nc-readme]). It exposes SQLite's file interface as public Go interfaces that a storage layer implements ([vfs/api.go#L16-L51][nc-vfsapi]). The version read for this record is v0.37.0 (tag commit `2d9b687b`). Its `go.mod` requires `go-sqlite3-wasm/v6` v6.5.35401 ([go.mod#L6][nc-gomod]), whose build script downloads the SQLite 3.54.1 amalgamation, `sqlite-autoconf-3540100` ([build/download.sh#L8][ncw-download]).

The risks are accepted, and stated plainly:

- **Commits are concentrated in one account.** Of the 100 commits up to tag v0.37.0, 87 are by the project's owner, 10 by Dependabot and 3 by three other accounts ([commit history at v0.37.0][nc-commits]). On 2026-11-15, GitHub's contributor list counted 1,098 commits for the owner and at most 5 for any other person. That is an observation of past activity, not a statement about the project's future.
- **It panics on out-of-memory** instead of returning an error ([conn.go#L493-L495][nc-nomem]).
- **Each connection has its own memory ceiling**: 256 MiB by default on 64-bit platforms and 32 MiB on 32-bit platforms, which `WithMaxMemory` can change ([wrap.go#L16-L35][nc-mem]).

These mitigations become requirements:

- Pin the engine version, and upgrade only through a pull request that runs the engine's tests.
- Recover from a panic at the connection boundary and discard that connection.
- Configure the per-connection memory ceiling explicitly.
- Keep the engine behind skylite's own storage layer, so it can be replaced.

### 3. Durable layout in object storage, per database

The names below are illustrative. The format package decides the exact keys and encodings.

- **Commit log.** One object per committed transaction, named by a fixed-width transaction number and written with create-if-absent. It holds the changed pages, the database size after the commit, the leader's epoch, the database identity and a checksum. An epoch-bump commit holds no pages. That single create is the commit point, the acknowledgement point and the fence for commits.
- **Base.** The database's pages, in fixed-size groups of consecutive page numbers. Each group is an immutable object under a name that is never reused, and carries the database identity. A page's group follows from arithmetic on its page number, so the index needs memory in proportion to the number of groups, not the number of pages.
- **Manifest.** One object, updated only by compare-and-swap. It holds the database identity, the base objects, the transaction number the base is current to, the epoch, and the list of epoch-bump commits ([decision 8](#8-compaction-and-garbage-collection)). It is the atomic fence for everything that is not a commit.
- **Lease.** An object updated by compare-and-swap, used only so that a takeover makes progress.

Every object must fit in one upload request on every backend, because 0002 finding 6 rules out multi-part uploads for objects that fence. The format package sets the group size and the maximum commit size with that in mind.

**The manifest has a maximum size.** The format sets it, below the single-upload limit of every backend. Compaction refuses to produce a manifest above it. The database then stops compacting and reports the condition, and the leader refuses writes once its uncompacted log reaches its own bound. This fails closed. A manifest format that scales further is later work. The arithmetic, with illustrative sizes that the format will fix: a group entry of 48 bytes (a 32-byte name and a 16-byte checksum) with groups of 4 MiB gives 23,842 entries for a 100 GB database, about 1.1 MiB. Each epoch-bump commit adds 8 bytes to the list, so 10,000 of them add 80,000 bytes.

### 4. Invariants

These are requirements on the engine and the format. Every other decision in this record assumes them.

1. **One number sequence, one epoch per candidacy.** Transaction numbers are one sequence across all epochs, and an epoch-bump commit consumes a number. An epoch is assigned only by a successful compare-and-swap of the manifest to the previous epoch plus one, so each epoch belongs to one candidacy and is never reused. Arithmetic on transaction numbers and epochs never wraps: reaching the maximum value stops the database, fail closed.
2. **Database identity.** Every database has an identity, created once when the database is created and recorded in the manifest and in every commit and base object. A reader rejects any object whose identity differs from the manifest's. A database recreated under the same prefix gets a new identity.
3. **The head is found by walking.** The head of the log is the last transaction number reached by walking contiguous numbers upward from the manifest's. Readers, followers, candidates and the compactor all reject a gap, a failed checksum, a wrong identity or an undecodable object among the records they need. They never skip a record, never take the highest listed key as the head, and never apply a commit without every commit before it. A name that is absent because a newer manifest has folded it is covered by rule 4, not treated as the end of the log. Epochs along the log are not necessarily non-decreasing ([decision 9](#9-takeover)), and no reader may require that they are.
4. **No mixing after a delete.** When an object a reader needs has been deleted because the reader's manifest is old, the read transaction fails, or restarts from a newly read manifest. It never substitutes newer data into a transaction that has already read older data.
5. **Three client outcomes.** A client is told exactly one of committed, not committed or unknown ([decision 6](#6-commit-outcomes-and-fail-stop)).

### 5. Commits are captured from SQLite's write-ahead log

On the leader, SQLite runs in WAL mode with a file-backed shared-memory index. The WAL and that index are ordinary local files. The main database file is virtual: the storage layer serves it from the local cache and from object storage.

- **Capture point.** With `synchronous=FULL`, the probes saw SQLite sync the WAL exactly once per commit, after the commit frame and before `COMMIT` returned. The storage layer builds the commit object from the WAL frames at that sync and uploads it. In SQLite's source the sync comes before the new frames are added to the shared index and before the index header is published ([wal.c#L4281-L4346][sq-walframes]). `synchronous=FULL` must be set explicitly, because ncruces compiles SQLite with the WAL default set to `NORMAL` ([sqlite_opt.h#L5][nc-opt]). Under `NORMAL`, the probes saw no sync at commit.
- **No SQL can bypass capture.** No client SQL may turn off `synchronous=FULL`, change the journal mode or the locking mode, or otherwise route a write around capture. The SQL lockdown enforces this.
- **Definite failure.** If the upload fails with an answer that proves the commit object was not stored, the storage layer truncates the WAL back to the end of the last uploaded commit, inside the failing call, and returns an error. SQLite then rolls back. In the probes, IOERR, IOERR_FSYNC, a plain Go error and FULL from the sync all produced an error to the caller and a rollback, and none made SQLite report success. An upload whose outcome is not definite is handled by [decision 6](#6-commit-outcomes-and-fail-stop).
- **Why the truncation is required.** After a failed sync, the frames and the commit marker are still in the WAL with valid checksums. SQLite's recovery accepts every valid frame up to the last commit marker ([wal.c#L1534-L1556][sq-recover]). In the probes, a failed transaction reappeared after a reopen whenever the WAL was kept. With the truncation it did not.
- **Visibility.** Other connections see a transaction only after its upload succeeded. In the probes, readers on other connections saw only the old value while an upload was blocked for 300 ms, and never saw a failed commit.
- **Checkpoints.** A local WAL checkpoint writes only to the local cache, because its pages are already in object storage. In the probes every page a checkpoint wrote was byte-identical to a page already uploaded. The one exception is listed below. How the leader reads the main file around checkpoints is in [decision 7](#7-reads).
- **Start-up.** Local files are wiped when a node starts. They are never a source of truth.

Two behaviours seen in the probes qualify the description above. The engine must handle both:

- **`BUSY` does not roll back an explicit `COMMIT`.** If the commit point returns `BUSY` inside an explicit transaction, `COMMIT` fails with "database is locked" and the transaction stays open with the write lock held. A retried `COMMIT` writes the frames again ([vdbeaux.c#L3438-L3446][sq-busy1], [vdbe.c#L4197-L4202][sq-busy2]). The decision: the storage layer never returns `BUSY` from the commit point. Every failure there is reported with an error code that the probes saw roll the transaction back.
- **Entering WAL mode writes page 1 outside the WAL.** `PRAGMA journal_mode=wal` changes page 1 through a rollback-journal transaction, whatever the journal mode ([vdbe.c#L8302-L8307][sq-jm], [btree.c#L11572-L11600][sq-ver]). That write never passes through WAL capture. On a new database the probes saw the first WAL commit send page 1 again, so storage ended up correct, by accident. The decision: the engine does not create a database through WAL capture. It builds the first commit object itself, holding an empty database whose header already says WAL mode, so that opening it never needs that write. The database identity ([invariant 2](#4-invariants)) is created at the same step.

WAL mode with exclusive locking, which allows one connection and needs no shared-memory index ([vfs/README.md#L69-L74][nc-walreadme]), is the recorded fallback if the shared-memory route fails the engine's tests.

### 6. Commit outcomes and fail-stop

A storage request can fail without saying whether it took effect: a timeout, a dropped connection or a server error after the request was sent. The storage interface must classify every failure of a create as either definite (the object was not stored) or not definite. When a backend's answer is not documented as definite, it is not definite.

**A transaction number is bound to one object.** For the life of the leader process, each transaction number the leader tries is bound to the exact bytes of the one commit object the leader built for it. The leader never builds a second, different object for a number it has already tried to create.

**Resolving a create.** After a create whose outcome is not definite, the leader repeats the identical create until it gets a definite answer, up to a bound the engine sets. The definite answers are:

- **Success.** The commit is committed. This holds even if the name was freed between the attempts. The design argument: a name below the manifest's transaction number is freed only after compaction folded the first object created at it, and that first object was this leader's own earlier attempt, because a bump at that name would never be deleted ([decision 8](#8-compaction-and-garbage-collection)) and no other leader creates an ordinary commit at the name after this leader's last commit. So the earlier attempt is already part of the history, and the object the retry created is unreferenced garbage below the base.
- **A failed precondition, then a read of the name that returns identical bytes.** Committed.
- **A failed precondition, then a read of the name that returns different bytes.** Another writer holds the name, and the leader is fenced for good. On a first attempt, this means not committed. After an attempt whose outcome was not definite, it means not committed only if a manifest read made after the read of the name shows a transaction number below the name, or lists the name as an epoch bump. Otherwise the outcome is unknown. The reason: if the leader's earlier attempt landed, compaction folded it and garbage collection freed the name, a candidate paused since before then can still create a bump there ([decision 8](#8-compaction-and-garbage-collection)). The leader then reads different bytes although its commit is in the history. That needs a manifest at or past the name that does not list it. This is a design argument.

Anything else is an **indeterminate** commit: no definite answer within the bound, or a failed precondition followed by a read that finds nothing.

**Fail-stop.** On an indeterminate commit, and whenever the leader is fenced, the node stops serving that database, discards all local state for it, and can come back only through the takeover path, which reads durable history. The same fail-stop applies in two more cases, because storage may hold a commit that the local image does not reflect, or the local image may hold one that storage does not:

- The storage layer cannot truncate its local WAL after a definite upload failure.
- SQLite reports an error after the upload has succeeded, for example while publishing to the shared-memory index or in the second phase of commit.

A candidate that cannot get a definite answer for its own epoch-bump create gives up the candidacy and never serves. Its bump, if it landed, is treated like that of any candidate that crashed.

**Client outcomes.** A client is told exactly one of:

- **Committed**, when storage confirmed the create, or a read returned identical bytes.
- **Not committed**, only when the commit object is known not to be in the durable history.
- **Unknown**, in every other case. A client is never told "not committed" for an indeterminate commit.

Unknown is a permitted final answer. The exception: when the client supplied a commit identifier, which the commit object carries, and that commit object is still retained, the engine must be able to answer committed for it. Once compaction and garbage collection have removed the object, unknown stands.

**Late acknowledgements are correct.** A leader whose commit reached storage may deliver the acknowledgement after another leader has taken office. That is correct, because the commit is in the history. Two processes may both believe they hold office at once. The design does not prevent that belief. It prevents conflicting history: no leader gets a transaction acknowledged that is not in the accepted history.

### 7. Reads

- **Lookup.** A page is looked up first in the commit log written since the manifest's transaction number, through a small in-memory index that compaction keeps bounded, and then in its base group. Fetched data goes into a bounded local cache.
- **Followers.** Followers use a read-only storage layer. It pins each read transaction to one version and moves to the newest version between transactions. In a probe, a follower that did not pin mixed two versions inside one scan.

**Leader reads of the main file.** SQLite expects the main file to hold only checkpointed state. In a probe, serving the newest uploaded version from the main file let a reader that began before a commit see that commit's data, which breaks snapshot isolation. The mechanism below is a design argument that the engine's tests must prove:

- The storage layer keeps a **base version**: the last commit at which the local WAL was empty and the main file was exactly a committed state. At takeover it is the head. Afterwards it is the last commit before each point where SQLite restarts the WAL from its beginning.
- In SQLite 3.54.1, the WAL restarts from its beginning only when every frame has been backfilled and no reader is using the log. A writer restarts it only if its own read used no WAL frames, the backfill count equals the last frame, and it gets an exclusive lock on every reader slot that reads the WAL ([wal.c#L3961-L3995][sq-restart]). A `RESTART` or `TRUNCATE` checkpoint restarts it only after the whole WAL is backfilled and it gets the same locks ([wal.c#L2435-L2467][sq-ckpt-restart]). Closing the last connection deletes the WAL only after taking an exclusive lock on the database and completing a checkpoint ([wal.c#L2610-L2629][sq-close]).
- Pages that checkpoints write to the main file after the base version, and the main file's size after them, are kept locally, and are not evicted until the base version next advances.
- A main-file read of a page not held locally fetches that page as of the base version. A page not held locally has not been written by a checkpoint since then.
- A checkpoint's completion callback is not treated as proof that the checkpoint completed. Only a WAL restart advances the base version.

**What a follower's storage layer presents.** SQLite decides whether to drop its page cache by comparing bytes 24 to 39 of the file header when it takes a shared lock ([pager.c#L5452-L5482][sq-cache]). In WAL mode the change counter there moves only when page 1 is part of a commit ([pager.c#L3272][sq-cc]). In a probe that did not change those bytes, the follower returned stale cached pages. The follower's storage layer therefore presents page 1 with three fields changed:

- **Bytes 18 and 19**, the file format write and read versions, set to 1. The database then reads as a rollback-journal database, and SQLite does not open a WAL for it, which it does when byte 19 is 2 ([btree.c#L3363-L3386][sq-walver]).
- **Bytes 24 to 27**, the file change counter.
- **Bytes 92 to 95**, the version-valid-for number, set equal to the change counter. SQLite trusts the database size in bytes 28 to 31 only when those two fields are equal ([btree.c#L3328-L3332][sq-hdrsize]).

The change counter is 32 bits. The decision: it is a per-connection counter that the storage layer increments whenever the version pinned for that connection changes, and the connection is closed and reopened before the counter can wrap. The probe wrote the version number itself into the counter; the per-connection counter has not been probed.

### 8. Compaction and garbage collection

- **Compaction.** The leader folds the commit log into new base group objects, under new names, and then swaps the manifest by compare-and-swap. A stale leader's compaction output stays unreachable unless its swap wins. A takeover changes the manifest first, so the stale leader's swap cannot win.
- **Deletes.** An object is deleted only when a manifest read from storage no longer references it and a grace period has passed. Every node is configured to re-read the manifest at an interval shorter than the grace period. That is configuration, not a guarantee: a paused node cannot promise to re-read anything on time.
- **Why a stale leader's deletes cannot remove live data.** Names are never reused and manifests only move forward. So an object that is garbage under one committed manifest is garbage under every later one, and a stale leader working from an old manifest only deletes objects that are already garbage. This also requires that a manifest never references an object that an earlier manifest dropped.
- **The grace period protects readers, not safety.** It only makes it likely that a reader still using an older manifest finishes first. A reader that does not finish in time is handled by [invariant 4](#4-invariants). It rests on timing, so it must not be part of any safety argument.

This closes, by construction, the window that [0002](/docs/decisions/0002-durability-spike/) measured in finding 7. That is a design argument. No test has shown it yet.

**Garbage collection must not reopen the commit fence.** The commit fence relies on the name of the next transaction number being taken. Deleting commit log objects once the base covers them frees their names again. Without further rules, a stale leader paused for longer than the grace period could win a create-if-absent at a transaction number that is already folded into the base, and acknowledge a write that no reader will ever see. The rules below close this without relying on time. They are design arguments. No test has shown them yet.

**Compaction records every epoch bump.** A takeover has no step that registers its bump. Compaction reads every commit object it folds, so the compaction that advances the manifest from transaction number T to T' writes into the new manifest the transaction number of every epoch-bump commit it found between them, added to the list it inherited. Compaction only reads names above the manifest's number, and those have never been freed, so what it reads at each name is the first object ever created there. By induction, the manifest lists every epoch-bump commit that was the first object at its name, at or below the manifest's own transaction number. That includes bumps from candidates that crashed, lost or never took office. The cost is one bump object and 8 bytes of manifest for every epoch-bump commit created, kept for the life of the database.

**What garbage collection may delete from the commit log.** Garbage collection deletes a commit log object only if its transaction number is at or below the manifest's and is not in the manifest's list of bumps. Objects above the manifest's transaction number are never deleted. So every bump that was the first object at its name is retained, either because it lies in the undeleted tail or because the manifest lists it. A bump that a candidate created at a freed name, and that then failed its final check ([decision 9](#9-takeover)), lies below the manifest's number and is not in the list. It is ordinary garbage.

**A deposed leader's next create always meets a retained object.** The argument, which is a design argument, rests on two facts:

- A leader creates an ordinary commit at a name only after holding the name before it, as its own commit or its own epoch bump. So the first object ever created at the name after a leader's own last commit is either that leader's own or an epoch bump.
- By [decision 6](#6-commit-outcomes-and-fail-stop), a leader's next create is either the identical retry of its unresolved create or the name after its last committed transaction.

Case by case, for a leader A that has been deposed:

- **No unresolved create.** A's next name is the one after its last commit. A takeover put a bump there, or an earlier candidate did and the takeover's bump follows it. Either way the first object there is a bump. It is retained, so A meets different bytes and is fenced. Closes.
- **An unresolved create that landed.** A's retry finds identical bytes, or succeeds at a name already folded and freed, and in both cases the commit is in the history and correctly reported as committed. A's next name is then covered by the first case. Closes.
- **An unresolved create that did not land.** The first object at that name is a bump, which is retained. A's retry fails its precondition and its read finds different bytes. The manifest either is below the name or lists it, so A reports not committed and is fenced. Closes.
- **Deposed several takeovers ago.** Later takeovers put their bumps at higher names. The bump at A's next name is the first takeover's, or an earlier candidate's, and the list never drops it. Closes.
- **A bump left by a candidate that crashed before serving.** Its bump is the first object at its name, so it is retained: compaction lists it when it folds that name. Closes.

One sequence does not end in a fence with a definite answer, and the rules make it safe instead. A's create landed with its response lost; a later leader folded it and garbage collection freed the name; a candidate paused since before then created a bump there and failed its final check. A's retry then reads different bytes, or nothing if that garbage bump was already deleted. A is fenced or indeterminate, and in both cases reports unknown ([decision 6](#6-commit-outcomes-and-fail-stop)), which is correct: its commit is in the history.

**Orphaned base objects.** Output from a compaction whose swap lost is never referenced by any manifest, so the delete rule above never reaches it. A base object's name carries the epoch of its writer. The leader may delete a base object that the current manifest does not reference when that epoch is lower than the manifest's epoch, because the writer of a lower epoch can no longer swap the manifest; and it may delete its own unreferenced output once the compaction that wrote it has ended. It never deletes an unreferenced object carrying an epoch higher than the manifest it read.

### 9. Takeover

1. A candidate takes the lease.
2. It swaps the manifest to the manifest's epoch plus one.
3. It reads the tail of the commit log, walking from the manifest's transaction number ([invariant 3](#4-invariants)).
4. It wins a create-if-absent for an epoch-bump commit at the next transaction number.
5. It reads the manifest again and takes office only if the manifest still carries its epoch.

The final check, step 5, is a design argument. From the moment of its swap, no older leader can change the manifest, so the base cannot have moved past the candidate's view and no name after it can have been freed, unless a newer candidate has swapped the manifest since, which the re-read detects. A candidate that fails the check stops and never acknowledges a write.

The old leader's next commit meets a retained bump and it stops for good ([decision 8](#8-compaction-and-garbage-collection)). If the old leader commits first, the candidate loses the create, reads the tail again and retries. The takeover involves no download of database content. Its time is not independent of database size, because the manifest it reads grows with the number of base groups, and it also depends on the length of the log tail. No safety argument rests on clocks or on the lease. An old leader that ignores the lease can delay a takeover, as 0002 finding 8 observed, but cannot break the history.

**A paused candidate can fence the live leader.** This is a liveness cost, not a safety problem. A candidate that swapped the manifest, was paused, and resumes after another candidate has taken office can still win the create for its bump at the live leader's next transaction number. Its final check then fails and it never serves. But the live leader finds different bytes at its next name and is fenced, so another takeover is needed. The paused candidate's epoch is lower than the epoch of the commits before its bump, so epochs along the log are not necessarily non-decreasing ([invariant 3](#4-invariants)).

### 10. Limits accepted in the first version

- **One upload per commit.** A leader can commit at most one transaction per storage round trip. Group commit is a possible later change, not a promise.
- **Maximum transaction size.** A transaction must fit in one commit object. A larger one fails with a clear error.
- **Maximum manifest size.** A database whose manifest would exceed it stops compacting ([decision 3](#3-durable-layout-in-object-storage-per-database)).
- **Long readers hold back checkpoints.** In the probes, an open read transaction on the leader stopped checkpoints from backfilling the WAL, and the WAL kept growing. SQLite restarts the WAL only once it is fully backfilled and no reader is using it ([wal.c#L3961-L3995][sq-restart]). Readers on the leader therefore need time limits, and the leader refuses writes once its local WAL grows past a bound.
- **Follower lag.** Followers poll at a configured interval. Their lag is not bounded by that interval: under delays or failures a follower can fall behind by any amount. A client that needs to read its own write presents the transaction number it was given.

## Alternatives considered

**Litestream as a library.** This is what the spike ran. Rejected for the four reasons in [decision 1](#1-skylite-builds-its-own-storage-engine-litestream-is-not-used-in-the-product).

**`modernc.org/sqlite`.** Its public VFS package exposes an `fs.FS` read-only: "it has no write path and refuses to create a journal" ([vfs/patches.go#L5-L25][mc-ro]). A writable Go storage layer would have to wire SQLite's method table by hand through `unsafe` pointer writes, the way that package does for its own read-only methods ([vfs/patches.go#L115-L123][mc-unsafe]). That is not a supported interface. In a probe, modernc's compile options did not include batch-atomic writes. Rejected.

**Capture through SQLite's batch-atomic write interface.** ncruces compiles this in ([sqlite_opt.h#L25-L26][nc-opt]) and exposes it to a Go storage layer ([vfs/api.go#L143-L153][nc-batch]). Rejected for four reasons:

- **A failed upload can be reported as success.** An IOERR from the commit makes SQLite create a rollback journal and write the same pages directly ([pager.c#L6698-L6705][sq-ba-ioerr]). A plain Go error from the storage layer maps to an IOERR code ([vfs/vfs.go#L415-L437][nc-errcode]). In a probe, the caller saw success and the change was applied outside the upload.
- **It is skipped for three kinds of transaction.** The first write to an empty database does not use it ([pager.c#L1207-L1210][sq-ba-empty]). In a probe, a transaction larger than the page cache fell back to a journal and direct writes. Truncation, as in `VACUUM` or `auto_vacuum`, happens after the commit, outside the batch ([pager.c#L2193-L2202][sq-trunc]).
- **Other connections read stale data without a workaround.** With both atomic-write options compiled in, as ncruces does, SQLite skips the change-counter update for a batch commit ([pager.c#L6595-L6653][sq-ba-cc]). In a probe, a second connection kept returning old data until the storage layer synthesised the counter.
- **Readers wait out every upload.** Under rollback-journal locking, in a probe, a reader on another connection got "database is locked", or waited for the whole 300 ms upload.

The WAL route showed none of these four in the probes.

**Watching the rollback journal.** The storage layer would treat direct writes to the main file as private until the journal is deleted, then publish them. A probe recorded the call sequences only. What SQLite does when that final step fails was not probed. Pages spilled during a large transaction would need a local buffer whose size is bounded only by the transaction. Readers wait out uploads as with batch-atomic. Rejected.

**WAL with exclusive locking.** It needs no shared-memory index, but it allows one connection per database ([vfs/README.md#L69-L74][nc-walreadme]), so leader reads cannot run during a commit. Kept as the fallback in [decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log).

**A compare-and-swap on one head object for every commit.** In its strongest form, a commit uploads the transaction's pages under a name that is never reused, then swaps one small head object by compare-and-swap to point at it, and is acknowledged on that swap. haqlite and hadb use a version compare-and-swap with an epoch check on each manifest write ([store.rs#L14-L37][hadb-epoch]). This makes one object the fence for commits and compaction alike, so there is no commit log name to free, no epoch bump to retain and no list of bumps in the manifest. Its safety argument is shorter than the log's. Rejected for two costs:

- **Two storage mutations in sequence on every commit.** The page upload must finish before the swap starts, so every acknowledged write waits for two storage round trips instead of one.
- **A write-rate limit on one object name.** Google Cloud Storage documents a "Maximum rate of writes to the same object name" of "One write per second", and notes that "Writing to the same object name at a rate above the limit might result in throttling errors" ([Cloud Storage quotas and limits][gcs-quota], read on 2026-11-15). A single head object written on every commit would hold a database on that backend near one commit per second. The commit log writes each name once.

The log keeps one storage mutation per commit, at the cost of the rules in [decision 8](#8-compaction-and-garbage-collection). That trade is a design argument, not a measurement.

**A storage-side lease as the fence.** Azure Blob can lease a blob, but the storage interface allows only create-if-absent and compare-and-swap, so a fence built on one backend's lease cannot be carried to the other three. Rejected.

**Prior-art systems.** None can be used as a library from pure Go. All are written in Rust. Each one contributed an idea:

- **Graft** writes one commit object per log position with an atomic create that fails if the object exists ([remote.rs#L190-L205][graft-put]). By default its clients commit locally first and push asynchronously ([consistency.mdx#L18][graft-async]), so a commit is acknowledged before object storage has it. skylite takes the create-if-absent commit log from Graft.
- **turbolite** keeps immutable page groups and makes the manifest write the atomic commit point ([README.md#L129][tl-manifest]). Its README warns that two writers on one prefix corrupt the manifest ([README.md#L212][tl-single]). Durability comes at checkpoint, when page groups are uploaded ([README.md#L351][tl-sync]). skylite takes the immutable groups and the manifest, not the commit model.
- **haqlite and hadb** create the lease with `If-None-Match: *` and renew it with `If-Match` ([lease_store.rs#L85-L93][hadb-create], [lease_store.rs#L147-L156][hadb-renew]), and check an epoch on each manifest write ([store.rs#L14-L37][hadb-epoch]). Only their opt-in Cloud mode uploads every commit before returning ([README.md#L81-L83][haq-modes]). skylite takes the lease protocol and the epoch on the manifest.
- **Rivet's storage engine** acknowledges a write once a quorum of storage replicas has written it to disk ([README.md#L1-L6][rivet-quorum]). It fences with client epochs, and a handle without quorum progress for thirty seconds is fenced ([README.md#L19-L22][rivet-epoch], [README.md#L38][rivet-fence]). An acknowledgement that waits for storage-node disks does not meet skylite's rule that only object storage holds durable state, so skylite takes only the epoch fence from Rivet.

## Consequences

- skylite owns the storage engine: the WAL parser, the commit format, the paged reader, compaction, garbage collection and takeover. These are the parts where a mistake loses data. The engine's own tests carry the weight that the spike's tests carried for Litestream.
- Every acknowledged write costs at least one object-storage round trip. This was already accepted in 0001. It has still not been measured against a real cloud.
- Some commits end with the answer unknown, and the node that gave it stops serving the database until a takeover. Clients must handle unknown.
- The engine depends on a project whose commits are concentrated in one account. The pinned version, the panic recovery and the storage layer boundary are what make replacing it possible.
- Every epoch-bump commit created, including those of candidates that never took office, leaves one commit object and 8 bytes in the manifest that are kept for the life of the database ([decision 8](#8-compaction-and-garbage-collection)).
- A paused candidate can force an extra takeover ([decision 9](#9-takeover)).
- Garbage collection of the commit log must not be enabled before the tests for the rules in decision 8 exist and pass.
- 0001's choice to embed Litestream is superseded. Its rejection of Litebase stands.
- In this pull request: the [architecture page](/docs/architecture/) and the [roadmap](/docs/roadmap/) now follow this decision.

## What the engine's tests must prove

The probes behind this record are exploratory code that is not in this repository. Nothing below may be relied on until a test in the engine proves it. Each test runs on emulators until real-cloud testing is funded, and says so.

**Commit capture and failure**

- A commit known not to have entered the durable history never reappears, after a crash or after every connection closes and the database reopens. This must cover every error the commit point can return, a failed create-if-absent, and a crash at each step of the truncation.
- No error from the commit point is turned into success. Every error code the storage layer can return must surface to the caller as an error.
- No connection sees a transaction before its upload has succeeded.
- The commit point never returns `BUSY`, whatever fails inside it. No failed `COMMIT` leaves a transaction open. No transaction is committed under two transaction numbers, and no transaction number is ever given a second, different commit object.
- A database created by the engine opens in WAL mode with no write to page 1 outside the WAL, and a database already in object storage opens the same way. A main-file write that is not part of a checkpoint is an error, not a silent local change.
- No client SQL can turn off `synchronous=FULL`, change the journal mode or the locking mode, or otherwise write around capture.
- The WAL header sync after each WAL restart is not taken for a commit.
- The commit-frame layout is checked on the storage layer the engine actually uses. The probes saw no sector padding because the WAL reported powersafe overwrite. That was not probed without it.
- The parser handles frames spilled before `COMMIT`, frame numbers reused after a rollback, dead frames past the commit frame, pages overwritten in place, and checksum-only rewrites after the commit frame.
- A process is killed at every step of a commit: before the upload, during it, after storage has accepted it but before the response, and after the response but before `COMMIT` returns. Every acknowledged write survives. A commit that storage accepted but that was never acknowledged may appear in the history, and if it does, it appears whole.
- The local WAL is on a real disk, and tests inject short writes, a full disk, and a failed truncate or fsync.
- A real process crash, not a simulated one, leaves the shared-memory file in place, and the restarted node still wipes and ignores it.
- Automatic checkpointing runs while commits are being captured.

**Commit outcomes**

- A create whose response is lost, followed by a pause long enough for compaction and garbage collection to free its name, is retried with identical bytes. A retry that succeeds is reported committed, and the history holds the commit once.
- A retry that meets a failed precondition is resolved by reading the name: identical bytes report committed; different bytes fence the leader, and report not committed only when the manifest rule in decision 6 allows it, and unknown otherwise; nothing found is indeterminate.
- An indeterminate commit, a failed WAL truncation and an error from SQLite after a successful upload each stop the node for that database and discard its local state. The node serves again only through takeover.
- A client is told exactly one of committed, not committed or unknown, and never not committed for a commit that is in the history. A commit identifier resolves to committed while its commit object is retained.
- A candidate whose bump create has no definite answer never serves.

**Invariants**

- Readers, followers, candidates and the compactor reject a gap, a failed checksum, a wrong database identity and an undecodable object, and never apply a later commit without every earlier one.
- A database deleted and recreated under the same prefix gets a new identity, and objects of the old one are rejected.
- A read transaction whose objects are deleted under it fails or restarts from a new manifest, and never returns a mix of old and new data.
- Transaction numbers and epochs at their maximum value stop the database instead of wrapping.

**Reads**

- Main-file reads on the leader return each page as of the base version, or as written locally by a checkpoint since then, with a local cache that evicts pages, under concurrent commits and checkpoints.
- The size reported for the main file is the size of the checkpointed image.
- A checkpoint held to a partial backfill by a reader, and a checkpoint whose page write, truncate or sync fails after some pages were written, each leave the leader reading correct data, including after eviction.
- Automatic checkpointing overlapping commit capture leaves the base version and the locally held pages correct.
- Followers pin each read transaction to one version and see the next version in their next transaction. Their storage layer presents the header fields in decision 7, so that no follower returns stale cached pages.
- Followers handle prepared statements cached across versions, schema changes and `VACUUM` on the leader, and a change counter about to wrap, which closes and reopens the connection instead of presenting a counter value already used.
- A reader whose base objects were deleted gets an error, never wrong data.

**Limits and size**

- The WAL grows under a long reader on the leader. The leader refuses writes at the configured bound and recovers once the reader ends.
- A transaction above the maximum size fails with a clear error and leaves nothing in storage.
- A compaction that would produce a manifest above the maximum size is refused, and the database reports the condition.
- Databases above 1 GiB work. SQLite reserves the page that holds its lock bytes at the 1 GiB offset ([os.h#L147-L160][sq-pending]), and the reader, the commit log, compaction and garbage collection must all handle that page.
- Each connection's memory ceiling is set, and an out-of-memory panic discards only that connection.

**Fencing, takeover and garbage collection**

- A takeover downloads no database content: at more than one database size, with the same log tail, it reads only the lease, the manifest and the log tail.
- After a takeover, a stale leader cannot get acknowledged a transaction that is not in the accepted history, cannot make its compaction reachable, and cannot delete an object that any later manifest references. This is tested with the stale leader paused before and after each of its storage requests.
- A stale leader paused until garbage collection has removed every commit log object it could delete still cannot get a transaction acknowledged that is not in the history: its next create meets a retained epoch-bump commit. This is tested for each case in decision 8: no unresolved create, an unresolved create that landed, one that did not land, a leader deposed several takeovers ago, and a bump left by a candidate that crashed before serving.
- Compaction lists every epoch-bump commit it folds, including those of crashed and failed candidates, and garbage collection never deletes a listed bump or any object above the manifest's transaction number. A bump created at a freed name by a candidate that failed its final check is deleted as garbage.
- A candidate paused at each step of a takeover, for long enough that the history has moved on and been garbage-collected, never acknowledges a write.
- A paused candidate that wins its bump at the live leader's next name fences that leader, never serves, and a further takeover restores service.
- An unreferenced base object is deleted only under the orphan rule in decision 8, and never while a compaction that could still reference it is running.
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
[nc-gomod]: https://github.com/ncruces/go-sqlite3/blob/v0.37.0/go.mod#L6
[nc-commits]: https://github.com/ncruces/go-sqlite3/commits/v0.37.0
[nc-opt]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L1-L26
[ncw-download]: https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/download.sh#L8
[sq-walframes]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L4281-L4346
[sq-recover]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L1534-L1556
[sq-restart]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L3961-L3995
[sq-ckpt-restart]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L2435-L2467
[sq-close]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L2610-L2629
[sq-busy1]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbeaux.c#L3438-L3446
[sq-busy2]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbe.c#L4197-L4202
[sq-jm]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbe.c#L8302-L8307
[sq-ver]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/btree.c#L11572-L11600
[sq-hdrsize]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/btree.c#L3328-L3332
[sq-walver]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/btree.c#L3363-L3386
[sq-cache]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L5452-L5482
[sq-cc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L3272
[sq-ba-ioerr]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L6698-L6705
[sq-ba-empty]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L1207-L1210
[sq-trunc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L2193-L2202
[sq-ba-cc]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L6595-L6653
[sq-pending]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/os.h#L147-L160
[mc-ro]: https://gitlab.com/cznic/sqlite/-/blob/v1.62.0/vfs/patches.go#L5-L25
[mc-unsafe]: https://gitlab.com/cznic/sqlite/-/blob/v1.62.0/vfs/patches.go#L115-L123
[gcs-quota]: https://docs.cloud.google.com/storage/quotas
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
