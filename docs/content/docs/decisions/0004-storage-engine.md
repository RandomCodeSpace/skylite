---
title: "0004: Build skylite's own storage engine"
description: "skylite will build its own storage engine on ncruces/go-sqlite3, with a commit log whose names are never freed, a paged base and a manifest in object storage, and will not use Litestream in the product."
weight: 4
---

**Status:** Accepted on condition. It supersedes the choice of Litestream in [0001](/docs/decisions/0001-litebase-evaluation/).

The condition: the SQLite-side mechanisms in decisions [5](#5-commits-are-captured-from-sqlites-write-ahead-log) and [7](#7-reads) (capture from the write-ahead log, the failure-and-truncate path, reader visibility during an upload, checkpoint and main-file coherence, bootstrap, follower invalidation, and failures after publication) rest on exploratory probes, not on tests in this repository. The first engine pull requests must land the tests in [What the engine's tests must prove](#what-the-engines-tests-must-prove) for those mechanisms, against the real virtual file system of `ncruces/go-sqlite3` v0.37.0 with SQLite 3.54.1. If any of them fails, the design moves to the fallback recorded in decision 5 (write-ahead log with exclusive locking, one connection), or comes back to this record.

**Date:** 2026-11-15

Nothing in this record exists yet. Every sentence about skylite is a decision, a plan or a requirement. The evidence comes from the [durability spike](/docs/decisions/0002-durability-spike/), which ran against the Azurite emulator only, and from exploratory probes run while preparing this record. The probes ran against in-memory models of object storage, not against an emulator or a cloud, and they are not in this repository.

Two labels mark claims that are not verified:

- **Probe, not reproducible:** an exploratory probe showed this once. The probe is not in this repository, so the claim cannot be checked from it, and it is not verification of any safety property.
- **Unproven:** nothing has checked this. It is a requirement or a design argument.

Each labelled claim names the section of [What the engine's tests must prove](#what-the-engines-tests-must-prove) whose tests will prove it.

## Context

### Requirements

These were fixed by the project owner before this decision:

- Go, one static binary, built with `CGO_ENABLED=0`.
- Object storage holds all durable state. Pod disk is a cache. More pod disk or memory may make skylite faster, but it must never be needed for capacity.
- A write is acknowledged only after object storage has confirmed it.
- Databases of 10 to 100 GB. A new leader must serve without first downloading the database, so the leader reads pages from object storage on demand, as readers do.
- One leader per database. Conditional writes must stop a stale leader. The lease alone is not enough.
- Azure Blob, S3, GCS and the local filesystem sit behind one storage interface whose only conditional writes are create-if-absent and compare-and-swap.
- All testing is on emulators for now. Nothing in this record has been checked against a real cloud.

### What the spike established

[0002](/docs/decisions/0002-durability-spike/) embedded Litestream as a library, with `modernc.org/sqlite`, against Azurite. Its decision section adopts three rules, and this record keeps all three:

1. A commit is acknowledged only after skylite's own storage layer has confirmed the commit object in object storage.
2. Every object that becomes part of the durable history is created with create-if-absent. A leader that loses a create to different content stops writing for good. A new leader takes office by winning a create-if-absent for an epoch-bump commit.
3. The lease exists so that a takeover makes progress. No safety argument may rest on it.

The spike also showed what it cost to keep Litestream in the write path. The spike needed a guard of its own for each of three Litestream behaviours (0002 findings 2, 3 and 4). It still left a window in which a stale leader, paused between its checks and its storage request, got a snapshot, a compaction file and a delete accepted by storage (finding 7). Its client could not publish a file above the single-upload limit atomically, so compacting a 257.8 MiB database failed (finding 6). And it kept a full local copy of the database.

### What the research established

The probes behind this record looked at two questions: which pure-Go SQLite engine can host a storage layer written in Go, and how such a layer can see each commit before `COMMIT` returns. The second question was probed three ways: through SQLite's batch-atomic write interface, by watching the rollback journal, and by reading SQLite's write-ahead log (WAL). The findings are cited, with their labels, where they are used below.

### What the design reviews found

Three rounds of independent design review of earlier drafts each found a new break in the same place: a commit-log name that garbage collection had freed could be created again by a stale actor, so that a leader got a write acknowledged that no reader would see, or a reader assembled a state that never existed. The earlier drafts added a rule each time to contain it. This revision removes the cause instead: no name in the commit log is ever freed ([decision 8](#8-compaction-retirement-and-garbage-collection)). The sequences the reviews found are replayed against this design under [Interleavings considered](#interleavings-considered).

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
- Recover from a panic at the connection boundary, under the rule in [decision 6](#6-commit-outcomes-and-fail-stop): outside the commit path, discard that connection; in the commit path once an upload may have started, stop the whole database on that node.
- Configure the per-connection memory ceiling explicitly.
- Keep the engine behind skylite's own storage layer, so it can be replaced.

### 3. Durable layout in object storage, per database

**Keys.** Every database lives under a prefix and has a database identifier: 16 random bytes, created once when the database is created. The identifier is part of the physical key of every log and base object, written as 32 lowercase hexadecimal characters:

```text
<prefix>/<identifier>/log/<transaction>             commit-log object
<prefix>/<identifier>/base/<group>-<transaction>-<epoch>  base group object
<prefix>/manifest                                   manifest
<prefix>/lease                                      lease
```

The manifest and the lease stay at fixed keys under the prefix. The manifest is where a reader learns the identifier. Every create, replace and delete that an actor issues for a log or base object therefore lands in the namespace of the identity it was working on, however late it lands. The format package decides the exact encodings of the numbers in the keys.

- **Commit log.** One object per transaction number, written with create-if-absent. It holds the changed pages, the database size after the commit, the leader's epoch, the database identifier, an optional client commit identifier and a checksum. An epoch-bump commit holds no pages. That single create is the commit point, the acknowledgement point and the fence for commits. **A name in the log, once created, is occupied for the life of the database:** nothing ever deletes a commit-log object. Once the base covers a commit, garbage collection replaces it with its **retired form** ([decision 8](#8-compaction-retirement-and-garbage-collection)), which keeps the commit's header (database identifier, transaction number, epoch, the epoch-bump flag and the client commit identifier) and drops the page table and the pages. Epoch-bump commits are never retired.
- **Base.** The database's pages, in fixed-size groups of consecutive page numbers. Each group is an immutable object whose key carries its group, the transaction number it is current to and its writer's epoch. A page's group follows from arithmetic on its page number, so the index needs memory in proportion to the number of groups, not the number of pages.
- **Manifest.** One object, updated only by compare-and-swap. It holds the database identifier, the base objects, the transaction number the base is current to, the epoch, and a 16-byte random **writer nonce** set by the candidate at its epoch swap and kept by that leader's later compaction manifests ([decision 6](#6-commit-outcomes-and-fail-stop)). Compaction and the epoch swap of a takeover change it. Lease updates and garbage collection do not.
- **Lease.** An object updated by compare-and-swap, used only so that a takeover makes progress.

Every object must fit in one upload request on every backend, because 0002 finding 6 rules out multi-part uploads for objects that fence. The format package sets the group size and the maximum commit size with that in mind ([decision 10](#10-limits-accepted-in-the-first-version)).

**The manifest has a maximum size.** The format sets it, below the single-upload limit of every backend. Compaction refuses to produce a manifest above it. The database then stops compacting and reports the condition, and the leader refuses writes once its uncompacted log reaches its own bound. This fails closed. A manifest format that scales further is later work. The arithmetic, with illustrative sizes that the format will fix: a group entry of 48 bytes (a 32-byte name and a 16-byte checksum) with groups of 4 MiB gives 23,842 entries for a 100 GB database, about 1.1 MiB. The manifest no longer grows with takeovers.

**Deleting or recreating a database is not designed in this record.** Whatever does it later must give the new database a new identifier. Reusing a prefix by wiping it by hand while any process of the old database may still run is unsupported: a delayed request from such a process can still create the manifest or the lease at the shared keys.

**What the design requires of storage.** These are requirements on every backend behind the storage interface. The safety arguments below assume all of them:

- Reads used for correctness are strongly consistent with completed writes: a read that starts after a write has completed sees that write or a later one, a read never returns an older version after a newer one has been seen, and a read that follows a conditional write which failed its precondition sees the object that made it fail, or a later version of it.
- An object becomes visible atomically and complete. A replace never makes its key read as absent, to a reader or to a create-if-absent.
- A version token identifies one stored version of one key and never repeats, including after the key is deleted and created again.
- A ranged read returns bytes of the version it reports.
- Correctness reads never go through a cache, a content-delivery network, a public endpoint or a secondary replica whose consistency is weaker than the above. Such endpoints may serve nothing that a safety argument relies on.

### 4. Invariants

These are requirements on the engine and the format. Every other decision in this record assumes them.

1. **One number sequence, one epoch per candidacy.** Transaction numbers are one sequence across all epochs, and an epoch-bump commit consumes a number. An epoch is assigned only by a successful compare-and-swap of the manifest to the previous epoch plus one, so each epoch belongs to one candidacy and is never reused. A candidacy ends with its process: a restarted node never continues at an old epoch. Arithmetic on transaction numbers and epochs never wraps: reaching the maximum value stops the database, fail closed.
2. **Log names are never freed, and the log is a contiguous prefix.** No commit-log object is ever deleted. An actor creates the object at transaction number *k* + 1 only after it has observed the object at *k*. So when any create at *k* + 1 succeeds, the object at *k* exists, and the occupied names are always exactly 1 to the head. A create that lands late, however late, can land only at the head plus one.
3. **Database identity.** The identifier is in the key of every log and base object, in the manifest and in the header of every commit and base object. A reader rejects any object whose identifier differs from the manifest's, and any object whose header does not match its key.
4. **The head is found by walking.** The head of the log is the last transaction number reached by walking contiguous numbers upward from the manifest's, by direct reads. The log is never listed. Readers, followers, candidates and the compactor all reject a gap, a failed checksum, a wrong identifier or an undecodable object among the records they need. They never skip a record and never apply a commit without every commit before it. Epochs along the log are not necessarily non-decreasing ([decision 9](#9-takeover)), and no reader may require that they are.
5. **No mixing after retirement or deletion.** Readers pin the storage version token of each log object they index and check it on every later read of that object. A reader that finds a retired commit above its manifest's transaction number, whose later read of a log object reports a different version, or whose base object is missing, abandons the read transaction, or restarts it from a newly read manifest. It never substitutes newer data into a transaction that has already read older data.
6. **Three client outcomes.** A client is told exactly one of committed, not committed or unknown ([decision 6](#6-commit-outcomes-and-fail-stop)).

### 5. Commits are captured from SQLite's write-ahead log

On the leader, SQLite runs in WAL mode with a file-backed shared-memory index. The WAL and that index are ordinary local files. The main database file is virtual: the storage layer serves it from the local cache and from object storage.

- **Capture point.** The storage layer builds the commit object from the WAL frames at the WAL sync that ends a commit, and uploads it before the sync returns. `synchronous=FULL` must be set explicitly, because ncruces compiles SQLite with the WAL default set to `NORMAL` ([sqlite_opt.h#L5][nc-opt]). In SQLite's source the sync comes before the new frames are added to the shared index and before the index header is published ([wal.c#L4281-L4346][sq-walframes]). *Probe, not reproducible:* with `synchronous=FULL`, SQLite synced the WAL exactly once per commit, after the commit frame and before `COMMIT` returned; under `NORMAL` it did not sync at commit. Test: [Commit capture and failure](#commit-capture-and-failure).
- **No SQL can bypass capture.** No client SQL may turn off `synchronous=FULL`, change the journal mode or the locking mode, write to an alternative output, or otherwise route a write around capture. The SQL lockdown enforces this. *Unproven.* Test: [Commit capture and failure](#commit-capture-and-failure).
- **Definite failure.** If the upload fails with an answer that proves the commit object was not stored, the storage layer truncates the WAL back to the end of the last uploaded commit, inside the failing call, and returns an error. SQLite then rolls back. *Probe, not reproducible:* IOERR, IOERR_FSYNC, a plain Go error and FULL from the sync each produced an error to the caller and a rollback, and none made SQLite report success. Test: [Commit capture and failure](#commit-capture-and-failure). An upload whose outcome is not definite is handled by [decision 6](#6-commit-outcomes-and-fail-stop).
- **Why the truncation is required.** After a failed sync, the frames and the commit marker are still in the WAL with valid checksums, and SQLite's recovery accepts every valid frame up to the last commit marker ([wal.c#L1534-L1556][sq-recover]). *Probe, not reproducible:* a failed transaction reappeared after a reopen whenever the WAL was kept, and did not with the truncation. Test: [Commit capture and failure](#commit-capture-and-failure).
- **Visibility.** Other connections must see a transaction only after its upload has succeeded. *Probe, not reproducible:* readers on other connections saw only the old value while an upload was blocked for 300 ms, and never saw a failed commit. Test: [Reads](#reads).
- **Checkpoints.** A local WAL checkpoint writes only to the local cache, because its pages are already in object storage. *Probe, not reproducible:* every page a checkpoint wrote was byte-identical to a page already uploaded, with the exception below. Test: [Reads](#reads). How the leader reads the main file around checkpoints is in [decision 7](#7-reads).
- **Start-up.** Local files are wiped when a node starts. They are never a source of truth.

Two behaviours qualify the description above. The engine must handle both:

- **`BUSY` does not roll back an explicit `COMMIT`.** If the commit point returns `BUSY` inside an explicit transaction, `COMMIT` fails with "database is locked" and the transaction stays open with the write lock held, and a retried `COMMIT` writes the frames again ([vdbeaux.c#L3438-L3446][sq-busy1], [vdbe.c#L4197-L4202][sq-busy2]). *Probe, not reproducible:* the probe saw this sequence. The decision: the storage layer never returns `BUSY` from the commit point. Every failure there is reported with an error code that rolls the transaction back. Test: [Commit capture and failure](#commit-capture-and-failure).
- **Entering WAL mode writes page 1 outside the WAL.** `PRAGMA journal_mode=wal` changes page 1 through a rollback-journal transaction, whatever the journal mode ([vdbe.c#L8302-L8307][sq-jm], [btree.c#L11572-L11600][sq-ver]). That write never passes through WAL capture. *Probe, not reproducible:* on a new database the first WAL commit sent page 1 again, so storage ended up correct by accident. The decision (bootstrap): the engine does not create a database through WAL capture. It builds the first commit object itself, holding an empty database whose header already says WAL mode, so that opening it never needs that write. The database identifier ([invariant 3](#4-invariants)) is created at the same step. Test: [Commit capture and failure](#commit-capture-and-failure).

WAL mode with exclusive locking, which allows one connection and needs no shared-memory index ([vfs/README.md#L69-L74][nc-walreadme]), is the recorded fallback if the shared-memory route fails the engine's tests.

### 6. Commit outcomes and fail-stop

A storage request can fail without saying whether it took effect: a timeout, a dropped connection or a server error after the request was sent. The storage interface classifies every failure of a mutation as definite (it had no effect) or as outcome unknown. When a backend's answer is not documented as definite, it is not definite.

**A transaction number is bound to one object.** For the life of the leader process, each transaction number the leader tries is bound to the exact bytes of the one commit object the leader built for it. The leader never builds a second, different object for a number it has already tried to create, and it does not try the next number until this one has a definite answer.

**Who can occupy a leader's next name.** A leader creates an ordinary commit at a name only while it holds the name before it, as its own commit or its own epoch bump. A leader's last commit *n* is its own object, and the name *n* is never freed ([invariant 2](#4-invariants)), so the object at *n* is that leader's for the life of the database. No other leader can therefore hold *n*, and no other leader creates an ordinary commit at *n* + 1. The first object ever created at *n* + 1 is this leader's own commit or some candidate's epoch bump, and because the name is never freed, the first object is the only object. This is a design argument. Test: [Commit outcomes](#commit-outcomes).

**Resolving a create.** After a create whose outcome is unknown, the leader repeats the identical create until it gets a definite answer, up to a bound the engine sets. The definite answers are:

- **Success.** Committed.
- **Precondition failed, then a read of the name finds a commit that is not an epoch bump.** By the argument above it is the leader's own, so the answer is committed, whether or not it has been retired since. The leader also checks that the object's transaction number, epoch and commit identifier are its own and, if the object is not retired, that its bytes are the ones it built. A mismatch means an invariant is broken: the leader stops as for an indeterminate commit and the client is told unknown.
- **Precondition failed, then a read of the name finds an epoch bump.** The leader is fenced, and the commit is definitely not committed: the bump is the only object that will ever be at that name.

A precondition failure followed by a read that finds nothing cannot happen under the storage requirements in [decision 3](#3-durable-layout-in-object-storage-per-database), because nothing deletes a log object. If it happens, an invariant is broken, and the leader stops as for an indeterminate commit.

The same steps apply to the first attempt at a create: success means committed, and a precondition failure is resolved by reading the name. On a first attempt only an epoch bump can be there.

**Indeterminate commit.** A commit is indeterminate only when the leader cannot get a definite answer from storage within the bound. The outstanding create may still land later. If it does, it lands at the head plus one ([invariant 2](#4-invariants)), as a valid continuation of the leader's own last commit, and the commit is then in the history.

**Fail-stop.** On an indeterminate commit, and whenever the leader is fenced, the node stops serving that database, closes every connection, discards all local state for it, and can come back only through the takeover path, which reads durable history. The same fail-stop applies in these cases, because storage may hold a commit that the local image does not reflect, or the local image may hold one that storage does not:

- The storage layer cannot truncate its local WAL after a definite upload failure.
- **Any panic, or any SQLite error the storage layer did not itself return, in the commit path once an upload may have started.** The commit path runs from the moment the storage layer begins the create for a commit object until `COMMIT` has returned on that connection. It includes adding the frames to the shared-memory index, publishing the index header and the second phase of commit. For example, SQLite can run out of memory while adding frames to the shared-memory index after the upload succeeded and before it publishes the new header. ncruces turns that into a panic ([conn.go#L493-L495][nc-nomem]). Discarding only that connection would leave the other connections on a local image that does not contain a durable transaction, so the whole database stops on that node.

Outside the commit path, a panic at the connection boundary discards only that connection. *Unproven:* that discarding a connection after a panic releases its locks and leaves the shared-memory index consistent for the other connections. Test: [Commit capture and failure](#commit-capture-and-failure). If that test fails, the fail-stop extends to every panic.

**Manifest replacements also have three outcomes.** After a replacement of the manifest whose outcome is unknown, the actor repeats the identical replacement until it gets a definite answer, up to a bound:

- **Success.** The replacement took effect.
- **Precondition failed, then a read of the manifest returns the actor's own bytes.** The replacement took effect.
- **Precondition failed, then a read returns anything else.** The version the request was conditioned on is gone for good, because version tokens never repeat, so the request can never take effect from now on. It may have taken effect earlier and been replaced since; nothing in this record depends on telling those two apart, and every rule that follows asks only whether the current manifest references an object.

Two rules make "the actor's own bytes" unambiguous. Every replacement changes the manifest's bytes: an epoch swap raises the epoch and sets a new writer nonce, and a compaction raises the transaction number. And the writer nonce, drawn from a cryptographically secure random source, identifies the candidacy: two candidates that read the same manifest would otherwise attempt byte-identical epoch swaps, and both would recognise the one that landed as their own. A candidate therefore recognises its own epoch swap by its nonce, never by the epoch number alone.

A candidate that cannot get a definite answer for its epoch swap gives up the candidacy and never serves, as for its epoch-bump create. A leader that cannot get a definite answer for a compaction's replacement keeps that compaction's output, stops compacting and garbage collecting, and leaves office through fail-stop. This last rule is an addition of this record's author to the project owner's decisions; it keeps the leader from running with two possible manifests.

**Client outcomes.** A client is told exactly one of:

- **Committed**, when storage confirmed the create, or a read after a precondition failure found the leader's own commit.
- **Not committed**, only when the commit object is known not to be in the durable history: a definite failure, or a precondition failure resolved to an epoch bump.
- **Unknown**, in every other case. A client is never told "not committed" for an indeterminate commit.

Unknown is a permitted final answer. When the client supplied a commit identifier, the commit object carries it, and the retired form keeps it, so it can be found for the life of the database. The log is not indexed by commit identifier and is never listed, so a lookup reads log objects by number from a starting transaction number the client supplies up to the head. A match means committed. No match means unknown, not "not committed". The client protocol decides how the starting number and the identifier are carried.

**Late acknowledgements are correct.** A leader whose commit reached storage may deliver the acknowledgement after another leader has taken office. That is correct, because the commit is in the history. Two processes may both believe they hold office at once. The design does not prevent that belief. It prevents conflicting history: no leader gets a transaction acknowledged that is not in the accepted history.

### 7. Reads

- **Lookup.** A page is looked up first in the commit log written since the manifest's transaction number, through a small in-memory index that compaction keeps bounded, and then in its base group. Fetched data goes into a bounded local cache. The index records the storage version token of each log object it covers, and every later ranged read of that object checks it ([invariant 5](#4-invariants)).
- **Followers.** Followers use a read-only storage layer. It pins each read transaction to one version and moves to the newest version between transactions. *Probe, not reproducible:* a follower that did not pin mixed two versions inside one scan. Test: [Reads](#reads).

**Leader reads of the main file.** SQLite expects the main file to hold only checkpointed state. *Probe, not reproducible:* serving the newest uploaded version from the main file let a reader that began before a commit see that commit's data, which breaks snapshot isolation. The mechanism below is unproven. Test: [Reads](#reads).

- The storage layer keeps a **base version**: the last commit at which the local WAL was empty and the main file was exactly a committed state. At takeover it is the head. Afterwards it is the last commit before each point where SQLite restarts the WAL from its beginning.
- In SQLite 3.54.1, the WAL restarts from its beginning only when every frame has been backfilled and no reader is using the log. A writer restarts it only if its own read used no WAL frames, the backfill count equals the last frame, and it gets an exclusive lock on every reader slot that reads the WAL ([wal.c#L3961-L3995][sq-restart]). A `RESTART` or `TRUNCATE` checkpoint restarts it only after the whole WAL is backfilled and it gets the same locks ([wal.c#L2435-L2467][sq-ckpt-restart]). Closing the last connection deletes the WAL only after taking an exclusive lock on the database and completing a checkpoint ([wal.c#L2610-L2629][sq-close]).
- Pages that checkpoints write to the main file after the base version, and the main file's size after them, are kept locally, and are not evicted until the base version next advances.
- A main-file read of a page not held locally fetches that page as of the base version. A page not held locally has not been written by a checkpoint since then. If a page the storage layer must hold locally is missing, the read fails. It never falls back to older base bytes.
- **The leader compacts only up to its base version.** A page as of the base version is built from the base of the current manifest and the log objects above it, so the manifest's transaction number must not pass the base version while the base version is in use; otherwise the log objects between them could be retired and the page could no longer be built. This rule is an addition of this record's author.
- A checkpoint's completion callback is not treated as proof that the checkpoint completed. Only a WAL restart advances the base version.

**What a follower's storage layer presents.** SQLite decides whether to drop its page cache by comparing bytes 24 to 39 of the file header when it takes a shared lock ([pager.c#L5452-L5482][sq-cache]). In WAL mode the change counter there moves only when page 1 is part of a commit ([pager.c#L3272][sq-cc]). *Probe, not reproducible:* a follower whose storage layer did not change those bytes returned stale cached pages. The follower's storage layer therefore presents page 1 with three fields changed:

- **Bytes 18 and 19**, the file format write and read versions, set to 1. The database then reads as a rollback-journal database, and SQLite does not open a WAL for it, which it does when byte 19 is 2 ([btree.c#L3363-L3386][sq-walver]).
- **Bytes 24 to 27**, the file change counter.
- **Bytes 92 to 95**, the version-valid-for number, set equal to the change counter. SQLite trusts the database size in bytes 28 to 31 only when those two fields are equal ([btree.c#L3328-L3332][sq-hdrsize]).

The change counter is 32 bits. The decision: it is a per-connection counter that the storage layer increments whenever the version pinned for that connection changes, and the connection is closed and reopened before the counter can wrap. *Unproven:* the probe wrote the version number itself into the counter; the per-connection counter has not been tried. Test: [Reads](#reads).

### 8. Compaction, retirement and garbage collection

**Compaction.** The leader folds the commit log into new base group objects, under new keys, and then replaces the manifest by compare-and-swap. The keys of its output carry a transaction number above the manifest it read, and its own epoch. It replaces the manifest only after every output object's create has a definite answer: success, or a precondition failure followed by a read that returns identical bytes (a group's content at a transaction number is fixed by the log, so a repeated compaction produces the same bytes under the same key). It never replaces a manifest whose epoch is higher than its own. A leader that reads a manifest whose epoch is higher than its own stops compacting and leaves office, so that the candidate that raised the epoch, or a later one, can finish a takeover. A takeover changes the manifest first, so a deposed leader's replacement, conditioned on an older version, cannot win.

**Retirement replaces deletion in the commit log.** Garbage collection never deletes a commit-log object. It retires one that the base covers. The rule:

- The collector retires the object at transaction number *k* only if *k* is at or below the transaction number of a manifest it read from storage, or of a manifest replacement of its own that got a definite success; the object is not an epoch bump; and the collector read the object and found it unretired. The retire is a replace conditioned on the version token the collector read.
- Retirement honours a grace period after the manifest that covers the object, so that readers still using older manifests usually finish. That is for reader progress only. Safety does not depend on it: a reader that does not finish in time is handled by [invariant 5](#4-invariants).
- A retire whose outcome is unknown needs no resolution. It is permitted whenever it lands.

**Why a stale collector's retire is safe.** The manifest's transaction number never decreases, so the condition "*k* is at or below the transaction number of a manifest read from storage" stays true forever once it holds, and the pages of commit *k* stay in the base that the current manifest names. The replace is conditioned on the object's version, so it can only turn the original commit into its retired form, never overwrite anything else, and it lands at most once. This is a design argument. Test: [Fencing, takeover and garbage collection](#fencing-takeover-and-garbage-collection).

**Base objects.** A base object that the current manifest references is never deleted. A base object that a manifest dropped is garbage under every later manifest, because manifests only move forward and a manifest never references an object that an earlier manifest dropped: an epoch swap copies the current references, and a compaction references only keys it created above the manifest's transaction number. Such an object is deleted only after a grace period, which, as for retirement, is for reader progress only.

An unreferenced base object that no manifest has referenced is garbage only once the manifest version its publication was, or could have been, conditioned on no longer exists. Until then it is kept, even if that leaks objects for a while:

- **The writer's own output.** The writer knows the version it conditioned on. Its output is garbage once it has a definite answer that the replacement did not take effect, or once it observes a different manifest version that does not reference the object. A writer never creates a base key for which it has issued a delete; a compaction that would produce such a key waits for a higher transaction number.
- **Output carrying an epoch lower than the manifest's.** Its writer's publication could only be conditioned on a manifest of the writer's own epoch, and the epoch swap that raised the epoch changed the version, so that version is gone.
- **Output carrying an epoch higher than the manifest's.** Never garbage on that manifest's evidence: it may belong to a candidate that has not yet replaced the manifest.

A delete whose outcome is unknown needs no resolution: the object is garbage under every later manifest, and its key is never created again by a writer that would publish it.

**Head discovery stays a walk.** A reader, candidate or compactor finds the head by reading consecutive names from the manifest's transaction number until a name is absent. The log is never listed.

**The cost of never freeing a name, stated plainly.** Every transaction leaves one small object for the life of the database, and every ordinary commit costs one conditional replace, amortised, where the earlier design had a delete. With the commit header of the format proposed in [pull request #7][format-pr], 144 bytes, and the retired form taken to be that header alone, a year of 365 days holds:

| Commits per second | Retired objects per year | Bytes per year |
| --- | --- | --- |
| 1 | 31,536,000 | 4,541,184,000 (about 4.5 GB) |
| 10 | 315,360,000 | 45,411,840,000 (about 45 GB) |
| 100 | 3,153,600,000 | 454,118,400,000 (about 454 GB) |

These are object bodies only. Providers may bill per-object metadata on top; that is unverified and not counted here. At 10 commits per second for a year, the retired log of a 100 GB database is close to half the database's own size. Reclaiming retired objects is future work and needs its own fencing argument.

This closes, by construction, the window that [0002](/docs/decisions/0002-durability-spike/) measured in finding 7. That is a design argument. No test has shown it yet.

### 9. Takeover

1. A candidate takes the lease.
2. It swaps the manifest to the manifest's epoch plus one, with a new writer nonce, and resolves the outcome as in [decision 6](#6-commit-outcomes-and-fail-stop). Without a definite success it gives up.
3. It reads the tail of the commit log, walking from the manifest's transaction number ([invariant 4](#4-invariants)). A retired commit above that number, or a log object whose version changes under it, sends it back to re-read the manifest.
4. Before each attempt at its epoch-bump commit it re-reads the manifest, and gives up if the manifest no longer carries its epoch and nonce. It then creates the bump at the head plus one and resolves the outcome as in decision 6, recognising its own bump by its epoch. If another object holds the name, it walks on and tries again. Without a definite answer it gives up.
5. It reads the manifest again and takes office only if the manifest still carries its epoch and nonce.

The final check, step 5, is a design argument. From the moment of its swap, no older leader can change the manifest, so the base cannot have moved past the candidate's view and no object above its view can have been retired, unless a newer candidate has swapped the manifest since, which the re-read detects. A candidate that fails the check stops and never acknowledges a write.

The old leader's next create meets an occupied name and it stops for good ([decision 6](#6-commit-outcomes-and-fail-stop)). If the old leader commits first, the candidate loses the create, walks on and retries. A candidate's create at a name that has since been occupied fails, so its bump can never land in a stale position: it lands at the true head plus one or not at all. The takeover involves no download of database content. Its time is not independent of database size, because the manifest it reads grows with the number of base groups, and it also depends on the length of the log tail. No safety argument rests on clocks or on the lease.

**A late candidate can fence the live leader.** This is a liveness cost, not a safety problem. A candidate that passed its re-read in step 4, was paused, and resumes after another candidate has taken office can still win the create for its bump at the live leader's next transaction number. Its final check then fails and it never serves. But the live leader finds a bump at its next name and is fenced, so another takeover is needed. The late candidate's epoch is lower than the epoch of the commits before its bump, so epochs along the log are not necessarily non-decreasing ([invariant 4](#4-invariants)).

### 10. Limits accepted in the first version

- **One upload per commit.** A leader can commit at most one transaction per storage round trip. Group commit is a possible later change, not a promise.
- **Maximum transaction size.** A transaction must fit in one commit object, and its frames must fit in the leader's local WAL bound. A larger one fails with a clear error and leaves nothing in storage. The maximum commit object size is set by the format below the smallest single-request upload limit of the backends: Amazon S3 accepts "a single object up to 5 GB in size" in a single `PUT` ([Uploading objects][s3-upload]); Azure Blob Storage accepts 5,000 MiB in one `Put Blob` from service version 2019-12-12, and 256 MiB for versions 2016-05-31 through 2019-07-07 ([Put Blob][az-putblob]); the same page says the Azurite emulator "supports blob sizes of up to 2 gibibytes (GiB) only". All three were read on 2026-11-15. The limit cannot be raised past these by staged uploads, because 0002 finding 6 rules them out for objects that fence. **Consequence:** `VACUUM`, and index builds that rewrite more pages than the limit, are unsupported on a database larger than the limit in this version, because they rewrite most of the database in one transaction. Multi-object commits are future work.
- **The local WAL bound is enforced while frames spill.** The leader's storage layer refuses a WAL write that would take the local WAL past its bound, during a transaction as well as at its commit, with an error that rolls the transaction back. It does not wait for the next transaction boundary. *Unproven.* Test: [Limits and size](#limits-and-size).
- **Maximum manifest size.** A database whose manifest would exceed it stops compacting ([decision 3](#3-durable-layout-in-object-storage-per-database)).
- **The tail bound counts epoch-bump commits.** The leader refuses writes once the log above the manifest's transaction number reaches a bound, and epoch bumps count towards it. Repeated failed takeovers each add a bump to the tail and can fill it. A candidate whose walk finds the tail at or past the bound still takes office if its final check passes, refuses client writes, and compacts first; a compaction folds bumps at no page cost, and writes resume once the tail is below the bound. What restores service is one takeover that completes and one compaction. An operator sees writes refused with the tail-bound condition and the epoch rising with each failed takeover.
- **Long readers hold back checkpoints.** *Probe, not reproducible:* an open read transaction on the leader stopped checkpoints from backfilling the WAL, and the WAL kept growing. SQLite restarts the WAL only once it is fully backfilled and no reader is using it ([wal.c#L3961-L3995][sq-restart]). Readers on the leader therefore need time limits. They also hold back the base version, and with it compaction ([decision 7](#7-reads)).
- **Follower lag.** Followers poll at a configured interval. Their lag is not bounded by that interval: under delays or failures a follower can fall behind by any amount. A client that needs to read its own write presents the transaction number it was given.
- **Takeover latency is not bounded.** An old leader that keeps winning creates and ignores the lease makes each of a candidate's bump attempts lose, as 0002 finding 8 observed. The old leader stops at its next compaction, when it reads the higher epoch, or at its tail bound. Late candidates can also repeatedly fence healthy leaders ([decision 9](#9-takeover)). Both are known behaviour. An operator sees candidates whose bump attempts keep losing to commits, leaders fenced by a bump at their next name, candidates that fail their final check, and the epoch rising without a lasting leader.
- **Cost per commit.** In the normal path a commit is one conditional create. Everything else is amortised over many commits: one conditional replace per ordinary commit for its retirement, the base group objects and one manifest replacement per compaction, deletes of dropped base objects, and lease renewals. This record gives no prices.
- **Sequential log keys.** Log keys are sequential numbers under one prefix. Google Cloud Storage advises to "avoid using sequential names" at high request rates, and to "introduce randomness to the object names by adding a hash value before the sequence number or timestamp" ([Request rate and access distribution guidelines][gcs-request-rate], read on 2026-11-15). Amazon S3 documents "at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix" ([Optimizing Amazon S3 performance][s3-perf], read on 2026-11-15). A hashed physical prefix for log keys is possible later without changing any argument in this record, because no argument depends on key order and the log is never listed. It is not adopted now.

## Interleavings considered

Each sequence below is replayed against this design, step by step, with the rule that stops it. All of them are design arguments; the tests under [Fencing, takeover and garbage collection](#fencing-takeover-and-garbage-collection) and [Commit outcomes](#commit-outcomes) must reproduce each one.

### The sequences the reviews found

**The lost-response commit (first review).**

1. Leader A holds 10 and creates 11. Storage stores it; the response is lost.
2. Candidate B takes over: swaps the manifest, walks to 11 (A's commit), bumps 12, takes office, compacts through 12. Garbage collection retires 11.
3. A repeats its identical create at 11. Precondition failed. A reads 11 and finds a retired commit that is not a bump.

Stopped by: the name is never freed (invariant 2), and a non-bump at A's next name is A's own (decision 6). A reports committed, which is true: B's base contains the commit.

**The garbage-collected takeover commit (first review).**

1. Leader A holds 10 and pauses.
2. Candidate B bumps 11, takes office, commits 12 and compacts through 12. In the earlier draft, garbage collection could then delete the bump at 11.
3. A resumes and creates 11, its next name.

Stopped by: the bump at 11 is never deleted or retired (decision 8). A's create fails; A reads a bump, is fenced, and reports not committed, which is true.

**The freed-name sequence from the previous revision.**

1. A creates 11; the response is lost; the commit is stored.
2. A later leader folds 11 into the base; garbage collection retires it.
3. A candidate paused since before step 1 resumes and tries to create its bump at 11.
4. A repeats its create at 11.

Stopped by: in step 3 the name is occupied, so the candidate's create fails (decision 9). In step 4, A reads its own retired commit and reports committed. The earlier draft's "unknown because the name may have been recycled" outcome does not exist any more.

**Blocker 1: a recycled name gives a follower a state that never existed.**

1. The base is complete to 10. Candidate C swaps the manifest to epoch 1, walks to 10 and pauses before creating its bump at 11.
2. Follower F reads that manifest (transaction number 10) and pauses.
3. Leader A commits 11 (x=1) and 12 (y=1).
4. Candidate B swaps to epoch 2, bumps 13, takes office and compacts through 13. Garbage collection retires 11 and 12.
5. C resumes. Its re-read of the manifest before its bump attempt finds epoch 2, so it gives up. Had it passed that re-read before pausing, its create at 11 fails because 11 is occupied, and its walk on finds a retired commit above its manifest's number and sends it back to the manifest, where it gives up.
6. F resumes on its manifest at 10 and reads 11. It finds a retired commit above its manifest's transaction number.

Stopped by: invariant 5. F restarts from the current manifest (13) and serves x=1, y=1. If F had indexed 11 before step 4, its later ranged read of 11 reports a different version and it restarts the same way. If F read 11, 12 and 13 whole before the retirements, it serves x=1, y=1 from them, which is a state that existed.

**Blocker 2: an unknown manifest swap during compaction publishes deleted output.**

1. Leader A, at manifest version v1, creates new base objects and submits the replacement to v2. The outcome is unknown.
2. A repeats the identical replacement. While no answer is definite, v1 may still exist, so A keeps the output. If A reaches its bound without a definite answer, it leaves office keeping the output (decision 6).
3. If the outstanding replacement lands, the manifest references the output, which still exists.
4. If instead a candidate swaps the manifest from v1, v1 is gone, the outstanding request can never land, and the output carries an epoch lower than the manifest's, so it becomes garbage then and only then. If a candidate swaps from v2, its manifest references the output, so it is kept.

Stopped by: the rule for unreferenced base objects (decision 8). The earlier draft's "the compaction has ended" is no longer a reason to delete.

**Blocker 4: a delayed delete after the database is recreated under the same prefix.**

1. An old collector issues a delete for the old database's log object 11. The request is delayed.
2. The database is recreated under the same prefix with a new identifier.
3. The delete lands.

Stopped by: decision 8 issues no deletes for log objects at all, and decision 3 puts the identifier in the key, so the request was for `<prefix>/<old identifier>/log/…` and lands in the old namespace. The same holds for a delayed retire, a delayed base delete and a delayed create. A delayed manifest or lease replacement is conditioned on a version token, which never repeats. A delayed create of the manifest or the lease is the hand-wiping case that decision 3 declares unsupported.

### Attempts to break the retired-object design

**A stale collector retires a commit that is not yet covered.** The collector retires *k* only below the transaction number of a manifest it read from storage or published with a definite success. That manifest's base covers *k*, and the current manifest's transaction number is at least as high, because it never decreases. A collector cannot retire on the evidence of a manifest replacement whose outcome is still unknown. Closed.

**A retire races a reader.** The reader either finished reading the commit before the retire, and holds a state that existed, or reads after it and sees a retired commit or a changed version, and restarts. The storage requirements in decision 3 rule out a read that mixes the two versions. Closed.

**A retire races a leader's read-back.** The leader reads either the unretired commit or its retired form. Both are a non-bump at its next name, so both mean committed. Epoch bumps are never retired, so a bump cannot turn into something else under the read. Closed.

**A retire races a takeover walk.** The candidate walks from the transaction number of the manifest it swapped. An object above that number can be retired only on the evidence of a manifest with a higher number, which means a newer manifest exists; the candidate meets the retired commit or the changed version, re-reads, and gives up. Closed.

**A retire races compaction.** Compaction reads objects above the manifest's transaction number; retirement touches only objects at or below it. The leader's own collector cannot outrun its compaction, because it retires only on a definite success or a read. Closed.

**The leader's base version is older than the manifest.** If the leader compacted past its base version, garbage collection could retire log objects that the leader still needs to build pages as of its base version, and the base of the older manifest could be deleted. Closed by the rule in decision 7 that the leader compacts only up to its base version.

**A delayed create lands after any amount of time.** By invariant 2 a create lands only at the head plus one. A delayed ordinary commit is the continuation of its leader's own last commit, so it is a valid commit, and its client was told unknown. A delayed epoch bump is a valid continuation with no pages; it may fence the leader that holds the head, which is the liveness cost of decision 9, and its candidate never serves because it gave up or failed its final check. After a takeover, the takeover's bump occupies the deposed leader's next name, so a delayed commit of that leader cannot land. After a compaction, every name the compaction folded is occupied. A delayed base create recreates an object with the same bytes under the same key; if no manifest references it, it is garbage under the rule in decision 8. Closed.

**A delayed replace lands after any amount of time.** A delayed retire is permitted whenever it lands. A delayed manifest replacement lands only if the manifest is still at the version it was conditioned on, in which case it is a legitimate step from that version: a compaction whose output still exists, because output is kept until that version is gone, or an epoch swap. A delayed epoch swap from a candidate that gave up leaves the manifest at an epoch nobody serves; the leader below it stops at its next compaction and a new takeover follows. That is a liveness cost. Closed for safety.

**A delayed delete lands after any amount of time.** Log objects are never deleted. A base delete is issued only for garbage, which stays garbage, and the writer never creates that key again. Another writer cannot create it, because the key carries the writer's epoch. Closed.

**Two candidates.** Both read the manifest at epoch *e*. Only one swap to *e* + 1 lands; the other candidate's read shows a different nonce, so it does not take the epoch, even after an unknown outcome. If both swap in turn (*e* + 1, then *e* + 2), the first one's re-read before its bump or its final check fails. If the first one's bump lands first, the second walks past it; if the second one's lands first, the first one's create fails and its re-read stops it. Closed. The late-candidate liveness cost in decision 9 remains.

**A candidate crashes at each step.** After taking the lease: nothing changed but the lease, which expires. After its swap, or with its swap's outcome unknown: the manifest carries an epoch nobody serves; the old leader keeps committing until its next compaction, then leaves office, and its output carries a lower epoch and becomes garbage. During its walk: the same. With its bump created or in flight: the bump is a valid continuation, it fences the leader at the head, and the next takeover walks past it. After its final check: it was the leader; this is a leader crash. Closed for safety.

**An unknown outcome on every conditional write.** Commit create: repeated to a definite answer or the node stops and the client is told unknown (decision 6). Epoch-bump create: repeated, recognised by epoch, or the candidate gives up. Epoch swap: repeated, recognised by nonce, or the candidate gives up. Compaction replacement: repeated, recognised by bytes, or the leader keeps its output and leaves office. Base object create: repeated until success or identical bytes; until then the manifest is not replaced, and different bytes are a broken invariant that stops the compaction. Retire: needs no resolution. Base delete: needs no resolution. Lease: not used for safety. Closed.

**Transaction number and epoch at their limits.** Neither wraps. A leader at the maximum transaction number cannot commit, and a candidate whose head is at the maximum cannot bump, so the database stops for good, fail closed. A manifest at the maximum epoch cannot be swapped, so no further takeover is possible and the database stops when its leader does. Neither limit is reachable in practice at the rates in decision 8 (inference), but the code must stop, not wrap.

**Sequences not closed.** None of the sequences tried here breaks safety under the storage requirements in decision 3. Two things are not closed by this record. First, the requirements in decision 3 are stronger than the storage contract states today, and every argument above depends on them. Second, whether discarding a connection after a panic outside the commit path leaves the other connections correct is unproven (decision 6).

## Alternatives considered

**Litestream as a library.** This is what the spike ran. Rejected for the four reasons in [decision 1](#1-skylite-builds-its-own-storage-engine-litestream-is-not-used-in-the-product).

**`modernc.org/sqlite`.** Its public VFS package exposes an `fs.FS` read-only: "it has no write path and refuses to create a journal" ([vfs/patches.go#L5-L25][mc-ro]). A writable Go storage layer would have to wire SQLite's method table by hand through `unsafe` pointer writes, the way that package does for its own read-only methods ([vfs/patches.go#L115-L123][mc-unsafe]). That is not a supported interface. *Probe, not reproducible:* modernc's compile options did not include batch-atomic writes. Rejected.

**Capture through SQLite's batch-atomic write interface.** ncruces compiles this in ([sqlite_opt.h#L25-L26][nc-opt]) and exposes it to a Go storage layer ([vfs/api.go#L143-L153][nc-batch]). Rejected for four reasons:

- **A failed upload can be reported as success.** An IOERR from the commit makes SQLite create a rollback journal and write the same pages directly ([pager.c#L6698-L6705][sq-ba-ioerr]). A plain Go error from the storage layer maps to an IOERR code ([vfs/vfs.go#L415-L437][nc-errcode]). *Probe, not reproducible:* the caller saw success and the change was applied outside the upload.
- **It is skipped for three kinds of transaction.** The first write to an empty database does not use it ([pager.c#L1207-L1210][sq-ba-empty]). Truncation, as in `VACUUM` or `auto_vacuum`, happens after the commit, outside the batch ([pager.c#L2193-L2202][sq-trunc]). *Probe, not reproducible:* a transaction larger than the page cache fell back to a journal and direct writes.
- **Other connections read stale data without a workaround.** With both atomic-write options compiled in, as ncruces does, SQLite skips the change-counter update for a batch commit ([pager.c#L6595-L6653][sq-ba-cc]). *Probe, not reproducible:* a second connection kept returning old data until the storage layer synthesised the counter.
- **Readers wait out every upload.** *Probe, not reproducible:* under rollback-journal locking, a reader on another connection got "database is locked", or waited for the whole 300 ms upload.

The WAL route showed none of these four in the probes, which is itself a probe result, not verification.

**Watching the rollback journal.** The storage layer would treat direct writes to the main file as private until the journal is deleted, then publish them. A probe recorded the call sequences only. What SQLite does when that final step fails was not probed. Pages spilled during a large transaction would need a local buffer whose size is bounded only by the transaction. Readers wait out uploads as with batch-atomic. Rejected.

**WAL with exclusive locking.** It needs no shared-memory index, but it allows one connection per database ([vfs/README.md#L69-L74][nc-walreadme]), so leader reads cannot run during a commit. Kept as the fallback in [decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log).

**Immutable payload plus one head object swapped on every commit.** In its strongest form, a commit uploads the transaction's pages under a name that is never reused, then swaps one small head object by compare-and-swap to point at it, and is acknowledged on that swap. haqlite and hadb use a version compare-and-swap with an epoch check on each manifest write ([store.rs#L14-L37][hadb-epoch]). This makes one object the fence for commits and compaction alike. Rejected for two costs:

- **Two storage mutations in sequence on every commit.** The page upload must finish before the swap starts, so every acknowledged write waits for two storage round trips instead of one.
- **A write-rate limit on one object name.** Google Cloud Storage documents a maximum rate of writes to the same object name of "One write per second", and notes that "Writing to the same object name at a rate above the limit might result in throttling errors" ([Cloud Storage quotas and limits][gcs-quota], read on 2026-11-15). A single head object written on every commit would cap a database on that backend at about one commit per second. The commit log writes each name once at commit and once more when it is retired, long after.

The log keeps one storage mutation per commit in the path that acknowledges it, at the cost of the retired objects in [decision 8](#8-compaction-retirement-and-garbage-collection). That trade is a design argument, not a measurement.

**Keeping every commit-log object whole forever.** No name would ever be freed, which removes the same class of problem this record removes. Rejected for unbounded storage of history: every page ever written would stay in storage twice, once in the log and once in the base. Retirement, which this record adopts, keeps the names and drops the bytes.

**Deleting folded commit-log objects, with rules to contain stale creates.** This is what the earlier drafts did: a list of retained epoch bumps in the manifest, a rule about which objects garbage collection may delete, and a third outcome for a commit whose name may have been recycled. Three review rounds each found a new break in it. Rejected.

**A storage-side lease as the fence.** Azure Blob can lease a blob, but the storage interface allows only create-if-absent and compare-and-swap as conditional writes, so a fence built on one backend's lease cannot be carried to the other three. Rejected.

**Prior-art systems.** None can be used as a library from pure Go. All are written in Rust. Each one contributed an idea:

- **Graft** writes one commit object per log position with an atomic create that fails if the object exists ([remote.rs#L190-L205][graft-put]). By default its clients commit locally first and push asynchronously ([consistency.mdx#L18][graft-async]), so a commit is acknowledged before object storage has it. skylite takes the create-if-absent commit log from Graft.
- **turbolite** keeps immutable page groups and makes the manifest write the atomic commit point ([README.md#L129][tl-manifest]). Its README warns that two writers on one prefix corrupt the manifest ([README.md#L212][tl-single]). Durability comes at checkpoint, when page groups are uploaded ([README.md#L351][tl-sync]). skylite takes the immutable groups and the manifest, not the commit model.
- **haqlite and hadb** create the lease with `If-None-Match: *` and renew it with `If-Match` ([lease_store.rs#L85-L93][hadb-create], [lease_store.rs#L147-L156][hadb-renew]), and check an epoch on each manifest write ([store.rs#L14-L37][hadb-epoch]). Only their opt-in Cloud mode uploads every commit before returning ([README.md#L81-L83][haq-modes]). skylite takes the lease protocol and the epoch on the manifest.
- **Rivet's storage engine** acknowledges a write once a quorum of storage replicas has written it to disk ([README.md#L1-L6][rivet-quorum]). It fences with client epochs, and a handle without quorum progress for thirty seconds is fenced ([README.md#L19-L22][rivet-epoch], [README.md#L38][rivet-fence]). An acknowledgement that waits for storage-node disks does not meet skylite's rule that only object storage holds durable state, so skylite takes only the epoch fence from Rivet.

## Consequences

- skylite owns the storage engine: the WAL parser, the commit format, the paged reader, compaction, retirement, garbage collection and takeover. These are the parts where a mistake loses data. The engine's own tests carry the weight that the spike's tests carried for Litestream.
- The SQLite-side mechanisms are accepted on condition, as the status above says; a failing test sends the design to the fallback or back to this record.
- Every acknowledged write costs at least one object-storage round trip. This was already accepted in 0001. It has still not been measured against a real cloud.
- Every transaction leaves a retired object of about one commit header for the life of the database, and every ordinary commit costs one amortised conditional replace ([decision 8](#8-compaction-retirement-and-garbage-collection)).
- Some commits end with the answer unknown, and the node that gave it stops serving the database until a takeover. Clients must handle unknown. A commit identifier can be looked up for the life of the database.
- `VACUUM` and large index builds are unsupported on databases larger than the maximum commit size ([decision 10](#10-limits-accepted-in-the-first-version)).
- The engine depends on a project whose commits are concentrated in one account. The pinned version, the panic rule and the storage layer boundary are what make replacing it possible.
- A late candidate can force an extra takeover, and takeover latency is not bounded ([decision 9](#9-takeover), [decision 10](#10-limits-accepted-in-the-first-version)).
- The storage contract must state the requirements in [decision 3](#3-durable-layout-in-object-storage-per-database) before any backend is relied on.
- Retirement of the commit log must not be enabled before the tests for decision 8 exist and pass.
- 0001's choice to embed Litestream is superseded. Its rejection of Litebase stands.
- The [architecture page](/docs/architecture/) and the [roadmap](/docs/roadmap/) follow this decision.

## What the engine's tests must prove

The probes behind this record are exploratory code that is not in this repository. Nothing below may be relied on until a test in the engine proves it. Each test runs against the real virtual file system of `ncruces/go-sqlite3` at the pinned versions, and on emulators until real-cloud testing is funded, and says so. Where a test checks data, it checks it against an oracle: a plain SQLite database that receives the same acknowledged transactions.

### Commit capture and failure

- Exactly one durable commit object per intended transaction, and no object for anything else, across WAL header syncs, checkpoint syncs, sector padding, spilled frames, overwritten frames, savepoint rollback, frame numbers reused after a rollback, dead frames past the commit frame and checksum-only rewrites after the commit frame. The WAL header sync after each WAL restart is not taken for a commit. The commit-frame layout is checked with and without powersafe overwrite.
- A commit known not to have entered the durable history never reappears, after a crash or after every connection closes and the database reopens. This covers each failure and each short write at each boundary (WAL write, WAL sync, upload, WAL truncate and its sync), each followed by a reopen and by a process kill, and a crash at each step of the truncation.
- No error from the commit point is turned into success. Every error code the storage layer can return surfaces to the caller as an error.
- The commit point never returns `BUSY`, whatever fails inside it. No failed `COMMIT` leaves a transaction open. No transaction is committed under two transaction numbers, and no transaction number is ever given a second, different commit object.
- Bootstrap: a database created by the engine opens in WAL mode with no write to page 1 outside the WAL, and a database already in object storage opens the same way. A main-file write that is not part of a checkpoint is an error, not a silent local change.
- Every journal-mode, locking-mode, `synchronous` and alternative-output setting a client can reach through SQL (pragmas, `ATTACH`, `VACUUM INTO` and any other route the lockdown leaves open) is either refused or leaves capture intact.
- A process is killed at every step of a commit: before the upload, during it, after storage has accepted it but before the response, and after the response but before `COMMIT` returns. Every acknowledged write survives. A commit that storage accepted but that was never acknowledged may appear in the history, and if it does, it appears whole.
- The local WAL is on a real disk, and tests inject short writes, a full disk, and a failed truncate or fsync.
- A real process crash, not a simulated one, leaves the shared-memory file in place, and the restarted node still wipes and ignores it.
- Automatic checkpointing runs while commits are being captured.
- Failures after publication: an out-of-memory in the shared-memory index after a successful upload, with a second connection open, stops the whole database on that node, and the second connection never serves a read from the old image afterwards. The same for every other panic and unexpected SQLite error in the commit path once an upload may have started.
- A panic in a connection outside the commit path, including one that holds the WAL write lock or is running a checkpoint, leaves every other connection reading correct data against the oracle. If this fails, the fail-stop rule extends to every panic.

### Commit outcomes

- A create whose response is lost is repeated with identical bytes. A repeat that succeeds is reported committed. A repeat that fails its precondition reads the name: the leader's own commit, retired or not, reports committed; an epoch bump fences the leader and reports not committed. Each case is tested with the name's object retired before, during and after the read.
- A non-bump at the leader's next name whose epoch, transaction number, commit identifier or bytes are not the leader's, and a precondition failure followed by a read that finds nothing, each stop the node with unknown.
- An indeterminate commit, a failed WAL truncation and an error in the commit path after an upload may have started each stop the node for that database and discard its local state. The node serves again only through takeover. A create left outstanding by such a node and landing later lands only at the head plus one.
- A client is told exactly one of committed, not committed or unknown, and never not committed for a commit that is in the history. A commit identifier resolves to committed for the life of the database, retired or not, from a starting transaction number at or below it.
- A manifest replacement with an unknown outcome is resolved as decision 6 says, for the epoch swap and for compaction. Two candidates that build byte-identical manifests apart from the nonce: only the one whose swap landed proceeds, also when both saw an unknown outcome.
- A candidate whose swap or bump create has no definite answer never serves. A leader whose compaction replacement has no definite answer keeps the output and leaves office.

### Invariants

- Readers, followers, candidates and the compactor reject a gap, a failed checksum, a wrong database identifier, an object whose header does not match its key and an undecodable object, and never apply a later commit without every earlier one.
- Every log and base object is written under the identifier of the database the actor was working on. A request delayed across a re-creation under a new identifier lands in the old namespace.
- A read transaction that meets a retired commit above its manifest's number, a changed version token on a log object it indexed, or a missing base object fails or restarts from a new manifest, and never returns a mix of old and new data.
- Transaction numbers and epochs at their maximum value stop the database instead of wrapping.

### Reads

- An upload held indefinitely while existing and newly opened connections read, both at first initialisation of the shared-memory index and with an established one, and ending in success, in definite failure and in unknown: no connection sees the transaction before its upload has succeeded, and none sees a failed one.
- Reads against the oracle through partial backfill, failed page writes during a checkpoint, a failed truncate and a failed sync, checkpoints held back by readers, writer restart, `RESTART` and `TRUNCATE` checkpoints and the last connection's close, with cache eviction, return each page as of the base version, or as written locally by a checkpoint since then. A required local page that has been removed makes the read fail; it never falls back to older base bytes.
- The size reported for the main file is the size of the checkpointed image.
- Automatic checkpointing overlapping commit capture leaves the base version and the locally held pages correct. The leader never compacts past its base version.
- Follower invalidation, checking the rows returned against the oracle: prepared statements cached across versions, schema changes, the database shrinking and growing, `VACUUM` on the leader, concurrent connections on the follower, a connection pinned to one version while others move, and a change counter about to wrap, which closes and reopens the connection instead of presenting a value already used. The follower's storage layer presents the header fields in decision 7.
- A reader whose base objects were deleted gets an error, never wrong data.

### Limits and size

- The local WAL bound is enforced while a transaction's frames spill, including at the commit frame: the transaction rolls back, nothing is uploaded, and the next transaction works.
- The WAL grows under a long reader on the leader. The leader refuses writes at the configured bound and recovers once the reader ends.
- A transaction above the maximum size fails with a clear error and leaves nothing in storage. `VACUUM` on a database above the maximum commit size fails that way.
- The tail bound counts epoch bumps. A tail filled by failed takeovers is cleared by one completed takeover and one compaction.
- A compaction that would produce a manifest above the maximum size is refused, and the database reports the condition.
- Databases above 1 GiB work. SQLite reserves the page that holds its lock bytes at the 1 GiB offset ([os.h#L147-L160][sq-pending]), and the reader, the commit log, compaction and retirement must all handle that page.
- Each connection's memory ceiling is set.

### Fencing, takeover and garbage collection

- A takeover downloads no database content: at more than one database size, with the same log tail, it reads only the lease, the manifest and the log tail. It never lists the log.
- Every sequence under [Interleavings considered](#interleavings-considered) is reproduced with the actors paused before and after each of their storage requests, and ends as that section says.
- After a takeover, a stale leader cannot get acknowledged a transaction that is not in the accepted history, cannot make its compaction reachable, and cannot delete an object that any later manifest references.
- Garbage collection never deletes a log object, never retires an epoch bump or an object above the transaction number of a manifest it read or published with a definite success, and a stale collector's retire, delayed for any time, lands only on an object the current base covers.
- An unreferenced base object is deleted only under the rule in decision 8, never while the manifest version its publication could have been conditioned on still exists, and a writer never creates a base key it has issued a delete for.
- A leader that reads a manifest with a higher epoch than its own stops compacting and leaves office.
- A candidate paused at each step of a takeover, for long enough that the history has moved on, been compacted and been retired, never acknowledges a write.
- A late candidate that wins its bump at the live leader's next name fences that leader, never serves, and a further takeover restores service.
- An old leader that ignores the lease never breaks the history. Only the takeover's progress may suffer.

### Storage backends

- Each backend's own tests show the requirements in decision 3: read-after-write for single-object reads, reads after a precondition failure, no absent key during a replace, version tokens that never repeat, ranged reads of the reported version, and no correctness read through a cache or secondary endpoint.

### Shared-memory index

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
[gcs-request-rate]: https://docs.cloud.google.com/storage/docs/request-rate
[s3-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
[s3-upload]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
[az-putblob]: https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob
[format-pr]: https://github.com/RandomCodeSpace/skylite/pull/7
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
