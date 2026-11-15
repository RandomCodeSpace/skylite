---
title: "0004: Build skylite's own storage engine"
description: "skylite will build its own storage engine on ncruces/go-sqlite3, with a commit log whose names are never freed, a paged base and a manifest in object storage, and will not use Litestream in the product."
weight: 4
---

**Status:** Accepted on condition. It supersedes the choice of Litestream in [0001](/docs/decisions/0001-litebase-evaluation/).

**Amended by [ADR 0005](/docs/decisions/0005-client-protocol/#requirements-on-the-engine-and-the-format)** in two points: a lookup that reaches the head without a match or a bump is answered from the leader's channel table, and the scan bound is replaced by 0005's resolve bound ([decision 6](#6-commit-outcomes-and-fail-stop)); and during maintenance the leader refuses new read transactions and ends running ones instead of waiting them out ([decision 10](#10-limits-accepted-in-the-first-version), steps 1 and 5).

The condition: the SQLite-side mechanisms in decisions [5](#5-commits-are-captured-from-sqlites-write-ahead-log) and [7](#7-reads) (capture from the write-ahead log, the failure-and-truncate path, reader visibility during an upload, checkpoint and main-file coherence, bootstrap, follower invalidation, failures after publication, and the engine-driven WAL reset of [decision 10](#10-limits-accepted-in-the-first-version)) rest on exploratory probes or on design alone, not on tests in this repository. The first engine pull requests must land the tests in [What the engine's tests must prove](#what-the-engines-tests-must-prove) for those mechanisms, against the real virtual file system of `ncruces/go-sqlite3` v0.37.0 with SQLite 3.54.1. If any of them fails, the design moves to the fallback recorded in decision 5 (write-ahead log with exclusive locking, one connection), or comes back to this record.

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

Each round of independent design review of earlier drafts found a new break of one kind: a name that one actor had used, or that garbage collection had freed, was used again by another, so that a leader got a write acknowledged that no reader would see, a reader assembled a state that never existed, or the manifest came to reference a deleted base object. The earlier drafts added a rule each time to contain it. This revision removes the cause instead: no name in the commit log is ever freed, the content of a base key never changes, and base objects are deleted by one rule that does not depend on who created them ([decision 8](#8-compaction-retirement-and-garbage-collection)). It also states what it needs from storage per kind of key, instead of one requirement for every key ([decision 3](#3-durable-layout-in-object-storage-per-database)). The sequences the reviews found are replayed against this design under [Interleavings considered](#interleavings-considered).

Implementing takeover, and its review, amended decisions 3, 6, 9 and 10, invariant 2 and the test lists: the lease's rules, epochs along the log, how a candidate recognises its own bump, the walk that finds a retired commit above its own manifest, and how much a lost race may read.

Implementing the read path added seven rules, each in the decision it belongs to: what a commit that shrinks the database changes and how compaction rewrites it (decisions [3](#3-durable-layout-in-object-storage-per-database) and [8](#8-compaction-retirement-and-garbage-collection)), how a creator recognises its manifest and leaves an existing database alone (decision 3), the probe above the head ([invariant 4](#4-invariants)), reads below the base ([decision 7](#7-reads)), cache hits ([invariant 5](#4-invariants)) and the bounds on a reader's walk ([decision 10](#10-limits-accepted-in-the-first-version)).

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
<prefix>/<identifier>/log/<transaction>                 commit-log object
<prefix>/<identifier>/base/<group>-<transaction>-<tag>  base group object
<prefix>/manifest                                       manifest
<prefix>/lease                                          lease
```

The manifest and the lease stay at fixed keys under the prefix. The manifest is where a reader learns the identifier. Every create, replace and delete that an actor issues for a log or base object therefore lands in the namespace of the identity it was working on, however late it lands. The `<tag>` of a base key is a random 64-bit number drawn by a compaction attempt for its output, written as 16 lowercase hexadecimal characters ([decision 8](#8-compaction-retirement-and-garbage-collection)). The format package decides the exact encodings of the numbers in the keys.

- **Commit log.** One object per transaction number, written with create-if-absent. It holds the changed pages, the database size after the commit, the leader's epoch, the database identifier, the commit identifier and a checksum. The commit identifier is composed by the server, is 24 bytes in this version, and identifies the transaction to a client that asks about it later ([decision 6](#6-commit-outcomes-and-fail-stop)); the client protocol decides how it is composed. An epoch-bump commit holds no pages and no commit identifier. The first commit of a database, which the engine builds itself ([decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log)), holds the pages of the empty database and no commit identifier, because no client made it. A transaction that changes no page creates no commit object and takes no transaction number ([decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log)). That single create is the commit point, the acknowledgement point and the fence for commits. **A name in the log, once created, is occupied for the life of the database:** nothing ever deletes a commit-log object. Once the base covers a commit, garbage collection replaces it with its **retired form** ([decision 8](#8-compaction-retirement-and-garbage-collection)), which keeps the commit's header (database identifier, transaction number, epoch, the epoch-bump flag and the commit identifier) and drops the page table and the pages. Epoch-bump commits are never retired. So the header at any transaction number up to the head, live or retired, can be read directly from its key, without a walk or a listing, and gives the commit identifier, the epoch and whether the commit is an epoch bump.
- **Base.** The database's pages, in fixed-size groups of consecutive page numbers. Each group is an immutable object whose key carries its group, the transaction number it is current to and the tag of the attempt that created it. A page's group follows from arithmetic on its page number, so the index needs memory in proportion to the number of groups, not the number of pages. A commit whose database size is below the size before it changes every group holding a page above the new size. A page above the smallest size of any commit since the manifest's transaction number, and not written since, reads as zeros.
- **Manifest.** One object, updated only by compare-and-swap. It holds the database identifier; one reference per group, which is the pair (transaction number, tag) of that group's base object; the transaction number the base is current to; the **retired-through** transaction number ([decision 8](#8-compaction-retirement-and-garbage-collection)); the epoch; and a 16-byte random **writer nonce** set by the candidate at its epoch swap and kept by that leader's later compaction manifests ([decision 6](#6-commit-outcomes-and-fail-stop)). Compaction and the epoch swap of a takeover change it. Lease updates and garbage collection do not.
- **Lease.** An object updated by compare-and-swap, used only so that a takeover makes progress. The lease names its holder, the epoch (the manifest's epoch plus one) and the holder's clock at its last renewal. A node treats it as expired when its clock reads at least the time to live after, or more than the time to live before, the renewal. A lease with the writer's own holder name and epoch found after a precondition failure is the writer's own write. A lease that is not decodable is replaced, by compare-and-swap on its version, unless it is a newer version, which is refused and never overwritten. Release rewrites it with renewal time 0. Two nodes holding one lease, clocks that differ by any amount and a holder name used by two processes change only how many candidacies run: the swap and the bump fence.

In this version every object must fit in one upload request on every backend. That is a restriction of this version's implementation, not a property of staged uploads ([decision 10](#10-limits-accepted-in-the-first-version)). The format package sets the group size and the maximum commit size with it in mind.

**The manifest has a maximum size.** The format sets it, below the single-upload limit of every backend. Compaction refuses to produce a manifest above it. The database then stops compacting and reports the condition, and the leader refuses writes once its uncompacted log reaches its own bound. This fails closed. A manifest format that scales further is later work. The arithmetic, with illustrative sizes that the format will fix: a group entry of 32 bytes (an 8-byte transaction number, an 8-byte tag and a 16-byte checksum) with groups of 4 MiB gives 23,842 entries for a 100 GB database, 762,944 bytes. The manifest does not grow with takeovers.

**Creating a database.** A creator first reads the manifest and, if one is present, uses that database and writes nothing. Otherwise it draws a new identifier and creates the first commit object, at transaction number 1, under that identifier: the empty WAL-mode database of [decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log), at epoch 1. Only after that create has resolved as its own ([decision 6](#6-commit-outcomes-and-fail-stop)) does it create the manifest, with create-if-absent: the identifier, transaction number 0, retired-through 0, epoch 1, a new writer nonce and no base references. Exactly one manifest create succeeds, and its identifier is the database. After an unknown outcome or a precondition failure, a creator recognises its own manifest by its identifier alone: only it ever knew the identifier, and the nonce changes at the first epoch swap. Any other manifest means it lost, and it uses the database that won. A creator that cannot resolve either create gives up. A losing or abandoned creator's objects stay under an identifier that no manifest names; they are a leak of one page plus 164 bytes per creator that races or crashes, and reclaiming them is part of the deletion work below. The first manifest references no base object, so it cannot reference incomplete output. Nobody serves at epoch 1: every node, the creator included, serves only through the takeover path of [decision 9](#9-takeover).

**Deleting or recreating a database is not designed in this record.** Whatever does it later must give the new database a new identifier. Reusing a prefix by wiping it by hand while any process of the old database may still run is unsupported: a delayed request from such a process can still create the manifest or the lease at the shared keys.

**What the design requires of storage.** These are requirements on every backend behind the storage interface, stated by kind of key. The safety arguments below assume them and nothing stronger. The provider pages cited here were read on 2026-11-15; a documentation page cannot be pinned to a revision, so each citation is the page as it stood that day.

What each kind of key goes through, and what it needs:

- **Manifest.** Created once, then replaced, never deleted. It needs two things. A replace conditioned on a version token succeeds only if that token is the key's current one. And the key's token never returns to an earlier value. Per backend:
  - Google Cloud Storage: the token is the object's generation. "An object version's generation value is unique", and a new object with the same name "has a different generation value" ([Object metadata][gcs-meta]). A replace conditioned on `ifGenerationMatch` fails with 412 when the generation differs ([Request preconditions][gcs-precond]).
  - Azure Blob Storage: the token is the ETag. Azure assigns an identifier to every object that "is updated every time a write operation is performed on an object", and with `If-Match` "verifies that the value of the ETag specified in the update request is the same as the ETag for the object being updated" ([Manage concurrency in Blob Storage][az-concurrency]). Whether an ETag can return to an earlier value across successive overwrites is not documented either way. This record states it as an **assumption**: across the overwrites of one blob, an ETag does not return to an earlier value.
  - Amazon S3: the token is the ETag, which `If-Match` compares ([conditional writes][s3-cond]). For a single `PutObject` with SSE-S3 or no encryption, the ETag is "an MD5 digest of their object data"; with SSE-C or SSE-KMS it is "not an MD5 digest" ([Object][s3-object]); the conditional-writes page says it "reflects a change to the object's content". The token is therefore content-derived, and two writes of the same bytes may share it. The manifest's bytes never repeat: every replacement raises the transaction number (a compaction) or the epoch with a new nonce (an epoch swap), and neither ever decreases ([invariant 1](#4-invariants)). So the token cannot return to an earlier value unless two different contents have the same ETag. This record accepts as an **assumption** that no two different contents ever written to the same key have the same S3 ETag: for MD5, short of a hash collision; for the non-MD5 ETags, that S3 gives different content a different ETag, which the pages above imply but do not state. The assumption covers two kinds of pair. Successive manifests: if two shared an ETag, an old manifest token could become current again (consequence below). A log object's live and retired forms: if they shared an ETag, a reader that pinned the live form's token would not see the retirement in its version check ([invariant 5](#4-invariants)), and whether its next ranged read failed safely would rest on other checks, such as page checksums, which this record does not establish; a delayed retire would also succeed a second time, writing the same retired bytes, which is harmless.
  - Local filesystem: the storage contract requires a token that differs from every earlier token of the key; that backend's tests must show it.

  If either manifest assumption is false, an old manifest token can become current again, and a delayed compaction conditioned on it can then overwrite a newer manifest with an older transaction number and references to base objects that have already been collected. Neither a fresh read nor a nonce inside the manifest stops a request that was already sent; removing the assumptions would take a different publication protocol or stronger backend conditions, which this version does not attempt.
- **Commit-log objects.** Created once, replaced at most once, by the retired form, and never deleted. The only conditional replace of a log object is the retire, conditioned on the live form's token. The retired form's token differs from the live form's: on Google Cloud Storage because generations are unique, on Azure Blob Storage under the assumption above, on Amazon S3 under the assumption above, because the two forms differ in bytes, and on the local filesystem by its contract. The live form's token never comes back, because the object is never written again.
- **Base objects.** The content of a base key never changes: it is created if absent, never replaced, and deleted only once it is garbage ([decision 8](#8-compaction-retirement-and-garbage-collection)). After a delete, a delayed create can make the key reappear, and only with the same bytes, because a base object's bytes are a function of its key and of the commit log: the database identifier, group and transaction number in the key fix the pages, and the format puts nothing in a base object that depends on which attempt or which leader wrote it, such as an epoch, a nonce or a time. A reappeared object is still garbage. Nothing is ever conditioned on a base object's token. *Correction marked by this record's author:* an earlier statement of this rule said a base object is "deleted at most once, never created again"; a delayed create landing after the delete makes that untrue, and no argument needs it.
- **Lease.** Used only for progress. Nothing about its tokens is relied on for safety.

No argument in this record relies on a version token across the deletion and re-creation of a key, for any kind of key.

Reads and writes, for every kind of key:

- A read started after a write completed reflects that write or a later one. Amazon S3 "provides strong read-after-write consistency for PUT and DELETE requests" ([What is Amazon S3?][s3-consistency]); Azure Storage guarantees "that after the service performs an insert or update operation, subsequent read or list operations return the latest update" ([Manage concurrency in Blob Storage][az-concurrency]); Google Cloud Storage provides "strong global consistency" for object read-after-write, read-after-delete and listing ([Cloud Storage consistency][gcs-consistency]).
- Responses are not assumed to arrive in request order. An older read can complete after a newer one. A decision to publish, to retire or to collect uses a read started after the observation it depends on.
- After a conditional write fails its precondition, a read started after the failure sees the object that made it fail or a later version, or, for a key that can be deleted, may find it absent. Only the never-deleted log guarantees presence. This follows from read-after-write and the atomic condition check; it is not a separate provider promise, and each backend's tests must show it.
- An object becomes visible atomically and complete. A replace is one atomic overwrite, never a delete followed by a create, so it never makes its key read as absent. Amazon S3 documents that "Updates to a single key are atomic" ([What is Amazon S3?][s3-consistency]).
- A ranged read returns bytes of the version it reports. The storage contract requires an open body to belong to one version.
- Correctness reads never go through a cache, a content-delivery network, a public endpoint or a secondary replica. Google Cloud Storage warns that "Cached objects that are publicly readable might not exhibit strong consistency" ([Cloud Storage consistency][gcs-consistency]).

**The storage contract promises more than this.** The contract on the storage-interface branch promises callers a version token that differs on every write, including for identical content and across deletion and re-creation. On S3 that means the backend must make each stored object's bytes unique per write, for example with a random prefix it adds on write and strips on read. That is a requirement on the S3 backend, not a design matter here: this record does not depend on it.

### 4. Invariants

These are requirements on the engine and the format. Every other decision in this record assumes them.

1. **One number sequence, one epoch per candidacy.** Transaction numbers are one sequence across all epochs, and an epoch-bump commit consumes a number. An epoch is assigned only by a successful compare-and-swap of the manifest to the previous epoch plus one, or, for epoch 1, by the create of the first manifest, so each epoch belongs to one candidacy and is never reused. A candidacy ends with its process: a restarted node never continues at an old epoch. Arithmetic on transaction numbers and epochs never wraps: reaching the maximum value stops the database, fail closed.
2. **Log names are never freed, and the log is a contiguous prefix.** No commit-log object is ever deleted. An actor creates the object at transaction number *k* + 1 only after it has observed the object at *k*, or read a manifest whose transaction number is *k*; the first commit, at 1, is created before any manifest exists ([decision 3](#3-durable-layout-in-object-storage-per-database)). So when any create at *k* + 1 succeeds, the object at *k* exists, and the occupied names are always exactly 1 to the head. A create that lands late, however late, can land only at the head plus one. A leader builds a second, different object for a number it has already tried only when every attempt to create the first definitely had no effect ([decision 6](#6-commit-outcomes-and-fail-stop)), so no request for the first is outstanding that could land later.
3. **Database identity.** The identifier is in the key of every log and base object, in the manifest and in the header of every commit and base object. A reader rejects any object whose identifier differs from the manifest's, and any object whose header does not match its key.
4. **The head is found by walking.** The head of the log is the last transaction number reached by walking contiguous numbers upward from the manifest's, by direct reads. The log is never listed. Readers, followers, candidates and the compactor all reject a gap, a failed checksum, a wrong identifier or an undecodable object among the records they need. They never skip a record and never apply a commit without every commit before it. Epochs along the log never decrease, because a late bump fences only a leader of a lower epoch than its own ([decision 9](#9-takeover)), but no reader may require that they do. A reader looking for the head also reads the name one above the first absent one: if that name is present and the first is still absent, the first is a gap, and the reader reports corruption. A gap higher above is not seen, and the reader serves the prefix below it. A reader asked for a transaction that is present above an absent name reports corruption; one asked for a transaction that is itself absent is beyond the head.
5. **No mixing after retirement or deletion.** Readers pin the storage version token of each log object they index and check it on every later read of that object. A reader that finds a retired commit above its manifest's transaction number, whose later read of a log object reports a different version, or whose base object is missing, abandons the read transaction, or restarts it from a newly read manifest. It never substitutes newer data into a transaction that has already read older data. A cache hit returns the bytes of the version the reader pinned and is not a storage read; the version check applies to the reads that reach storage.
6. **Three client outcomes.** A client is told exactly one of committed, not committed or unknown ([decision 6](#6-commit-outcomes-and-fail-stop)).

### 5. Commits are captured from SQLite's write-ahead log

On the leader, SQLite runs in WAL mode with a file-backed shared-memory index. The WAL and that index are ordinary local files. The main database file is virtual: the storage layer serves it from the local cache and from object storage.

- **Capture point.** The storage layer builds the commit object from the WAL frames at the WAL sync that ends a commit, and uploads it before the sync returns. `synchronous=FULL` must be set explicitly, because ncruces compiles SQLite with the WAL default set to `NORMAL` ([sqlite_opt.h#L5][nc-opt]). In SQLite's source the sync comes before the new frames are added to the shared index and before the index header is published ([wal.c#L4281-L4346][sq-walframes]). *Probe, not reproducible:* with `synchronous=FULL`, SQLite synced the WAL exactly once per commit, after the commit frame and before `COMMIT` returned; under `NORMAL` it did not sync at commit. Test: [Commit capture and failure](#commit-capture-and-failure).
- **A transaction that changes no page creates no commit object.** The storage layer creates a commit object only at a WAL sync that ends a commit, from the frames before it. In SQLite's source, a transaction that has not made any page writable returns from the first phase of commit before writing anything ([pager.c#L6550-L6551][sq-nochange]), so no frame is written and no commit object is created. A statement that makes a page writable and leaves its bytes unchanged still produces frames and a commit object (inference from the same function, not probed). *Unproven.* Test: [Commit capture and failure](#commit-capture-and-failure).
- **No SQL can bypass capture.** No client SQL may turn off `synchronous=FULL`, change the journal mode or the locking mode, write to an alternative output, or otherwise route a write around capture. The SQL lockdown enforces this. *Unproven.* Test: [Commit capture and failure](#commit-capture-and-failure).
- **Known failure.** If the commit object is known not to have been stored, because every attempt to create it, the backend's own retries included, had no effect ([decision 6](#6-commit-outcomes-and-fail-stop)), the storage layer truncates the WAL back to the end of the last uploaded commit, inside the failing call, and returns an error. SQLite then rolls back and the node keeps serving: once the truncation has completed, the leader builds its next transaction for the same transaction number ([decision 6](#6-commit-outcomes-and-fail-stop)). *Probe, not reproducible:* IOERR, IOERR_FSYNC, a plain Go error and FULL from the sync each produced an error to the caller and a rollback, and none made SQLite report success. Test: [Commit capture and failure](#commit-capture-and-failure). An upload of which any attempt had an unknown outcome is handled by [decision 6](#6-commit-outcomes-and-fail-stop).
- **Why the truncation is required.** After a failed sync, the frames and the commit marker are still in the WAL with valid checksums, and SQLite's recovery accepts every valid frame up to the last commit marker ([wal.c#L1534-L1556][sq-recover]). *Probe, not reproducible:* a failed transaction reappeared after a reopen whenever the WAL was kept, and did not with the truncation. Test: [Commit capture and failure](#commit-capture-and-failure).
- **Visibility.** Other connections must see a transaction only after its upload has succeeded. *Probe, not reproducible:* readers on other connections saw only the old value while an upload was blocked for 300 ms, and never saw a failed commit. Test: [Reads](#reads).
- **Checkpoints.** A local WAL checkpoint writes only to the local cache, because its pages are already in object storage. *Probe, not reproducible:* every page a checkpoint wrote was byte-identical to a page already uploaded, with the exception below. Test: [Reads](#reads). How the leader reads the main file around checkpoints is in [decision 7](#7-reads).
- **Start-up.** Local files are wiped when a node starts. They are never a source of truth.

Two behaviours qualify the description above. The engine must handle both:

- **`BUSY` does not roll back an explicit `COMMIT`.** If the commit point returns `BUSY` inside an explicit transaction, `COMMIT` fails with "database is locked" and the transaction stays open with the write lock held, and a retried `COMMIT` writes the frames again ([vdbeaux.c#L3438-L3446][sq-busy1], [vdbe.c#L4197-L4202][sq-busy2]). *Probe, not reproducible:* the probe saw this sequence. The decision: the storage layer never returns `BUSY` from the commit point. Every failure there is reported with an error code that rolls the transaction back. Test: [Commit capture and failure](#commit-capture-and-failure).
- **Entering WAL mode writes page 1 outside the WAL.** `PRAGMA journal_mode=wal` changes page 1 through a rollback-journal transaction, whatever the journal mode ([vdbe.c#L8302-L8307][sq-jm], [btree.c#L11572-L11600][sq-ver]). That write never passes through WAL capture. *Probe, not reproducible:* on a new database the first WAL commit sent page 1 again, so storage ended up correct by accident. The decision (bootstrap): the engine does not create a database through WAL capture. It builds the first commit object itself, holding an empty database whose header already says WAL mode, so that opening it never needs that write. The database identifier ([invariant 3](#4-invariants)) is created at the same step. Test: [Commit capture and failure](#commit-capture-and-failure).

WAL mode with exclusive locking, which allows one connection and needs no shared-memory index ([vfs/README.md#L69-L74][nc-walreadme]), is the recorded fallback if the shared-memory route fails the engine's tests.

### 6. Commit outcomes and fail-stop

A storage request can fail without saying whether it took effect: a timeout, a dropped connection or a server error after the request was sent. The storage interface classifies every failure of a mutation as no effect or as outcome unknown. When a backend's answer is not documented as no effect, it is unknown.

**Uncertainty is sticky.** A "no effect" answer is about one attempt. A logical operation (a commit create, an epoch-bump create, a manifest replacement, a retire, a base object create) may be sent as several attempts, by the engine or inside a backend's own retries. Once any attempt of it has had an unknown outcome, later attempts that had no effect do not resolve the operation, because the earlier attempt may still land. The operation then resolves only by success (an attempt that succeeded, or a read that shows the operation's own content), or by its conditional predecessor being permanently displaced (a log name occupied by another object, or a manifest token that is gone for good), as the rules below describe; otherwise it stays unknown. Every "known not to have taken effect" in this record means that every attempt had no effect. The same holds inside a backend: the storage contract already requires a backend that retries to classify the whole call, not its last attempt.

**A transaction number is bound to one object.** Each transaction number the leader tries is bound to the exact bytes of the commit object the leader built for it, and the leader does not try the next number until this one has a definite answer. The binding is released in one case only: every attempt to create that object, the backend's own retries included, definitely had no effect, and the local rollback, the WAL truncation of [decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log), completed. Then no request for the object exists that could still land, the local image holds nothing of it, the client is told not committed, and the leader builds its next transaction for the same number. In every other case the binding stands for the life of the leader process: the leader never builds a second, different object for that number, and the fail-stop below applies when the create cannot be resolved or the truncation fails. Uncertainty stays sticky: once any attempt has had an unknown outcome, the binding is never released, whatever later attempts report.

**A leader's head.** A leader's head, the number it builds its next commit on, is exactly its own last commit: at takeover its own epoch bump, and afterwards its latest commit resolved as committed. A definitely failed commit leaves the head where it was. A leader is deposed once an epoch bump of another candidacy occupies the name after its last commit in the log, which every takeover's bump and every late candidate's bump does ([decision 9](#9-takeover)). From then on no further commit of the deposed leader enters the log: it has at most one create outstanding, always for the name after its last commit in the log, and that name is occupied for good. Its head can still rise once, to a commit that was already in the log below the bump: if the response to its last create was delayed, that commit landed before the bump did, and the leader resolves it as committed when the response arrives. Its head never reaches or passes the bump.

**Who can occupy a leader's next name.** A leader creates an ordinary commit at a name only while it holds the name before it, as its own commit or its own epoch bump. A leader's last commit *n* is its own object, and the name *n* is never freed ([invariant 2](#4-invariants)), so the object at *n* is that leader's for the life of the database. No other leader can therefore hold *n*, and no other leader creates an ordinary commit at *n* + 1. The first object ever created at *n* + 1 is this leader's own commit or some candidate's epoch bump, and because the name is never freed, the first object is the only object. This is a design argument. Test: [Commit outcomes](#commit-outcomes).

**Resolving a create.** After a create whose outcome is unknown, the leader repeats the identical create until it gets a definite answer, up to a bound the engine sets. The definite answers are:

- **Success.** Committed.
- **Precondition failed, then a read of the name finds a commit that is not an epoch bump.** By the argument above it is the leader's own, so the answer is committed, whether or not it has been retired since. The leader also checks that the object's transaction number, epoch and commit identifier are its own and, if the object is not retired, that its bytes are the ones it built. A mismatch means an invariant is broken: the leader stops as for an indeterminate commit and the client is told unknown.
- **Precondition failed, then a read of the name finds an epoch bump.** The leader is fenced, and the commit is definitely not committed: the bump is the only object that will ever be at that name.

A precondition failure followed by a read that finds nothing cannot happen under the storage requirements in [decision 3](#3-durable-layout-in-object-storage-per-database), because nothing deletes a log object. If it happens, an invariant is broken, and the leader stops as for an indeterminate commit. An object that is not a well-formed commit of this database at this name, and a bump at the leader's own epoch, are invariant breaks and stop the node with unknown.

The same steps apply to the first attempt at a create: success means committed, and a precondition failure is resolved by reading the name. On a first attempt only an epoch bump can be there.

**Indeterminate commit.** A commit is indeterminate only when the leader cannot get a definite answer from storage within the bound. The outstanding create may still land later. If it does, it lands at the head plus one ([invariant 2](#4-invariants)), as a valid continuation of the leader's own last commit, and the commit is then in the history.

**Fail-stop.** On an indeterminate commit, and whenever the leader is fenced, the node stops serving that database, closes every connection, discards all local state for it, and can come back only through the takeover path, which reads durable history. A node that fail-stops issues no further write for the database, its lease included. The same fail-stop applies in these cases, because storage may hold a commit that the local image does not reflect, or the local image may hold one that storage does not:

- The storage layer cannot truncate its local WAL after an upload known to have failed.
- **Any panic, or any SQLite error the storage layer did not itself return, in the commit path once an upload may have started.** The commit path runs from the moment the storage layer begins the create for a commit object until `COMMIT` has returned on that connection. It includes adding the frames to the shared-memory index, publishing the index header and the second phase of commit. For example, SQLite can run out of memory while adding frames to the shared-memory index after the upload succeeded and before it publishes the new header. ncruces turns that into a panic ([conn.go#L493-L495][nc-nomem]). Discarding only that connection would leave the other connections on a local image that does not contain a durable transaction, so the whole database stops on that node.

Outside the commit path, a panic at the connection boundary discards only that connection. *Unproven:* that discarding a connection after a panic releases its locks and leaves the shared-memory index consistent for the other connections. Test: [Commit capture and failure](#commit-capture-and-failure). If that test fails, the fail-stop extends to every panic.

**Manifest replacements also have three outcomes.** After a replacement of the manifest whose outcome is unknown, the actor repeats the identical replacement until it gets a definite answer, up to a bound:

- **Success.** The replacement took effect.
- **Precondition failed, then a read of the manifest returns the actor's own bytes.** The replacement took effect.
- **Precondition failed, then a read returns anything else.** The version the request was conditioned on is gone for good, because the manifest's token never returns to an earlier value ([decision 3](#3-durable-layout-in-object-storage-per-database)), so no attempt of the request can take effect from now on. It may have taken effect earlier and been replaced since; nothing in this record depends on telling those two apart, and every rule that follows asks only whether the current manifest references an object.

Two rules make "the actor's own bytes" unambiguous. Every replacement changes the manifest's bytes: an epoch swap raises the epoch and sets a new writer nonce, and a compaction raises the transaction number. And the writer nonce, drawn from a cryptographically secure random source, identifies the candidacy: two candidates that read the same manifest would otherwise attempt byte-identical epoch swaps, and both would recognise the one that landed as their own. A candidate therefore recognises its own epoch swap by its nonce, never by the epoch number alone.

A candidate that cannot get a definite answer for its epoch swap gives up the candidacy and never serves, as for its epoch-bump create. A leader that cannot get a definite answer for a compaction's replacement stops compacting and garbage collecting and leaves office through fail-stop. This rule is an addition of this record's author to the project owner's decisions; it keeps the leader from running with two possible manifests. The compaction's output needs no special keeping: the garbage rule in [decision 8](#8-compaction-retirement-and-garbage-collection) keeps it until the outstanding replacement can no longer land.

**Client outcomes.** A client is told exactly one of:

- **Committed**, when storage confirmed the create, or a read after a precondition failure found the leader's own commit.
- **Not committed**, only when the commit object is known not to be in the durable history: every attempt to create it had no effect, or a precondition failure resolved to an epoch bump at its name, or the transaction was rolled back before any create for it was sent.
- **Unknown**, in every other case. A client is never told "not committed" for an indeterminate commit.

Every transaction number skylite reports to a client, with a commit or with a read, is the number of an object that is durable in the log: a leader reports a number only for a create it resolved as committed, and readers serve only what they read from storage or what passed its upload.

Unknown is a permitted final answer. Every ordinary commit made for a client carries its commit identifier, and the retired form keeps it, so it can be found for the life of the database. The log is not indexed by commit identifier and is never listed, so a lookup reads headers by number, upward from the point where the client's channel was opened: the leader's head at the time it opened the channel, before it composed any identifier for it. Only that leader composes identifiers for the channel, and the objects above that point, up to the first epoch bump above it, are all that leader's commits, because no other leader creates an ordinary commit after a leader's own last commit. The answers:

- **A match** is final: committed.
- **An epoch bump above the opening point, reached without a match**, is final: not committed. The bump deposes the leader that composed the identifier, and no commit of a deposed leader enters the log after the bump that deposed it. Every commit of that leader is therefore below the bump, where the lookup has already looked, so the commit is not in the log and can never enter it.
- **Running past a scan bound** the engine sets, before either, is unknown. Reaching an absent name before either is not an answer yet: the leader may still be in office with the create outstanding, and the lookup reads on from there.

The client protocol decides how the opening point and the identifier are carried.

**Late acknowledgements are correct.** A leader whose commit reached storage may deliver the acknowledgement after another leader has taken office. That is correct, because the commit is in the history. Two processes may both believe they hold office at once. The design does not prevent that belief. It prevents conflicting history: no leader gets a transaction acknowledged that is not in the accepted history.

### 7. Reads

- **Lookup.** A page is looked up first in the commit log written since the manifest's transaction number, through a small in-memory index that compaction keeps bounded, and then in its base group. Fetched data goes into a bounded local cache. The index records the storage version token of each log object it covers, and every later ranged read of that object checks it ([invariant 5](#4-invariants)).
- **Below the base.** A reader serves only transactions at or above the manifest's transaction number. Below it no base exists to build the state from, and the reader reports the transaction as compacted.
- **Followers.** Followers use a read-only storage layer. It pins each read transaction to one version and moves to the newest version between transactions. *Probe, not reproducible:* a follower that did not pin mixed two versions inside one scan. Test: [Reads](#reads).

**Leader reads of the main file.** SQLite expects the main file to hold only checkpointed state. *Probe, not reproducible:* serving the newest uploaded version from the main file let a reader that began before a commit see that commit's data, which breaks snapshot isolation. The mechanism below is unproven. Test: [Reads](#reads).

- The storage layer keeps a **base version**: the last commit at which the local WAL was empty and the main file was exactly a committed state. At takeover it is the head. Afterwards it is the last commit before each point where SQLite restarts the WAL from its beginning.
- In SQLite 3.54.1, the WAL restarts from its beginning only when every frame has been backfilled and no reader is using the log. A writer restarts it only if its own read used no WAL frames, the backfill count equals the last frame, and it gets an exclusive lock on every reader slot that reads the WAL ([wal.c#L3961-L3995][sq-restart]). A `RESTART` checkpoint does not itself restart the WAL: once the whole WAL is backfilled it waits for the same reader locks, so that the next writer restarts it. A `TRUNCATE` checkpoint does the same, then resets the WAL header and truncates the file to zero bytes ([wal.c#L2435-L2471][sq-ckpt-restart]). Closing the last connection deletes the WAL only after taking an exclusive lock on the database and completing a checkpoint ([wal.c#L2610-L2629][sq-close]).
- Pages that checkpoints write to the main file after the base version, and the main file's size after them, are kept locally, and are not evicted until the base version next advances.
- A main-file read of a page not held locally fetches that page as of the base version. A page not held locally has not been written by a checkpoint since then. If a page the storage layer must hold locally is missing, the read fails. It never falls back to older base bytes.
- **The leader compacts only up to its base version.** A page as of the base version is built from the base of the current manifest and the log objects above it, so the manifest's transaction number must not pass the base version while the base version is in use; otherwise the log objects between them could be retired and the page could no longer be built. This rule is an addition of this record's author.
- A checkpoint's completion callback is not treated as proof that the checkpoint completed. Only a WAL restart advances the base version: a writer's restart, or a `TRUNCATE` checkpoint's truncation of the WAL to zero bytes. The WAL file is opened through the storage layer, so the storage layer can observe both.
- Client writes are not needed to advance it. When writes are refused at a bound, the engine resets the WAL itself ([decision 10](#10-limits-accepted-in-the-first-version)).

**What a follower's storage layer presents.** SQLite decides whether to drop its page cache by comparing bytes 24 to 39 of the file header when it takes a shared lock ([pager.c#L5452-L5482][sq-cache]). In WAL mode the change counter there moves only when page 1 is part of a commit ([pager.c#L3272][sq-cc]). *Probe, not reproducible:* a follower whose storage layer did not change those bytes returned stale cached pages. The follower's storage layer therefore presents page 1 with three fields changed:

- **Bytes 18 and 19**, the file format write and read versions, set to 1. The database then reads as a rollback-journal database, and SQLite does not open a WAL for it, which it does when byte 19 is 2 ([btree.c#L3363-L3386][sq-walver]).
- **Bytes 24 to 27**, the file change counter.
- **Bytes 92 to 95**, the version-valid-for number, set equal to the change counter. SQLite trusts the database size in bytes 28 to 31 only when those two fields are equal ([btree.c#L3328-L3332][sq-hdrsize]).

The change counter is 32 bits. The decision: it is a per-connection counter that the storage layer increments whenever the version pinned for that connection changes, and the connection is closed and reopened before the counter can wrap. *Unproven:* the probe wrote the version number itself into the counter; the per-connection counter has not been tried. Test: [Reads](#reads).

### 8. Compaction, retirement and garbage collection

**Compaction.** The leader folds the commit log into new base group objects and then replaces the manifest by compare-and-swap. Each **compaction attempt** reads the manifest, targets a transaction number *t* strictly above that manifest's and at or below the leader's base version ([decision 7](#7-reads)), draws a random 64-bit tag from a cryptographically secure source, and conditions its publication on the token of the manifest it read. Its output keys carry *t* and the tag. Its manifest carries *t*, carries forward unchanged the predecessor's references for the groups the commits up to *t* did not change, and references its own output for the groups they did. So a compaction rewrites only changed groups, not the whole base. A commit up to *t* whose database size is below the size before it has changed every group holding a page above that size, and the compaction rewrites every such group, with zero bodies for the pages above the smallest size not written since. An epoch swap carries every reference forward unchanged. A manifest never references any other object, and never re-introduces a reference that a predecessor dropped. The attempt never replaces a manifest whose epoch is higher than its own. A leader that reads a manifest whose epoch is higher than its own stops compacting and leaves office, so that the candidate that raised the epoch, or a later one, can finish a takeover. A takeover changes the manifest first, so a deposed leader's replacement, conditioned on an older version, cannot win.

**What an attempt may reference.** An actor cannot observe who created an object, so nothing in this record depends on it. Three statements carry the argument instead, and each can be checked:

- **The content of a base key never changes.** It is created if absent, never replaced, and can reappear after a deletion only with the same bytes ([decision 3](#3-durable-layout-in-object-storage-per-database)).
- **An attempt references a new object only on evidence of its bytes.** It references an object only if its own create of it succeeded, or if its create failed its precondition and a read of the key, started after that failure, returned bytes identical to the ones it meant to write. Otherwise, including when that read finds the key absent or different bytes, and when the create cannot be resolved, the attempt draws a new tag for that group and creates it under the new key; an outstanding request for the old key may land or not. The attempt replaces the manifest only after every object it newly references has passed one of those two tests.
- **The garbage rule does not depend on who created an object** (below).

By the first two, every object a manifest references holds exactly the bytes its referencing attempt meant to write, which are the group's pages at the key's transaction number, whichever attempt's request put them there. The tag is not what makes this safe. Its purpose is to make a collision between attempts improbable, so that a precondition failure nearly always comes from the attempt's own earlier request, the byte comparison is rare, and overlapping attempts do not block or interfere with each other's output.

An attempt whose replacement is known not to have taken effect, or whose predecessor is gone, ends. Its output is left to the garbage rule below. The leader can start a new attempt at once, at the same target transaction number if the manifest is still below it, with a new tag: a key already present is resolved by the byte comparison, so nothing waits for earlier output to be collected, and no leader can be stranded at a transaction number.

**Retirement replaces deletion in the commit log.** Garbage collection never deletes a commit-log object. It retires one that the base covers. The rule:

- The collector retires the object at transaction number *k* only if *k* is at or below the transaction number of a manifest it read from storage, or of a manifest replacement of its own that got a definite success; the object is not an epoch bump; and the collector read the object and found it unretired. The retire is a replace conditioned on the version token the collector read.
- Retirement honours a grace period after the manifest that covers the object, so that readers still using older manifests usually finish. That is for reader progress only. Safety does not depend on it: a reader that does not finish in time is handled by [invariant 5](#4-invariants).
- A retire whose outcome is unknown needs no resolution for safety: it is permitted whenever it lands. A retire is confirmed by a read of the object that returns the retired form. The retired form is final, so that read is definite whoever's retire landed.

**The retired-through cursor.** The manifest carries a transaction number, retired-through: every log object at or below it is retired or is an epoch bump. It never decreases and is at most the manifest's transaction number. A database is created with it at 0, and an epoch swap carries it unchanged. The leader advances it when it publishes a compaction, to the highest *k* such that every object from the predecessor's retired-through plus one to *k* has been read as retired or as an epoch bump, with *k* at most the predecessor's transaction number. A leader that takes office resumes retirement from retired-through plus one, so finding unretired objects never needs a scan from the start of the log. The cursor is for progress only: no reader and no safety argument uses it. A cursor that claimed too much would leave payloads unretired, not break history; and its claims stay true, because a retired form and a bump never change.

**Why a stale collector's retire is safe.** The manifest's transaction number never decreases, so the condition "*k* is at or below the transaction number of a manifest read from storage" stays true forever once it holds, and the pages of commit *k* stay in the base that the current manifest names. The replace is conditioned on the object's version, so it can only turn the original commit into its retired form, never overwrite anything else, and it lands at most once. This is a design argument. Test: [Fencing, takeover and garbage collection](#fencing-takeover-and-garbage-collection).

**One garbage rule for base objects.** A base object is garbage when a manifest read from authoritative storage, begun after the object was listed, does not reference it and has a transaction number at or above the transaction number *t* in the object's key. An unreferenced object above the manifest's transaction number is kept. No other rule deletes a base object. The collector lists the database's base prefix, reads the manifest, and deletes what this rule makes garbage, after a grace period that, as for retirement, is for reader progress only. It acts only on keys under the identifier of the manifest it read.

The argument:

1. The first manifest that references a key can only be the publication of an attempt whose target is the key's transaction number *t*, because a compaction newly references output only at its own target and every other reference is carried forward. Which attempt that is, and who created the object, does not matter.
2. That publication is conditioned on the token of a predecessor whose transaction number is strictly below *t*.
3. The manifest's transaction number never decreases, and its token never returns to an earlier value ([decision 3](#3-durable-layout-in-object-storage-per-database)). Once a manifest at or above *t* has been current, the predecessor that publication needs is gone for good, so the publication can never take effect.
4. So if a manifest at or above *t* does not reference the object, either the object was never referenced and never will be, or a manifest dropped it, and no manifest re-introduces a dropped reference. In both cases no current or future manifest references it.

The rule does not depend on who wrote the object, on epochs, on whether a compaction has ended, or on the outcome of any request. A failed or abandoned attempt's output waits, unreferenced, until the manifest reaches its transaction number. This is a design argument. Test: [Fencing, takeover and garbage collection](#fencing-takeover-and-garbage-collection).

A delete whose outcome is unknown needs no resolution: what it removes is garbage for good, whenever it lands. A delayed create can re-create the object after a delete, with the same bytes; the object is then garbage under the same rule, and a later pass deletes it again. A listing cannot show that no such create is still in flight, and nothing depends on it.

**Head discovery stays a walk.** A reader, candidate or compactor finds the head by reading consecutive names from the manifest's transaction number until a name is absent. The log is never listed.

**The cost of never freeing a name, stated plainly.** Every transaction leaves one small object for the life of the database, and every ordinary commit costs two storage mutations: its create, on the acknowledgement path, and later its retire, off it. With the commit header of the format proposed in [pull request #7][format-pr], 144 bytes, and the retired form taken to be that header alone, a year of 365 days holds:

| Commits per second | Retired objects per year | Bytes per year |
| --- | --- | --- |
| 1 | 31,536,000 | 4,541,184,000 (about 4.5 GB) |
| 10 | 315,360,000 | 45,411,840,000 (about 45 GB) |
| 100 | 3,153,600,000 | 454,118,400,000 (about 454 GB) |

These are object bodies only. Providers may bill per-object metadata on top; that is unverified and not counted here. At 10 commits per second for a year, the retired log of a 100 GB database is close to half the database's own size. Reclaiming retired objects is future work and needs its own fencing argument.

This closes, by construction, the window that [0002](/docs/decisions/0002-durability-spike/) measured in finding 7. That is a design argument. No test has shown it yet.

### 9. Takeover

1. A candidate takes the lease ([decision 3](#3-durable-layout-in-object-storage-per-database)).
2. It swaps the manifest to the manifest's epoch plus one, with a new writer nonce, and resolves the outcome as in [decision 6](#6-commit-outcomes-and-fail-stop). Without a definite success it gives up.
3. It reads the tail of the commit log, walking from the manifest's transaction number ([invariant 4](#4-invariants)). A retired commit above that number, or a log object whose version changes under it, sends it back to re-read the manifest. If the re-read still shows the candidate's own epoch and nonce, a retired commit above the manifest contradicts the invariants, and the candidate gives up with an error.
4. Before each attempt at its epoch-bump commit it re-reads the manifest, and gives up if the manifest no longer carries its epoch and nonce. It then creates the bump at the head plus one and resolves the outcome as in decision 6, recognising its own bump by its bytes, which its epoch fixes at that name; any other object at its epoch is corruption and ends the candidacy. If another object holds the name, it walks on and tries again. Without a definite answer it gives up.
5. It reads the manifest again and takes office only if the manifest still carries its epoch and nonce.

**A candidacy creates at most one epoch-bump commit.** The candidate moves its bump to a higher name only after another object has occupied the name it tried, so no attempt at the lower name can ever land, and it gives up rather than move on from a name it cannot resolve. So at most one of its bumps is ever in the log. This is a design argument. Test: [Fencing, takeover and garbage collection](#fencing-takeover-and-garbage-collection).

The final check, step 5, is a design argument. From the moment of its swap, no older leader can change the manifest, so the base cannot have moved past the candidate's view and no object above its view can have been retired, unless a newer candidate has swapped the manifest since, which the re-read detects. A candidate that fails the check stops and never acknowledges a write. A candidate that gives up leaves its lease to expire, or releases it if its swap definitely did not land.

The old leader's next create meets an occupied name and it stops for good ([decision 6](#6-commit-outcomes-and-fail-stop)). If the old leader commits first, the candidate loses the create, walks on and retries. A candidate's create at a name that has since been occupied fails, so its bump can never land in a stale position: it lands at the true head plus one or not at all. The takeover involves no download of database content. Its time is not independent of database size, because the manifest it reads grows with the number of base groups, and it also depends on the length of the log tail. No safety argument rests on clocks or on the lease.

**A late candidate can fence the leader it was taking over from.** This is a liveness cost, not a safety problem. A candidate that passed its re-read in step 4 and was paused can still win the create for its bump after another candidate has swapped the manifest, but only while that candidate's bump is not yet in the log. It then fences the leader at the head, fails its final check and never serves, and the other candidate's bump follows it. Had it observed the other candidate's bump, its re-read would have shown a higher epoch. So a late bump fences only a leader of a lower epoch than its own. The object before a bump has an epoch no higher than the bump's, because the candidate observed it before its re-read and the manifest's epoch never decreases; epochs along the log are non-decreasing. Readers must not require this ([invariant 4](#4-invariants)).

### 10. Limits accepted in the first version

- **One upload per commit.** A leader can commit at most one transaction per storage round trip. Group commit is a possible later change, not a promise.
- **Maximum transaction size.** A transaction must fit in one commit object, and its frames must fit in the leader's local WAL bound. A larger one fails with a clear error and leaves nothing in storage. The maximum commit object size is set by the format below the smallest single-request upload limit of the backends: Amazon S3 accepts "a single object up to 5 GB in size" in a single `PUT` ([Uploading objects][s3-upload]); Azure Blob Storage accepts 5,000 MiB in one `Put Blob` from service version 2019-12-12, and 256 MiB for versions 2016-05-31 through 2019-07-07 ([Put Blob][az-putblob]); the same page says the Azurite emulator "supports blob sizes of up to 2 gibibytes (GiB) only". All three were read on 2026-11-15. This version publishes every object with one upload request. That is a restriction of this version's implementation: 0002 finding 6 shows only that the spike's client could not publish a staged upload atomically, and Amazon S3 documents `If-None-Match` and `If-Match` on `CompleteMultipartUpload` ([conditional writes][s3-cond]). Whether staged uploads can fence on every backend is unverified. **Consequence:** `VACUUM`, and index builds that rewrite more pages than the limit, are unsupported on a database larger than the limit in this version, because they rewrite most of the database in one transaction. Multi-object commits are future work.
- **The local WAL bound is enforced while frames spill.** The leader's storage layer refuses a WAL write that would take the local WAL past its bound, during a transaction as well as at its commit, with an error that rolls the transaction back. It does not wait for the next transaction boundary. *Unproven.* Test: [Limits and size](#limits-and-size).
- **Maximum manifest size.** A database whose manifest would exceed it stops compacting ([decision 3](#3-durable-layout-in-object-storage-per-database)).
- **The tail bound counts epoch-bump commits.** The leader refuses writes once the log above the manifest's transaction number reaches a bound, and epoch bumps count towards it. Repeated failed takeovers each add a bump to the tail and can fill it. A candidate whose walk finds the tail at or past the bound still takes office if its final check passes, refuses client writes, and compacts first; its base version is the head, so nothing holds the compaction back, and a compaction folds bumps at no page cost. What restores service is one takeover that completes and one compaction. An operator sees writes refused with the tail-bound condition and the epoch rising with each failed takeover.
- **A reader's walk is bounded.** A reader walks at most 65,536 commits above the manifest's transaction number and indexes at most 4,194,304 page entries from them, and fails closed with a clear error beyond either. It refuses to walk on when the names it would read reach the largest transaction number, so no number wraps. The leader's tail bound is at most half of each, 32,768 commits and 2,097,152 page entries, so that the commit that crosses the bound, at most about two million entries at the maximum commit size and 512-byte pages, and the epoch bumps of up to 32,767 failed takeovers and the one that succeeds stay within the reader's bounds. More than that make readers fail closed until a compaction.
- **Long readers hold back checkpoints.** *Probe, not reproducible:* an open read transaction on the leader stopped checkpoints from backfilling the WAL, and the WAL kept growing. SQLite restarts the WAL only once it is fully backfilled and no reader is using it ([wal.c#L3961-L3995][sq-restart]). Readers on the leader therefore need time limits. They also hold back the base version, and with it compaction ([decision 7](#7-reads)).
- **The bounds leave room for the maintenance that clears them.** Refusing client writes must not stop the work that brings the WAL and the tail back under their bounds, because a writer's restart of the WAL needs a client write. Closing admission alone is not enough either: a write transaction admitted before it closed can hold SQLite's writer lock, idle, for as long as its client likes, and the checkpoint below needs that lock. When the leader refuses client writes at the local WAL bound or at the tail bound, the engine, in this order:
  1. closes admission: new client writes are refused before they reach SQLite, and stay refused until step 8. Rollbacks are never refused, the client's or the engine's;
  2. stops the storage layer from beginning a create for any commit it has not already begun; a commit that reaches the commit point after this fails there as a known failure with no attempt sent, so its WAL is truncated and it rolls back ([decision 5](#5-commits-are-captured-from-sqlites-write-ahead-log));
  3. lets a commit already in its upload complete or fail by the normal rules of [decision 6](#6-commit-outcomes-and-fail-stop), within the bound on resolving a create; if that commit ends in fail-stop, maintenance ends with the node;
  4. rolls back every other open write transaction, whether queued for the writer lock, running a statement or idle, and tells its client "not committed" with a reason that says the transaction can be retried;
  5. ends or waits out leader read transactions that use the WAL, under their time limits;
  6. runs a `TRUNCATE` checkpoint on a connection of its own, which takes SQLite's checkpoint and writer locks and the reader locks itself ([wal.c#L2435-L2471][sq-ckpt-restart]), and retries it after a `BUSY`;
  7. on seeing the WAL truncated to zero bytes, advances the base version to the head ([decision 7](#7-reads)), then compacts up to the new base version, publishes, and retires what the new manifest covers;
  8. readmits client writes once both bounds are clear.

  No acknowledged or possibly committed transaction is affected. An acknowledged transaction is already in the log, and a rollback discards only the rolled-back transaction's own frames, which were never uploaded (inference from SQLite's WAL design; the rollback cases under [Commit capture and failure](#commit-capture-and-failure) test it). A transaction rolled back in step 2 or step 4 had no create sent for it, so it is definitely not in the durable history and "not committed" is true; its binding to a transaction number is released ([decision 6](#6-commit-outcomes-and-fail-stop)). The only transaction for which a create may have been sent is the one in its upload, and step 3 leaves it to the rules that decide every other commit. Step 2 is what makes the line between those two cases exact: a commit is either begun before it, and handled by step 3, or refused by it. After step 4 no client transaction holds or waits for the writer lock, so step 6 waits only for readers, which step 5 bounds, and for SQLite's own checkpoint lock. A `RESTART` checkpoint is not enough for step 6, because it leaves the restart to the next writer. *Unproven.* Test: [Limits and size](#limits-and-size).
- **Follower lag.** Followers poll at a configured interval. Their lag is not bounded by that interval: under delays or failures a follower can fall behind by any amount. A client that needs to read its own write presents the transaction number it was given.
- **Takeover latency is not bounded.** An old leader that keeps winning creates and ignores the lease makes each of a candidate's bump attempts lose, as 0002 finding 8 observed. The old leader stops at its next compaction, when it reads the higher epoch, or at its tail bound. Late candidates can also fence the leader they were taking over from, which has a lower epoch than theirs ([decision 9](#9-takeover)). Both are known behaviour. So is repeated loss of service: a candidate raises the epoch and then stalls or crashes; the leader below it reads the higher epoch at its next compaction and leaves office; a further candidate supersedes the stalled one; a delayed bump from an earlier candidate fences the leader at the head, whose epoch is lower than that candidate's. Service returns only when one candidacy and the maintenance after it run undisturbed long enough; under pauses that never end, nothing guarantees that. An operator sees candidates whose bump attempts keep losing to commits, leaders fenced by a bump at their next name, candidates that fail their final check, leaders leaving office on a higher epoch, and the epoch rising without a lasting leader.
- **Cost per commit.** Each ordinary commit costs two storage mutations: one conditional create, on the acknowledgement path, and one conditional replace for its retirement, later and off that path. Everything else is amortised over many commits: the base group objects and one manifest replacement per compaction, deletes of dropped base objects, and lease renewals. Reads depend on the cache and are not counted. The mutation count per commit is two, but the request count is higher: retries, the reads that confirm retirement, compaction, base listing, deletes and the lease all add requests. This record gives no prices.
- **The retirement backlog has no stated bound.** During a long storage outage, commits that the base covers but that are not yet retired, and the reads that confirm retirement before the cursor passes them, can accumulate without a bound this record states.
- **Writes to the manifest's name are rate-limited on Google Cloud Storage.** Google Cloud Storage documents a maximum of "One write per second" to the same object name, above which writes "might result in throttling errors" ([Cloud Storage quotas and limits][gcs-quota], read on 2026-11-15). Compactions and epoch swaps both write the manifest. The engine paces compactions at most once per second on that backend, and treats throttling of a compaction or an epoch swap as a reason to back off and retry, with backoff that grows, not as a failed attempt that ends a candidacy or sends a leader out of office. Until the storage interface classifies throttling, an epoch swap that fails with no effect ends the candidacy after its bounded attempts, and the caller backs off before another.
- **Policies that keep old versions cost storage.** A bucket with object versioning or soft delete keeps the live form of a retired commit and every deleted base object as a noncurrent or soft-deleted version, so retirement and garbage collection then free nothing until that policy expires them (inference from how the providers describe those features; not checked per backend). That is a deployment matter: an operator who wants the space back configures a lifecycle rule for noncurrent versions. No argument in this record depends on versioning being on or off.
- **Policies that refuse mutations are unsupported on the database's prefix.** Retention periods, object locks, legal holds and immutability policies are a different kind of policy: they protect an object or an object version against replacement or deletion, and no lifecycle rule removes that protection. The manifest, the lease and the log objects must be replaceable, so where a backend refuses the replacement of a protected object the database cannot run: the first takeover's epoch swap is refused, and each later replacement would start a new retention age. Per backend, as documented on 2026-11-15:
  - Google Cloud Storage: under a bucket's retention policy, objects "can only be deleted or replaced once their age is greater than the retention period" ([Bucket Lock][gcs-bucket-lock]); an object retention configuration means that before its retain-until time "the object cannot be deleted or replaced" ([Object Retention Lock][gcs-object-lock]); and while an object has a hold, it "cannot be deleted or replaced" ([Object holds][gcs-holds]). With versioning on, a protected live version "can still be made noncurrent" ([Bucket Lock][gcs-bucket-lock]), which turns the case into one of kept versions that cannot be removed before their retention ends.
  - Azure Blob Storage: with a time-based retention policy or a legal hold, "objects can be created and read, but not modified or deleted", and "For a write to succeed, you must either have versioning enabled or have neither a legal hold nor a time-based retention policy on the data" ([Immutable storage overview][az-immutable]). Under version-level policies, which need versioning, an overwrite "creates a new version" and the protected version "can't be deleted" ([Version-level WORM policies][az-vlw]).
  - Amazon S3: Object Lock retention periods and legal holds protect object versions and work only with versioning; they "don't prevent new versions of the object from being created", and a protected version cannot be permanently deleted until its protection ends ([Locking objects with Object Lock][s3-object-lock]). So on S3 the manifest, lease and log objects can still be replaced, but nothing retirement or garbage collection releases is freed before the lock expires.

  Where a backend refuses the replacement, the database cannot run; where it keeps a locked version instead, the database runs and frees nothing for the length of the lock. This record supports neither on the database's prefix, so that one deployment rule holds on every backend.
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

1. Leader A, at manifest version v1 (transaction number 10), creates new base objects at transaction number 20 and submits the replacement to v2. The outcome is unknown.
2. A repeats the identical replacement. If it reaches its bound without a definite answer, it leaves office (decision 6).
3. If the outstanding replacement lands, the manifest references the output, which still exists: no manifest at or above 20 has been current before it, so the garbage rule never applied to the output.
4. If instead a candidate swaps the manifest from v1, v1 is gone and the outstanding request can never land. The candidate's manifest is still at 10, so the output is kept until a manifest at or above 20 that does not reference it exists. If a candidate swaps from v2, its manifest references the output, so it is kept.

Stopped by: the one garbage rule (decision 8). Nothing is deleted because a compaction has ended.

**Blocker 4: a delayed delete after the database is recreated under the same prefix.**

1. An old collector issues a delete for the old database's log object 11. The request is delayed.
2. The database is recreated under the same prefix with a new identifier.
3. The delete lands.

Stopped by: decision 8 issues no deletes for log objects at all, and decision 3 puts the identifier in the key, so the request was for `<prefix>/<old identifier>/log/…` and lands in the old namespace. The same holds for a delayed retire, a delayed base delete and a delayed create. A delayed manifest replacement is conditioned on a token that never returns to an earlier value; a delayed lease replacement carries no safety. A delayed create of the manifest or the lease is the hand-wiping case that decision 3 declares unsupported.

**Base deletion races a publisher of the same key (third review, blocker 1).** All within leader A's candidacy at epoch 7, in the previous revision's naming, where a key was group, transaction number and epoch:

1. The manifest is v1, at 10. The log runs to 20, and A's base version is 20.
2. Attempt C1 reads v1, targets 20 and creates `base/0-20-7`.
3. Attempt C2 publishes a base through 15: v1 becomes v2.
4. C1's replacement, conditioned on v1, fails. Under the old writer's-own-output rule its output was garbage.
5. Attempt C3 reads v2, targets 20, finds `base/0-20-7` present with identical bytes, adopts it, and prepares its replacement.
6. C1's collector issues a delete of `base/0-20-7`; the outcome is unknown.
7. C3's replacement lands, giving v3, which references the key. The delete lands. The current manifest references a deleted object.

Against this design: in step 2 C1 creates `base/0-20-<a>` under its tag *a*. In step 5 C3 draws its own tag *c* and creates `base/0-20-<c>`, so it meets C1's key only if *c* equals *a*. Step 4 does not make C1's output garbage: v2 is at 15, below 20, so no delete is issued in step 6. C1's output becomes garbage only once a manifest at or above 20 does not reference it; by then C1's replacement, conditioned on v1, can never land. If the tags did collide, C3 would reference C1's object only after reading the bytes it meant to write, v3 would then reference it, and a referenced object is never garbage. Stopped by: the one garbage rule (decision 8). The tag only makes the shared key improbable.

**The sequential variant (third review).** A compaction to 20 is known to have failed, and its cleanup queues a delete of its output; a later compaction to 20 prepares a publication of the same key before the queued delete is issued. Against this design: the later compaction draws a new tag, so the key it publishes is the one being deleted only by a collision, and the failed compaction's output is not garbage while the manifest is below 20. The queued delete is never issued before a manifest at or above 20 exists that does not reference the key, and then it removes only an object that no current or future manifest can reference, whoever created it. Stopped by: decision 8.

**Sticky uncertainty in a commit (third review).**

1. Leader A sends the create of `log/21`; attempt R1 is delayed and its outcome is unknown.
2. A, or the backend inside its own retries, sends R2 with identical bytes. R2 is rejected before dispatch: no effect.
3. If R2's answer were taken as "not committed", the WAL would roll back and R1 could land at 21 later, making the answer false.

Stopped by: decision 6. R2's answer is about R2 only. After R1's unknown outcome, the create resolves only by success, by a precondition failure and a read that finds A's own commit (committed) or an epoch bump (not committed), or it stays unknown, and A stops with the client told unknown. The same holds for a manifest replacement: after one unknown attempt, a later no-effect attempt does not make the output garbage; only the garbage rule does.

### Attempts to break the retired-object design

**A stale collector retires a commit that is not yet covered.** The collector retires *k* only below the transaction number of a manifest it read from storage or published with a definite success. That manifest's base covers *k*, and the current manifest's transaction number is at least as high, because it never decreases. A collector cannot retire on the evidence of a manifest replacement whose outcome is still unknown. Closed.

**A retire races a reader.** The reader either finished reading the commit before the retire, and holds a state that existed, or reads after it and sees a retired commit or a changed version, and restarts. The storage requirements in decision 3 rule out a read that mixes the two versions. Closed.

**A retire races a leader's read-back.** The leader reads either the unretired commit or its retired form. Both are a non-bump at its next name, so both mean committed. Epoch bumps are never retired, so a bump cannot turn into something else under the read. Closed.

**A retire races a takeover walk.** The candidate walks from the transaction number of the manifest it swapped. An object above that number can be retired only on the evidence of a manifest with a higher number, which means a newer manifest exists; the candidate meets the retired commit or the changed version, re-reads, and gives up. Closed.

**A retire races compaction.** Compaction reads objects above the manifest's transaction number; retirement touches only objects at or below it. The leader's own collector cannot outrun its compaction, because it retires only on a definite success or a read. Closed.

**The leader's base version is older than the manifest.** If the leader compacted past its base version, garbage collection could retire log objects that the leader still needs to build pages as of its base version, and the base of the older manifest could be deleted. Closed by the rule in decision 7 that the leader compacts only up to its base version.

**A delayed create lands after any amount of time.** By invariant 2 a create lands only at the head plus one. A delayed ordinary commit is the continuation of its leader's own last commit, so it is a valid commit, and its client was told unknown. A delayed epoch bump is a valid continuation with no pages; it may fence the leader that holds the head, which is the liveness cost of decision 9, and its candidate never serves because it gave up or failed its final check. After a takeover, the takeover's bump occupies the deposed leader's next name, so a delayed commit of that leader cannot land. After a compaction, every name the compaction folded is occupied. Delayed base creates are covered below. Closed.

**A delayed replace lands after any amount of time.** A delayed retire is permitted whenever it lands. A delayed manifest replacement lands only if the manifest is still at the version it was conditioned on, in which case it is a legitimate step from that version: a compaction whose output still exists, because no manifest at or above its transaction number has yet been current, or an epoch swap. A delayed epoch swap from a candidate that gave up leaves the manifest at an epoch nobody serves; the leader below it stops at its next compaction and a new takeover follows. That is a liveness cost. Closed for safety.

**A delayed delete lands after any amount of time.** Log objects are never deleted. A base delete is issued only for an object that the garbage rule shows no current or future manifest can reference, and that stays true for ever, so the delete removes nothing a manifest needs whenever it lands. If a delayed create, from any attempt, re-creates the key, it holds the same bytes and is garbage under the same rule, because a publication that would newly reference it needs a predecessor below its transaction number, and that predecessor is gone. Closed.

**A delayed create of a base object by a deposed leader.** Its attempt targets some transaction number *t* and conditions on a manifest below *t*. The takeover that deposed it changed the manifest, so that publication can never land. The object lands under the deposed attempt's tag. Unless a later attempt drew the same tag and references it after finding the bytes it meant to write, which are then the right bytes, nothing references it; it is kept while the manifest is below *t* and collected once a manifest at or above *t* does not reference it. If it lands after a delete of the same key, the same holds. Closed; the cost is objects that wait for collection.

**Two compaction attempts by one leader overlap, in every order.** Attempts P and Q read manifests m_P and m_Q and target *t_P* and *t_Q*, each with its own tag. Their creates meet only by a tag collision, the next case. Their replacements are each conditioned on the manifest each read. If both read the same manifest, at most one lands, and the other fails its precondition and ends. If Q read the manifest P published, Q carries forward P's references and can land as P's successor. If Q read a manifest older than one that has since landed, Q fails and ends, and a new attempt starts from the current manifest, at the same target if the manifest is still below it. The loser's output is garbage only once a manifest at or above its target does not reference it, and by then its replacement can never land. Closed.

**A tag collision while a create is unknown (fourth review).**

1. Manifest M1, token v1, is at transaction number 10.
2. Compaction P targets 20, draws tag *a* and sends a create of `base/0-20-<a>` holding bytes X. The request is delayed; its outcome is unknown.
3. Overlapping compaction Q also targets 20, draws the same tag *a*, and its create of the same key succeeds, also with bytes X: a group's pages at 20 are fixed by the log, and nothing else goes into a base object (decision 3).
4. P repeats its create. It fails its precondition. P reads the key and finds X, the bytes it meant to write, so it references the object.
5. P's replacement, conditioned on v1, succeeds and gives M2 at 20, referencing `base/0-20-<a>`. Q's replacement, conditioned on v1, fails, and Q ends.

The manifest references an object that Q's request created, which neither actor can tell, and which nothing depends on: the key holds X, the correct pages of group 0 at 20. The garbage rule never deletes it while it is referenced: M2 and every manifest that carries the reference forward reference it, and once a manifest at or above 20 drops it, no manifest ever references it again. P's delayed request, if it lands while the object exists, fails its precondition and changes nothing; if it lands after the object was collected, it re-creates X under a key that is garbage for good. A delete can come between P's read in step 4 and its publication in step 5 only if a manifest at or above 20 that does not reference the key was current first, and then P's publication, conditioned on v1, cannot land.

The variant with different bytes, which a correct attempt cannot produce and the tests inject: in step 4 P reads bytes other than X, does not reference the key, draws a new tag *b*, and creates and references `base/0-20-<b>`. Closed. The chance of a collision is one in 2 to the power 64 per pair of attempts at one group and transaction number.

**A collector acts on a manifest read that is stale by the time its delete lands.** The collector reads manifest M at or above the object's *t* that does not reference it, and issues the delete; the manifest moves on before the delete lands. Every later manifest is at or above *t* and, by the argument in decision 8, references the object only if M did. M did not. Closed.

**The retired-through cursor advanced by a leader that is then deposed mid-retirement.** The cursor in a published manifest covers only objects that were read as retired or as bumps, and those never change. A deposed leader's later retires are conditioned on the live form's token and land at most once, and only on objects the base covers. A candidate's swap carries the cursor unchanged; the new leader reads each object from the cursor plus one, finds some retired by the old leader and some not, and retires the rest. A deposed leader cannot advance the cursor any further, because its compaction replacement can no longer land. Closed.

**A candidate takes over between a compaction's publication and its retirement work.** The candidate's swap is conditioned on the manifest that the compaction published, or it fails and the candidate re-reads. So the candidate walks from the new manifest's transaction number, and every retire the old leader still sends is at or below it. The new leader resumes retirement from the cursor, which may be behind; it re-reads those objects and retires only the unretired ones. Closed.

**Maintenance while writes are refused.** A long reader stops the WAL restart; the tail reaches its bound and client writes are refused; the reader ends; a checkpoint backfills the WAL, but the base version has not advanced. In the previous revision only a client write could restart the WAL, and client writes were refused. Against this design, the engine runs a `TRUNCATE` checkpoint itself, advances the base version, compacts and retires, with client writes refused throughout (decision 10). Closed, subject to its test.

**Maintenance with an admitted writer (fourth review).** Connection W is admitted before the bound closes admission and waits for the writer lock while the writer ahead of it commits. That commit brings the tail to its bound, and admission closes. W then takes the released lock and sits idle in an explicit write transaction. Before this revision, maintenance ended read transactions and retried its checkpoint, which could not take the writer lock while W held it. Against this design, maintenance rolls W back and tells its client "not committed", retryable (decision 10, step 4); no create was ever sent for W, so the answer is true. A writer queued for the lock is rolled back the same way, and a writer that reaches its commit point after step 2 fails there with no attempt sent. Closed, subject to its test.

**A definitely failed commit (fourth review).**

1. Leader A owns `log/10` and builds transaction X for `log/11`.
2. Every attempt to create X definitely has no effect. The storage layer truncates the WAL, SQLite rolls X back, and X's client is told not committed.
3. A receives transaction Y.

Before this revision, A could neither build different bytes for 11 nor create 12. Against this design, the binding of 11 to X is released, because no request for X exists that could land (decision 6), and A builds Y for 11. If any attempt for X had had an unknown outcome, the binding would stand, and A would resolve X or stop. Closed.

**Two candidates.** Both read the manifest at epoch *e*. Only one swap to *e* + 1 lands; the other candidate's read shows a different nonce, so it does not take the epoch, even after an unknown outcome. If both swap in turn (*e* + 1, then *e* + 2), the first one's re-read before its bump or its final check fails. If the first one's bump lands first, the second walks past it; if the second one's lands first, the first one's create fails and its re-read stops it. Closed. The late-candidate liveness cost in decision 9 remains.

**A candidate crashes at each step.** After taking the lease: nothing changed but the lease, which expires. After its swap, or with its swap's outcome unknown: the manifest carries an epoch nobody serves; the old leader keeps committing until its next compaction, whose replacement fails, then leaves office; its output waits for the garbage rule. During its walk: the same. With its bump created or in flight: the bump is a valid continuation, it fences the leader at the head, and the next takeover walks past it. After its final check: it was the leader; this is a leader crash. Closed for safety.

**An unknown outcome on every conditional write.** Commit create: repeated to a definite answer or the node stops and the client is told unknown (decision 6). Epoch-bump create: repeated, recognised by epoch, or the candidate gives up. Epoch swap: repeated, recognised by nonce, or the candidate gives up. Compaction replacement: repeated, recognised by bytes, or the leader leaves office and its output waits for the garbage rule. Base object create: resolved by success, or by a precondition failure and a read that returns the bytes the attempt meant to write, or the group moves to a new tag; the manifest is not replaced until every object it newly references has resolved one of those two ways. Retire: needs no resolution for safety, and is confirmed by reading the retired form before the cursor passes it. Base delete: needs no resolution. Lease: not used for safety. Closed.

**Transaction number and epoch at their limits.** Neither wraps. A leader at the maximum transaction number cannot commit, and a candidate whose head is at the maximum cannot bump, so the database stops for good, fail closed. A manifest at the maximum epoch cannot be swapped, so no further takeover is possible and the database stops when its leader does. Neither limit is reachable in practice at the rates in decision 8 (inference), but the code must stop, not wrap.

**Sequences not closed.** None of the sequences tried here breaks safety under the storage requirements in decision 3. What this record does not close: the two assumptions in decision 3 (that an Azure ETag does not return to an earlier value across overwrites, and that no two different contents written to one S3 key share an ETag); the derived read-after-precondition-failure property, which each backend's tests must show; whether discarding a connection after a panic outside the commit path leaves the other connections correct (decision 6); and the engine-driven WAL reset (decision 10). Each is a test obligation or an accepted assumption, and every argument above depends on them.

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
- Every transaction leaves a retired object of about one commit header for the life of the database, and every ordinary commit costs a second storage mutation, its retire, off the acknowledgement path ([decision 8](#8-compaction-retirement-and-garbage-collection)).
- Some commits end with the answer unknown, and the node that gave it stops serving the database until a takeover. Clients must handle unknown. A commit identifier can be looked up for the life of the database.
- `VACUUM` and large index builds are unsupported on databases larger than the maximum commit size ([decision 10](#10-limits-accepted-in-the-first-version)).
- The engine depends on a project whose commits are concentrated in one account. The pinned version, the panic rule and the storage layer boundary are what make replacing it possible.
- A late candidate can fence the leader it was taking over from before another candidate's bump does, and takeover latency is not bounded ([decision 9](#9-takeover), [decision 10](#10-limits-accepted-in-the-first-version)).
- Every backend must meet the requirements in [decision 3](#3-durable-layout-in-object-storage-per-database), stated there per kind of key, before it is relied on. The storage contract promises more than they need; on S3 meeting the contract means making each stored object's bytes unique per write.
- Retirement of the commit log must not be enabled before the tests for decision 8 exist and pass.
- 0001's choice to embed Litestream is superseded. Its rejection of Litebase stands.
- The [architecture page](/docs/architecture/) and the [roadmap](/docs/roadmap/) follow this decision.

## What the engine's tests must prove

The probes behind this record are exploratory code that is not in this repository. Nothing below may be relied on until a test in the engine proves it. Each test runs against the real virtual file system of `ncruces/go-sqlite3` at the pinned versions, and on emulators until real-cloud testing is funded, and says so. Where a test checks data, it checks it against an oracle: a plain SQLite database that receives the transactions the durable history actually admitted, in log order, including transactions that storage accepted while their client was told unknown, not only the acknowledged ones. The answers given to clients are checked separately against that history: committed only for a transaction in it, not committed only for one that never enters it.

### Commit capture and failure

- Exactly one durable commit object per intended transaction, and no object for anything else, across WAL header syncs, checkpoint syncs, sector padding, spilled frames, overwritten frames, savepoint rollback, frame numbers reused after a rollback, dead frames past the commit frame and checksum-only rewrites after the commit frame. The WAL header sync after each WAL restart is not taken for a commit. The commit-frame layout is checked with and without powersafe overwrite.
- A commit known not to have entered the durable history never reappears, after a crash or after every connection closes and the database reopens. This covers each failure and each short write at each boundary (WAL write, WAL sync, upload, WAL truncate and its sync), each followed by a reopen and by a process kill, and a crash at each step of the truncation.
- No error from the commit point is turned into success. Every error code the storage layer can return surfaces to the caller as an error.
- The commit point never returns `BUSY`, whatever fails inside it. No failed `COMMIT` leaves a transaction open. No transaction is committed under two transaction numbers, and no transaction number is ever given a second, different commit object, except after every attempt to create the first definitely had no effect and the WAL truncation completed.
- A write transaction that changes no page, and a read-only transaction, create no commit object and take no transaction number.
- Bootstrap: a database created by the engine opens in WAL mode with no write to page 1 outside the WAL, and a database already in object storage opens the same way. A main-file write that is not part of a checkpoint is an error, not a silent local change.
- Database creation: competing creators under one prefix, and a crash, a lost response and an unknown outcome around the first commit object and the first manifest. Exactly one identifier wins, every creator ends up using it, no first manifest is created before its first commit object resolved as its creator's own, and no first manifest references a base object.
- Every journal-mode, locking-mode, `synchronous` and alternative-output setting a client can reach through SQL (pragmas, `ATTACH`, `VACUUM INTO` and any other route the lockdown leaves open) is either refused or leaves capture intact.
- A process is killed at every step of a commit: before the upload, during it, after storage has accepted it but before the response, and after the response but before `COMMIT` returns. Every acknowledged write survives. A commit that storage accepted but that was never acknowledged may appear in the history, and if it does, it appears whole.
- The local WAL is on a real disk, and tests inject short writes, a full disk, and a failed truncate or fsync.
- A real process crash, not a simulated one, leaves the shared-memory file in place, and the restarted node still wipes and ignores it.
- Automatic checkpointing runs while commits are being captured.
- Failures after publication: an out-of-memory in the shared-memory index after a successful upload, with a second connection open, stops the whole database on that node, and the second connection never serves a read from the old image afterwards. The same for every other panic and unexpected SQLite error in the commit path once an upload may have started.
- Fail-stop with a second writer: while the database stops on a node, another connection's upload, or the processing of its response, overlaps the shutdown. That writer's client is never told not committed while its create can still land, and if it lands it is in the history the oracle checks.
- A panic in a connection outside the commit path, including one that holds the WAL write lock or is running a checkpoint: within a time bound, another connection completes a later write and a checkpoint, and a newly opened connection reads, with every result checked against the oracle. Reading correct rows on the existing connections is not enough, because leaked locks would pass that. If this fails, the fail-stop rule extends to every panic.

### Commit outcomes

- A create whose response is lost is repeated with identical bytes. A repeat that succeeds is reported committed. A repeat that fails its precondition reads the name: the leader's own commit, retired or not, reports committed; an epoch bump fences the leader and reports not committed. Each case is tested with the name's object retired before, during and after the read.
- A non-bump at the leader's next name whose epoch, transaction number, commit identifier or bytes are not the leader's, and a precondition failure followed by a read that finds nothing, each stop the node with unknown.
- An indeterminate commit, a failed WAL truncation and an error in the commit path after an upload may have started each stop the node for that database and discard its local state. The node serves again only through takeover. A create left outstanding by such a node and landing later lands only at the head plus one.
- An upload whose every attempt definitely had no effect, followed by the WAL truncation: the client is told not committed, and a different transaction then succeeds on the same leader at the same transaction number. The same with one attempt of unknown outcome among them: the leader never builds a different object for that number.
- A client is told exactly one of committed, not committed or unknown, and never not committed for a commit that is in the history. A commit identifier resolves to committed for the life of the database, retired or not, from the point where its channel was opened. A leader whose create landed, and whose response arrives only after another candidate's bump occupies the next name, reports that commit committed and commits nothing further. A lookup for an identifier that is not in the log ends not committed at the first epoch bump above that point, including when the leader's last create landed late, just below the bump; it ends unknown only past its scan bound.
- A manifest replacement with an unknown outcome is resolved as decision 6 says, for the epoch swap and for compaction. Two candidates that build byte-identical manifests apart from the nonce: only the one whose swap landed proceeds, also when both saw an unknown outcome.
- A candidate whose swap or bump create has no definite answer never serves. A leader whose compaction replacement has no definite answer leaves office, and its output is not deleted while the manifest is below the output's transaction number.

### Invariants

- Readers, followers and the compactor reject a gap; a candidate rejects a gap in the tail it walks, and a gap above its head is invisible to a walk that never lists. All of them reject a failed checksum, a wrong database identifier, an object whose header does not match its key and an undecodable object, and never apply a later commit without every earlier one.
- Every log and base object is written under the identifier of the database the actor was working on. A request delayed across a re-creation under a new identifier lands in the old namespace.
- A read transaction that meets a retired commit above its manifest's number, a changed version token on a log object it indexed, or a missing base object fails or restarts from a new manifest, and never returns a mix of old and new data.
- Transaction numbers and epochs at their maximum value stop the database instead of wrapping.

### Reads

- An upload held indefinitely while existing and newly opened connections read, both at first initialisation of the shared-memory index and with an established one, and ending in success, in a known failure and in unknown: no connection sees the transaction before its upload has succeeded, and none sees a failed one.
- Reads against the oracle through partial backfill, failed page writes during a checkpoint, a failed truncate and a failed sync, checkpoints held back by readers, writer restart, `RESTART` and `TRUNCATE` checkpoints and the last connection's close, with cache eviction, return each page as of the base version, or as written locally by a checkpoint since then. A required local page that has been removed makes the read fail; it never falls back to older base bytes.
- The size reported for the main file is the size of the checkpointed image.
- Automatic checkpointing overlapping commit capture leaves the base version and the locally held pages correct. The leader never compacts past its base version.
- Follower invalidation, checking the rows returned against the oracle: prepared statements cached across versions, schema changes, the database shrinking and growing, `VACUUM` on the leader, concurrent connections on the follower, a connection pinned to one version while others move, and a change counter about to wrap, which closes and reopens the connection instead of presenting a value already used. The follower's storage layer presents the header fields in decision 7.
- A reader whose base objects were deleted gets an error, never wrong data.

### Limits and size

- The local WAL bound is enforced while a transaction's frames spill, including at the commit frame: the transaction rolls back, nothing is uploaded, and the next transaction works.
- The WAL grows under a long reader on the leader, and the tail reaches its bound. The leader refuses client writes at the bounds; once the reader ends, the engine's own `TRUNCATE` checkpoint resets the WAL, the base version advances, and a compaction and retirement run, with client writes refused throughout; then writes resume. The same with a `RESTART` checkpoint in place of `TRUNCATE` shows that it does not advance the base version.
- Maintenance with an idle open write transaction that holds the writer lock, and with a writer queued for the lock before admission closed: each is rolled back and its client told not committed with a retryable reason, the `TRUNCATE` checkpoint completes, and maintenance finishes, each within a time bound. A commit already in its upload when admission closes completes or fails by the normal rules, and a commit that reaches its commit point after creates were stopped fails there with nothing sent to storage. No acknowledged transaction is lost and no client is told not committed for a transaction in the history.
- A transaction above the maximum size fails with a clear error and leaves nothing in storage. `VACUUM` on a database above the maximum commit size fails that way.
- The tail bound counts epoch bumps. A tail filled by failed takeovers is cleared by one completed takeover and one compaction.
- A compaction that would produce a manifest above the maximum size is refused, and the database reports the condition.
- Databases above 1 GiB work. SQLite reserves the page that holds its lock bytes at the 1 GiB offset ([os.h#L147-L160][sq-pending]), and the reader, the commit log, compaction and retirement must all handle that page.
- Each connection's memory ceiling is set.
- Capacity: on a database far larger than the pod's memory and disk budgets (a sparse database is acceptable), page misses, partial checkpoints, the WAL reset, compaction and retirement stay within fixed budgets. The measurement counts the Go-side indexes and buffers and SQLite's temporary files, not only Wasm memory.

### Fencing, takeover and garbage collection

- A takeover downloads no database content: at more than one database size, with the same log tail, it reads only the lease, the manifest and the log tail. It never lists the log. This is measured in bytes read, not only in requests; a lost bump race must not download the winning commit.
- Every sequence under [Interleavings considered](#interleavings-considered) is reproduced with the actors paused before and after each of their storage requests, and ends as that section says.
- After a takeover, a stale leader cannot get acknowledged a transaction that is not in the accepted history, cannot make its compaction reachable, and cannot delete an object that any later manifest references.
- Garbage collection never deletes a log object, never retires an epoch bump or an object above the transaction number of a manifest it read or published with a definite success, and a stale collector's retire, delayed for any time, lands only on an object the current base covers.
- A base object is deleted only under the one garbage rule in decision 8: never while it is referenced, and never while the manifest is below its transaction number. A compaction attempt references a new base object only after its create succeeded, or after a precondition failure and a read that returned the bytes it meant to write. A tag collision is forced while a create is unknown: with identical bytes the attempt references the object and the manifest references correct bytes that are not deleted while referenced; with different bytes the attempt moves the group to a new tag. Both base-deletion schedules under [Interleavings considered](#interleavings-considered) are reproduced, with the delete delayed past the later publication, and the current manifest never references a missing object.
- Sticky uncertainty, at the backend's own retry level and at the engine's: an attempt with an unknown outcome followed by attempts with no effect, for a commit create, an epoch-bump create, a manifest replacement, a retire and a base create. None of them is resolved as not having taken effect, and the delayed first attempt is then allowed to land.
- After every not-committed answer in these tests, the test drains every delayed request, then runs later takeovers and compactions, and only then checks the history against the answer.
- The retired-through cursor never decreases, is never above the manifest's transaction number, and never passes an unretired ordinary commit. A leader that takes office after its predecessor was deposed mid-retirement resumes from the cursor and leaves no unretired object at or below the manifest's transaction number.
- A leader that reads a manifest with a higher epoch than its own stops compacting and leaves office.
- A candidate paused at each step of a takeover, for long enough that the history has moved on, been compacted and been retired, never acknowledges a write.
- A late candidate that wins its bump at the next name of the leader it was taking over from fences that leader and never serves; the other candidate's bump follows it, and that candidate restores service.
- A candidacy whose bump attempts lose to other objects, or end unknown, never has more than one bump in the log. A deposed leader's last commit in the log never rises after the bump that deposed it.
- An old leader that ignores the lease never breaks the history. Only the takeover's progress may suffer.

### Storage backends

- Each backend's own tests show the requirements in decision 3: read-after-write for single-object reads, reads after a precondition failure (for log keys, never absent), no absent key during a replace, a manifest token that never returns to an earlier value across replacements, distinct tokens for a log object's live and retired forms, ranged reads of the reported version, and no correctness read through a cache or secondary endpoint.

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
[sq-nochange]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L6550-L6551
[sq-restart]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L3961-L3995
[sq-ckpt-restart]: https://github.com/sqlite/sqlite/blob/version-3.54.1/src/wal.c#L2435-L2471
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
[az-concurrency]: https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage
[gcs-meta]: https://docs.cloud.google.com/storage/docs/metadata#generation-number
[gcs-precond]: https://docs.cloud.google.com/storage/docs/request-preconditions
[gcs-consistency]: https://docs.cloud.google.com/storage/docs/consistency
[s3-cond]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
[s3-object]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
[s3-object-lock]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
[gcs-bucket-lock]: https://docs.cloud.google.com/storage/docs/bucket-lock
[gcs-object-lock]: https://docs.cloud.google.com/storage/docs/object-lock
[gcs-holds]: https://docs.cloud.google.com/storage/docs/object-holds
[az-immutable]: https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-storage-overview
[az-vlw]: https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-version-level-worm-policies
[s3-consistency]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
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
