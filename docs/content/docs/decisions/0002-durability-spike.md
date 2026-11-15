---
title: "0002: Durability spike against Azurite"
description: Results of a spike that tested acknowledged-write durability and stale-leader fencing with Litestream as a library against Azurite. Accepted as evidence; the architecture decision follows separately.
weight: 2
---

**Status:** Accepted

**Date:** 2026-11-15

This record reports a spike. The code is throwaway test code under `internal/spike/durability/`. It is not part of skylite, nothing on this page describes a feature, and the spike may be deleted or replaced.

Every result on this page comes from the Azurite emulator. None was checked against real Azure, S3 or Google Cloud Storage.

## Context

### Question

Two properties decide the architecture:

- **P1, no acknowledged write is lost.** A write is acknowledged only after its change file is confirmed in object storage. If the process dies at any moment and a new one restores on an empty disk, every acknowledged write is there.
- **P2, a stale leader cannot commit.** Once a new leader has taken office, a leader that was paused or partitioned and then resumes cannot get another write acknowledged or into the durable history, and cannot damage the history: no overwrite, no deletion, no divergent file that a restore would apply.

The plan under test embeds [Litestream](https://github.com/benbjohnson/litestream) v0.5.18 (Apache-2.0, the latest tag when the spike ran, commit [`057d3956`](https://github.com/benbjohnson/litestream/tree/057d39564c75ebd67ba042410a7ffa05ad4c242d)) as a library, with the pure-Go SQLite driver `modernc.org/sqlite`. It fences stale leaders with conditional writes in a replica client of our own. Every Litestream source link on this page is pinned to that commit. Every Azure SDK link is pinned to commit [`cf0d0a3e`](https://github.com/Azure/azure-sdk-for-go/tree/cf0d0a3e3657f5e432912f09a384a65ed574bb02/sdk/storage/azblob), the `sdk/storage/azblob/v1.8.0` tag, except in [Dependency versions](#dependency-versions), which also cites the v1.8.1 and v1.8.3 tags.

### Method

- **Replica client.** A `litestream.ReplicaClient` for Azure Blob Storage. It keeps Litestream's key layout and metadata. It uploads every LTX file, at every level, with one Put Blob request carrying `If-None-Match: *`. It refuses a file larger than the SDK's single-request limit of 256 MiB ([`MaxUploadBlobBytes`](https://github.com/Azure/azure-sdk-for-go/blob/cf0d0a3e3657f5e432912f09a384a65ed574bb02/sdk/storage/azblob/blockblob/constants.go#L14-L15)) before uploading. Above that limit, the SDK's `UploadBuffer` switches from one Put Blob ([client.go#L449-L460](https://github.com/Azure/azure-sdk-for-go/blob/cf0d0a3e3657f5e432912f09a384a65ed574bb02/sdk/storage/azblob/blockblob/client.go#L449-L460)) to staged blocks ([client.go#L462-L530](https://github.com/Azure/azure-sdk-for-go/blob/cf0d0a3e3657f5e432912f09a384a65ed574bb02/sdk/storage/azblob/blockblob/client.go#L462-L530)), which finding 6 rules out.
- **Admission checks.** The client tracks its head: the highest L0 transaction it has confirmed in storage itself. It accepts an L0 file only at head + 1. It accepts a higher-level file (compaction or snapshot) only if the file ends at or below its head and a LIST shows that the remote L0 head is still its own. Deletes need the same LIST check. When a create conflicts, the client downloads the existing blob. If the blob already holds exactly these bytes, the write has had its effect, for example because an earlier attempt of the same request succeeded and only the response was lost, and the client counts it as success. Equal content does not show that this client created the blob. Different bytes at L0, or any failed check, latch the client into a permanent fenced state. After that it refuses every write and delete.
- **Leader.** It restores to an explicit TXID on an empty directory, then takes office by committing an epoch bump (an integer in a metadata table) and uploading it with put-if-absent. If it loses that slot, it discards the directory and starts again, up to 20 attempts. A write is one SQLite transaction followed by `db.SyncAndWait`. It is acknowledged only if that returns nil *and* Litestream's local position equals the client's uploaded head. Litestream's background monitors are off.
- **Lease.** A JSON blob with an epoch, created with `If-None-Match: *` and taken over or renewed with `If-Match: <etag>`. It is advisory and has no expiry. The tests drive takeover explicitly.
- **Records.** Every test writes records whose identity never depends on restored state. In the kill tests, round *i* writes IDs (*i* + 1) × 1,000,000 + 1, + 2, and so on, so no round reuses another round's ID. A record's content is derived from its writer and ID: a 16-byte tag, and for a quarter of the records a 1.5 MiB row whose bytes are also derived from the identity. To keep the database small, a large row replaces one of four slots, chosen by ID. A restore is compared with what was acknowledged by ID, writer, size class and content. Each slot must hold the bytes of the highest-numbered large record mapped to it.
- **Environment.** Azurite 3.37.0 in Docker, with the race detector on in CI.

### What each test demonstrates

| Test | Demonstrates | How it can fail |
|---|---|---|
| `TestEmulatorSemantics` | Azurite's conditional-write behaviour (below), the identical-content rule and the lease CAS | The conditional-write subtests assert the HTTP status and error code; the others assert the outcome |
| `TestCheckLTXSize` (unit) | The client refuses an LTX file one byte over the single Put Blob limit | Asserts both sides of the limit, without uploading |
| `TestP1KillRestore` | P1 under `SIGKILL` at random moments | Fails on any lost or altered acknowledged record, or if no record was acknowledged at all |
| `TestP1KillDuringUpload` | P1 when the kill lands inside an L0, a compaction and a snapshot upload | Fails if the upload is not reached and held, or if the restore differs from the acknowledged prefix |
| `TestP1NegativeControl` | That the P1 checks see a lost write | Acknowledges before uploading; must report the acknowledged record missing in every round |
| `TestP2StaleLeaderCannotCommit` | P2 in six orderings (see below) | Fails on any post-takeover acknowledgement, changed, added or removed blob, restore difference, or a fence by a check other than the one expected |
| `TestP2NegativeControl` | That the P2 checks see each kind of damage | Turns A's fencing off; must show each damage kind separately |
| `TestP2StaleMutationWindow` | What a stale leader paused between its admission checks and the storage request does | Fails if A acknowledges anything, if B fails any later step, or if either restore differs from what was acknowledged |
| `TestTakeoverRace` | B's takeover attempts while A is acknowledging writes. Whether and when B takes office is observed, not enforced | Exact restore comparison in every iteration; when B takes office, a later write by A must be fenced |
| `TestMeasure` | Latency, requests per commit, restore time (reported, not asserted) | — |

- **P1 kill test.** A child process (the test binary re-executed) restores into a new empty directory, takes office and writes records. It compacts L0 into L1 when it takes office and after every tenth record in the history. It writes a snapshot every tenth round and after every fiftieth record. It prints `ack <id>` only after `Exec` returns. The parent kills it with `SIGKILL` after a random, seeded delay. A quarter of the kills count from process start, so they can land in the restore or the epoch bump. The soak asserts that some record was acknowledged. It does not show that any kill landed inside an upload; the next test does that. A final restore on an empty directory must hold every acknowledged record with its content, and `PRAGMA integrity_check` must return `ok`. A record that is present but was never acknowledged is allowed only if it was the write a round had begun and not acknowledged when it was killed, with that write's content. Its upload may have finished before the kill. Epochs must increase by one per round that took office. A round killed after its bump landed but before it reported shows up as a jump of two in the next round. The L0 sequence must have no gaps.
- **Kill during an upload.** The replica client has a test hook that runs after the HTTP transport has read half of an upload's request body and before it reads more. For each of an L0 commit, a compaction file and a snapshot, the child first gets three records acknowledged, the first with a 1.5 MiB row, so every held file is at least 1.5 MiB. It then starts that upload. The hook prints the file name and blocks. The parent waits for that line (the case fails if the line does not arrive within two minutes) and sends `SIGKILL`. A new leader then takes office on the same history and writes one record. The restore must hold exactly the three records and the new leader's record. The L0 case also requires the killed record to be absent.
- **P1 negative control.** The child acknowledges its first record after the local commit, then blocks before any upload until it is killed. Every one of five rounds must have its acknowledged record reported missing.
- **P2 stale-leader test.** Leader A is held in-process rather than stopped with `SIGSTOP`. With both monitors off, neither of Litestream's monitoring goroutines runs ([db.go#L886-L890](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L886-L890), [replica.go#L103-L108](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L103-L108)), so a leader the test does not call sends nothing to storage, as a paused process would not. Holding it in-process also lets the test drive the compaction, snapshot and retention calls a `litestream.Store` would make. A acknowledges writes, then pauses in one of these states: with a commit whose L0 file is built but not uploaded; clean; or with its replica position reset, as Litestream does after any failed sync. B takes the lease, restores, takes office, writes and, except in one ordering, compacts. A then resumes. It writes, calls `SyncAndWait` directly (Litestream's own retry path) and runs the maintenance steps: snapshot; snapshot retention, TXID retention and L0 retention; compaction. The ordering decides which of those A runs first. The test asserts that A acknowledges nothing and stays fenced, by the check expected for that ordering. It also asserts that A's lease renewal fails with 412, that no blob changed ETag, appeared or disappeared, that B can still write, and that a fresh restore holds exactly A's pre-takeover records and all of B's, with their content.
- **P2 negative control.** The same scenario with A's fencing off. It must show, each asserted on its own: a post-takeover write acknowledged by A; an L0 blob of B's whose ETag changed; a deleted blob; an added snapshot. A restore failure or a lease error other than 412 fails the control instead of counting as damage.
- **Stale mutation window.** The client has a second test hook that runs after the admission checks have passed and before the storage request. A acknowledges eight records, with a snapshot and an L1 file over the first four. It then starts one operation and is paused in this hook, in its own subtest for each operation: a snapshot write, a compaction write, or the delete of L0 retention. While A is paused, B takes the lease, restores, takes office, writes four records and runs maintenance with Litestream's default retention settings, under which nothing is yet old enough to delete: compaction, snapshot, snapshot retention, TXID retention and L0 retention. A's operation is then released and completes. The test records which blobs it added or removed, checks that A's next write fails, and restores. B then writes four more records and runs the same maintenance with retention that deletes everything it may, and the test restores again.
- **Takeover race.** A writes continuously while B takes the lease and office. In half the iterations A renews its lease every 250 ms and stops when renewal fails. In the other half A ignores the lease and writes back to back for 3 s. A restore must hold exactly the acknowledged writes of both, and A must be fenced on any write after B took office.

### Results

All numbers below come from runs of the final code. The 500-round soak ran before a one-comment edit to the client; no code behaviour changed after it. The CI configuration (20 kill rounds, race detector on) passed three consecutive local `make integration` runs, taking 107 to 136 s each.

**P1 held in the tested scenarios.** The 500-round soak (seed `1794733407373009394`, race detector on) acknowledged 1,316 records. It killed 118 writers before they reported taking office and 199 after they had begun a write with a 1.5 MiB row and before its acknowledgement. The final restore held every acknowledged record with its content. It also held 14 unacknowledged in-flight writes, all within the rule above, and `integrity_check` returned `ok`. In the CI runs the soak acknowledged 70 to 80 records in 20 rounds.

Kills inside an upload, each after three acknowledged records:

| Upload held when killed | File | Restore |
|---|---|---|
| L0 commit | `0000/0000000000000006-0000000000000006` | the three records and the next leader's record; the killed record absent |
| Compaction | `0001/0000000000000003-0000000000000005` | the three records and the next leader's record |
| Snapshot | `0009/0000000000000001-0000000000000005` | the three records and the next leader's record |

In the L0 case the next leader's epoch bump was written under the same name as the killed upload, so the killed request left nothing that blocked that name. These are one kill per kind, at one point in the body. They do not cover kills at other byte offsets or after the body has been sent.

In the negative control, which acknowledges before uploading, every one of the five rounds lost the record it had acknowledged.

**P2 held in the six orderings.** A acknowledged nothing after B took office, A's lease renewal got 412, and no blob changed ETag, appeared or disappeared. Each restore held exactly A's five pre-takeover records and B's six, at epoch 2. Right after the operation named below, the test asserts that A is fenced and by which check, so a later operation cannot supply the fence:

| Ordering | Check that fenced A |
|---|---|
| Pending upload, writes first | the L0 create conflict (409, different content) |
| Clean, snapshot first | the remote-head LIST before the snapshot |
| Pending upload, snapshot first | the snapshot ending past A's uploaded head |
| Clean, retention first | the remote-head LIST before snapshot retention's delete call, which Litestream makes even when it has nothing to delete ([db.go#L3124-L3129](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L3124-L3129)) |
| Clean, compaction first, B has not compacted | the compaction file ending past A's uploaded head |
| Replica position reset | the position-equals-head acknowledgement check, after `SyncAndWait` had returned nil without uploading |

When B has compacted, A's compaction lists the destination level and stops; it never reaches a storage write. Litestream starts a compaction after the newest file at the destination level ([compactor.go#L104-L117](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/compactor.go#L104-L117)). That file is B's, so A finds nothing to compact ([compactor.go#L154-L156](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/compactor.go#L154-L156)). The compaction-first ordering therefore skips B's compaction, so that A's compaction reaches the client.

In the negative control (A's fencing off), A acknowledged four post-takeover writes. It overwrote five of B's six L0 blobs, which its later retention then deleted along with others, 12 deletions in all. It added a snapshot of its own state. Two manual controls also turned the suite red; both were run on a copy of the final code and not committed. With the position-equals-head check removed, A acknowledged four writes in the position-reset ordering that are not in the restore, and the race test failed. With `If-None-Match` removed, the pending-upload ordering failed (A acknowledged four writes and overwrote five of B's L0 blobs), the identical-content subtest failed, and the race test failed: A's write after B took office succeeded and the restored epoch fell back to 1.

**Stale mutation window: storage accepted A's stale operation in every case.** P2 as defined above does not hold for a leader paused between its admission checks and the storage request. In each case A's head was TXID `a` (TXIDs are hexadecimal), and the operation returned nil once released:

| A paused in | What A's released operation did to storage |
|---|---|
| Snapshot write | added `0009/0000000000000001-000000000000000a`, beside B's snapshot `0009/…01-…0f` |
| Compaction write | added `0001/0000000000000007-000000000000000a`, which overlaps B's `0001/…07-…0f` |
| L0 retention delete | removed L0 files `1` to `6`, which A's L1 file `0001/…01-…06` covers and B's first maintenance had kept |

In all three cases A's next write failed with `ErrFenced`, and B's later writes, compaction, snapshot and retention all succeeded. Both restores, one right after A's operation and one after B's second maintenance, held exactly A's eight pre-takeover records and all of B's records with their content, at B's epoch 2. `integrity_check` returned `ok` and the L0 sequence had no gaps. B's second maintenance removed A's stale snapshot and stale L1 file through its own retention. It left the same five blobs in all three cases: the lease, one snapshot, two L1 files and the newest L0 file.

That bears out, for these three orderings only, the hypothesis the test was written to check: a stale higher-level file that ends at or below the stale leader's confirmed head holds true shared history, and a stale retention delete removes only files that something still present covers. Why the restore was unaffected is an inference from Litestream's source. The restore plan takes the last snapshot in the listing, which, since every snapshot starts at TXID 1, is the one that reaches furthest ([replica.go#L1628-L1652](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L1628-L1652)), prefers the candidate that reaches furthest ([replica.go#L1805-L1816](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L1805-L1816)), and B's files always reach past A's head. The test does not show this for other orderings: a stale file landing before B's own compaction or snapshot, a pause inside a TXID-retention or snapshot-retention delete, a stale leader whose deletes reach files B still needs, or several stale operations in a row.

**Takeover race.** Over 12 iterations of the final code with the race detector on: when A honoured the lease, B took office in one or two attempts. When A ignored the lease for 3 s, B took office only after A had stopped, 0.75 to 1.26 s later, in eight or nine attempts. In every iteration the restore held exactly the acknowledged writes of both leaders, and the L0 sequence had no gaps.

**Measurements.** These come from one run of `TestMeasure` on local Azurite 3.37.0 in Docker on a developer machine, without the race detector, with `SKYLITE_SPIKE_MEASURE_MIB=192`. They are single runs, not benchmarks. With `SKYLITE_SPIKE_MEASURE_MIB=256`, the database grew to 257.8 MiB and Litestream's compaction into one L1 file produced 272,715,363 bytes. The client refused that file under the 256 MiB limit and the compaction failed (finding 6). An earlier version of this record quoted a restore time after that compaction; it was measured before the limit was enforced, when the SDK uploaded the file in staged blocks, and is withdrawn.

| Measure | Result |
|---|---|
| Acknowledged small commit, 200 in a row | p50 8.7 ms, p90 13.3 ms, p99 20.0 ms, max 21.4 ms |
| Storage requests per acknowledged commit | 1.00 (one Put Blob, no LIST) |
| Commit with a 1.5 MiB row | 52 ms, one request |
| Restore of a 193.8 MiB database, 48 L0 files of 4 MiB | 3.6 s |
| The same after compaction to one L1 file | 2.9 s |
| Restore after the 500-round soak, 7.6 MiB, snapshot + L1 + L0 | 5.5 s (race detector on, during the soak run) |

With the race detector on, as in CI, the median small commit was 9.9 to 13.0 ms across four runs.

Emulator timings say nothing about real Azure latency or throughput.

### Azurite's conditional-write responses

| Operation | Response |
|---|---|
| Put Blob `If-None-Match: *` on an existing blob | HTTP 409 `BlobAlreadyExists` |
| 32 concurrent Put Blob `If-None-Match: *` on one name | exactly one succeeds, the rest get 409 `BlobAlreadyExists` |
| Put Blob `If-Match` with a stale ETag | HTTP 412 `ConditionNotMet` |
| Put Block ×5 + Put Block List `If-None-Match: *` on an existing blob | each Put Block is accepted; Put Block List gets 409 `BlobAlreadyExists`; content unchanged |
| Multi-block upload interrupted before Put Block List | no visible blob (Get Blob Properties 404, absent from listing), **but a later Put Blob `If-None-Match: *` on the name gets 409 `BlobAlreadyExists`** |
| Put Blob whose body is cut off mid-stream | nothing left behind; the SDK's retry with `If-None-Match: *` succeeds |
| Put Blob whose writer is killed with half the body read | a later Put Blob `If-None-Match: *` on the name succeeds (the L0 kill case above) |

### Findings that matter for the architecture

1. **Litestream's Azure client sends no write precondition.** Its `WriteLTXFile` uploads with no access conditions ([abs/replica_client.go#L241-L249](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L241-L249)), and its deletes pass none either ([abs/replica_client.go#L306](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/abs/replica_client.go#L306)). From the source, a stale writer using it can therefore overwrite another writer's files. That is an inference: the spike did not run Litestream's own client. The P2 negative control shows what an unfenced stale leader does with this spike's client in its `unsafe` mode, which drops both the preconditions and the acknowledgement check. It overwrote B's L0 files, deleted them through retention and added a snapshot of its own divergent state.
2. **`SyncAndWait` returning nil does not prove the write was uploaded.** After any failed sync, Litestream clears the replica position ([replica.go#L171-L177](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L171-L177)). The next sync recomputes it from the remote L0 maximum ([replica.go#L184-L190](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L184-L190), [replica.go#L273-L299](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L273-L299)) and uploads only the files after it ([replica.go#L206-L227](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L206-L227)). If another writer is ahead, nothing is uploaded and nil is returned. The spike saw this both for a stale leader and for a candidate's epoch bump. An acknowledgement therefore also requires Litestream's local position to equal the head the client uploaded.
3. **Litestream re-numbers a freshly restored database from whatever the remote holds.** On the first sync, `checkDatabaseBehindReplica` ([db.go#L1689-L1766](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L1689-L1766)) downloads the newest remote L0 file and continues numbering after it. It does not check that the local pages match that position. A candidate that restored to TXID *T* while the old leader appended up to *T+k* would label its stale pages *T+k+1*. Restoring to an explicit TXID, accepting L0 writes only at head + 1, and the position check in finding 2 together catch this. In the recorded race runs it happened on every slot a candidate lost; the test logs it and does not assert it.
4. **Put-if-absent on the next L0 name is not enough on its own.** Snapshots and compactions write ranges (`0009/1-<pos>`, `0001/<min>-<max>`) whose names a stale leader can choose freely. `DB.Snapshot` writes even when a snapshot exists at that position ([db.go#L3056-L3077](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L3056-L3077)). The client therefore also requires a higher-level file to end at or below its own head, and the remote L0 head to be its own (one LIST). In the clean snapshot-first ordering, only that LIST caught A's snapshot.
5. **A fenced client stays stopped.** After a fenced upload, Litestream resumes at remote max + 1, and in the recorded runs a direct `SyncAndWait` on the fenced leader returned nil. The client therefore latches. The semantics test shows a latched client refusing a write its other checks admit: the identical bytes at an existing name. That nothing else would keep a fenced leader from writing a divergent chain is an inference from the source above. No test isolates it.
6. **Azurite holds uncommitted blocks against `If-None-Match: *`.** The test shows a create-if-absent on that name refused right after an interrupted multi-block upload. By inference, a leader killed between Put Block and Put Block List would block that slot for later leaders for as long as the uncommitted blocks remain; how long that is was not tested. The client therefore uploads every LTX file with one Put Blob, and refuses a file above 256 MiB rather than let the SDK split it. Litestream does not keep files below that limit: compacting a 257.8 MiB database into L1 produced one 272,715,363-byte file, which the client refused, so that compaction failed. A snapshot of a database that size would be refused the same way. A product must split LTX files below the limit itself, or use another way to make a multi-part upload safe against a killed writer. Whether real Azure behaves the same as Azurite here is not verified.
7. **The admission checks leave a window, and storage accepts stale mutations in it.** A leader paused after its checks and before the request still adds its snapshot or compaction file, or deletes the files it had chosen. In the three orderings tested, restores were unaffected and the new leader kept working (see Results). Narrowing the window with a further check does not close it. A target blob's ETag can be unchanged across a takeover, so a conditional delete on that ETag still succeeds. Another head check before the request has the same gap between check and request. Closing the window needs an atomic fence in storage. Choosing one is left to the architecture decision. Two candidates are a commit-point manifest updated by compare-and-swap, with file names that are never reused, and a storage-side lease on the blobs. Neither was tested.
8. **In these runs, a takeover made progress only once the old leader stopped writing; the lease is the cooperative way to make it stop.** Nothing in the log reserves a slot for either side: a candidate that finishes its restore and wins the next create-if-absent takes office, and the old leader then loses that slot. In the runs recorded here, an old leader that ignored the lease and committed back to back won every slot while it was writing, because the candidate needs a restore and an upload between two of that leader's commits. B took office only after A had stopped writing. That is what was observed, not a guarantee in either direction. An earlier version of the test let that leader write without limit; the candidate then lost every one of its attempts in every such iteration, and the history stayed intact. When the old leader renewed every 250 ms and stopped on failure, the candidate took office within a few attempts.
9. **Cost, as observed.** In the recorded measurement run an acknowledged commit took one Put Blob and no LIST. `TestMeasure` logs that figure and does not assert it. From the client's code, compactions, snapshots and delete calls each add one LIST for the head check, and snapshot retention, which makes its delete call even when it has nothing to delete, would cost a LIST every time it runs. Those maintenance figures are read from the code, not measured.
10. **Restore time depends on compaction keeping up, and compaction needs to be able to finish.** Earlier versions of the spike showed this; no numbers are given because those versions are gone. In one, a lease-ignoring leader with no time bound wrote a long uncompacted L0 tail, each of the candidate's restores grew with it, and Azurite stopped accepting connections during a restore with a plan of many thousands of files. In another, the kill test compacted only during writes; once a compaction took longer than the kill interval it never completed, the L0 tail grew with every round, and restores slowed and sometimes failed with `max retries exceeded reading ltx file … reopen ltx file`. Litestream opens a stream for every file in the restore plan, up to 32 at a time, before it reads any of them ([replica.go#L790-L823](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L790-L823)), so a long tail means that many open HTTP streams. That this is why the restores failed is a hypothesis; the failing HTTP status was not captured. The kill test now catches up on compaction when it takes office and snapshots every tenth round, as a `Store` schedule would, and `make integration` prints the Azurite log on failure.

### Dependency versions

The spike uses `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` v1.8.0 rather than a later v1.8.x because of what the later versions pull in. In `go mod graph`, azblob v1.8.1 requires `github.com/apache/arrow-go/v18` v18.7.0 ([go.mod#L10](https://github.com/Azure/azure-sdk-for-go/blob/04c2e789a89c5ac2652380748fcdb6d3c7f9a2a0/sdk/storage/azblob/go.mod#L10)), which azblob imports through its `internal/arrow` package from `azblob/container` ([container/client.go#L24](https://github.com/Azure/azure-sdk-for-go/blob/04c2e789a89c5ac2652380748fcdb6d3c7f9a2a0/sdk/storage/azblob/container/client.go#L24)). arrow-go v18.7.0 in turn requires `modernc.org/sqlite` v1.53.0 ([go.mod#L53](https://github.com/apache/arrow-go/blob/0716bc64bc48e7322fcae137e14cf74cd94747fe/go.mod#L53)). Upgrading azblob to v1.8.1 or v1.8.3 therefore raises the SQLite driver from v1.49.1, the version Litestream v0.5.18 requires ([go.mod#L35](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/go.mod#L35)), to v1.53.0, and adds five modules: arrow-go, `goccy/go-json`, `google/flatbuffers`, `klauspost/cpuid/v2` and `zeebo/xxh3` (v1.8.3 also adds `sdk/storage/internal`, [go.mod#L10-L11](https://github.com/Azure/azure-sdk-for-go/blob/28e4858eb8038b76d50dc0a6c9cc983aa737a9b7/sdk/storage/azblob/go.mod#L10-L11)). v1.8.0 requires none of them ([go.mod](https://github.com/Azure/azure-sdk-for-go/blob/cf0d0a3e3657f5e432912f09a384a65ed574bb02/sdk/storage/azblob/go.mod)). To reproduce, in a scratch copy of the module run `go get github.com/Azure/azure-sdk-for-go/sdk/storage/azblob@v1.8.1`, then `go mod graph | grep -E 'azblob@v1.8.1 github.com/apache|arrow-go/v18@v18.7.0 modernc'`.

### Limits: what is not verified

- Emulator only. Real Azure latency, throughput, Entra ID authentication, and conditional-write behaviour under load and across the real service's front ends were not tested. Neither was the uncommitted-block behaviour in finding 6.
- No network partition below the process: in-flight HTTP requests that land after a pause were exercised only through `SIGKILL`, and there was no clock skew or slow storage.
- Kills inside an upload were placed at one point, half of the body read by the transport, once per upload kind. The random soak does not show where its kills landed.
- The stale-mutation window was measured in three orderings only (see Results). Other orderings may behave differently, and nothing in the spike closes the window.
- The lease has no expiry, so lease timing and detection of an expired holder were not tested.
- `litestream.Store` was not run on its schedule; its operations were called directly. Group commit, concurrent writers, follower reads and the VFS were not covered.
- S3, Google Cloud Storage and the local filesystem were not covered. Litestream's own replica clients were not run.
- Databases larger than 193.8 MiB. The client refuses any compaction or snapshot file above 256 MiB, so as it stands it cannot maintain a database whose files grow past that.
- The exact cause of the long-plan restore failures in finding 10: neither the HTTP status nor why Azurite stopped was captured.

### Reproduce

```sh
make integration                                                     # what CI runs: 20 kill rounds, race detector on
SKYLITE_SPIKE_KILL_ROUNDS=500 INTEGRATION_TIMEOUT=90m make integration   # the soak quoted above
```

`make integration` needs Docker. It starts a uniquely named Azurite container on a free local port and removes it afterwards. Its `go test` timeout is 6 minutes, below the CI job's 10; raise `INTEGRATION_TIMEOUT` for a longer soak. Each randomised test logs its seed; set `SKYLITE_SPIKE_SEED` to replay one. `SKYLITE_SPIKE_RACE_ROUNDS` and `SKYLITE_SPIKE_MEASURE_MIB` size the race and measurement tests.

## Decision

The results are accepted as evidence for the architecture decision, which is recorded separately. Three rules are adopted from them. Each is a requirement on whatever storage engine skylite builds, not a description of one:

1. A commit is acknowledged only after skylite's own storage layer has confirmed the commit object in object storage. A nil return from a replication library is not that confirmation (finding 2).
2. Every object that becomes part of the durable history is written with create-if-absent. A leader that loses a create to different content stops writing for good (findings 4 and 5). A new leader takes office by winning a create-if-absent for an epoch-bump commit.
3. The lease is for takeover progress only. No safety argument may rest on it (finding 8).

Two questions are left to the architecture decision on purpose:

- **How to close the stale-mutation window** (finding 7). The tested client does not close it.
- **Whether Litestream stays in the write path.** The spike shows it can be contained from outside without a fork, for a database whose files stay under the single-upload limit. It also shows that limit (finding 6), and that findings 2, 3 and 4 each needed a guard of our own. The spike kept a full local copy of the database; it did not test on-demand page reads or databases of the size skylite is planned for.

The evidence supports only this:

- Against Azurite, with Litestream v0.5.18 embedded as a library, the replica client and the acknowledgement rule described under Method, P1 held in the tested kill scenarios and P2 held in the six tested orderings. P2 does not hold for a stale leader paused between its admission checks and the storage request: storage accepted its stale snapshot, compaction file and deletes, although in the three tested orderings the restores were unaffected.
- The early-acknowledgement control shows that acknowledging before the upload loses acknowledged writes on a kill, and that the checks detect it.
- Litestream's Azure client sends no write precondition, so, from the source, a stale writer using it could overwrite the history. The spike did not run that client.
- Litestream as a library needs no fork for this. The LTX writes, deletes and restore reads the spike exercised all go through `Replica.Client` ([db.go#L884](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L884), [db.go#L3067](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/db.go#L3067), [replica.go#L256](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L256), [replica.go#L698](https://github.com/benbjohnson/litestream/blob/057d39564c75ebd67ba042410a7ffa05ad4c242d/replica.go#L698)). The two behaviours that would break the properties, findings 2 and 3, can be contained from outside.
- In the tested scenarios, safety did not depend on the lease. A takeover made progress once the old leader had stopped writing; a failed lease renewal is one way to make it stop (finding 8).

## Alternatives considered

None were evaluated in this spike.

## Consequences

- If the design goes ahead, the custom replica client and the acknowledgement check become product code with their own tests. The window in finding 7 must be closed with an atomic fence in storage, or accepted with its consequences stated. Choosing the fence is part of the architecture decision.
- The architecture page stays design direction until the architecture decision is recorded.
- On 2026-11-15 the project owner decided to do all development and testing on emulators for now, because real cloud accounts cost money. Verification against real Azure, S3 and Google Cloud Storage is deferred by that decision. Until it is done, every result on this page is emulator-only.
