---
title: "0002: Durability spike against Azurite"
description: Results of a spike that tested acknowledged-write durability and stale-leader fencing with Litestream as a library against Azurite. Proposed; the architecture decision follows separately.
weight: 2
---

**Status:** Proposed

**Date:** 2026-11-15

This record reports a spike. The code is throwaway test code under `internal/spike/durability/`; it is not part of skylite, nothing on this page describes a feature, and the spike may be deleted or replaced.

## Context

### Question

Two properties decide the architecture:

- **P1, no acknowledged write is lost.** A write is acknowledged only after its change file is confirmed in object storage. If the process dies at any moment and a new one restores on an empty disk, every acknowledged write is there.
- **P2, a stale leader cannot commit.** Once a new leader has taken office, a leader that was paused or partitioned and then resumes cannot get another write acknowledged or into the durable history, and cannot damage the history: no overwrite, no deletion, no divergent file that a restore would apply.

The plan under test embeds [Litestream](https://github.com/benbjohnson/litestream) v0.5.18 (Apache-2.0, the latest tag when the spike ran) as a library, with the pure-Go SQLite driver `modernc.org/sqlite`, and fences stale leaders with conditional writes in a replica client of our own.

### Method

- **Replica client.** A `litestream.ReplicaClient` for Azure Blob Storage. It keeps Litestream's key layout and metadata and creates every LTX file, at every level, with `If-None-Match: *`. It tracks its head: the highest L0 transaction it has confirmed in storage itself. It accepts an L0 file only at head + 1, and a higher-level file (compaction or snapshot) only if the file ends at or below its head and a LIST shows the remote L0 head is still its own; deletes need the same LIST check. When a create conflicts, it downloads the existing blob. Identical bytes mean an earlier attempt of the same request succeeded, so it counts as success. Different bytes at L0, or any failed check, latch the client into a permanent fenced state. After that it refuses every write and delete.
- **Leader.** It restores to an explicit TXID on an empty directory, then takes office by committing an epoch bump (an integer in a metadata table) and uploading it with put-if-absent. If it loses that slot, it discards the directory and starts again, up to 20 attempts. A write is one SQLite transaction followed by `db.SyncAndWait`. It is acknowledged only if that returns nil *and* Litestream's local position equals the client's uploaded head. Litestream's background monitors are off.
- **Lease.** A JSON blob with an epoch, created with `If-None-Match: *` and taken over or renewed with `If-Match: <etag>`. It is advisory and has no expiry. The tests drive takeover explicitly.
- **Environment.** Azurite 3.37.0 in Docker, with the race detector on in CI.

### What each test demonstrates

| Test | Demonstrates | How it can fail |
|---|---|---|
| `TestEmulatorSemantics` | Azurite's conditional-write behaviour, recorded below; the identical-content retry rule; the lease CAS | Each subtest asserts the exact status and error code |
| `TestP1KillRestore` | P1 under `SIGKILL` at random moments | `TestP1NegativeControl` acknowledges before the upload and must lose writes |
| `TestP2StaleLeaderCannotCommit` | P2 in four orderings (see below) | `TestP2NegativeControl` turns A's fencing off and must show damage |
| `TestTakeoverRace` | One linear history when B takes office while A writes | Exact restore comparison in every iteration |
| `TestMeasure` | Latency, requests per commit, restore time (reported, not asserted) | — |

- **P1 kill test.** A child process (the test binary re-executed) restores into a new empty directory, takes office and inserts rows with increasing IDs. A quarter of the rows also carry a 1.5 MiB blob, so kills land in the middle of uploads. It compacts L0 into L1 when it takes office and after every tenth row, and writes a snapshot every tenth round and after every fiftieth row, so kills also land in compaction and snapshot uploads. It prints `ack <id>` only after `Exec` returns. The parent kills it with `SIGKILL` after a random, seeded delay: a quarter of the kills count from process start, so they can land in the restore or the epoch bump. A final restore on an empty directory must contain every acknowledged ID and pass `PRAGMA integrity_check`. An ID that is present but was never acknowledged is allowed only if it was the write a round had begun and not acknowledged when it was killed; its upload may have finished before the kill. Epochs must increase by one per round that took office. A round killed after its bump landed but before it reported shows up as a jump of two in the next round. The L0 sequence must have no gaps.
- **P2 stale-leader test.** Leader A is held in-process rather than stopped with `SIGSTOP`. With the monitors off, Litestream runs no goroutine of its own, so a leader the test does not call looks exactly like a paused process to storage. Holding it in-process also lets the test drive the compaction, snapshot and retention calls a `litestream.Store` would make. A acknowledges writes, then pauses in one of four states: with a commit whose L0 file is built but not uploaded; clean; clean but resumed with maintenance first; or with its replica position reset, as Litestream does after any failed sync. B takes the lease, restores, takes office, writes and compacts. A then resumes: it writes, calls `SyncAndWait` directly (Litestream's own retry path) and runs snapshot, snapshot retention, TXID retention, L0 retention and compaction. The test asserts that A acknowledges nothing and stays fenced, that A's lease renewal fails, that every blob keeps its ETag and none is added or removed, that B can still write, and that a fresh restore contains exactly A's pre-takeover writes and all of B's.
- **Takeover race.** A writes continuously while B takes the lease and office. In half the iterations A renews its lease every 250 ms and stops when renewal fails. In the other half A ignores the lease and writes back to back for 3 s. A restore must hold exactly the acknowledged writes of both, and A must be fenced on any write after B took office.

### Results

**P1 held.** The 500-round soak (seed `1794729174195333951`) killed 113 writers before they reported taking office and 91 during a write carrying a 1.5 MiB row. It acknowledged 3,185 IDs, and the final restore held all of them. It also held 64 unacknowledged in-flight writes, all within the rule above, and `integrity_check` returned `ok`. The CI configuration of 20 rounds passed in five consecutive local `make integration` runs of the final code, and in ten runs of earlier revisions. In the negative control, which acknowledges before uploading, every round lost the write it had acknowledged.

**P2 held** in all four orderings. A's writes all failed with `ErrFenced`, A's lease renewal got 412, and no blob changed ETag, appeared or disappeared. The restore held exactly A's five pre-takeover rows and B's six, at epoch 2. Each ordering was stopped by a different check:

| Ordering | Check that fenced A |
|---|---|
| Pending upload, writes first | the L0 create conflict (409, different content) |
| Clean, maintenance first | the remote-head LIST before the snapshot |
| Pending upload, maintenance first | the snapshot ending past A's uploaded head |
| Replica position reset | the position-equals-head acknowledgement check, after `SyncAndWait` had returned nil without uploading |

In the negative control (A's fencing off), A acknowledged four post-takeover writes. It overwrote B's L0 files, deleted twelve of them through snapshot retention and added a snapshot of its own state, and the checks reported all of it. Two manual controls also turned the suite red, and both were reverted. With the acknowledgement check removed, A acknowledged four writes that never reached storage. With `If-None-Match` removed, the pending-upload ordering failed, and so did the race test: the restored epoch fell back to 1.

**Takeover race.** When A honoured the lease, B took office in one to three attempts, and A was fenced on its next write. When A ignored the lease for 3 s, B lost every attempt it made while A was writing, seven to nine of them. B took office 0.7 to 1.7 s after A stopped. That was six iterations with the race detector on. In every iteration the restore held exactly the acknowledged writes of both leaders, and the L0 sequence had no gaps.

**Measurements.** These come from local Azurite 3.37.0 in Docker on a developer machine, without the race detector. Single runs, not benchmarks.

| Measure | Result |
|---|---|
| Acknowledged small commit, 200 in a row | p50 11.2 ms, p90 19.1 ms, p99 26.4 ms, max 28.7 ms (an earlier run on the same container: p90 52 ms, p99 141 ms) |
| Storage requests per acknowledged commit | 1.00 (one Put Blob, no LIST) |
| Commit with a 1.5 MiB row | 62–97 ms, one request |
| Restore of a 257.8 MiB database, 64 L0 files of 4 MiB | 4.5 s |
| The same after compaction to one L1 file | 3.3 s |
| Restore after the 500-round soak, 7.6 MiB, snapshot + L1 + L0 | 0.6 s |

With the race detector on, as in CI, the median small commit was about 26 ms.

Emulator timings say nothing about real Azure latency or throughput.

### Azurite's conditional-write responses

| Operation | Response |
|---|---|
| Put Blob `If-None-Match: *` on an existing blob | HTTP 409 `BlobAlreadyExists` |
| 32 concurrent Put Blob `If-None-Match: *` on one name | exactly one succeeds, the rest get 409 `BlobAlreadyExists` |
| Put Blob `If-Match` with a stale ETag | HTTP 412 `ConditionNotMet` |
| Put Block ×5 + Put Block List `If-None-Match: *` on an existing blob | blocks accepted; Put Block List gets 409 `BlobAlreadyExists`; content unchanged |
| Multi-block upload interrupted before Put Block List | no visible blob (Get Blob Properties 404, absent from listing), **but a later Put Blob `If-None-Match: *` on the name gets 409 `BlobAlreadyExists`** |
| Put Blob whose body is cut off mid-stream | nothing left behind; the SDK's retry with `If-None-Match: *` succeeds |

### Findings that matter for the architecture

1. **With Litestream's stock clients, P2 fails.** The negative control shows a stale leader overwriting the new leader's L0 files, deleting them through retention and adding a snapshot of its own divergent state. None of Litestream's storage clients sends a precondition.
2. **`SyncAndWait` returning nil does not prove the write was uploaded.** After any failed sync, Litestream clears the replica position (`replica.go:171-177` at v0.5.18). The next sync recomputes it from the remote L0 maximum (`replica.go:184-190, 274-299`). If another writer is ahead, nothing is uploaded and nil is returned. The spike saw this both for a stale leader and for a candidate's epoch bump. An acknowledgement therefore also requires Litestream's local position to equal the head the client uploaded.
3. **Litestream re-numbers a freshly restored database from whatever the remote holds.** On the first sync, `checkDatabaseBehindReplica` (`db.go:1689-1766`) downloads the newest remote L0 file and continues numbering after it. It does not check that the local pages match that position. A candidate that restored to TXID *T* while the old leader appended up to *T+k* would label its stale pages *T+k+1*. Restoring to an explicit TXID, accepting L0 writes only at head + 1, and the position check in finding 2 together catch this. In the race test it happened on every slot a candidate lost.
4. **Put-if-absent on the next L0 name is not enough on its own.** Snapshots and compactions write ranges (`0009/1-<pos>`, `0001/<min>-<max>`) whose names a stale leader can choose freely. `DB.Snapshot` writes even when a snapshot exists at that position (`db.go:3056-3060`). The client therefore also requires a higher-level file to end at or below its own head, and the remote L0 head to be its own (one LIST). In the maintenance-first ordering, only that LIST caught A's snapshot.
5. **The latch is necessary.** After a fenced upload, Litestream resumes at remote max + 1 as predicted, and a direct `SyncAndWait` on the fenced leader returned nil. Only the latch keeps that leader from writing a divergent chain.
6. **Azurite holds uncommitted blocks against `If-None-Match: *`.** A leader killed between Put Block and Put Block List would wedge that slot for every later leader. The client therefore uploads every LTX file with one Put Blob, which is atomic. The SDK does this up to 256 MiB, so a product must keep LTX files below that or split them. Whether real Azure behaves the same is not verified.
7. **Liveness comes from the lease, not from the log.** An old leader that ignores the lease and commits back to back wins the next slot every time, because a candidate needs a restore and an upload between two of its commits. An earlier version of the test let that leader write without limit. The candidate then lost all 20 of its attempts in every such iteration (six iterations locally), and the history stayed intact. When the old leader renewed every 250 ms and stopped on failure, the candidate took office within one to three attempts.
8. **Cost.** In steady state, an acknowledged commit costs one Put Blob and no LIST. Compactions, snapshots and deletes each add one LIST for the head check.
9. **Restore time depends on compaction keeping up, and compaction needs to be able to finish.** On the first CI run, the unbounded lease-ignoring leader from finding 7 acknowledged 14,022 writes without compacting. Each of the candidate's restores grew with that L0 tail. During the 16th restore, a plan of about 13,000 files, Azurite stopped accepting connections, and every later test in the run failed. The Azurite log was lost with its container; `make integration` now prints it on failure. An earlier version of the kill test compacted only during writes. Once a compaction took longer than the kill interval, it never completed, and the L0 tail grew with every round. After about 300 rounds the restore plan held 1,752 files: one snapshot, 29 L1 files and an L0 tail of 1.5 MiB files. That restore took 60 s for a 7.5 MiB database. One attempt made while another restore was running failed with `max retries exceeded reading ltx file … reopen ltx file`, and a candidate then took more than two minutes to take office. Litestream opens a stream for every file in the plan before it reads any of them (`replica.go:796-824`), so a long tail means that many concurrent HTTP streams. That this is why the restore failed is a hypothesis; the failing HTTP status was not captured. The kill test now catches up on compaction when it takes office and snapshots every tenth round, as a `Store` schedule would.

### Limits: what is not verified

- Emulator only. Real Azure latency, throughput, Entra ID authentication, and conditional-write behaviour under load and across the real service's front ends were not tested. Neither was the uncommitted-block behaviour in finding 6.
- No network partition below the process: in-flight HTTP requests that land after a pause were exercised only through `SIGKILL`, and there was no clock skew or slow storage.
- The lease has no expiry, so lease timing and detection of an expired holder were not tested.
- A residual window remains between the head-check LIST and a higher-level write or delete. A leader paused inside it can still add a level-1 or snapshot file ending at or below its head, whose content is true history, or delete files that Litestream's retention considers covered. Deletes carry no precondition. This window was reasoned about, not exercised.
- `litestream.Store` was not run on its schedule; its operations were called directly. Group commit, concurrent writers, follower reads and the VFS were not covered.
- S3, Google Cloud Storage and the local filesystem were not covered.
- Databases larger than 258 MiB, LTX files larger than 256 MiB, and the exact cause of the long-plan restore failures in finding 9: neither the HTTP status nor why Azurite stopped was captured.

### Reproduce

```sh
make integration                                 # what CI runs: 20 kill rounds, race detector on
SKYLITE_SPIKE_KILL_ROUNDS=300 make integration   # longer kill soak
```

`make integration` needs Docker. It starts a uniquely named Azurite container on a free local port and removes it afterwards. Each randomised test logs its seed; set `SKYLITE_SPIKE_SEED` to replay one. `SKYLITE_SPIKE_RACE_ROUNDS` and `SKYLITE_SPIKE_MEASURE_MIB` size the race and measurement tests.

## Decision

To be written by the project lead. The evidence supports only this:

- Against Azurite, P1 and P2 held with Litestream v0.5.18 embedded as a library, provided the replica client and the acknowledgement rule described under Method are used. They do not hold with Litestream's own clients.
- Litestream as a library needs no fork for this. Every LTX write, delete and restore read goes through `Replica.Client` (`db.go:884, 3067`; `replica.go:698`). The two behaviours that would break the properties, findings 2 and 3, can be contained from outside.
- Safety does not depend on the lease. Liveness does.

## Alternatives considered

None were evaluated in this spike.

## Consequences

- If the design goes ahead, the custom replica client and the acknowledgement check become product code with their own tests. The residual window in the limits must be closed or accepted. One way to close it is conditional deletes plus writing higher-level files only after a fresh head check.
- The architecture page stays design direction until the lead records a decision.
- Before relying on the results, the conditional-write tests should be repeated against a real Azure storage account.
