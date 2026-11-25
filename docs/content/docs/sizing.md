---
title: Sizing
description: How much memory and local disk a skylite server needs, and the fixed limits that bound one server.
weight: 30
navgroup: guides
status: current
---

A skylite server keeps no durable state on its own disk, so it needs no persistent volume. It does need enough memory for its fixed budget and a little local scratch space.

## Memory

The server's memory budget is three quarters of the container's memory limit, read at start from the cgroup (`memory.max` on cgroup v2, `memory.limit_in_bytes` on v1), or of `GOMEMLIMIT` if that is lower. Without either it is 2 GiB. The server logs the budget it chose at start, with where it came from, the request body limit and the SQLite connections, and warns when `memory_budget` is above the container's limit. The quarter left over is for what the budget does not hold:

| Component | Size |
|---|---|
| Memory budget: request bodies and response frames over 1 MiB (half the budget, less 1 MiB for closes, cancels and resolves and 32 MiB per listed database for resolve's commit headers), channels and held results (128 MiB), SQLite connections (64 MiB each, in the rest, at most 256) | three quarters of the limit |
| Page cache for pages read from storage, shared by every database the server leads or follows | 64 MiB |
| Go runtime, HTTP connections and response frames up to 1 MiB | not budgeted |

Under steady writes the server uses far less than its budget: on Azure Blob Storage at about 26 commits a second, a leader's resident memory was 215–242 MiB and a follower's 186–191 MiB. The budget is the worst case, with every body, channel and connection at its limit.

| Container memory limit | Budget | Request body limit, one listed database | SQLite connections, less one per led database | Per principal, with one led database |
|---|---|---|---|---|
| 512 MiB, the least the server starts with, leading no database | 384 MiB | 5.97 MiB with none listed | 1 | none: one led database needs 683 MiB |
| 1 GiB | 768 MiB | 10.97 MiB | 4 | 1 |
| 1,454 MiB | 1,090 MiB | 16 MiB | 6 | 1 |
| 2 GiB | 1.5 GiB | 16 MiB | 10 | 2 |
| 2,731 MiB (about 2.67 GiB) | 2 GiB | 16 MiB | 14 | 3 |
| 4 GiB | 3 GiB | 16 MiB | 22 | 5 |
| 8 GiB | 6 GiB | 16 MiB | 46 | 11 |

Set the container's memory request to the same value as the limit, so the pod is not evicted first under node pressure. Below a budget of 1,026 MiB plus 64 MiB per listed database (1,090 MiB, a limit of about 1,454 MiB, with one) the request body limit is a 32nd of the body share instead of 16 MiB, so that a principal's quarter of the share still holds one body charged at 8 ×; clients read it from `request_body_bytes` in the `limits` object, and a larger body gets `413` `request_too_large`. The server does not start only when the budget cannot hold a SQLite connection for each database in `databases` and one for clients: that takes a budget of 384 MiB plus 128 MiB per listed database (a limit of 512 MiB plus about 171 MiB per database), and the error names both. `memory_budget` overrides the derived budget in general; it is for a pod whose memory limit also holds other things, such as a memory-backed temporary directory. If the temporary directory is memory-backed (a Kubernetes `emptyDir` with `medium: Memory`, or a tmpfs), its files count against the memory limit too, up to 320 MiB per led database (see below), and the quarter the budget leaves does not cover them: add them to the limit and set `memory_budget` to three quarters of what the limit would otherwise be.

Give every pod that serves a database the same memory budget: the same memory limit, `GOMEMLIMIT` and `memory_budget`. The request body limit follows from the budget, a pod that forwards a request checks the body against its own limit, and a channel's `limits` advertise the limit of the leader that opened it. With unequal budgets below that threshold, a request can pass on one pod and get `413` `request_too_large` on another. A refused request is not executed, so nothing is lost, but the client sees a refusal that depends on which pod it reached.

## Local disk

The server writes these files under the system temporary directory, `TMPDIR` (`/tmp` when unset):

| Files | Size |
|---|---|
| Each led database's local write-ahead log, before its commits are uploaded | up to 64 MiB per led database |
| SQLite's temporary files | up to 256 MiB per led database; a statement that needs more fails with SQLite's `SQLITE_FULL` |
| Spool files for uploads larger than 4 MiB | one per upload in progress, deleted when it ends |

Each server process keeps its files in a directory of its own, `skylite-<host name>_<random>`, removed on a clean shutdown. A process that crashes leaves its directory behind, and the next process does not reuse or remove it: an `emptyDir` keeps it across container restarts until the pod is deleted. Pages are read from object storage on demand and cached in memory, so there is no local copy of the database to keep. A small `emptyDir` mounted at `TMPDIR` (or at `/tmp`) is enough. The first two rows add up to 320 MiB per led database; leave room above that for spool files and for directories left by crashed processes. A read-only root filesystem works as long as `TMPDIR` is writable.

## Object storage

Each commit is one object in the database's commit log. Once a compaction has folded a commit into the base and the five-minute grace period has passed, the leader rewrites the commit to a 144-byte header, 32 commits at a time; the object itself is never deleted. So storage holds the base, about the size of the database, the commits of the last few minutes in full, and 144 bytes for every commit ever made: 144 MB per million commits, about 4.5 GB a year at one commit a second. Object versioning or soft delete on the container keeps the full form of each rewritten commit until that policy expires it.

## CPU

CPU use has not been measured. Write throughput is bound by the round trip to object storage, not by CPU; see the [measured numbers on Azure](/docs/azure-blob/#performance).

## Limits of one server

These are fixed in this version; none is configurable but through the memory budget, from which the first two follow.

| Limit | Value |
|---|---|
| Databases one server lists in `databases` | one fewer than the SQLite connections; 13 at a 2 GiB budget |
| SQLite connections, shared by reads and writes | what the budget holds, 14 at 2 GiB and at most 256, less one for each led database |
| Concurrent client connections | 1,024; more wait in the operating system's listen queue |
| Concurrent requests: all but closes, cancels and resolves; of them, queries and describes | 1,024 and 256 per server; 256 and 64 per principal |
| Concurrent closes and cancels; concurrent resolves | 64 each per server, 16 each per principal |
| Concurrent streams on one HTTP/2 connection | 16 |
| Request body | 16 MiB; less with a budget under 1,026 MiB plus 64 MiB per listed database (see the table above) |
| Result of one query | 64 MiB |
| Rows returned by one autocommit statement on a write channel | 1 MiB |
| Statement time | 30 seconds; a client can lower it |
| Write lock wait | 5 seconds, then `busy` |
| Writers waiting for one database's write lock | 64, and 16 per principal |
| Open transaction: idle time, lifetime | 30 seconds, 5 minutes |
| Write channels: per server, per database, per principal on one database | 16,384, 4,096, 256 |

A principal (one issuer and subject) may use at most a quarter of the connections and of the request body budget at once, so that one client cannot take the whole server. The [server reference](/docs/server/) gives the exact rules.
