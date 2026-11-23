---
title: Sizing
description: How much memory and local disk a skylite server needs, and the fixed limits that bound one server.
weight: 30
navgroup: guides
status: current
---

A skylite server keeps no durable state on its own disk, so it needs no persistent volume. It does need enough memory for its fixed budget and a little local scratch space.

## Memory

The server's memory budget is fixed at 2 GiB. It is **not** read from the container's memory limit, so a lower limit does not make the server use less: under load it can exceed the limit and be killed.

| Component | Size |
|---|---|
| Memory budget: request bodies (up to 1 GiB), channels and held results (128 MiB), SQLite connections (up to 64 MiB each, in the remaining 896 MiB) | 2 GiB at most |
| Page cache for pages read from storage, shared by every database the server leads or follows | 64 MiB |
| Go runtime, HTTP connections and response buffers | not budgeted; not measured separately |

Set the container's memory limit to about **2.5 GiB**, and the request to the same value so the pod is not evicted first under node pressure. If the temporary directory is memory-backed (a Kubernetes `emptyDir` with `medium: Memory`, or a tmpfs), its files count against the memory limit as well; see below.

## Local disk

The server writes these files under the system temporary directory, `TMPDIR` (`/tmp` when unset):

| Files | Size |
|---|---|
| Each led database's local write-ahead log, before its commits are uploaded | up to 64 MiB per led database |
| SQLite's temporary files | up to 256 MiB per led database; a statement that needs more fails with SQLite's `SQLITE_FULL` |
| Spool files for uploads larger than 4 MiB | one per upload in progress, deleted when it ends |

Each server process keeps its files in a directory of its own, `skylite-<host name>_<random>`, removed on a clean shutdown. A process that crashes leaves its directory behind, and the next process does not reuse or remove it: an `emptyDir` keeps it across container restarts until the pod is deleted. Pages are read from object storage on demand and cached in memory, so there is no local copy of the database to keep. A small `emptyDir` mounted at `TMPDIR` (or at `/tmp`) is enough. The first two rows add up to 320 MiB per led database; leave room above that for spool files and for directories left by crashed processes. A read-only root filesystem works as long as `TMPDIR` is writable.

## CPU

CPU use has not been measured. Write throughput is bound by the round trip to object storage, not by CPU; see the [measured numbers on Azure](/docs/azure-blob/#performance).

## Limits of one server

These are fixed in this version; none is configurable.

| Limit | Value |
|---|---|
| Databases one server lists in `databases` | 13 |
| SQLite connections, shared by reads and writes | 14, less one for each led database |
| Concurrent client connections | 1,024; more wait in the operating system's listen queue |
| Concurrent streams on one HTTP/2 connection | 16 |
| Request body | 16 MiB |
| Result of one query | 64 MiB |
| Rows returned by one autocommit statement on a write channel | 1 MiB |
| Statement time | 30 seconds; a client can lower it |
| Write lock wait | 5 seconds, then `busy` |
| Writers waiting for one database's write lock | 64, and 16 per principal |
| Open transaction: idle time, lifetime | 30 seconds, 5 minutes |
| Write channels: per server, per database, per principal on one database | 16,384, 4,096, 256 |

A principal (one issuer and subject) may use at most a quarter of the connections and of the request body budget at once, so that one client cannot take the whole server. The [server reference](/docs/server/) gives the exact rules.
