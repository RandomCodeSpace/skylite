---
title: Roadmap and status
linkTitle: Roadmap and status
description: What is done, what is planned, and in which order.
weight: 30
status: current
---

skylite is pre-alpha. The binary's server authenticates requests and creates databases, and runs no SQL yet. The milestones below are in planned order. Only the first is done. Parts of the storage milestone exist as library packages, described in [Storage backends](/docs/storage-backends/).

{{< roadmap >}}

## Why the order changed

The spike tested the two claims the design rests on, that no acknowledged write is lost when a pod dies and that a stale leader cannot commit, against Azurite, the Azure Storage emulator. Its results are in [0002](/docs/decisions/0002-durability-spike/). The storage architecture that followed is [0004](/docs/decisions/0004-storage-engine/): skylite will build its own storage engine instead of embedding Litestream. That engine now comes straight after the storage interface, because leader failover and read replicas are planned on top of its commit log and manifest. Every milestone after the first is a plan, and the record lists what the engine's tests must prove before the design is relied on.
