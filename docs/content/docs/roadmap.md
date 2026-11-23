---
title: Roadmap and status
linkTitle: Roadmap and status
description: What is done, what is planned, and in which order.
weight: 30
status: current
navgroup: project
---

skylite is beta. The binary's server authenticates requests, creates databases, leads the databases it is configured for, answers stateless reads on leaders and opt-in followers, runs autocommit writes and transactions sent on leader-owned write channels, and forwards a request it cannot serve one hop to the pod that serves it. Leading a database includes compacting its commit log. The [Go driver](/docs/go-driver/), with writes and transactions, passes a GORM acceptance test, and the [JDBC driver](/docs/jdbc-driver/) with writes, transactions and a Hibernate dialect passes the Spring Data JPA acceptance suite against the server. The done milestones come first, then the planned ones in order. A done milestone that left work behind says what, and that work is a planned item below it. The storage backends are tested against emulators, and the Azure Blob backend also by hand against the real service, as [Storage backends](/docs/storage-backends/) says.

{{< roadmap >}}

## Why the order changed

The spike tested the two claims the design rests on, that no acknowledged write is lost when a pod dies and that a stale leader cannot commit, against Azurite, the Azure Storage emulator. Its results are in [0002](/docs/decisions/0002-durability-spike/). The storage architecture that followed is [0004](/docs/decisions/0004-storage-engine/): skylite builds its own storage engine instead of embedding Litestream. That engine came straight after the storage interface, because leader failover and read replicas are built on top of its commit log and manifest. The record lists what the engine's tests must prove before the design is relied on.
