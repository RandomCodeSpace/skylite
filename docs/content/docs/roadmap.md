---
title: Roadmap and status
linkTitle: Roadmap and status
description: What is done, what is planned, and in which order.
weight: 30
status: current
navgroup: project
---

skylite is beta. The binary's server authenticates requests, creates databases, leads the databases it is configured for, answers stateless reads on leaders and opt-in followers, runs autocommit writes, batches and transactions on leader-owned write channels and read-only transactions on leader-owned read-only channels, settles lost writes with resolve, and forwards a request it cannot serve one hop to the pod that serves it. Leading a database includes compacting its commit log. The [Go driver](/docs/go-driver/), with writes, transactions, read-only transactions, retries and resolve, passes a GORM acceptance test, and the [JDBC driver](/docs/jdbc-driver/) with writes, transactions and a Hibernate dialect passes the Spring Data JPA acceptance suite against the server. The done milestones come first, then the planned ones in order. A done milestone that left work behind says what, and that work is a planned item below it. The storage backends are tested against emulators, and the Azure Blob backend also by hand against the real service, failover included, as [Storage backends](/docs/storage-backends/) says.

{{< roadmap >}}
