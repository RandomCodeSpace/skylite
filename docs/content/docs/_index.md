---
title: Documentation
linkTitle: Overview
description: What skylite is, who it is for, what has been verified, and where to start.
status: current
listChildren: false
---

skylite is a SQL database server whose only durable state is in object storage: Azure Blob Storage, Amazon S3, Google Cloud Storage, or a local directory for trials. It runs SQLite underneath, so the SQL dialect is SQLite's. Applications connect over HTTPS with a Go `database/sql` driver or a JDBC driver, and authenticate with OIDC JWT bearer tokens. A server keeps nothing on its own disk that it cannot lose, so it runs without a persistent volume.

## Who it is for

skylite suits services that want a small relational database without running a stateful database tier, and that can accept its trade-offs:

- **Writes wait for object storage.** Every commit is uploaded and confirmed before it is acknowledged. Against a real Azure account that measured a median of about 17 ms per commit with one writer, and about 50 to 70 commits per second per database ([measurements](/docs/azure-blob/#performance)).
- **One writer per database.** Each database has one leader at a time; writes and transactions run on it, one after another. Other servers can answer reads and take over when the leader stops.
- **SQLite's SQL**, behind a server-side [SQL policy](/docs/decisions/0007-sql-policy/) that limits what statements may do.

## Status

skylite is **beta**. The current release is v0.0.3.

What exists and is tested on every pull request:

- The server: database creation, reads, autocommit writes, batches and transactions, read-only transactions, resolve of lost writes, leader election with fencing, failover, follower reads, and forwarding between servers.
- The Go driver, through a GORM acceptance suite against a real server.
- The JDBC driver and Hibernate dialect, through a 29-test Spring Data JPA acceptance suite against a real server.
- The storage backends, against emulators: Azurite (Azure), RustFS (S3) and fake-gcs-server (Google Cloud Storage).

Also verified by hand against a real Azure storage account: the Azure backend's integration tests and the GORM suite, authenticated with an Entra ID service principal, and a failover soak of three servers on one machine in which no acknowledged write was lost ([failover timings](/docs/operations/#failover)).

Not verified yet: Amazon S3 and Google Cloud Storage themselves (emulators only), a Kubernetes cluster, a service mesh or ingress, a real Entra ID tenant as the token issuer for clients, driver failover across several pods, Azure workload and managed identity, and a 24-hour soak.

Not available: a container image, a Helm chart, and published Maven artifacts. Releases are signed binaries for Linux and macOS, and the Java artifacts are built from source. The [roadmap](/docs/roadmap/) lists what is planned.

## Where to start

- [Quick start](/docs/getting-started/): download, verify, run on a local directory, connect from Go and Java.
- [Azure Blob Storage](/docs/azure-blob/): account, container, permissions, credentials and measured performance.
- [Sizing](/docs/sizing/) and [Running in production](/docs/operations/): memory, disk, several servers, failover, errors to handle.
- [Go driver](/docs/go-driver/) and [JDBC driver](/docs/jdbc-driver/): connecting applications, GORM and Spring Boot.
- [Configuration reference](/docs/configuration/): every key of the configuration file.

The design records, architecture and roadmap are under **Design and project** in the menu.
