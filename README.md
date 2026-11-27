# skylite

[![CI](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/ci.yml)
[![Docs](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/RandomCodeSpace/skylite/actions/workflows/docs.yml)

A SQL database server where object storage is the source of truth.

skylite runs SQLite behind an HTTPS API and keeps all durable state in object
storage: Azure Blob Storage (the primary backend), Amazon S3, Google Cloud
Storage, or a local directory for trials. A server needs no persistent volume.
Applications connect with a Go `database/sql` driver (works with GORM) or a
JDBC driver with a Hibernate dialect (works with Spring Data JPA), and
authenticate with OIDC JWT bearer tokens.

Documentation: https://randomcodespace.github.io/skylite/

## Is it for you

skylite fits services that want a small relational database without running a
stateful database tier, and that accept:

- **Writes wait for object storage.** A commit is acknowledged only after it is
  uploaded. Measured once against a real Azure account: one writer about 53
  commits/s with a median of 17 ms; 4 to 64 writers about 63 to 67 commits/s in
  total. These are observations of one setup, not guarantees.
- **One writer per database.** Each database has one leader; other servers
  answer reads and take over when it stops.
- **SQLite's SQL dialect.**

## Status: beta (v0.0.3)

Tested on every pull request: the server (reads, writes, transactions,
read-only transactions, batches, resolve, leader election and failover,
follower reads, forwarding between servers), the Go driver through a GORM
acceptance suite, and the JDBC driver and Hibernate dialect through a Spring
Data JPA acceptance suite, against a real server. The storage backends are
tested against emulators (Azurite, RustFS, fake-gcs-server).

Run by hand against a real Azure storage account: the Azure backend's
integration and conformance tests, the GORM acceptance suite, and a failover
soak of three servers on one machine (SIGTERM, SIGKILL and SIGSTOP of the
leader) that lost no acknowledged write.

Not verified yet: Amazon S3 and Google Cloud Storage themselves (emulators
only), a Kubernetes cluster, a service mesh or ingress, a real Entra ID tenant
as token issuer, driver failover across several pods, and a 24-hour soak.

Releases are signed binaries; there is no container image or Helm chart, and none
is planned. Maven artifacts are not published (build the Java driver with
`mvn install`). See the
[roadmap](https://randomcodespace.github.io/skylite/docs/roadmap/).

## Quick start

Download a release for Linux or macOS, verify it and run it:

```sh
VERSION=0.0.3
gh release download "v$VERSION" -R RandomCodeSpace/skylite \
  -p "skylite_${VERSION}_linux_amd64.tar.gz" -p 'SHA256SUMS*'
sha256sum --ignore-missing -c SHA256SUMS
tar -xzf "skylite_${VERSION}_linux_amd64.tar.gz"
./skylite version
```

Or install with Go 1.26 or later:

```sh
go install github.com/RandomCodeSpace/skylite/cmd/skylite@v0.0.3
```

The [quick start](https://randomcodespace.github.io/skylite/docs/getting-started/)
checks the release signature, runs a server on a local directory and connects
to it from Go and from Java.

## Documentation

- [Quick start](https://randomcodespace.github.io/skylite/docs/getting-started/)
- [Azure Blob Storage](https://randomcodespace.github.io/skylite/docs/azure-blob/)
- [Sizing](https://randomcodespace.github.io/skylite/docs/sizing/) and
  [running in production](https://randomcodespace.github.io/skylite/docs/operations/)
- [Go driver](https://randomcodespace.github.io/skylite/docs/go-driver/) and
  [JDBC driver](https://randomcodespace.github.io/skylite/docs/jdbc-driver/)
- [Configuration reference](https://randomcodespace.github.io/skylite/docs/configuration/)
- [Architecture](https://randomcodespace.github.io/skylite/docs/architecture/)

## Links

- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [License](LICENSE) (Apache-2.0)
