---
title: Getting started
description: Install the skylite binary, check its version, and run the server, which creates databases, answers queries and runs autocommit writes and transactions on channels.
weight: 10
status: current
---

skylite is pre-alpha. The binary prints its version and its help text, and `skylite serve` runs a server that authenticates requests, creates databases in object storage, answers read-only SQL queries on the databases it leads, and runs autocommit writes and transactions sent on write channels. This page covers everything that works today.

## Download a release

Each [release](https://github.com/RandomCodeSpace/skylite/releases) has a `tar.gz` archive of the binary for Linux and macOS on amd64 and arm64, a `SHA256SUMS` file, an SBOM for each archive, and a [Cosign](https://docs.sigstore.dev/) signature of `SHA256SUMS`. Download an archive and the checksum files, for example with the GitHub CLI:

```sh
VERSION=0.0.1
gh release download "v$VERSION" -R RandomCodeSpace/skylite \
  -p "skylite_${VERSION}_linux_amd64.tar.gz" -p 'SHA256SUMS*'
```

Check that `SHA256SUMS` was signed by this repository's release workflow for that tag, then check the archive against it:

```sh
cosign verify-blob \
  --certificate SHA256SUMS.pem --signature SHA256SUMS.sig \
  --certificate-identity "https://github.com/RandomCodeSpace/skylite/.github/workflows/release.yml@refs/tags/v$VERSION" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
```

Then unpack it and put `skylite` on your `PATH`:

```sh
tar -xzf "skylite_${VERSION}_linux_amd64.tar.gz"
./skylite version
```

## Install with Go

You need a Go toolchain. Install the latest release into your Go bin directory, or name a version such as `@v0.0.1`:

```sh
go install github.com/RandomCodeSpace/skylite/cmd/skylite@latest
```

Then check that it runs:

```sh
skylite version
```

If your shell cannot find `skylite`, add the output of `go env GOBIN` (or `$(go env GOPATH)/bin` when `GOBIN` is empty) to your `PATH`.

## Build from a clone

```sh
git clone https://github.com/RandomCodeSpace/skylite.git
cd skylite
make build
./bin/skylite version
```

## See the help text

`skylite help` prints the commands the binary knows about. The same text is published as the [CLI reference](/docs/reference/), generated from the binary on every docs build.

## Run the server

`skylite serve -config skylite.json` starts the server. The [server page](/docs/server/) describes the configuration file, the requests it serves (creating a database, `query` and `describe` for pure reads, and write channels for autocommit writes and transactions) and how to try it on a local directory.

## What comes next

Writes and transactions run on write channels, which the [Go driver](/docs/go-driver/), GORM included, and the [JDBC driver](/docs/jdbc-driver/) use. To follow the work, read the [architecture direction](/docs/architecture/) and the [roadmap](/docs/roadmap/). The planned storage design is recorded in [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/).
