---
title: Getting started
description: Install the skylite binary, check its version, and run the server, which creates databases, answers queries and runs autocommit writes and transactions on channels.
weight: 10
status: current
---

skylite is pre-alpha. The binary prints its version and its help text, and `skylite serve` runs a server that authenticates requests, creates databases in object storage, answers read-only SQL queries on the databases it leads, and runs autocommit writes and transactions sent on write channels. This page covers everything that works today.

## Install with Go

You need a Go toolchain. Install the binary into your Go bin directory:

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

Writes and transactions are sent on write channels by the [JDBC driver](/docs/jdbc-driver/), or as raw protocol requests; the [Go driver](/docs/go-driver/) has a read path only and sends no writes. To follow the work, read the [architecture direction](/docs/architecture/) and the [roadmap](/docs/roadmap/). The planned storage design is recorded in [0004: Build skylite's own storage engine](/docs/decisions/0004-storage-engine/).
