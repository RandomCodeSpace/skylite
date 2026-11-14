---
title: Getting started
description: Install the skylite binary and run the one command that works today.
weight: 10
status: current
---

skylite is pre-alpha. The binary prints its version and its help text. It does not store data, open a port or accept SQL yet. This page covers everything that works today.

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

## What comes next

There is nothing to configure or connect to yet. To follow the work, read the [architecture direction](/docs/architecture/) and the [roadmap](/docs/roadmap/). The first milestone is a durability spike, and the architecture is decided after it.
