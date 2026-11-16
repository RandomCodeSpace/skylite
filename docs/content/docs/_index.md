---
title: Documentation
linkTitle: Overview
description: What skylite is meant to be, what exists today, and where the design is going.
status: current
listChildren: false
---

skylite aims to be a SQL database server where object storage is the source of truth. It is a Go project, and the plan is to build it on SQLite. It is meant to run in Kubernetes pods that have no persistent volumes. In the design, all durable state lives in object storage, and pod disk is only a cache.

The project is pre-alpha. The binary prints its version and help, and its server authenticates requests and creates databases; it runs no SQL yet. Most of these pages describe design direction rather than behaviour. Pages that describe goals say so at the top.

## Where to start

- [Getting started](/docs/getting-started/) installs the binary and shows the only commands that work today.
- [Architecture](/docs/architecture/) sets out the design direction and the questions that are still open.
- [Go driver](/docs/go-driver/) describes the Go `database/sql` driver's read path.
- [Storage backends](/docs/storage-backends/) describes the storage interface's backends, which are library packages the server does not use yet.
- [Roadmap and status](/docs/roadmap/) lists the planned milestones in order.
- [Decisions](/docs/decisions/) is where architecture decision records will live.
- [Reference](/docs/reference/) holds pages generated from the source, starting with the CLI.
- [Contributing to docs](/docs/contributing-docs/) explains how these pages are written and built.
