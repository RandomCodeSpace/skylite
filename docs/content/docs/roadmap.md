---
title: Roadmap and status
linkTitle: Roadmap and status
description: What is done, what is planned, and in which order.
weight: 30
status: current
---

skylite is pre-alpha. The repository contains project scaffolding and a binary that prints its version and help. The milestones below are in planned order. Only the first is done.

{{< roadmap >}}

## Why the spike comes first

The whole design rests on two claims: no acknowledged write is lost when a pod dies, and a stale leader cannot commit. The spike tests both against Azurite, the Azure Storage emulator, before the later milestones start. It also decides whether Litebase is a suitable base. The [architecture](/docs/architecture/) is decided after this milestone, so everything on this site is design direction, subject to change.
