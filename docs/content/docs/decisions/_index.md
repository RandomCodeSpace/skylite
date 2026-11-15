---
title: Decisions
description: Architecture decision records for skylite.
weight: 40
---

This section holds the project's architecture decision records (ADRs). Each record captures one decision: the context that forced it, what was chosen, what was rejected and what follows from it. Records are kept after they are superseded, so the history of the design stays readable.

The first record covers the evaluation of Litebase as a base. More are expected after the durability spike, starting with the [open questions](/docs/architecture/#open-questions) on the architecture page.

## Adding a record

1. Create a file in `docs/content/docs/decisions/` named with the next number and a short slug, for example `0001-leader-fencing.md`, and paste in the [ADR template](/docs/decisions/template/).
2. Set its `weight` to the record number so the sidebar lists records in order.
3. Open a pull request. The record is accepted when the pull request merges.
4. To change a decision, write a new record that supersedes the old one, and set the old record's status to "Superseded by" with a link to the new one.
