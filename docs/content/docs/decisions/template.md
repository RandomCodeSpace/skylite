---
title: ADR template
description: The template every architecture decision record starts from.
weight: 10000
---

Paste this into a new file and replace each section. Keep it short: a record that takes more than a few minutes to read is usually two decisions.

```markdown
---
title: "0000: Short decision title"
description: One sentence stating what was decided.
weight: 0
---

**Status:** Proposed | Accepted | Superseded by [0000](/docs/decisions/0000-slug/)

**Date:** YYYY-MM-DD

## Context

What forces a decision now? Include the constraints, the evidence (spike
results, measurements, incidents) and links to the issues or pull requests
where it was discussed.

## Decision

What is being done, stated so that someone can check the code against it.

## Alternatives considered

Each option that was rejected, and why.

## Consequences

What becomes easier, what becomes harder, and what has to be done next,
including documentation that must change in the same pull request.
```
