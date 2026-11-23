---
title: Contributing to docs
description: Where the docs live, when to update them, and how to build them locally.
weight: 60
navgroup: project
---

## The docs live in the repository

Every page on this site is a Markdown file under `docs/content/` in the [skylite repository](https://github.com/RandomCodeSpace/skylite). Each page has an "Edit this page" link that opens its source on GitHub.

## Docs change with the code

Any pull request that changes behaviour, configuration, the API or the CLI updates the docs in the same pull request. A reviewer should be able to read the diff and the docs change side by side.

Keep the status honest. Pages that describe goals rather than working software carry `status: design` in their front matter, which shows the design-direction marker at the top of the page. When something described as a goal is built, update the page and its status in the same pull request.

## Where a page appears in the menu

The menu groups the pages directly under `docs/content/docs/` by their `navgroup` front matter, ordered by `weight`: `guides` (task-oriented pages for people who run skylite or connect an application), `reference` (exact behaviour and every option) or `project` (architecture, decision records, roadmap and these notes). A top-level page without `navgroup` is not listed in the menu. Pages inside a section, such as the decision records, are listed under their section.

## Reference pages are generated

Reference pages are generated from source and are not committed. The [CLI reference](/docs/reference/) is written by `make docs-gen` from the output of `skylite help`. To change it, change the help text in the code.

## Build the site locally

You need Go and [Hugo](https://gohugo.io/) v0.170.0, the standard edition. No Node or Python is needed.

```sh
make docs           # generate reference pages, then build into docs/public/
make docs-serve     # live-reloading server at http://localhost:1313/skylite/
make docs-linkcheck # check every internal link and anchor in docs/public/, offline
```

`make docs` fails on any Hugo warning, and CI runs the same build and link check on every pull request. The link check needs [lychee](https://github.com/lycheeverse/lychee) v0.24.2 on your `PATH`; `make docs-linkcheck LYCHEE="docker run --rm -v $PWD:$PWD -w $PWD lycheeverse/lychee:0.24.2"` runs it through Docker instead.

## Writing style

- Plain, precise sentences. Say what something does, or say that it is planned.
- Never describe a planned feature in the present tense.
- Link to other pages with their path from the site root, such as `/docs/architecture/`. The build resolves it to the correct URL.

Site generator: Hugo — one static Go binary with no Node or Python dependency tree to audit, templates we fully control, and fast builds.
