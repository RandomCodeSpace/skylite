# Contributing to skylite

skylite is pre-alpha. Read the [README](README.md) for the current status and
roadmap before proposing larger changes; opening an issue first is the best way
to check that a change fits.

## Pull requests and the merge queue

- All changes go through pull requests. Nobody pushes directly to `main`.
- Pull requests are merged through the merge queue using squash merge, so
  `main` keeps a linear history.
- Pull request titles follow [Conventional Commits](https://www.conventionalcommits.org/),
  for example `feat(storage): add Azure Blob backend` or `fix(cli): exit 2 on unknown command`.
  The title becomes the squashed commit message.
- New CI jobs arrive in the same pull request as the thing they test.

### What has to pass

A ruleset on `main` enforces the following, with no bypass for anyone:

- Required status checks, on the pull request and again on the merge queue's
  combined commit: `build`, `vet`, `lint`, `test`, `govulncheck`, `actionlint`
  and `docs`.
- CodeQL code scanning must report no new alert of severity error, or security
  severity high or above, before a pull request can enter the queue. Merge
  queue groups are not scanned again; GitHub does not apply this rule to them,
  and it also exempts Dependabot pull requests analysed by default setup.
- No approving review is required. The project has one maintainer, and a
  required approval would block their own pull requests.
- No force pushes to `main` and no deleting it.

To merge, use "Merge when ready" on the pull request, or `gh pr merge --squash`.
The queue runs the required checks again and then lands the squashed commit.

## Running the checks locally

You need Go on the 1.26 line, at or above the version in `go.mod`, and
[golangci-lint](https://golangci-lint.run/) v2. CI builds with the newest
1.26 release. Do not use a toolchain newer than 1.27.1: that is the newest
one this project is permitted to use, and CI stays on 1.26 because
`govulncheck` reports standard-library advisories against 1.27.1. The `make`
targets select Go 1.26.11 unless `GOTOOLCHAIN` is already set.

| Command            | What it runs                                                    |
|--------------------|-----------------------------------------------------------------|
| `make build`       | `CGO_ENABLED=0` build of `bin/skylite`                          |
| `make test`        | `go test -race -count=1 ./...`                                  |
| `make vet`         | `go vet ./...`                                                  |
| `make lint`        | `golangci-lint run`                                             |
| `make vuln`        | `govulncheck ./...` at the pinned version                       |
| `make check`       | all of the above                                                |
| `make integration` | Azurite in Docker, then `go test -race -tags integration ./...` |

Run `make check` before opening a pull request. `make integration` needs
Docker; it picks a free local port and removes its Azurite container when it
finishes. The durability kill test runs 20 rounds by default; set
`SKYLITE_SPIKE_KILL_ROUNDS` for a longer soak and raise the 6-minute
`INTEGRATION_TIMEOUT` with it, for example
`SKYLITE_SPIKE_KILL_ROUNDS=300 INTEGRATION_TIMEOUT=60m make integration`. CI runs the same checks plus
`go mod tidy -diff` and workflow linting. CodeQL code scanning runs through
GitHub's default setup, which is managed at the organisation level, so there is
no CodeQL workflow in this repository.

Pure-Go builds are a project requirement: skylite must not depend on cgo, and
`CGO_ENABLED=0 go build ./...` must pass.

## Documentation

- Docs live in this repository under `docs/`.
- Any pull request that changes behaviour, configuration, an API or the CLI
  updates the docs in the same pull request.
- Reference pages are generated from source. The CLI reference is generated
  from `skylite help` at build time and is not committed.

Docs site generator: Hugo — one static Go binary with no Node or Python dependency tree to audit, templates we fully control, and fast builds.

## Security

Do not report vulnerabilities in public issues or pull requests. See
[SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
