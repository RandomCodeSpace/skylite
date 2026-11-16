# The newest Go this project may use is 1.27.1, and govulncheck reports
# standard-library advisories against it, so builds stay on the 1.26 line.
# CI sets GOTOOLCHAIN=local after installing the newest 1.26 release.
export GOTOOLCHAIN ?= go1.26.11

GOVULNCHECK_VERSION := v1.9.0

# Azurite 3.37.0, pinned by the digest of its multi-architecture index.
AZURITE_IMAGE := mcr.microsoft.com/azure-storage/azurite:3.37.0@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5
# Shorter than the CI job's 10-minute limit, so a hang prints its goroutine dump
# and the Azurite log before the runner cancels the job.
INTEGRATION_TIMEOUT ?= 6m

# RustFS 1.0.5, pinned by the digest of its multi-architecture index.
RUSTFS_IMAGE := rustfs/rustfs:1.0.5@sha256:b81d16a90ee25bd9511280127ca1471788c6e874cf0f0da759645f46b2dc1d74
# Shorter than the CI job's limit, for the same reason as INTEGRATION_TIMEOUT.
INTEGRATION_S3_TIMEOUT ?= 15m

# fake-gcs-server 1.56.1, pinned by the digest of its multi-architecture index.
FAKE_GCS_IMAGE := fsouza/fake-gcs-server:1.56.1@sha256:797ce226d62f947c009dc40246b30cfb456b8473d8241407f9d6f2c04e4d69ef
# Shorter than the CI job's 15-minute limit, for the same reason.
INTEGRATION_GCS_TIMEOUT ?= 12m

.PHONY: build test vet lint vuln check integration integration-s3 integration-gcs

build:
	CGO_ENABLED=0 go build -o bin/skylite ./cmd/skylite

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

lint:
	golangci-lint run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

check: build vet lint test vuln

# Starts a uniquely named Azurite blob container on a free local port, runs
# the integration tests against it with the race detector, and removes it.
# SKYLITE_SPIKE_KILL_ROUNDS in the environment sets the kill-test round count.
# Azurite keeps its data on the container's disk, not in memory: it frees the
# data of deleted blobs only in a garbage collection every 10 minutes, and the
# azureblob tests write about 7.5 GiB in 4. Its in-memory store is limited to
# half the machine's RAM (about 8 GiB on a 16 GB CI runner), and with a 6 GiB
# limit the conformance suite failed with MemoryExtentStoreAtSizeLimit. On
# disk, a full run peaked at 7.5 GiB of data and 0.8 GiB of memory.
integration:
	@set -e; name="skylite-azurite-$$$$"; \
	docker run -d --rm --name "$$name" -p 127.0.0.1::10000 $(AZURITE_IMAGE) \
	  azurite-blob --blobHost 0.0.0.0 --blobPort 10000 >/dev/null; \
	trap 'docker stop "$$name" >/dev/null 2>&1' EXIT; \
	port=$$(docker port "$$name" 10000/tcp | head -n 1 | sed 's/.*://'); \
	endpoint="http://127.0.0.1:$$port/devstoreaccount1"; \
	for i in $$(seq 60); do curl -s -o /dev/null "$$endpoint" && break; sleep 1; done; \
	curl -s -o /dev/null "$$endpoint" || { echo "integration: Azurite did not start" >&2; exit 1; }; \
	rc=0; SKYLITE_TEST_AZURITE_BLOB_ENDPOINT="$$endpoint" go test -race -count=1 -v -timeout $(INTEGRATION_TIMEOUT) -tags integration ./... || rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "integration: last Azurite log lines:" >&2; docker logs --tail 50 "$$name" >&2 || true; fi; \
	exit $$rc

# Starts a uniquely named RustFS container on a free local port, with its data
# in memory, runs the S3 backend's tests (the conformance suite included)
# against it with the race detector, and removes it. RustFS's documented
# default credentials are public and only work against the emulator. RustFS
# does not reclaim the space under /data/.rustfs.sys during the run (5.3 GB at
# its peak, in one measured run), so a 4 GB tmpfs filled up and RustFS
# answered 503. The conformance suite's ValidKeys subtest is skipped: RustFS
# rejects its key of one 1,024-byte segment (any segment over 255 bytes), which
# S3 accepts; the backend's TestValidKeys makes the same checks with that key
# split into segments of at most 255 bytes.
integration-s3:
	@set -e; name="skylite-s3-rustfs-$$$$"; \
	docker run -d --rm --name "$$name" -p 127.0.0.1::9000 --tmpfs /data:rw,size=10g,mode=1777 \
	  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin $(RUSTFS_IMAGE) /data >/dev/null; \
	trap 'docker stop "$$name" >/dev/null 2>&1' EXIT; \
	port=$$(docker port "$$name" 9000/tcp | head -n 1 | sed 's/.*://'); \
	endpoint="http://127.0.0.1:$$port"; \
	for i in $$(seq 60); do curl -s -o /dev/null "$$endpoint" && break; sleep 1; done; \
	curl -s -o /dev/null "$$endpoint" || { echo "integration-s3: RustFS did not start" >&2; exit 1; }; \
	rc=0; SKYLITE_TEST_S3_ENDPOINT="$$endpoint" go test -race -count=1 -v -timeout $(INTEGRATION_S3_TIMEOUT) -tags integration -skip '^TestConformance$$/^ValidKeys$$' ./internal/storage/s3/ || rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "integration-s3: last RustFS log lines:" >&2; docker logs --tail 50 "$$name" >&2 || true; fi; \
	exit $$rc

# Starts a uniquely named in-memory fake-gcs-server on a free local port, runs
# the gcs backend's tests (fault tests and conformance suite) against it with
# the race detector, and removes it. The emulator puts its own URL into
# resumable upload session URLs, so it must be told the published port: the
# port is chosen before the container starts.
integration-gcs:
	@set -e; name="skylite-fake-gcs-$$$$"; \
	port=$$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'); \
	docker run -d --rm --name "$$name" -p "127.0.0.1:$$port:$$port" $(FAKE_GCS_IMAGE) \
	  -scheme http -port "$$port" -backend memory -external-url "http://127.0.0.1:$$port" -public-host "127.0.0.1:$$port" >/dev/null; \
	trap 'docker stop "$$name" >/dev/null 2>&1' EXIT; \
	endpoint="http://127.0.0.1:$$port/storage/v1/"; \
	for i in $$(seq 60); do curl -sf -o /dev/null "$${endpoint}b" && break; sleep 1; done; \
	curl -sf -o /dev/null "$${endpoint}b" || { echo "integration-gcs: fake-gcs-server did not start" >&2; exit 1; }; \
	rc=0; SKYLITE_TEST_GCS_ENDPOINT="$$endpoint" go test -race -count=1 -v -timeout $(INTEGRATION_GCS_TIMEOUT) -tags integration ./internal/storage/gcs/ || rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "integration-gcs: last fake-gcs-server log lines:" >&2; docker logs --tail 50 "$$name" >&2 || true; fi; \
	exit $$rc

# Documentation site (Hugo). Run from the repository root.
#
#   make docs-gen        write generated reference pages (docs/content/docs/reference/cli.md)
#   make docs            docs-gen, then build the site into docs/public/ (fails on warnings)
#   make docs-serve      docs-gen, then run the Hugo dev server at http://localhost:1313/skylite/
#   make docs-linkcheck  check internal links and anchors in docs/public/ with lychee, offline
#
# Overridable: HUGO, LYCHEE, SKYLITE_HELP_CMD, DOCS_LINKCHECK_MODE.

HUGO ?= hugo
LYCHEE ?= lychee
# Contract: prints plain-text usage to stdout and exits 0.
SKYLITE_HELP_CMD ?= go run ./cmd/skylite help
# --offline: internal links only, no network. Set empty to also check external links.
DOCS_LINKCHECK_MODE ?= --offline

DOCS_CLI_REF := docs/content/docs/reference/cli.md
# The site is served under /skylite/, so the link checker sees docs/public as <root>/skylite.
DOCS_LINKROOT := $(CURDIR)/docs/linkcheck
DOCS_SITE_URL := https://randomcodespace.github.io/skylite/

.PHONY: docs-gen docs docs-serve docs-linkcheck

docs-gen:
	@set -e; tmp="$(DOCS_CLI_REF).tmp"; trap 'rm -f "$$tmp"' EXIT; \
	{ \
	  printf -- '---\ntitle: CLI reference\ndescription: Usage of the skylite binary, generated from its help output.\nweight: 10\nstatus: generated\ngenerated: true\n---\n\n'; \
	  printf 'This page is generated from `skylite help` by `make docs-gen`. Do not edit it; change the help text in the source instead.\n\n'; \
	  printf '```text\n'; \
	  $(SKYLITE_HELP_CMD); \
	  printf '```\n'; \
	} > "$$tmp"; \
	mv "$$tmp" "$(DOCS_CLI_REF)"
	@echo "docs-gen: wrote $(DOCS_CLI_REF)"

docs: docs-gen
	$(HUGO) --source docs --minify --gc --panicOnWarning --printPathWarnings --cleanDestinationDir

docs-serve: docs-gen
	$(HUGO) server --source docs

docs-linkcheck:
	@test -f docs/public/index.html || { echo "docs-linkcheck: docs/public is missing; run 'make docs' first" >&2; exit 1; }
	rm -rf "$(DOCS_LINKROOT)"
	mkdir -p "$(DOCS_LINKROOT)"
	cp -R docs/public "$(DOCS_LINKROOT)/skylite"
	$(LYCHEE) $(DOCS_LINKCHECK_MODE) --no-progress --no-ignore \
	  --root-dir "$(DOCS_LINKROOT)" \
	  --remap '^$(DOCS_SITE_URL)(.*)$$ file://$(DOCS_LINKROOT)/skylite/$$1' \
	  --index-files index.html \
	  --include-fragments \
	  --extensions html,css,xml \
	  "$(DOCS_LINKROOT)/skylite"

# JPA acceptance suite (clients/java/acceptance) against the reference SQLite JDBC driver.
# Needs a JDK and Maven.
.PHONY: java-acceptance
java-acceptance:
	mvn -B -ntp -f clients/java/acceptance/pom.xml test

# Checks the suite itself: runs it once per injected driver fault, each of which must fail a test.
.PHONY: java-fault-check
java-fault-check:
	mvn -B -ntp -f clients/java/acceptance/pom.xml test -Psqlite,fault-check

# Checks that the suite rejects, or ignores, connection settings from sources other than -Djdbc.*.
# Needs Python 3 as well; not run in CI (about 13 minutes).
.PHONY: java-config-check
java-config-check:
	python3 clients/java/acceptance/config-check.py
