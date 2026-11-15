GOVULNCHECK_VERSION := v1.9.0

# Azurite 3.37.0, pinned by the digest of its multi-architecture index.
AZURITE_IMAGE := mcr.microsoft.com/azure-storage/azurite:3.37.0@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5
INTEGRATION_TIMEOUT ?= 30m

.PHONY: build test vet lint vuln check integration

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
integration:
	@set -e; name="skylite-azurite-$$$$"; \
	docker run -d --rm --name "$$name" -p 127.0.0.1::10000 $(AZURITE_IMAGE) \
	  azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --inMemoryPersistence >/dev/null; \
	trap 'docker stop "$$name" >/dev/null 2>&1' EXIT; \
	port=$$(docker port "$$name" 10000/tcp | head -n 1 | sed 's/.*://'); \
	endpoint="http://127.0.0.1:$$port/devstoreaccount1"; \
	for i in $$(seq 60); do curl -s -o /dev/null "$$endpoint" && break; sleep 1; done; \
	curl -s -o /dev/null "$$endpoint" || { echo "integration: Azurite did not start" >&2; exit 1; }; \
	rc=0; SKYLITE_TEST_AZURITE_BLOB_ENDPOINT="$$endpoint" go test -race -count=1 -timeout $(INTEGRATION_TIMEOUT) -tags integration ./... || rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "integration: last Azurite log lines:" >&2; docker logs --tail 50 "$$name" >&2 || true; fi; \
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
