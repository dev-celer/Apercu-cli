# apercu-cli — build, test and image targets.
#
# Override any variable on the command line, e.g.
#   make docker-pgproxy PGPROXY_IMAGE=smontard/apercu-pgproxy:v2

BIN_DIR        ?= bin
CLI_BIN        ?= $(BIN_DIR)/apercu
PGPROXY_BIN    ?= $(BIN_DIR)/apercu-pgproxy

# The pgproxy image is consumed by helper/docker/pgproxy.go (PGPROXY_IMAGE).
PGPROXY_IMAGE  ?= smontard/apercu-pgproxy:dev
PGPROXY_OS     ?= linux
PGPROXY_ARCH   ?= amd64

GO             ?= go
GOFLAGS        ?=
LDFLAGS        ?= -s -w

# Package selection.
PKGS           ?= ./...
CATALOG_PKG    := ./helper/pg_catalog/

INTEGRATION_TIMEOUT ?= 30m

.DEFAULT_GOAL := help

## help: list the available targets
help:
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /' | sort

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

## build: build both binaries into bin/
build: build-cli build-pgproxy

## build-cli: build the apercu CLI for the host platform
build-cli:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(CLI_BIN) .

## build-pgproxy: build the pgproxy binary the docker image ships (static linux/amd64)
build-pgproxy:
	CGO_ENABLED=0 GOOS=$(PGPROXY_OS) GOARCH=$(PGPROXY_ARCH) \
		$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(PGPROXY_BIN) ./pgproxy

## clean: remove the built binaries
clean:
	rm -f $(CLI_BIN) $(PGPROXY_BIN)

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

## test: run the unit tests (no docker required)
test:
	$(GO) test $(GOFLAGS) $(PKGS)

## test-race: run the unit tests with the race detector
test-race:
	$(GO) test $(GOFLAGS) -race $(PKGS)

## test-integration: run the integration tests (requires a running docker daemon)
test-integration:
	$(GO) test $(GOFLAGS) -tags integration -timeout $(INTEGRATION_TIMEOUT) -count=1 $(PKGS)

## test-all: run the unit and integration tests
test-all: test test-integration

## fixtures: regenerate helper/pg_catalog/testdata snapshots from live containers
fixtures:
	APERCU_UPDATE_FIXTURES=1 $(GO) test $(GOFLAGS) -tags integration \
		-timeout $(INTEGRATION_TIMEOUT) -count=1 -v \
		-run TestCollectAgainstServer $(CATALOG_PKG)

## fmt: gofmt the tree in place
fmt:
	gofmt -w .

## vet: run go vet over every package, integration tests included
vet:
	$(GO) vet $(PKGS)
	$(GO) vet -tags integration $(PKGS)

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

## docker-pgproxy: rebuild the pgproxy binary and its docker image
docker-pgproxy: build-pgproxy
	docker build -f pgproxy/Dockerfile -t $(PGPROXY_IMAGE) .

## docker-pgproxy-push: push the pgproxy image to its registry
docker-pgproxy-push: docker-pgproxy
	docker push $(PGPROXY_IMAGE)

.PHONY: help build build-cli build-pgproxy clean \
	test test-race test-integration test-all fixtures fmt vet \
	docker-pgproxy docker-pgproxy-push
