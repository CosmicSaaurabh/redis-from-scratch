# redis-from-scratch
#
# Two languages, one repository. Every target here works from a clean checkout
# with only the Go and Rust toolchains installed; anything that needs more says
# so and degrades rather than failing.

SHELL := /bin/bash
.DEFAULT_GOAL := help

GO       ?= go
CARGO    ?= cargo
BIN      := bin
ENGINE   := engine
PROTO    := proto/engine.proto

# The protobuf plugins are pinned rather than installed with @latest, so that
# regenerating on two machines produces identical bytes. The generated code is
# committed, and a check that compares it against a freshly generated copy is
# only meaningful if the generator is deterministic.
PROTOC_GEN_GO_VERSION      ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.2
PKG      := github.com/CosmicSaaurabh/redis-from-scratch
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT   ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS  := -X $(PKG)/internal/version.Version=$(VERSION) -X $(PKG)/internal/version.Commit=$(COMMIT)

# Race detection is on for every Go test that touches concurrency, and this
# project treats a race detector finding as a build failure rather than a
# warning: a data race that has not crashed yet is a bug that has not been
# noticed yet.
GOTEST := $(GO) test -race -count=1

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: build
build: build-go build-engine ## Build everything

.PHONY: build-go
build-go: ## Build the Go server and tools
	@mkdir -p $(BIN)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/rfs-server ./cmd/server
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/rfs-bench  ./cmd/rfs-bench

.PHONY: build-engine
build-engine: ## Build the Rust storage engine in release mode
	cd $(ENGINE) && $(CARGO) build --release --bins

.PHONY: build-engine-debug
build-engine-debug: ## Build the storage engine unoptimised, for faster iteration
	cd $(ENGINE) && $(CARGO) build --bins

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

.PHONY: test
test: test-go test-rust ## Run every test suite

.PHONY: test-go
test-go: ## Unit and integration tests, with the race detector
	$(GOTEST) ./internal/... ./cmd/...

.PHONY: test-e2e
test-e2e: ## End-to-end tests over a real socket
	$(GOTEST) ./test/e2e/...

.PHONY: test-crash
test-crash: ## Kill the real server with SIGKILL and check what survived
	$(GO) test -count=1 -timeout 600s ./test/crash/...

.PHONY: test-compat
test-compat: ## Compare replies against a real redis-server (skips if absent)
	$(GOTEST) ./test/compat/...

.PHONY: test-all
test-all: test-go test-e2e test-compat test-crash test-rust ## Everything, including the slow suites

.PHONY: test-rust
test-rust: ## Rust unit and integration tests
	cd $(ENGINE) && $(CARGO) test

.PHONY: fuzz
fuzz: ## Fuzz the protocol parser and the glob matcher for 60s each
	$(GO) test -run=NONE -fuzz=FuzzReadCommand  -fuzztime=60s ./internal/resp/
	$(GO) test -run=NONE -fuzz=FuzzMatchPattern -fuzztime=60s ./internal/command/

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------

.PHONY: lint
lint: lint-go lint-rust ## Lint both languages

.PHONY: lint-go
lint-go: ## go vet plus golangci-lint if it is installed
	$(GO) vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
	  golangci-lint run; \
	else \
	  echo "golangci-lint is not installed; ran go vet only"; \
	fi

.PHONY: lint-rust
lint-rust: ## clippy with warnings as errors, plus a format check
	cd $(ENGINE) && $(CARGO) clippy --all-targets -- -D warnings
	cd $(ENGINE) && $(CARGO) fmt --check

.PHONY: fmt
fmt: ## Format both languages
	$(GO) fmt ./...
	cd $(ENGINE) && $(CARGO) fmt

# ---------------------------------------------------------------------------
# Protobuf
# ---------------------------------------------------------------------------

.PHONY: proto-tools
proto-tools: ## Install the pinned protobuf plugins into ./bin
	@mkdir -p $(BIN)
	GOBIN="$(CURDIR)/$(BIN)" $(GO) install \
	  google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN="$(CURDIR)/$(BIN)" $(GO) install \
	  google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: proto
proto: proto-tools ## Regenerate the Go bindings from proto/engine.proto
	@command -v protoc >/dev/null 2>&1 || { echo "protoc is not installed"; exit 1; }
	PATH="$(CURDIR)/$(BIN):$$PATH" protoc \
	  --proto_path=proto \
	  --go_out=. --go_opt=module=$(PKG) \
	  --go-grpc_out=. --go-grpc_opt=module=$(PKG) \
	  $(PROTO)
	@$(MAKE) --no-print-directory proto-normalise
	@echo "the Rust side regenerates from build.rs on the next cargo build"

.PHONY: proto-normalise
proto-normalise: ## Strip the protoc binary version from the generated headers
	@# The plugins are pinned, but the protoc binary itself is whatever the
	@# machine has - 33.x from Homebrew, 3.21 from Debian - and it stamps its
	@# own version into a comment. That is a property of the environment, not
	@# of the contract, so it is normalised away rather than allowed to make
	@# proto-check fail on a difference that means nothing.
	@for f in internal/enginepb/*.pb.go; do \
	  sed -i.bak -E 's|(protoc)([[:space:]]+)v[0-9][0-9.]*|\1\2(pinned via the Makefile)|' "$$f"; \
	  rm -f "$$f.bak"; \
	done

.PHONY: proto-check
proto-check: ## Fail if the generated bindings are stale
	@$(MAKE) --no-print-directory proto
	@if ! git diff --quiet -- internal/enginepb; then \
	  echo "internal/enginepb is out of date; run make proto and commit the result"; \
	  git --no-pager diff --stat -- internal/enginepb; \
	  exit 1; \
	fi

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

.PHONY: run
run: build-go ## Run a node with the in-process engine
	$(BIN)/rfs-server -dir ./data

.PHONY: run-lsm
run-lsm: build ## Run a node backed by the Rust LSM engine
	@echo "starting the storage engine on :50051"
	@$(ENGINE)/target/release/rfs-engine --dir ./engine-data & \
	  ENGINE_PID=$$!; \
	  sleep 1; \
	  trap "kill $$ENGINE_PID 2>/dev/null" EXIT; \
	  $(BIN)/rfs-server -engine lsm -engine-addr 127.0.0.1:50051

.PHONY: bench
bench: build ## Run the published benchmark suite
	./scripts/bench-suite.sh 10s docs/benchmarks

.PHONY: bench-quick
bench-quick: build-go ## A single fast benchmark against a running node
	$(BIN)/rfs-bench -addr 127.0.0.1:6379 -workload mixed -c 50 -P 1 -d 10s

# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -t rfs:$(VERSION) -t rfs:latest .

.PHONY: up
up: ## Start a local node with the LSM engine via Docker Compose
	docker compose up --build

.PHONY: down
down: ## Stop the Compose stack
	docker compose down -v

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

.PHONY: clean
clean: ## Remove build output and local data directories
	rm -rf $(BIN) data engine-data
	cd $(ENGINE) && $(CARGO) clean

.PHONY: tidy
tidy: ## Tidy Go modules
	$(GO) mod tidy
