# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Official Go client library for the NATS messaging system. Provides core pub/sub, request/reply, JetStream (streams, consumers, KV, object store), and a micro services framework. Module path: `github.com/nats-io/nats.go`.

## Build and Test Commands

This project uses a **dual module** setup: `go.mod` for production (minimal deps) and `go_test.mod` for testing (includes nats-server, protobuf). Always use `-modfile=go_test.mod` when running tests or any command that needs test dependencies.

```bash
# Build
go build ./...

# Run all tests (race detector + internal_testing tag, sequential)
go test -modfile=go_test.mod -race -v -p=1 ./... --failfast -vet=off -tags=internal_testing

# Run NoRace tests (must be run separately, without -race flag)
go test -modfile=go_test.mod -v -run=TestNoRace -p=1 ./... --failfast -vet=off

# Run a specific test
go test -modfile=go_test.mod -race -run TestName ./... -tags=internal_testing

# Run tests for a specific package
go test -modfile=go_test.mod -race ./jetstream/... --failfast
go test -modfile=go_test.mod -race ./micro/... --failfast

# Coverage
./scripts/cov.sh

# Formatting
go fmt ./...

# Vet
go vet -modfile=go_test.mod ./...

# Static analysis (as CI does it)
staticcheck -modfile=go_test.mod ./...

# Linting (golangci-lint runs only on jetstream/)
golangci-lint run --timeout 5m0s ./jetstream/...

# Spell check
find . -type f -name "*.go" | xargs misspell -error -locale US

# Update test dependencies (never change go.mod for test deps)
go mod tidy -modfile=go_test.mod
```

## Important Build Tags

- **`internal_testing`** -- Exposes internal test helpers (e.g., `AddMsgFilter`, `CloseTCPConn`) from `testing_internal.go`. Required for many tests in `./test/`.
- **`skip_no_race_tests`** -- Skips the NoRace tests. Used by coverage scripts.
- **`!race && !skip_no_race_tests`** -- NoRace tests in `test/norace_test.go` only run when the race detector is OFF.
- **`compat`** -- Compatibility tests in `test/compat_test.go`.

## CI Pipeline (ci.yaml)

1. **lint** -- `go fmt`, `go vet`, `staticcheck`, `misspell` (all packages), `golangci-lint` (jetstream only).
2. **test** -- Matrix of Go 1.24 and 1.25. Runs NoRace tests first (`-run=TestNoRace` without `-race`), then full race-enabled tests with `-tags=internal_testing`.

## Project Structure

```
nats.go                 # Core connection, pub/sub, request/reply (~6500 lines)
parser.go               # Client-side protocol parser
ws.go                   # WebSocket transport support
js.go                   # Legacy JetStream API (deprecated, see jetstream/)
jsm.go                  # Legacy JetStream management
kv.go                   # Legacy KeyValue API
object.go               # Legacy Object Store API
enc.go                  # EncodedConn (deprecated)
netchan.go              # Go channel bindings
timer.go                # Internal timer utilities
context.go              # Context-aware request methods
nats_iter.go            # Go 1.23+ iterator support (go:build go1.23)
testing_internal.go     # Internal test hooks (go:build internal_testing)

jetstream/              # New JetStream API (preferred over legacy)
  jetstream.go          #   Top-level JetStream interface
  stream.go             #   Stream management
  stream_config.go      #   Stream configuration types
  consumer.go           #   Consumer management
  consumer_config.go    #   Consumer configuration types
  pull.go               #   Pull consumer implementation
  push.go               #   Push consumer (deprecated)
  ordered.go            #   Ordered consumer
  publish.go            #   JetStream publish methods
  kv.go                 #   KeyValue store
  object.go             #   Object store
  message.go            #   JetStream message types
  errors.go             #   JetStream error types
  test/                 #   Integration tests (package test, uses nats-server)

micro/                  # Micro services framework
  service.go            #   Service interface and implementation
  request.go            #   Request handling
  test/                 #   Integration tests

internal/
  parser/               # NATS protocol parser (used by core client)
  syncx/                # Concurrent map utility

encoders/
  builtin/              # Default encoders (JSON, GOB, string)
  protobuf/             # Protocol Buffers encoder

test/                   # Integration tests for core package (package test)
  helper_test.go        #   Server setup helpers (RunDefaultServer, RunBasicJetStreamServer, etc.)
  norace_test.go        #   Tests that cannot run with -race (build tag guarded)
  js_internal_test.go   #   Tests requiring internal_testing tag
  configs/              #   NATS server config files for tests

bench/                  # Benchmarking utilities
examples/               # Example command-line tools (nats-pub, nats-sub, etc.)
scripts/cov.sh          # Coverage collection script
```

## Test Architecture

- **Root `nats_test.go`** (package `nats`) -- White-box unit tests with access to unexported internals.
- **`test/`** (package `test`) -- Black-box integration tests. Tests start an embedded nats-server using helpers from `test/helper_test.go`. These require `-modfile=go_test.mod` since nats-server is a test-only dependency.
- **`jetstream/test/`** (package `test`) -- Integration tests for the new JetStream API, also use embedded nats-server.
- **`micro/test/`** (package `test`) -- Integration tests for the micro services framework.
- **NoRace tests** -- Prefixed `TestNoRace*`, guarded by `//go:build !race && !skip_no_race_tests`. Must be run separately without `-race`.
- Tests always run with `-p=1` (no parallel packages) because they start embedded servers on shared ports.

## Code Conventions

- **License header** -- Every `.go` file starts with the Apache 2.0 license header (Copyright year range).
- **Error variables** -- Exported errors defined as `var Err... = errors.New("nats: ...")` in `nats.go`. JetStream errors in `jetstream/errors.go` follow the same pattern.
- **Options pattern** -- Connection options use functional options: `nats.Connect(url, nats.Name("myapp"), nats.MaxReconnects(5))`. JetStream and micro use similar patterns.
- **No external dependencies in production** -- Only `klauspost/compress`, `nkeys`, `nuid` in `go.mod`. Test deps (nats-server, protobuf) are isolated in `go_test.mod`. PRs adding dependencies are scrutinized heavily.
- **Commits require sign-off** -- Use `git commit -s` (DCO: `Signed-off-by`).
- **US English spelling** -- Enforced by `misspell -locale US` in CI.
- **Interface-driven design** -- JetStream and micro packages define interfaces (`JetStream`, `Stream`, `Consumer`, `Service`) with concrete unexported implementations.

## Key Types

- `nats.Conn` -- Core connection, handles all NATS protocol operations.
- `nats.Msg` -- Message type for pub/sub and request/reply.
- `nats.Subscription` -- Represents a subscription (sync, async, or channel-based).
- `jetstream.JetStream` -- Entry point for new JetStream API (created via `jetstream.New(nc)`).
- `jetstream.Stream`, `jetstream.Consumer` -- Stream and consumer management.
- `micro.Service` -- Micro service instance (created via `micro.AddService(nc, config)`).
