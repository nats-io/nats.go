# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Official Go client library for the NATS messaging system. Provides core pub/sub, request/reply, JetStream (streams, consumers, KV, object store), and a micro services framework. Module path: `github.com/nats-io/nats.go`.

## Build and Test Commands

This project uses a **dual module** setup: `go.mod` for production (minimal deps) and `go_test.mod` for testing (protobuf encoder + jwt + nkeys + nuid + the ntf tester). Always use `-modfile=go_test.mod` when running tests.

Integration tests (everything in `./test/`, `./jetstream/test/`, `./micro/test/`) run against real servers spawned by a **tester** service, driven through `github.com/synadia-io/orbit.go/ntf-client`. There are two ways to run it:

- **In-process, no docker (the default)** — a `TestMain` in each of the three test packages starts `github.com/synadia-io/orbit.go/ntf` inside the test binary. No build tag; this is what a plain `go test` does.
- **Docker** (`synadia/ntf-server`, tag pinned in `.github/workflows/ci.yaml`) — located via `TESTER_NATS_URL`. This is what CI uses.

`TESTER_NATS_URL` takes precedence: when set, it is used instead of starting an in-process tester. The two can run different nats-server builds, so `TestMain` hard-fails when `GITHUB_ACTIONS` is set without `TESTER_NATS_URL` rather than silently changing what CI tests against.

```bash
# In-process tester (default, no docker)
make test                            # full race-enabled suite
make test T=TestName PKG=./test/...  # single test, verbose
make test-norace                     # NoRace suite (race detector off)

# Raw equivalent of `make test`
go test -modfile=go_test.mod -tags=internal_testing -race -p=1 ./... --failfast -vet=off

# Docker tester; use test-docker, plain `make test` ignores the container
make tester-up-host
make test-docker T=TestName PKG=./test/...
make tester-down

# Another nats-server via a go_test.mod replace: never commit it, never `go get ...@main`
make server-replace V=main
make server-replace-drop

# Lint as CI runs it (staticcheck and golangci-lint take the modfile through GOFLAGS)
go fmt -modfile=go_test.mod ./...
go vet -modfile=go_test.mod ./...
GOFLAGS="-mod=mod -modfile=go_test.mod" staticcheck ./...
GOFLAGS="-mod=mod -modfile=go_test.mod" golangci-lint run --timeout 5m0s ./jetstream/...
find . -type f -name "*.go" | xargs misspell -error -locale US

# Test dependencies go in go_test.mod only
go mod tidy -modfile=go_test.mod
```

A plain `go test -modfile=go_test.mod ./...` runs the integration suites too, since `TestMain` starts a tester on its own. It takes minutes rather than seconds — that is expected, not a hang.

[TESTING.md](TESTING.md) covers the rest: sibling-container mode (`make test-tester`, matches CI), coverage, the `server-replace` caveats and troubleshooting.

## Important Build Tags

- **`internal_testing`** -- Exposes internal test helpers (e.g., `AddMsgFilter`, `CloseTCPConn`) from `testing_internal.go`. Required for some tests in `./test/`.
- **`!race && !skip_no_race_tests`** -- NoRace tests in `test/norace_test.go` only run when the race detector is OFF.
- **`compat`** -- Compatibility tests in `test/compat_test.go` (connect to an external NATS server via `NATS_URL`).
- **`go1.23`** -- Iterator-based tests in `test/nats_iter_test.go` and `nats_iter.go`.

## CI Pipeline (ci.yaml)

1. **lint** -- `go fmt`, `go vet`, `staticcheck`, `misspell` (all packages), `golangci-lint` (jetstream only).
2. **test** -- Matrix of Go 1.26 and 1.27; 1.26 is the floor because `go_test.mod` declares it (a lower row would silently fetch 1.26 via `GOTOOLCHAIN=auto`). The 1.27 row runs coverage. Runs inside an `alpine` container on the same docker network as the `synadia/ntf-server` service, which is started with `command: serve --advertise nats` (the integration tests dial the spawned NATS servers by service name). CI sets `TESTER_NATS_URL` at job level so it uses the docker tester rather than the in-process default; `TestMain` fails the run if that variable ever goes missing under `GITHUB_ACTIONS`. Two steps: NoRace tests (without `-race`), then full race-enabled tests with `-tags=internal_testing` (`scripts/cov.sh` runs for coverage instead of the plain race run).

## Project Structure

```
nats.go                 # Core connection, pub/sub, request/reply
parser.go               # Client-side protocol parser
ws.go                   # WebSocket transport support
js.go                   # Legacy JetStream API (deprecated, see jetstream/)
jsm.go                  # Legacy JetStream management
jserrors.go             # Legacy JetStream errors
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
  api.go                #   JetStream API subjects and request helpers
  *_options.go          #   Functional options (jetstream, kv, object)
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
  test/                 #   Integration tests (package test, uses testservice)

micro/                  # Micro services framework
  service.go            #   Service interface and implementation
  request.go            #   Request handling
  conn_hooks.go         #   Shared per-connection status/error callbacks
  test/                 #   Integration tests

internal/
  parser/               # JetStream ack reply subject parsing (message metadata)
  syncx/                # Concurrent map utility

util/                   # TLS config cloning

encoders/
  builtin/              # Default encoders (JSON, GOB, string)
  protobuf/             # Protocol Buffers encoder

test/                   # Integration tests for core package (package test)
  testservice_helper_test.go # withServer / withServerInstance / newTester / dialInstance helpers
  main_test.go          # TestMain: starts the tester (in-process by default)
  helper_test.go        #   Shared utility helpers (Wait, checkFor, getStableNumGoroutine, ...)
  norace_test.go        #   Tests that cannot run with -race (build tag guarded)
  js_internal_test.go   #   Tests requiring internal_testing tag
  configs/certs/        #   CA/server/key PEMs loaded by TLS tests (the *.conf files are unused)

bench/                  # Benchmarking utilities
examples/               # Example command-line tools (nats-pub, nats-sub, etc.)
scripts/cov.sh          # Coverage collection script (run by the CI coverage matrix row)
```

The tester client is an external test-only dependency: `github.com/synadia-io/orbit.go/ntf-client`
(package `ntf`, imported under the alias `testservice`). It lives in `go_test.mod` only.

## Test Architecture

- **White-box tests** -- Unit tests with access to unexported internals: root `nats_test.go` (package `nats`, plus `js_test.go`, `headers_test.go`, `ws_test.go`, `timer_test.go`), `jetstream/jetstream_test.go` (package `jetstream`), `micro/service_internal_test.go` (package `micro`).
- **`test/`** (package `test`) -- Black-box integration tests. Tests bring up a NATS server via the testservice helpers (`withServer`, `withJSServer`, `withJSCluster`, ...) which talk to the tester over NATS — either the in-process one started by `TestMain` (the default) or the `synadia/ntf-server` docker service via `TESTER_NATS_URL`. Use the `testerURL` package var; never read `TESTER_NATS_URL` directly, or the test will bypass the in-process tester (this exact bug hit `headers_test.go`).
- **`jetstream/test/`** (package `test`) -- Integration tests for the new JetStream API, same testservice harness.
- **`micro/test/`** (package `micro_test`) -- Integration tests for the micro services framework, same testservice harness.
- **NoRace tests** -- Prefixed `TestNoRace*`, guarded by `//go:build !race && !skip_no_race_tests`. Must be run separately without `-race`.
- Tests always run with `-p=1` (no parallel packages): the shared docker tester doesn't tolerate concurrent CreateServer calls from independent test binaries, and the in-process path keeps it to limit server churn.

### Adding tests

A bug fix comes with a regression test that fails without the fix.

Add tests to the existing test file for the code you're changing. Don't create a new `_test.go` file for a bug fix or a small addition; a new file is only for a new area with no existing home, named after the source file it covers. Reuse the helpers in `helper_test.go` / `testservice_helper_test.go` rather than adding near-duplicates.

- `jetstream/<file>.go` -> `jetstream/test/<file>_test.go` (`pull_test.go`, `kv_test.go`, `ordered_test.go`, ...); white-box -> the existing `jetstream/*_test.go` closest to the change, else `jetstream_test.go`.
- `micro/` -> `micro/test/service_test.go`; white-box -> `micro/service_internal_test.go`.
- Legacy JetStream (`js.go`, `jsm.go`, `kv.go`, `object.go`) -> `test/js_test.go`, `test/kv_test.go`, `test/object_test.go`; tests needing `internal_testing` hooks -> `test/js_internal_test.go`.
- Core client -> the matching `test/` file (`conn_test.go`, `sub_test.go`, `reconnect_test.go`, `headers_test.go`, `ws_test.go`, ...); white-box (including `parser.go`) -> root `nats_test.go`, or the root `*_test.go` named after the source file.

## Code Conventions

- **License header** -- Every `.go` file starts with the Apache 2.0 license header (Copyright year range).
- **Error variables** -- Core exported errors are `var Err... = errors.New("nats: ...")` in `nats.go`. JetStream errors (`jetstream/errors.go`, legacy `jserrors.go`) are `JetStreamError` values (`&jsError{...}`), wrapping an `APIError` with the server's error code where one exists.
- **Options pattern** -- Connection options use functional options: `nats.Connect(url, nats.Name("myapp"), nats.MaxReconnects(5))`. JetStream and micro use similar patterns.
- **No external dependencies in production** -- Only `klauspost/compress`, `nkeys`, `nuid` in `go.mod`. Test deps (protobuf, jwt, etc.) are isolated in `go_test.mod`. PRs adding dependencies are scrutinized heavily.
- **PR titles** -- Start with a tag, then a short summary of the change: `[FIXED]` bug fix, `[ADDED]` new API or option, `[IMPROVED]` performance, docs, tests or internals, `[CHANGED]` something existing users will notice (behavior change, removal, Go version bump). For example: `[FIXED] Preserve options when following object links`. Commit subjects on `main` follow the same format.
- **Sign-off required** -- `Signed-off-by` (DCO) in every commit (`git commit -s`) and in the PR description, under a legal name.
- **US English spelling** -- Enforced by `misspell -locale US` in CI.
- **Interface-driven design** -- JetStream and micro packages define interfaces (`JetStream`, `Stream`, `Consumer`, `Service`) with concrete unexported implementations.

## Key Types

- `nats.Conn` -- Core connection, handles all NATS protocol operations.
- `nats.Msg` -- Message type for pub/sub and request/reply.
- `nats.Subscription` -- Represents a subscription (sync, async, or channel-based).
- `jetstream.JetStream` -- Entry point for new JetStream API (created via `jetstream.New(nc)`).
- `jetstream.Stream`, `jetstream.Consumer` -- Stream and consumer management.
- `micro.Service` -- Micro service instance (created via `micro.AddService(nc, config)`).
