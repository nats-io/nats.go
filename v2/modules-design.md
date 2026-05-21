# Module Structure

## Context

v2 separates the monolithic `nats.go` module into multiple Go modules within the same repository. Each module is independently versioned, allowing satellite functionality (jetstream, kv, object, micro) to evolve at its own pace without forcing a core release.

`kv`, `object`, and `micro` stay in this repo — they're first-class client features (KV and Object are client-side abstractions over JetStream streams; micro is a client-side framework over subjects and conventions), not ecosystem extensions. (Compare `orbit.go`, which hosts community extensions on top of these capabilities.)

## Layout

```text
github.com/nats-io/nats.go/
├── go.mod                             # github.com/nats-io/nats.go/v2 (core)
├── go.work                            # committed; ties all modules together
├── *.go                               # Conn, Sub, Msg, parser, …
│
├── jetstream/
│   ├── go.mod                         # github.com/nats-io/nats.go/v2/jetstream
│   ├── internal/parser/               # moved here from nats.go/internal/
│   ├── internal/syncx/                # moved here from nats.go/internal/
│   └── *.go
│
├── kv/
│   ├── go.mod                         # github.com/nats-io/nats.go/v2/kv
│   └── *.go                           # depends on core + jetstream
│
├── object/
│   ├── go.mod                         # github.com/nats-io/nats.go/v2/object
│   └── *.go                           # depends on core + jetstream
│
├── micro/
│   ├── go.mod                         # github.com/nats-io/nats.go/v2/micro
│   └── *.go                           # depends on core only
│
├── internal/tools/
│   └── go.mod                         # staticcheck, golangci-lint, misspell pins
│
└── tests/
    ├── go.mod                         # nats-server, jwt, protobuf,
    │                                  # synadia-labs/testing.go;
    │                                  # replaces all prod modules to ../
    ├── core/                          # integration tests for core
    ├── jetstream/
    ├── kv/
    ├── object/
    ├── micro/
    └── internal/                      # tests under -tags=internal_testing
```

## Dependency graph

```
                ┌──────────────┐
                │  nats.go/v2  │  ← protocol, Conn, Sub, Msg, Header
                └──────┬───────┘
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
  ┌──────────┐   ┌──────────┐    ┌──────────┐
  │jetstream │   │  micro   │    │ (future) │
  └────┬─────┘   └──────────┘    └──────────┘
       │
   ┌───┴───┐
   ▼       ▼
 ┌────┐ ┌──────┐
 │ kv │ │object│
 └────┘ └──────┘
```

## Key decisions

### `internal/` packages move into `jetstream`

Today's `nats.go/internal/parser` and `nats.go/internal/syncx` are used **only** by the `jetstream` package and the legacy JetStream code (`js.go`, `kv.go`, `object.go`). Legacy JetStream is removed in v2 (item #1), so these `internal/` packages have a single consumer.

They move to:
- `jetstream/internal/parser/`
- `jetstream/internal/syncx/`

No duplication, no shared "sub-internal" module, no public re-export. Pure refactor — can land on `main` before v2 is cut.

### Test infrastructure: consolidated `tests/` module

Test-only dependencies (`nats-server`, `jwt`, `protobuf`, `synadia-labs/testing.go`) are isolated from every production `go.mod` via a single consolidated `tests/` module at the repo root. A committed `go.work` ties the production modules, `tests/`, and `internal/tools/` together so local builds work without manual `replace` plumbing.

This replaces the current `go_test.mod` workaround, which silently poisons production `go.mod` files on `go mod tidy` and requires removing test directories to update test deps.

**Where tests live:**

- **White-box tests** (need access to unexported package symbols) stay co-located with their production package: `nats_test.go` next to `nats.go`, `jetstream/consumer_test.go`, etc. These compile against the production module's own `go.mod` (no test deps needed for white-box).
- **Integration / black-box tests** that exercise public APIs and need an embedded server live in `tests/<module>/`. They compile against `tests/go.mod` and reach production code via `replace` directives.
- **`-tags=internal_testing` tests** that need exported-but-internal helpers live in `tests/internal/<module>/`. Each production module exposes a thin `testing_internal.go` shim (already the pattern today) re-exporting needed helpers under the `internal_testing` build tag.

**Embedded server**: `synadia-labs/testing.go` provides a programmatic NATS server for use inside integration tests, replacing today's per-module `test/helper_test.go` duplication. It's required exactly once — in `tests/go.mod`.

**Why one consolidated `tests/` module instead of per-module `*/test/go.mod`:**

- One place to bump `nats-server`, `synadia-labs/testing.go`, and other shared test deps; no version drift across module test suites.
- One `replace` block, one `go mod tidy` for tests.
- Smaller CI surface: `go test ./...` (workspace-resolved white-box) + `go test ./tests/...` (black-box).
- `go.work` stays small and stable — entries change only when a new production module is added.

**Coverage**: per-module and total coverage are tracked via `-coverpkg`. Black-box runs from `tests/` instrument the production paths; white-box runs cover internal tests in each production module. `gocovmerge` combines profiles.

```bash
# Black-box coverage per module (from tests/):
go test -coverpkg=github.com/nats-io/nats.go/v2/jetstream/... \
        -coverprofile=jetstream-bb.cov ./tests/jetstream/...

# White-box coverage per module:
go test -coverprofile=jetstream-wb.cov ./jetstream/...

# Merge for the module's full profile:
gocovmerge jetstream-bb.cov jetstream-wb.cov > jetstream.cov

# Or instrument everything in one black-box pass for the total:
go test -coverpkg=github.com/nats-io/nats.go/v2/... \
        -coverprofile=total-bb.cov ./tests/...
```

`scripts/cov.sh` updates to drive this pipeline.

**Migration sequence** (can land on `main` before v2 is cut — pure refactor, production code unchanged):

1. Add `internal/tools/go.mod`; move linter version pins out of `go_test.mod`.
2. Add `tests/go.mod` with `replace` directives; migrate one package's tests at a time.
3. Add `go.work` to repo root once all packages are migrated.
4. Update CI to drive the new layout.
5. Remove `go_test.mod`.

### Tag scheme

Standard Go submodule tagging:
- Core: `v2.0.0`
- jetstream: `jetstream/v1.0.0`
- kv: `kv/v1.0.0`
- object: `object/v1.0.0`
- micro: `micro/v3.0.0` (continues numbering from current `micro` v2)

Modules can release independently after v2.0; pre-v2.0 they're tagged in lockstep.

### Cross-module references during development

Use `go.work` for local development. Releases ship without `replace` directives — each `go.mod` points to real published versions. No vendor directory.

## Out of scope for v2.0

- **Release scripting.** v2.0 modules are coordinated manually. A release tool (detect changed modules, bump versions, tag in dependency order) is a follow-up.
- **Migration tooling.** No `go fix` style codemod for now. Migration guide doc covers the rename pass.
- **Backwards-compat shims.** No v1 import path aliases — users update their imports as part of moving to v2.

## Risks

- **CI runtime increase** — each production module runs its own white-box test/lint/race pass plus the single consolidated `tests/` pass for integration tests. ~2x today's monolithic baseline, mitigated with job parallelism.
- **Diamond dependency confusion.** Users on `kv@v1.2.0` may resolve different `jetstream` and `core` versions than tested. Release notes call out tested combinations.
- **PR review burden** for cross-module changes (touching jetstream + kv simultaneously). Today's repo has effectively the same issue across packages; tooling-wise no worse.
