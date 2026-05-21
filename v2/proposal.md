# nats.go v2 Breaking Changes Proposal

## Context

Breaking changes for nats.go v2. Goal: improve DX, clean up API debt, modernize — not a rewrite. Pre-agreed scope-setters: remove encoded connections and the legacy JetStream API.

---

## Module Structure

v2 splits the single `nats.go` module into multiple, independently-versioned Go modules within the same repository:

```text
github.com/nats-io/nats.go/v2            # core: Conn, Sub, Msg, …
github.com/nats-io/nats.go/v2/jetstream  # JetStream API
github.com/nats-io/nats.go/v2/kv         # KeyValue (depends on jetstream)
github.com/nats-io/nats.go/v2/object     # ObjectStore (depends on jetstream)
github.com/nats-io/nats.go/v2/micro      # micro services (depends on core only)
```

`kv`, `object`, and `micro` stay in this repo — they're first-class client features (KV and Object are client-side abstractions over JetStream streams; micro is a client-side framework over subjects and conventions). The `internal/parser` and `internal/syncx` packages move into the `jetstream` module (their only consumer once legacy JetStream is removed via item #1).

See [modules-design.md](modules-design.md) for full layout, dependency graph, tagging scheme, and risks.

---

## Test Infrastructure

Test-only dependencies (embedded `nats-server`, `jwt`, `protobuf`, [`synadia-labs/testing.go`](https://github.com/synadia-labs/testing.go)) are isolated from production `go.mod` files via a **single consolidated `tests/` module** at the repository root, plus an `internal/tools/` module for linter version pins. A committed `go.work` ties everything together.

```text
nats.go/
├── go.mod, go.work
├── jetstream/go.mod
├── kv/go.mod
├── object/go.mod
├── micro/go.mod
├── internal/tools/go.mod          # staticcheck, golangci-lint, misspell pins
└── tests/
    ├── go.mod                     # nats-server, jwt, testing.go, etc.
    ├── core/                      # integration tests per production module
    ├── jetstream/
    ├── kv/
    ├── object/
    ├── micro/
    └── internal/                  # tests under -tags=internal_testing
```

White-box tests (those needing access to unexported package symbols) stay co-located with their production package. The `tests/` module hosts integration / black-box tests that exercise public APIs.

Replaces today's `go_test.mod` workaround, which silently poisons production `go.mod` files on `go mod tidy` and requires removing test directories to update test deps.

Coverage is tracked via `-coverpkg` instrumentation against production module paths; `gocovmerge` combines black-box (from `tests/`) and white-box (from each production module) profiles into per-module and total reports. `scripts/cov.sh` updates accordingly.

The new layout can land on `main` as a pre-v2 refactor — production module code is unchanged. See [modules-design.md](modules-design.md) for the migration sequence and coverage commands.

---

## Core `nats` Package

### 1. Remove deprecated APIs

**Effort:** Low | **Migration impact:** Low

Remove everything marked `// Deprecated`:

- `DefaultOptions` var, `DisconnectedCB`, `DisconnectHandler()`, `SetDisconnectHandler()`
- `Dialer` field and `Dialer()` option
- `QueuedMsgs()` on Subscription
- Entire `EncodedConn` system (`enc.go`, `netchan.go`, `context.go` partial, `encoders/` directory)
- Legacy JetStream API (`js.go`, `jserrors.go`, `jsm.go`, `kv.go`, `object_store.go` in root)
- `ErrInvalidDurableName`
- `StreamsInfo()`, `ConsumersInfo()`

### 2. Subscription API consolidation

**Effort:** High | **Migration impact:** High

Consolidate 7 subscribe methods to 2 (`Subscribe` for async callback, `SubscribeSync` for sync/iterator) with functional options replacing positional variants for queue group, pending limits, and error handler. Drop channel subscriptions entirely. **Single `*Subscription` type — no type split.** Type-safety lives in option types: `SubscribeOption` (shared) and `AsyncSubscribeOption` (compile-rejected on `SubscribeSync`).

Additional improvements bundled in:

- `sub.Stats() SubStats` snapshot replaces `Pending`/`Delivered`/`Dropped`/`MaxPending`/`PendingLimits` (5 race-prone methods → 1 atomic snapshot)
- `sub.Drained() <-chan struct{}` for drain-completion signaling
- `sub.MsgsContext(ctx)` iterator alongside `Msgs()`
- `MsgsTimeout` dropped — `NextMsg(timeout)` in a loop covers it
- `SetPendingLimits` post-subscribe removed — limits set once via the `PendingLimits` option

The `jetstream` package's internal `ChanSubscribe` usage in pull-consumer delivery rewrites to `SubscribeSync + MsgsContext()`. Benchmark required to confirm throughput parity (acceptance: within ~5% of today's path).

See [subscriptions-design.md](subscriptions-design.md) for full design, option hierarchy, and migration mapping.

### 3. Unified error types and async error handler redesign

**Effort:** Medium-High | **Migration impact:** Medium

Redesign error types and async error handler as one coherent system. Two kinds of error types:

**Fine-grained types** for errors with per-instance context — `PermissionError` (Subject, Queue, Operation, Sub) and `SlowConsumerError` (Sub, PendingMsgs, PendingBytes, Dropped). Own sentinels via `Is()` matching by type.

**Broad category types** for groups of simple sentinel errors — `ConnectionError`, `AuthError`, `ValidationError`, `ServerError`. Each sentinel is an instance of its category type. `errors.As` catches the whole category, `errors.Is` matches specific sentinels via pointer identity.

**Async error handler** changes from `ErrHandler func(*Conn, *Subscription, error)` to `ErrorHandler func(*Conn, error)`. `*Subscription` is removed from the signature — it's a field on `PermissionError` and `SlowConsumerError`.

Sub-level error handler (set via `SubErrorHandler` subscribe option) is exclusive when set — connection-level handler does NOT also fire for that subscription's errors. Single handler per connection (no multi-handler API).

`PermissionErrOnSubscribe` option removed (always-on).

See [errors-design.md](errors-design.md) for full design with code examples, matching semantics, and handler scope.

### 4. Auth options cleanup

**Effort:** Medium | **Migration impact:** Medium

Consolidate today's 10+ auth options around clearer naming and split JWT/NKey support into a sub-package so non-JWT users don't pay the `nkeys` dependency cost. Callback shape stays — no new `Authenticator` interface. The runtime mutex between `Token`/`TokenHandler` becomes a documented "last-wins" semantic. The eager file-read in `UserJWT` is dropped.

```go
// nats package (no nkeys dep):
AuthUserPass(user, pass string) Option
AuthUserPassFunc(cb UserPassHandler) Option
AuthToken(t string) Option
AuthTokenFunc(cb AuthTokenHandler) Option

// nats/auth/creds sub-package (pulls nkeys):
AuthCredentials(file string) Option
AuthCredentialsBytes(b []byte) Option
AuthJWTAndSeed(jwt, seed string) Option
AuthJWT(jwtCB, sigCB) Option
AuthNKey(pubKey, sigCB) Option
AuthNKeyBytes(seed []byte) Option   // NEW — fills today's asymmetry
```

See [auth-design.md](auth-design.md) for migration mapping and key decisions.

### 5. TLS options cleanup

**Effort:** Low | **Migration impact:** Medium

Rename TLS-related options for consistency with the `Auth*` namespace and fill in file/PEM parity. Same semantics underneath — rename pass plus the missing `TLSRootCAPEM` helper.

```go
// Enablement
func TLS() Option
func TLSConfig(cfg *tls.Config) Option   // mutually exclusive with granular options below

// Trust roots (additive — multiple calls accumulate)
func TLSRootCA(files ...string) Option
func TLSRootCAPEM(pems ...[]byte) Option

// Client certificate / mTLS (last-wins)
func TLSClientCert(certFile, keyFile string) Option
func TLSClientCertPEM(certPEM, keyPEM []byte) Option

// Handshake ordering
func TLSHandshakeFirst() Option
```

Interactions:

- Granular `TLS*` options imply TLS is enabled (current behavior — see `nats.go:1084`).
- `TLSConfig(cfg)` combined with any granular `TLS*` option is an error at option evaluation.
- Multiple `TLSRootCA` / `TLSRootCAPEM` calls accumulate roots. Multiple `TLSClientCert*` calls last-wins.
- A `tls://` URL with no TLS options implies `TLS()`.

Migration mapping:

| v1                               | v2                            |
| -------------------------------- | ----------------------------- |
| `Secure()`                       | `TLS()`                       |
| `Secure(cfg)`                    | `TLSConfig(cfg)`              |
| `RootCAs("a.pem", "b.pem")`      | `TLSRootCA("a.pem", "b.pem")` |
| `ClientCert(c, k)`               | `TLSClientCert(c, k)`         |
| `TLSCertificateChain(cert, key)` | `TLSClientCertPEM(cert, key)` |
| `TLSHandshakeFirst()`            | `TLSHandshakeFirst()`         |

### 6. Msg cleanup

**Effort:** Low | **Migration impact:** Low

Single `Msg` type stays. After legacy JetStream methods (`Ack`/`Nak`/`Term`/`InProgress`) are removed via item #1, the only remaining asymmetry between published and received messages is the `Sub` field — public but only meaningful on received messages.

- Make `Sub` private (no accessor). The only legitimate user-visible use today is via `Respond`/`RespondMsg`, which retain their method form.
- `Respond`/`RespondMsg` return `ErrMsgNotBound` when called on a message that wasn't received from a subscription.
- `NewMsg(subject string)` constructor stays.
- Document ownership contract on the `Msg` type:
  - `Data` is owned by the receiver after delivery (client already copies incoming payload — see `nats.go:3701`). Retain freely.
  - `Header` is owned by the receiver after delivery. Do **not** mutate a Msg's `Header` after passing it to `Publish`/`PublishMsg`.

Net surface:

```go
type Msg struct {
    Subject string
    Reply   string
    Header  Header
    Data    []byte
    // sub is private — see Respond() / RespondMsg()
}

func NewMsg(subject string) *Msg
func (m *Msg) Equal(msg *Msg) bool
func (m *Msg) Size() int
func (m *Msg) Respond(data []byte) error
func (m *Msg) RespondMsg(reply *Msg) error
```

### 7. Make `Conn.Opts` private

**Effort:** Low | **Migration impact:** Medium

`Conn.Opts` is public and modifying it after connect is a data race. Make it private. Expose read-only getters for fields users need (e.g., `Conn.Name()`, `Conn.Servers()`).

### 8. Improve `Statistics`

**Effort:** Medium | **Migration impact:** Low

- Add reconnect timestamps/durations, per-server stats, last error info to the `Statistics` struct.
- `Statistics` struct stays public. On `Conn`, the currently-embedded `Statistics` becomes a private `stats Statistics` field — users can no longer read or mutate stats via promoted access (`nc.OutBytes` etc.). The only access path is `nc.Stats() Statistics`, which returns an atomic snapshot.

### 9. Add `PublishWithHeader` / `PublishRequestWithHeader` methods

**Effort:** Low | **Migration impact:** Low (additive)

`Publish(subj, data)` and `PublishRequest(subj, reply, data)` predate NATS headers. Today header-using publishes go through `PublishMsg` which requires constructing a `*Msg`. Add direct methods so callers with headers don't need to build a message struct:

```go
PublishWithHeader(subj string, data []byte, h Header) error
PublishRequestWithHeader(subj, reply string, data []byte, h Header) error
```

`Publish` and `PublishRequest` keep their existing signatures — no `, nil` migration tax for the no-header case. `PublishMsg` stays unchanged.

### 10. Change `MaxReconnects` default to infinite

**Effort:** Low | **Migration impact:** Low (behavioral)

Default changes from 60 to unlimited (`-1`). Long-lived services routinely outlast a 60-reconnect window during a server outage and lose their connection silently. Unlimited matches user expectation for a NATS client. Users who want a bounded retry continue to set `MaxReconnects(n)` explicitly.

### 11. Remove `NoCallbacksAfterClientClose` option

**Effort:** Low | **Migration impact:** Low

Make "no callbacks after close" the default. Ensure `ClosedHandler` callback is still properly invoked (fires before close completes, not after).

### 12. Standardize handler type naming

**Effort:** Medium | **Migration impact:** Medium

All handler / callback types use a consistent `*Handler` suffix. Example: `UserInfoCB` → `UserInfoHandler`. Sweep covers every `*CB` / `*Cb` and similar non-conforming type.

### 13. Rename `ErrJsonParse` → `ErrJSONParse`

**Effort:** Low | **Migration impact:** Low

Follow Go naming conventions for acronyms. Same sweep applies to any other types/vars with non-canonical acronym casing (`JsAlreadyExistsErr` → `JSAlreadyExistsErr`, etc.).

### 14. Add `Header.Keys()` method

**Effort:** Low | **Migration impact:** None (additive)

`Header` (currently `map[string][]string`) lacks a `Keys()` method. Adding it makes `nats.Header` directly usable as an OpenTelemetry `TextMapCarrier` (which requires `Get` / `Set` / `Keys`), so distributed-tracing integration becomes a trivial wrapper rather than requiring a roadmap of client-side hooks.

```go
func (h Header) Keys() []string
```

Returns the header keys in unspecified order. Users can already iterate via `for k := range msg.Header`; the method exists so `nats.Header` satisfies common header-propagation interfaces directly.

Tracing / metrics hooks beyond this (`PublishContext` variants, per-publish interceptors, a first-party OTel sub-package) are explicitly **not** v2 client responsibility. Manual span injection / extraction using `Header.Get` / `Header.Set` / `Header.Keys` is the supported integration path.

---

## `jetstream` Package

### 15. Change `Messages()` to Go iterator

**Effort:** Medium | **Migration impact:** High

Currently `Messages()` returns a `MessagesContext` with a `Next()` method. Change it to return a Go 1.23+ iterator:

```go
for msg, err := range consumer.Messages(ctx) {
    // process msg
}
```

More idiomatic and removes the need for the `MessagesContext` wrapper type.

### 16. Unify option patterns

**Effort:** High | **Migration impact:** Medium

Today's `jetstream` API mixes func-type options and interface-based options inconsistently. Pick one pattern and apply consistently across the package. Standardize naming (e.g., `ConsumeOpt` instead of `PullConsumeOpt` where the distinction adds nothing).

### 17. Restrict ordered consumer to streaming-only methods

**Effort:** Low-Medium | **Migration impact:** Medium

`Stream.OrderedConsumer()` currently returns the full `Consumer` interface, which exposes `Fetch`, `FetchBytes`, `FetchNoWait`, and `Next` — none of which make sense for ordered consumers (the in-place sequence-gap reset machinery only works under the streaming `Consume`/`Messages` paths).

Introduce a new `OrderedConsumer` interface exposing only streaming methods, and change `Stream.OrderedConsumer()` return type accordingly:

```go
type OrderedConsumer interface {
    Info(ctx context.Context) (*ConsumerInfo, error)
    CachedInfo() *ConsumerInfo
    Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)
    Messages(ctx context.Context, opts ...PullMessagesOpt) iter.Seq2[Msg, error]  // see item #15
}

func (s *Stream) OrderedConsumer(ctx context.Context, cfg OrderedConsumerConfig) (OrderedConsumer, error)
```

Breaking API change for users storing the result as `Consumer` or calling `Fetch`/`Next` on an ordered consumer (both should already be considered misuse today).

### 18. ObjectStore `List()` → iterator

**Effort:** Medium | **Migration impact:** Medium

`List()` currently returns `[]*ObjectInfo` loading everything into memory. Return an iterator (`iter.Seq2[*ObjectInfo, error]`) to match item #15's shape and KV's `ListKeys()` pattern.

### 19. Comprehensive JetStream server error coverage

**Effort:** Medium | **Migration impact:** Low (additive)

The `jetstream` package currently names ~50 of the 222 server error codes defined in `server/errors.json`. Users matching on uncommon errors fall back to comparing `APIError.ErrorCode` against integer literals, which is verbose and error-prone.

Generate the full mapping from the server's canonical error schema. Each server code gets:

- A `JSErrCodeXxx` constant (mirrors the server's `constant` field).
- A typed `ErrXxx` sentinel matching `APIError` with that code via `Is()`.

```go
// Today — manual code comparison
var apiErr *jetstream.APIError
if errors.As(err, &apiErr) && apiErr.ErrorCode == 10040 { ... }

// v2 — typed sentinel
if errors.Is(err, jetstream.ErrClusterPeerNotMember) { ... }
```

Implementation:

- `go generate` directive vendors `server/errors.json` from `nats-io/nats-server` and produces `jetstream/errors_generated.go`.
- Hand-maintained errors in `jetstream/errors.go` stay separate — these are client-side conditions, not server responses.
- New server error codes land in subsequent v2.x releases by re-running the generator against the latest server schema.

Client-side config validation is **not** part of this item — validation remains server-side; the client just surfaces the responses with typed access.

### 20. Remove deprecated `Keys()` from KV

**Effort:** Low | **Migration impact:** Low

Already deprecated in favor of `ListKeys()`.

### 21. Remove `StreamConfig.Template`

**Effort:** Low | **Migration impact:** Low

Feature no longer supported by server.

---

## `micro` Package

### 22. Use `StatusChanged()` for lifecycle observation

**Effort:** Medium | **Migration impact:** Low

Currently `AddService()` replaces the connection's `ClosedHandler` / `ErrorHandler`. Use `Conn.StatusChanged()` channel to observe connection lifecycle instead. Fixes issues with multiple services on one connection and stops `micro` from interfering with user-set handlers.

### 23. Add panic recovery in handlers

**Effort:** Low | **Migration impact:** None (additive)

A panicking handler currently crashes the service. Add recovery that sends a 500 error response and calls `ErrorHandler`.

### 24. Standardize handler / callback naming

**Effort:** Low | **Migration impact:** Low

Fix inconsistencies: `HandlerFunc` vs `DoneHandler` vs `ErrHandler` vs `StatsHandler`. Aligns with item #12's connection-level handler naming sweep.

---

## Summary Table

| #  | Change                                                       | Package   | Effort     | Migration  |
| -- | ------------------------------------------------------------ | --------- | ---------- | ---------- |
| 1  | Remove deprecated APIs                                       | core      | Low        | Low        |
| 2  | Subscription API consolidation                               | core      | High       | High       |
| 3  | Unified error types and async error handler redesign         | core      | Med-High   | Medium     |
| 4  | Auth options cleanup                                         | core      | Medium     | Medium     |
| 5  | TLS options cleanup                                          | core      | Low        | Medium     |
| 6  | Msg cleanup                                                  | core      | Low        | Low        |
| 7  | Make `Conn.Opts` private                                     | core      | Low        | Medium     |
| 8  | Improve `Statistics`                                         | core      | Medium     | Low        |
| 9  | Add `PublishWithHeader` / `PublishRequestWithHeader` methods | core      | Low        | Low        |
| 10 | `MaxReconnects` default → infinite                           | core      | Low        | Low        |
| 11 | Remove `NoCallbacksAfterClientClose`                         | core      | Low        | Low        |
| 12 | Standardize handler type naming                              | core      | Medium     | Medium     |
| 13 | `ErrJsonParse` → `ErrJSONParse`                              | core      | Low        | Low        |
| 14 | Add `Header.Keys()` method                                   | core      | Low        | None       |
| 15 | `Messages()` as Go iterator                                  | jetstream | Medium     | High       |
| 16 | Unify option patterns                                        | jetstream | High       | Medium     |
| 17 | Restrict ordered consumer to streaming-only methods          | jetstream | Low-Medium | Medium     |
| 18 | ObjectStore `List()` → iterator                              | jetstream | Medium     | Medium     |
| 19 | Comprehensive JS server error coverage                       | jetstream | Medium     | Low        |
| 20 | Remove deprecated `Keys()` from KV                           | jetstream | Low        | Low        |
| 21 | Remove `StreamConfig.Template`                               | jetstream | Low        | Low        |
| 22 | Use `StatusChanged()` for lifecycle observation              | micro     | Medium     | Low        |
| 23 | Panic recovery in handlers                                   | micro     | Low        | None       |
| 24 | Standardize handler / callback naming                        | micro     | Low        | Low        |
