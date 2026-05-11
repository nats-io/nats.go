# nats.go v2 Breaking Changes Proposal

## Context

Preparing a list of breaking changes for nats.go v2. Goal: improve DX, clean up API debt, modernize — not a rewrite. Pre-agreed: remove encoded connections/encoders, remove legacy JetStream API.

Items marked **APPROVED** have sign-off. Items marked **OPEN** need further discussion. Items marked **DROPPED** were considered and rejected.

---

## Core `nats` Package

### 1. Remove all deprecated APIs — APPROVED
**Effort:** Low | **Usefulness:** 8/10 | **Migration impact:** Low

Remove everything marked `// Deprecated`:
- `DefaultOptions` var, `DisconnectedCB`, `DisconnectHandler()`, `SetDisconnectHandler()`
- `Dialer` field and `Dialer()` option
- `QueuedMsgs()` on Subscription
- Entire `EncodedConn` system (`enc.go`, `netchan.go`, `context.go` partial, `encoders/` directory)
- Legacy JetStream API (`js.go`, `jserrors.go`, `jsm.go`, `kv.go`, `object_store.go` in root)
- `ErrInvalidDurableName`
- `StreamsInfo()`, `ConsumersInfo()`

### 2. Make `Conn.Opts` private — APPROVED
**Effort:** Low | **Usefulness:** 7/10 | **Migration impact:** Medium

`Conn.Opts` is public and modifying it after connect is a data race. Make it private, expose read-only getters for fields users need (e.g., `Conn.Name()`, `Conn.Servers()`).

### 3. Subscription type safety with embedded concrete types — APPROVED

**Effort:** Medium-High | **Usefulness:** 8/10 | **Migration impact:** High

Single `*Subscription` type exposes methods that only work for specific subscription types, returning errors at runtime instead of being prevented at compile time. Split into two concrete types using embedding:

- Remove channel subscriptions (`ChanSubscribe`, `ChanQueueSubscribe`, `QueueSubscribeSyncWithChan`)
- `Subscription` is the base type with all shared methods and fields, returned by `Subscribe()` and `QueueSubscribe()`
- `SyncSubscription` embeds `*Subscription` and adds `NextMsg`, `Msgs`, `MsgsTimeout` — returned by `SubscribeSync()` and `QueueSubscribeSync()`
- Result: calling `NextMsg()` on an async subscription is a compile error instead of a runtime error

See [subscriptions-design.md](subscriptions-design.md) for full design.

### 4. Add headers parameter to core `Publish()` — APPROVED (new)

**Effort:** Low | **Usefulness:** 7/10 | **Migration impact:** Medium

Current `Publish(subj string, data []byte)` predates NATS headers. Add a direct `Header` parameter:
```go
Publish(subj string, data []byte, h Header) error
PublishRequest(subj, reply string, data []byte, h Header) error
```
Callers without headers pass `nil`. `PublishMsg` stays as-is since `Msg` already carries headers.

### 5. Clean up callback handler type naming — APPROVED
**Effort:** Medium | **Usefulness:** 6/10 | **Migration impact:** Medium

Standardize: `UserInfoCB` → `UserInfoHandler`. All handler types should use `*Handler` suffix consistently.

### 6. Remove `NoCallbacksAfterClientClose` option — APPROVED
**Effort:** Low | **Usefulness:** 4/10 | **Migration impact:** Low

Make "no callbacks after close" the default. Ensure `ClosedHandler` callback is still properly invoked (it should fire before close completes, not after).

### 7. Improve `Statistics` — APPROVED
**Effort:** Medium | **Usefulness:** 5/10 | **Migration impact:** Low

- Add reconnect timestamps/durations, per-server stats, last error info
- Make `Statistics` private on `Conn` (currently embedded as public field), only accessible via `Stats()` method

### 8. Rename `ErrJsonParse` → `ErrJSONParse` — APPROVED
**Effort:** Low | **Usefulness:** 3/10 | **Migration impact:** Low

Follow Go naming conventions for acronyms.

### 9. Unified error types + async error handler redesign — APPROVED

**Effort:** Medium-High | **Usefulness:** 8/10 | **Migration impact:** Medium

Redesign error types and async error handler as one coherent system. Two kinds of error types:

**Fine-grained types** for errors with per-instance context — `PermissionError` (Subject, Queue, Operation, Sub) and `SlowConsumerError` (Sub, PendingMsgs, PendingBytes, Dropped). These are their own sentinels via `Is()` matching by type.

**Broad category types** for groups of simple sentinel errors — `ConnectionError`, `AuthError`, `ValidationError`, `ServerError`. Each sentinel is an instance of its category type. `errors.As` catches the whole category, `errors.Is` matches specific sentinels.

**Async error handler** changes from `ErrHandler func(*Conn, *Subscription, error)` to `ErrorHandler func(*Conn, error)`. Subscription is removed from the signature — it's a field on `PermissionError` and `SlowConsumerError`. The handler receives the same typed errors from above.

Other changes: `PermissionErrOnSubscribe` option removed (always-on). PermissionError is NOT grouped under AuthError (different user response). Single handler per connection (no multi-handler API).

See [errors-design.md](errors-design.md) for full design with code examples and matching semantics.

### ~~UseOldRequestStyle removal~~ — DROPPED
Must stay. Some users depend on it.

### ~~Remove Set*Handler() methods~~ — DROPPED
Must stay for runtime handler changes.

---

## `jetstream` Package

### 10. Remove deprecated `Keys()` from KV — APPROVED
**Effort:** Low | **Usefulness:** 6/10 | **Migration impact:** Low

Already deprecated in favor of `ListKeys()`.

### 11. Remove `StreamConfig.Template` — APPROVED
**Effort:** Low | **Usefulness:** 5/10 | **Migration impact:** Low

Feature no longer supported by server.

### 12. Unify option patterns — APPROVED
**Effort:** High | **Usefulness:** 7/10 | **Migration impact:** Medium

Currently mixes func-type options and interface-based options inconsistently. Pick one pattern and apply consistently. Standardize naming (e.g., `ConsumeOpt` instead of `PullConsumeOpt`).

### 13. ObjectStore `List()` → iterator — APPROVED
**Effort:** Medium | **Usefulness:** 6/10 | **Migration impact:** Medium

`List()` currently returns `[]*ObjectInfo` loading everything into memory. Return an iterator/lister like KV's `ListKeys()`.

### 14. Change `Messages()` to Go iterator — APPROVED (new)
**Effort:** Medium | **Usefulness:** 8/10 | **Migration impact:** High

Currently `Messages()` returns a `MessagesContext` with `Next()` method. Change it to return a Go 1.23+ iterator so consumers can use:
```go
for msg, err := range consumer.Messages(ctx) {
    // process msg
}
```
This is more idiomatic and removes the need for the `MessagesContext` wrapper type.

### 15. Remove `Fetch`/`Next` from `OrderedConsumer` — APPROVED (new)
**Effort:** Low | **Usefulness:** 6/10 | **Migration impact:** Medium

Ordered consumers should only support `Consume()` and `Messages()`. `Fetch()` and `Next()` don't make sense for ordered consumers and can lead to misuse. Remove them from the `OrderedConsumer` interface.

### ~~Consumer/PushConsumer shared interface~~ — DROPPED
The only truly shared methods are `Info()` and `CachedInfo()` — not enough to justify a shared interface. `Consume()` doesn't unify cleanly (different option types: `PullConsumeOpt` vs `PushConsumeOpt`). Pull consumers have `Fetch`/`FetchBytes`/`FetchNoWait`/`Next`/`Messages` that push consumers will never have. A two-method base interface adds a type without adding value.

### 21. Move KV/ObjectStore to orbit.go — OPEN
**Effort:** Medium | **Usefulness:** 6/10 | **Migration impact:** Medium (import path change)

Currently KV and ObjectStore are tightly coupled to jetstream internals (unexported `*jetStream` type, `legacyJetStream()` for push-based watchers). Once PushConsumer is available in the new jetstream API, the main coupling point goes away. Remaining dependencies (API prefix, context wrapping, config preparation) are small and easily addressed through public getters or moving code with KV/ObjectStore.

Benefits: lighter jetstream package, independent versioning, cleaner architecture. Concern: KV/ObjectStore are core NATS features (not add-ons like micro), and users would need additional imports.

Depends on PushConsumer being available in the jetstream package first.

### ~~Rename FetchNoWait~~ — DROPPED
Matches server naming, consistency is more important.

### ~~PublishAsync + context~~ — DROPPED
Intentional design: async publish does core NATS publish without waiting for ack, context doesn't apply.

### ~~Ack method consolidation (Term/TermWithReason, Nak/NakWithDelay)~~ — DROPPED
Separate methods are fine, explicit is better.

### ~~KV watcher nil-signal change~~ — DROPPED
The nil signal pattern works well for select-based consumption. Changing it would force users to add an additional select case for a done channel, which is worse ergonomically.

---

## `micro` Package

### 17. Move `micro` to orbit.go — APPROVED
**Effort:** Low | **Usefulness:** 7/10 | **Migration impact:** Medium (import path change)

`micro` only uses public `nats.Conn` API, zero internal dependencies. Moving to orbit.go keeps core client lean and allows independent versioning. Import path changes from `github.com/nats-io/nats.go/micro` to `github.com/nats-io/orbit.go/micro`.

### 18. Add panic recovery in handlers — APPROVED
**Effort:** Low | **Usefulness:** 7/10 | **Migration impact:** None (additive)

Panicking handler currently crashes the service. Add recovery that sends 500 error response and calls ErrorHandler.

### 19. Standardize handler/callback naming — APPROVED
**Effort:** Low | **Usefulness:** 4/10 | **Migration impact:** Low

Fix inconsistencies: `HandlerFunc` vs `DoneHandler` vs `ErrHandler` vs `StatsHandler`.

### 20. Use `StatusChanged()` instead of hijacking conn handlers — APPROVED
**Effort:** Medium | **Usefulness:** 6/10 | **Migration impact:** Low

Currently `AddService()` replaces connection's `ClosedHandler`/`ErrorHandler`. Use `Conn.StatusChanged()` channel to observe lifecycle instead. Fixes issues with multiple services on one connection.

### ~~Context in Request~~ — DROPPED
Not worth the complexity for now. Handlers can create their own contexts.

---

## Summary Table

| # | Change | Package | Effort | Useful | Migration | Status |
|---|--------|---------|--------|--------|-----------|--------|
| 1 | Remove all deprecated APIs | core | Low | 8 | Low | APPROVED |
| 2 | Make `Conn.Opts` private | core | Low | 7 | Medium | APPROVED |
| 3 | Subscription type safety (embedded concrete types, remove ChanSubscribe) | core | Med-High | 8 | High | APPROVED |
| 4 | Add headers to `Publish()` | core | Low-Med | 7 | Medium | APPROVED |
| 5 | Clean up handler type naming | core | Medium | 6 | Medium | APPROVED |
| 6 | Remove `NoCallbacksAfterClientClose` | core | Low | 4 | Low | APPROVED |
| 7 | Improve `Statistics` | core | Medium | 5 | Low | APPROVED |
| 8 | `ErrJsonParse` → `ErrJSONParse` | core | Low | 3 | Low | APPROVED |
| 9 | Unified error types + async error handler redesign | core | Med-High | 8 | Medium | APPROVED |
| 10 | Remove deprecated `Keys()` | jetstream | Low | 6 | Low | APPROVED |
| 11 | Remove `StreamConfig.Template` | jetstream | Low | 5 | Low | APPROVED |
| 12 | Unify option patterns | jetstream | High | 7 | Medium | APPROVED |
| 13 | ObjectStore `List()` → iterator | jetstream | Medium | 6 | Medium | APPROVED |
| 14 | `Messages()` as Go iterator | jetstream | Medium | 8 | High | APPROVED |
| 15 | Remove Fetch/Next from OrderedConsumer | jetstream | Low | 6 | Medium | APPROVED |
| 16 | Consumer/PushConsumer shared interface | jetstream | High | 6 | High | DROPPED |
| 21 | Move KV/ObjectStore to orbit.go | jetstream | Medium | 6 | Medium | OPEN |
| 17 | Move micro to orbit.go | micro | Low | 7 | Medium | APPROVED |
| 18 | Panic recovery in handlers | micro | Low | 7 | None | APPROVED |
| 19 | Standardize naming | micro | Low | 4 | Low | APPROVED |
| 20 | Use StatusChanged() for lifecycle | micro | Medium | 6 | Low | APPROVED |
