# Subscription API Consolidation

## Context

The subscription API today has accumulated 7 subscribe methods reflecting three orthogonal axes:

- **Receive mode**: callback (async), `NextMsg` (sync), channel
- **Queue group**: plain or queue-distributed
- **Channel + sync combo**: `QueueSubscribeSyncWithChan`

```text
Subscribe / QueueSubscribe                  (callback)
SubscribeSync / QueueSubscribeSync          (NextMsg)
ChanSubscribe / ChanQueueSubscribe          (channel)
QueueSubscribeSyncWithChan                  (queue + sync + chan combo)
```

Three runtime errors enforce mode discipline at the API boundary:

- `ErrSyncSubRequired` — `NextMsg` called on async sub
- `ErrAsyncSubRequired` — async-specific methods called on sync sub
- `ErrChanSubRequired` — chan-specific methods called on others

These rarely fire in practice (call sites are usually a few lines from the constructor), so type-level protection of the *subscription type* is low-value. What does pay off:

- Reducing the subscribe surface to a single decision (sync vs async) with options carrying everything else.
- Removing channel subscriptions entirely (paradigm overlap with sync is real; niche use cases are recoverable via `SubscribeSync + Msgs()`).
- Better backpressure / lifecycle / stats ergonomics on the resulting `*Subscription`.
- Type-level safety on **options** so async-only options can't be passed to sync subscribes.

## Design

### Two subscribe methods

Sync vs async is a fundamental consumption-model choice, so it gets its own method. Everything else — queue group, pending limits, error handler — is an option.

```go
// Async callback
func (nc *Conn) Subscribe(subject string, handler MsgHandler, opts ...AsyncSubscribeOption) (*Subscription, error)

// Sync (NextMsg / Msgs iterator)
func (nc *Conn) SubscribeSync(subject string, opts ...SubscribeOption) (*Subscription, error)
```

Both return the same `*Subscription` type — no type split.

### Single `*Subscription` type

```go
type Subscription struct {
    Subject string
    Queue   string
    // private fields
}

// Receive
func (s *Subscription) NextMsg(timeout time.Duration) (*Msg, error)
func (s *Subscription) Msgs() iter.Seq2[*Msg, error]
func (s *Subscription) MsgsContext(ctx context.Context) iter.Seq2[*Msg, error]

// Lifecycle
func (s *Subscription) Unsubscribe() error
func (s *Subscription) Drain() error
func (s *Subscription) Drained() <-chan struct{}
func (s *Subscription) AutoUnsubscribe(max int) error

// Status
func (s *Subscription) IsValid() bool
func (s *Subscription) IsDraining() bool
func (s *Subscription) Type() SubscriptionType
func (s *Subscription) StatusChanged(statuses ...SubStatus) <-chan SubStatus
func (s *Subscription) SetClosedHandler(handler SubClosedHandler)

// Stats
func (s *Subscription) Stats() SubStats
```

Sync-only receive methods (`NextMsg`, `Msgs`, `MsgsContext`) called on a subscription created via `Subscribe` continue to return `ErrSyncSubRequired` at runtime. Two methods + option-level type discipline keep this rare without paying for a type split.

### Stats snapshot

Replaces five separate, race-prone getters (`Pending`, `Delivered`, `Dropped`, `MaxPending`, `PendingLimits`) with one atomic snapshot:

```go
type SubStats struct {
    Pending    uint64    // current pending msgs
    Delivered  uint64    // cumulative
    Dropped    uint64    // cumulative
    MaxPending uint64    // high-water mark
    Limits     SubLimits // configured limits
}

type SubLimits struct {
    Msgs  int
    Bytes int
}
```

`SetPendingLimits` becomes the `PendingLimits` option at subscribe time. Limits are set once and not mutated; if runtime mutability becomes necessary, expose explicitly later.

### Option type hierarchy

Sync-only and shared options use a marker interface so async-only options are compile-rejected when passed to `SubscribeSync`:

```go
type SubscribeOption interface {
    applyTo(*subOpts)
}

type AsyncSubscribeOption interface {
    SubscribeOption
    asyncOnly() // unexported marker — compile-time block
}
```

Built-in options:

```go
// Shared (SubscribeOption — applies to both Subscribe and SubscribeSync)
func Queue(group string) SubscribeOption
func PendingLimits(msgs, bytes int) SubscribeOption

// Async-only (AsyncSubscribeOption — compile error on SubscribeSync)
func SubErrorHandler(fn SubErrorHandlerFn) AsyncSubscribeOption
```

Resulting call sites:

```go
// Async with queue
nc.Subscribe("orders", handleOrder, nats.Queue("workers"))

// Async with sub-level error handler (async-only)
nc.Subscribe("orders", handleOrder, nats.SubErrorHandler(onErr))

// Sync with queue and pending limits
nc.SubscribeSync("audits", nats.Queue("auditors"), nats.PendingLimits(10000, 0))

// Compile error — async-only option on sync:
nc.SubscribeSync("audits", nats.SubErrorHandler(onErr))   // ✗
```

### Drain completion signal

`Drained()` returns a channel that closes when drain completes. Allocated lazily — callers that don't need it pay nothing.

```go
done := sub.Drained()
if err := sub.Drain(); err != nil { /* ... */ }
<-done   // wait for drain to finish
```

### Iterators

```go
// Iterate until subscription closes:
for msg, err := range sub.Msgs() {
    if err != nil { /* sub closed / drain / unsubscribe */ break }
    handle(msg)
}

// Iterate with cancellation:
for msg, err := range sub.MsgsContext(ctx) {
    if err != nil { break }
    handle(msg)
}
```

`MsgsTimeout(timeout)` is intentionally **not** provided — `NextMsg(timeout)` in a `for` loop covers the same per-call-timeout case in one extra line.

### Iterator termination semantics

Iterators terminate in three cases:

1. **User `break`** — explicit `break` (or `return`) out of `for ... range`. Subscription remains active; any messages already queued in the subscription's internal buffer stay there. Range over `Msgs()`/`MsgsContext()` again to resume, or call `sub.Unsubscribe()` to fully tear down.
2. **Clean shutdown** — `sub.Unsubscribe()`, `sub.Drain()` completing, or (for `MsgsContext`) the supplied `ctx` being canceled. Iterator yields no further messages and the loop exits without yielding an error.
3. **Fatal error** — connection permanently closed, server-initiated subscription termination, or other non-recoverable state. Iterator yields one final `(nil, err)` pair and terminates.

Transient conditions like slow-consumer drops do **not** interrupt iteration. They surface via `sub.Stats().Dropped` and via the connection-level or sub-level error handler. The iterator continues with the next available message.

## What is removed

- `QueueSubscribe`, `QueueSubscribeSync` — folded into `Queue(group)` option.
- `ChanSubscribe`, `ChanQueueSubscribe`, `QueueSubscribeSyncWithChan` — channel paradigm dropped.
- `MsgsTimeout` iterator — covered by `NextMsg(timeout)`.
- `Pending()`, `Delivered()`, `Dropped()`, `MaxPending()`, `ClearMaxPending()`, `PendingLimits()` accessor methods — replaced by `Stats()` snapshot.
- `SetPendingLimits()` post-subscribe — becomes the `PendingLimits` option at subscribe time.

## Net method count

```text
Before:                              After:
  7 subscribe methods                  2 subscribe methods
  ~18 *Subscription methods            ~13 *Subscription methods
  3 type-mismatch runtime errors       1 type-mismatch runtime error
                                       (post legacy-JS removal)
  1 subscription type                  1 subscription type
```

## Migration mapping

| v1 | v2 |
| -- | -- |
| `Subscribe(subj, cb)` | `Subscribe(subj, cb)` |
| `QueueSubscribe(subj, q, cb)` | `Subscribe(subj, cb, nats.Queue(q))` |
| `SubscribeSync(subj)` | `SubscribeSync(subj)` |
| `QueueSubscribeSync(subj, q)` | `SubscribeSync(subj, nats.Queue(q))` |
| `ChanSubscribe(subj, ch)` | `SubscribeSync(subj)` + `Msgs()` iterator |
| `ChanQueueSubscribe(subj, q, ch)` | `SubscribeSync(subj, nats.Queue(q))` + `Msgs()` |
| `QueueSubscribeSyncWithChan(subj, q, ch)` | `SubscribeSync(subj, nats.Queue(q))` + `Msgs()` |
| `sub.SetPendingLimits(m, b)` | `nats.PendingLimits(m, b)` option at subscribe |
| `sub.Pending()` | `sub.Stats().Pending` |
| `sub.Delivered()` | `sub.Stats().Delivered` |
| `sub.Dropped()` | `sub.Stats().Dropped` |
| `sub.MaxPending()` | `sub.Stats().MaxPending` |
| `sub.PendingLimits()` | `sub.Stats().Limits` |
| `sub.ClearMaxPending()` | removed |
| `sub.Drain()` | unchanged |
| — | `sub.Drained() <-chan struct{}` (new — completion signal) |
| — | `sub.MsgsContext(ctx)` (new — ctx-aware iterator) |
| `sub.NextMsg(timeout)` | unchanged |

## Internal `jetstream` rewrite

The `jetstream` package uses `nc.ChanSubscribe(inbox, ch)` in `pull.go` (and a handful of other places) to deliver pull-consumer messages directly into a channel without a per-sub dispatcher goroutine. Since `ChanSubscribe` is removed from the public API and `jetstream` is a separate module post-v2 (can't import unexported primitives), pull-consumer message delivery rewrites to:

```go
sub, _ := nc.SubscribeSync(inbox)
for msg, err := range sub.MsgsContext(ctx) {
    // process — direct mch receive underneath
}
```

`Msgs()` / `MsgsContext()` are `iter.Seq2` wrappers over `nextMsgNoTimeout` (see `nats_iter.go`), which is a direct `<-sub.mch` receive. Go 1.23+ range-over-func compiles to a direct call (no goroutine, no allocation on yield), so overhead should be within noise of today's `ChanSubscribe` path.

**Validation required before committing**: benchmark the rewrite under high message rate. Acceptance criterion: within ~5% of today's `ChanSubscribe`-based pull throughput.

**Pending-buffer semantics**: `ChanSubscribe` lets the caller own the channel and its capacity. `SubscribeSync` uses an internal queue sized by `SyncQueueLen` (default 65536). For pull's batch-driven flow control this should be sufficient; verify during the rewrite.

## Out of scope for v2.0

- **Context-aware subscription lifecycle.** `SubscribeContext(ctx, ...)` / `SubscribeSyncContext(ctx, ...)` with auto-Drain-on-cancel — useful but adds a fourth axis (sync × async × ctx × no-ctx) and the "magic auto-cleanup" semantics warrant more user discussion. Save for v2.x as additive methods if demand surfaces.
- **Pluggable slow-consumer policy.** Per-sub policy options (drop-oldest, block, custom) — purely additive, would benefit from cross-client parity. v2.x.
- **Generic / typed subscriptions.** `Subscribe[T any](subj, func(T))` with header-driven decoding — encoding concern, not subscription concern. Out of scope.
