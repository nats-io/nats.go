# Unified Error Types + Async Error Handler Redesign

## Context

The current error system has two problems:
1. ~55 flat sentinel errors (`var ErrFoo = errors.New(...)`) with no grouping — users can't catch categories like "any connection error"
2. The async error handler `ErrHandler func(*Conn, *Subscription, error)` loses context (nil subscription for permission violations, no subject/direction info, no pending counts for slow consumer)

This design merges both into one coherent system: typed error structs serve as both sentinel categories and structured async error events.

## Design

Two kinds of error types:

1. **Fine-grained types** — for errors that carry per-instance context (permission violations, slow consumer). Structured fields, own `Is()` method for sentinel matching.
2. **Broad category types** — for groups of simple sentinel errors with similar user response patterns (connection problems, auth failures, validation, server limits). No per-instance context.

### Fine-grained types

```go
// PermissionError — server denied a publish or subscribe operation.
type PermissionError struct {
    Subject   string
    Queue     string           // empty for non-queue
    Operation PermissionOp     // Publish or Subscribe
    Sub       *Subscription    // non-nil for subscribe violations
}

var ErrPermissionViolation = &PermissionError{}

func (e *PermissionError) Error() string {
    return fmt.Sprintf("nats: permission violation: %s to %q", e.Operation, e.Subject)
}
func (e *PermissionError) Is(target error) bool {
    _, ok := target.(*PermissionError)
    return ok
}

// SlowConsumerError — subscription can't keep up with message rate.
type SlowConsumerError struct {
    Sub          *Subscription
    PendingMsgs  int
    PendingBytes int
    Dropped      int64
}

var ErrSlowConsumer = &SlowConsumerError{}

func (e *SlowConsumerError) Error() string {
    return fmt.Sprintf("nats: slow consumer: pending=%d msgs/%d bytes, dropped=%d",
        e.PendingMsgs, e.PendingBytes, e.Dropped)
}
func (e *SlowConsumerError) Is(target error) bool {
    _, ok := target.(*SlowConsumerError)
    return ok
}
```

### Broad category types

Each broad category is a single struct with a message field. No custom `Is()` method — sentinel matching uses pointer identity (handled by `errors.Is` natively), and type matching uses `errors.As`.

```go
type ConnectionError struct{ msg string }

func (e *ConnectionError) Error() string { return e.msg }

var ErrConnectionClosed       = &ConnectionError{"nats: connection closed"}
var ErrDisconnected           = &ConnectionError{"nats: disconnected"}
var ErrConnectionReconnecting = &ConnectionError{"nats: connection reconnecting"}
var ErrConnectionDraining     = &ConnectionError{"nats: connection draining"}
var ErrNoServers              = &ConnectionError{"nats: no servers available for connection"}
var ErrStaleConnection        = &ConnectionError{"nats: stale connection"}
var ErrDrainTimeout           = &ConnectionError{"nats: drain timeout"}
var ErrReconnectBufExceeded   = &ConnectionError{"nats: outbound buffer limit exceeded"}


type AuthError struct{ msg string }

func (e *AuthError) Error() string { return e.msg }

var ErrAuthorization      = &AuthError{"nats: authorization violation"}
var ErrAuthExpired        = &AuthError{"nats: authentication expired"}
var ErrAuthRevoked        = &AuthError{"nats: authentication revoked"}
var ErrAccountAuthExpired = &AuthError{"nats: account authentication expired"}


type ValidationError struct{ msg string }

func (e *ValidationError) Error() string { return e.msg }

var ErrBadSubject      = &ValidationError{"nats: invalid subject"}
var ErrBadQueueName    = &ValidationError{"nats: invalid queue name"}
var ErrInvalidArg      = &ValidationError{"nats: invalid argument"}
var ErrInvalidMsg      = &ValidationError{"nats: invalid message or message nil"}
var ErrBadTimeout      = &ValidationError{"nats: timeout invalid"}
var ErrMaxPayload      = &ValidationError{"nats: maximum payload exceeded"}
var ErrInvalidContext   = &ValidationError{"nats: invalid context"}
var ErrNoDeadlineContext = &ValidationError{"nats: context requires a deadline"}


type ServerError struct{ msg string }

func (e *ServerError) Error() string { return e.msg }

var ErrMaxConnectionsExceeded   = &ServerError{"nats: server maximum connections exceeded"}
var ErrMaxSubscriptionsExceeded = &ServerError{"nats: server maximum subscriptions exceeded"}
var ErrNoInfoReceived           = &ServerError{"nats: protocol exception, INFO not received"}
var ErrNoEchoNotSupported       = &ServerError{"nats: server does not support no echo"}
var ErrHeadersNotSupported      = &ServerError{"nats: headers not supported by this server"}
```

### How matching works

`errors.Is` performs pointer comparison along the unwrap chain, so sentinel matching works out of the box without a custom `Is()` method. Type matching (any error of a given category) uses `errors.As`.

```go
// Specific sentinel — pointer identity through unwrap chain:
errors.Is(err, nats.ErrConnectionClosed)     // true iff err is (or wraps) this sentinel
errors.Is(err, nats.ErrPermissionViolation)  // true for any *PermissionError (custom Is)

// Broad category (any connection error):
var connErr *nats.ConnectionError
errors.As(err, &connErr)  // true for ANY *ConnectionError, including wrapped

// Fine-grained with context:
var permErr *nats.PermissionError
errors.As(err, &permErr)  // true — and permErr has Subject, Operation, Sub fields
```

### Returning broad-category errors from internal code

Because identity is pointer-based, **internal code must return a named sentinel** (optionally wrapped with `fmt.Errorf` for context) rather than constructing a fresh `&ConnectionError{...}` with an ad-hoc message. A fresh instance is a different allocation and matches no sentinel.

```go
// Wrong — invisible to errors.Is(err, ErrConnectionWriteFailed):
return &ConnectionError{"flush: write failed: " + err.Error()}

// Right — wraps the sentinel with context:
return fmt.Errorf("flush: %w", ErrConnectionWriteFailed)
```

When a new condition needs to be expressible as a broad-category error, add a new sentinel rather than creating ad-hoc instances.

### Key decisions

- **PermissionError is NOT an AuthError** — different user response (check subject config vs refresh credentials)
- **SlowConsumerError is standalone** — not grouped under a "SubscriptionError" category, because it carries rich context that other subscription errors don't
- **ErrTimeout stays uncategorized** — cross-cutting, used in too many contexts to fit one category. TBD during implementation

## Async Error Handler Redesign

### Handler signature change

```go
// Old:
type ErrHandler func(*Conn, *Subscription, error)

// New:
type ErrorHandler func(*Conn, error)
```

`*Subscription` is removed from the signature — it's now a field on `PermissionError` and `SlowConsumerError` where relevant. No more nil-checking.

### Scope of the connection `ErrorHandler`

The connection-level `ErrorHandler` receives async errors — errors that are NOT the synchronous return value of an API call. Specifically:

- Protocol-level read/write errors (`*ConnectionError`)
- Permission violations on publish or subscribe (`*PermissionError`) — when no sub-level `SubErrorHandler` is set
- Slow-consumer drops (`*SlowConsumerError`) — when no sub-level handler is set
- Authentication expirations / revocations (`*AuthError`)
- Server-side protocol violations and limit errors (`*ServerError`)

It does NOT receive:

- Errors returned directly from synchronous API calls (those are the caller's responsibility)
- JetStream-package async errors — publish-ack failures, consumer heartbeat misses, ordered-consumer reset events, etc. These surface via the JetStream API's own callbacks (`PublishAsync` ack/err channels, `ConsumeContext`, iterator yields). They do not bubble up to the core connection handler.
- Subscription-scoped errors (permission, slow consumer for a specific sub) when that subscription has a `SubErrorHandler` set — those route to the sub handler exclusively, per the "sub-only when set" delivery rule below.

### What the handler receives at each call site

| Situation | Error received | Type |
|-----------|---------------|------|
| Permission violation (pub) | `&PermissionError{Subject: "foo", Operation: Publish}` | `*PermissionError` |
| Permission violation (sub) | `&PermissionError{Subject: "foo", Operation: Subscribe, Sub: sub}` | `*PermissionError` |
| Slow consumer | `&SlowConsumerError{Sub: sub, PendingMsgs: 65536, ...}` | `*SlowConsumerError` |
| Auth expired | `ErrAuthExpired` | `*AuthError` |
| Auth revoked | `ErrAuthRevoked` | `*AuthError` |
| Flush/write error | `fmt.Errorf("flush: %w", ErrConnectionWriteFailed)` (new sentinel, TBD name) | `*ConnectionError` |
| Bad header | `ErrBadHeaderMsg` | TBD category |
| Max subs exceeded | `ErrMaxSubscriptionsExceeded` | `*ServerError` |

### Usage

```go
nc, _ := nats.Connect(url, nats.ErrorHandler(func(c *nats.Conn, err error) {
    switch e := err.(type) {
    case *nats.PermissionError:
        log.Printf("denied: %s to %q (sub=%v)", e.Operation, e.Subject, e.Sub)
    case *nats.SlowConsumerError:
        log.Printf("slow: %q pending=%d/%d dropped=%d",
            e.Sub.Subject, e.PendingMsgs, e.PendingBytes, e.Dropped)
    case *nats.AuthError:
        log.Printf("auth: %v", e)
    case *nats.ConnectionError:
        log.Printf("conn: %v", e)
    case *nats.ServerError:
        log.Printf("server: %v", e)
    default:
        log.Printf("nats error: %v", err)
    }
}))
```

### Per-subscription error handler

Subscription-level errors (permission violations, slow consumer) can also be delivered directly to the subscription via a subscribe option:

```go
sub, _ := nc.Subscribe("foo", msgHandler, nats.SubErrorHandler(func(err error) {
    var permErr *nats.PermissionError
    if errors.As(err, &permErr) {
        log.Printf("sub denied: %s", permErr.Subject)
    }
}))
```

**Delivery semantics:** when a subscription has a `SubErrorHandler` set, subscription-level errors are delivered **only** to the per-subscription handler. The connection-level `ErrorHandler` does NOT fire for that subscription's errors. The user opts in to per-sub handling by setting the handler, and that's an explicit transfer of responsibility — they're handling it, the framework should not also fire global side-effects.

Subscriptions without a `SubErrorHandler` continue to surface errors through the connection-level `ErrorHandler` as today.

Suppression follows naturally: registering an empty handler silences a subscription's errors.

```go
sub, _ := nc.Subscribe("noisy-debug-topic", cb,
    nats.SubErrorHandler(func(err error) { /* intentionally ignored */ }))
```

If a user wants per-sub side-effects AND global behavior, they chain explicitly — no framework convention required:

```go
func globalErrorHandler(c *nats.Conn, err error) { /* … */ }

nc, _ := nats.Connect(url, nats.ErrorHandler(globalErrorHandler))
nc.Subscribe("foo", cb, nats.SubErrorHandler(func(err error) {
    notifyOps(err)               // sub-specific
    globalErrorHandler(nc, err)  // explicit chain to global
}))
```

For sync subscriptions, permission errors surface through `NextMsg()` directly — a `SubErrorHandler` is available but typically unnecessary.

**Key points:**

- Set at subscribe time only (subscribe option), not mutable after creation
- Lives on `*Subscription` (base type), so available for both async and sync subscriptions
- Setting a sub handler transfers responsibility for that subscription's errors to the user — connection-level handler does not also fire

### Other changes

- **`PermissionErrOnSubscribe` option removed** — permission errors always surface through `NextMsg()` on sync subscriptions. No special option needed.
- **`SetErrorHandler()` stays** — takes the new `ErrorHandler func(*Conn, error)` type.
- **Single handler per connection** — no `OnError()` multi-handler API. Moving micro to orbit.go + using `StatusChanged()` solves the handler-hijacking problem.
