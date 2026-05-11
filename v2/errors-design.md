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

```go
type ConnectionError struct{ msg string }

func (e *ConnectionError) Error() string { return e.msg }
func (e *ConnectionError) Is(target error) bool {
    t, ok := target.(*ConnectionError)
    if !ok { return false }
    if t.msg == "" { return true }  // bare type match (any ConnectionError)
    return e.msg == t.msg           // specific sentinel match
}

var ErrConnectionClosed       = &ConnectionError{"nats: connection closed"}
var ErrDisconnected           = &ConnectionError{"nats: disconnected"}
var ErrConnectionReconnecting = &ConnectionError{"nats: connection reconnecting"}
var ErrConnectionDraining     = &ConnectionError{"nats: connection draining"}
var ErrNoServers              = &ConnectionError{"nats: no servers available for connection"}
var ErrStaleConnection        = &ConnectionError{"nats: stale connection"}
var ErrDrainTimeout           = &ConnectionError{"nats: drain timeout"}
var ErrReconnectBufExceeded   = &ConnectionError{"nats: outbound buffer limit exceeded"}


type AuthError struct{ msg string }
// Same Is() pattern as ConnectionError

var ErrAuthorization      = &AuthError{"nats: authorization violation"}
var ErrAuthExpired        = &AuthError{"nats: authentication expired"}
var ErrAuthRevoked        = &AuthError{"nats: authentication revoked"}
var ErrAccountAuthExpired = &AuthError{"nats: account authentication expired"}


type ValidationError struct{ msg string }
// Same Is() pattern

var ErrBadSubject      = &ValidationError{"nats: invalid subject"}
var ErrBadQueueName    = &ValidationError{"nats: invalid queue name"}
var ErrInvalidArg      = &ValidationError{"nats: invalid argument"}
var ErrInvalidMsg      = &ValidationError{"nats: invalid message or message nil"}
var ErrBadTimeout      = &ValidationError{"nats: timeout invalid"}
var ErrMaxPayload      = &ValidationError{"nats: maximum payload exceeded"}
var ErrInvalidContext   = &ValidationError{"nats: invalid context"}
var ErrNoDeadlineContext = &ValidationError{"nats: context requires a deadline"}


type ServerError struct{ msg string }
// Same Is() pattern

var ErrMaxConnectionsExceeded   = &ServerError{"nats: server maximum connections exceeded"}
var ErrMaxSubscriptionsExceeded = &ServerError{"nats: server maximum subscriptions exceeded"}
var ErrNoInfoReceived           = &ServerError{"nats: protocol exception, INFO not received"}
var ErrNoEchoNotSupported       = &ServerError{"nats: server does not support no echo"}
var ErrHeadersNotSupported      = &ServerError{"nats: headers not supported by this server"}
```

### How matching works

```go
// Specific sentinel:
errors.Is(err, nats.ErrConnectionClosed)     // true for that exact error
errors.Is(err, nats.ErrPermissionViolation)  // true for any *PermissionError

// Broad category (any connection error):
var connErr *nats.ConnectionError
errors.As(err, &connErr)  // true for ANY ConnectionError

// Fine-grained with context:
var permErr *nats.PermissionError
errors.As(err, &permErr)  // true — and permErr has Subject, Operation, Sub fields
```

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

### What the handler receives at each call site

| Situation | Error received | Type |
|-----------|---------------|------|
| Permission violation (pub) | `&PermissionError{Subject: "foo", Operation: Publish}` | `*PermissionError` |
| Permission violation (sub) | `&PermissionError{Subject: "foo", Operation: Subscribe, Sub: sub}` | `*PermissionError` |
| Slow consumer | `&SlowConsumerError{Sub: sub, PendingMsgs: 65536, ...}` | `*SlowConsumerError` |
| Auth expired | `ErrAuthExpired` | `*AuthError` |
| Auth revoked | `ErrAuthRevoked` | `*AuthError` |
| Flush/write error | `&ConnectionError{...}` | `*ConnectionError` |
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

**Delivery semantics:** when a subscription has a `SubErrorHandler`, subscription-level errors are delivered to **both** the per-subscription handler and the connection-level `ErrorHandler`. The connection handler provides global observability (logging, metrics), while the sub handler enables subscription-specific reactions.

For sync subscriptions, permission errors surface through `NextMsg()` directly — a `SubErrorHandler` is available but typically unnecessary.

**Key points:**

- Set at subscribe time only (subscribe option), not mutable after creation
- Lives on `*Subscription` (base type), so available for both async and sync subscriptions
- Both the per-sub handler and connection-level handler fire — not either/or

### Other changes

- **`PermissionErrOnSubscribe` option removed** — permission errors always surface through `NextMsg()` on sync subscriptions. No special option needed.
- **`SetErrorHandler()` stays** — takes the new `ErrorHandler func(*Conn, error)` type.
- **Single handler per connection** — no `OnError()` multi-handler API. Moving micro to orbit.go + using `StatusChanged()` solves the handler-hijacking problem.
