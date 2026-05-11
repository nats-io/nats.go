# Subscription Type Safety with Embedded Concrete Types

## Context

There is one `*Subscription` type returned by all 7 subscribe methods. It has 18 public methods and 2 public fields (`Subject`, `Queue`). The problem: 8 of those methods return runtime errors depending on which subscribe method created the subscription (e.g., `NextMsg()` returns `ErrSyncSubRequired` on async subscriptions).

## Design

### Remove channel subscriptions

`ChanSubscribe`, `ChanQueueSubscribe`, `QueueSubscribeSyncWithChan` are removed in v2. Users can achieve the same with a callback that sends to a channel. This leaves 4 creation methods.

### Method breakdown after removing Chan

Shared by both Async and Sync (15 methods + 2 fields):
```
Subject, Queue (fields)
Type(), IsValid(), Drain(), IsDraining(), StatusChanged(), Unsubscribe(),
AutoUnsubscribe(), SetClosedHandler(), Delivered(), Dropped(),
Pending(), MaxPending(), ClearMaxPending(), PendingLimits(), SetPendingLimits()
```

Sync-only (3 methods — currently return `ErrSyncSubRequired` on Async):
```
NextMsg(timeout time.Duration) (*Msg, error)
Msgs() iter.Seq2[*Msg, error]
MsgsTimeout(timeout time.Duration) iter.Seq2[*Msg, error]
```

### Two concrete types using embedding

```go
// Subscription is the base type for async (callback) subscriptions.
// Returned by Subscribe() and QueueSubscribe().
type Subscription struct {
    Subject string
    Queue   string
    // private fields...
}
// Has all shared methods: Unsubscribe, Drain, Pending, Delivered, etc. (15 methods)

// SyncSubscription embeds *Subscription and adds sync-specific methods.
// Returned by SubscribeSync() and QueueSubscribeSync().
type SyncSubscription struct {
    *Subscription
}
// Adds: NextMsg, Msgs (Go iterator), MsgsTimeout (Go iterator)
```

### Creation methods

```go
func (nc *Conn) Subscribe(subj string, cb MsgHandler) (*Subscription, error)
func (nc *Conn) QueueSubscribe(subj, queue string, cb MsgHandler) (*Subscription, error)
func (nc *Conn) SubscribeSync(subj string) (*SyncSubscription, error)
func (nc *Conn) QueueSubscribeSync(subj, queue string) (*SyncSubscription, error)
```

### Usage

```go
// Async — Subject/Queue stay as fields, unchanged from v1
sub, _ := nc.Subscribe("foo", func(msg *nats.Msg) { ... })
fmt.Println(sub.Subject)
sub.Unsubscribe()
// sub.NextMsg(time.Second)  // compile error — not on *Subscription

// Sync — NextMsg, iterators available
ss, _ := nc.SubscribeSync("foo")
msg, _ := ss.NextMsg(time.Second)
ss.Unsubscribe()                    // promoted from embedded *Subscription
fmt.Println(ss.Subject)             // promoted field

// Go 1.23+ iterator
for msg, err := range ss.Msgs() {
    // ...
}

// Mixed collections — extract the base
subs := []*nats.Subscription{asyncSub, syncSub.Subscription}
for _, s := range subs {
    s.Drain()
}
```

### Internal implementation

```go
// Subscription holds all the real state
type Subscription struct {
    Subject string
    Queue   string
    mu      sync.Mutex
    sid     int64
    mcb     MsgHandler
    mch     chan *Msg
    typ     SubscriptionType
    // ... all existing private fields
}

// SyncSubscription is a thin wrapper
type SyncSubscription struct {
    *Subscription
}

func (s *SyncSubscription) NextMsg(timeout time.Duration) (*Msg, error) {
    return s.Subscription.nextMsg(timeout)
}
```

### Key decisions

- **Embedding over interfaces** — `Subject` and `Queue` stay as fields (no migration for field access). Embedding is idiomatic Go and easy to extend with new methods.
- **Mixed collections use base extraction** — `syncSub.Subscription` is slightly awkward but only matters in the "store for cleanup" case where you only need `Drain()`/`Unsubscribe()`.
- **Single internal struct** — `SyncSubscription` is a thin wrapper. Minimal internal changes, no code duplication.
