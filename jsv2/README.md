# JetStream Simplified Client

This doc covers the basic usage of the `jetstream` package in `nats.go` client.

- [JetStream Simplified Client](#jetstream-simplified-client)
  - [Overview](#overview)
  - [Basic usage](#basic-usage)
  - [Context support](#context-support)
  - [JetStream](#jetstream)
  - [Streams](#streams)
    - [Stream management (CRUD)](#stream-management-crud)
    - [Listing streams and stream names](#listing-streams-and-stream-names)
    - [Stream-specific operations](#stream-specific-operations)
  - [Consumers](#consumers)
    - [Consumers management](#consumers-management)
    - [Listing consumers and consumer names](#listing-consumers-and-consumer-names)
    - [Receiving messages from the consumer](#receiving-messages-from-the-consumer)
      - [Polling consumer](#polling-consumer)
      - [Event-Driven consumer](#event-driven-consumer)
  - [Publishing on stream](#publishing-on-stream)
    - [Synchronous publish](#synchronous-publish)
    - [Async publish](#async-publish)

## Overview

`jetstream` package is a new, experimental client API to interact with NATS JetStream, aiming to replace the JetStream client implementation from `nats` package.
The main goal of this package is to provide a simple and clear way to interact with JetStream API.
Key differences between `jetstream` and `nats` packages include:

- Using smaller, simlpler interfaces to manage streams and consumers
- Using more granular and predictable approach to consuming messages from a stream, instead of relying on often complicated and unpredictable `Subscribe()` method (and all of its flavors)
- Allowing the usage of pull consumers to asynchronously receive incoming messages
- Separating JetStream context from core NATS
- Simplifying timeout management by extensive use of `Context`

`jetstream` package provides several for interacting with the API:

- `JetStream` - top-level interface, used to create and manage streams, consumers and publishing messages
- `Stream` - used to manage consumers for a specific stream, as well as performing stream-specific operations (purging, fetching and deleting messages by sequence number, fetching stream info)
- `Consumer` - used to get information about a consumer as well as consuming messages
- `Msg` - used for message-specific operations - reading data, headers and metadata, as well as performing various types of acknowledgements

## Basic usage

```go
import (
    "context"
    "fmt"
    "time"

    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jsv2/jetstream"
)

func main() {
    // In the `jetstream` package, almost all methods rely on `context.Context` for timeout/cancellation handling
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()
    nc, _ := nats.Connect(nats.DefaultURL)

    // Create a JetStream management interface
    js, _ := jetstream.New(nc)

    // Create a stream
    s, _ := js.CreateStream(ctx, jetstream.StreamConfig{
        Name:     "ORDERS",
        Subjects: []string{"ORDERS.*"},
    })

    // Publish some messages
    for i := 0; i < 100; i++ {
        js.Publish(ctx, "ORDERS.new", []byte("hello"))
    }

    // Create durable consumer
    c, _ := s.CreateConsumer(ctx, jetstream.ConsumerConfig{
        Durable:   "CONS",
        AckPolicy: jetstream.AckExplicitPolicy,
    })

    // Get a single message from the consumer
    msg, _ := c.Next(ctx)
    if msg != nil {
        msg.Ack()
        fmt.Printf("Received a JetStream message: %s\n", string(msg.Data()))
    }

    // Receive messages continuously
    c.Subscribe(ctx, func(msg jetstream.Msg, err error) {
        msg.Ack()
        fmt.Printf("Received a JetStream message: %s\n", string(msg.Data()))
    })
}
```

## Context support

This version of JetStream API relies heavily on the use of `context.Context` in order to manage request timeouts and cancellation. Nearly all methods accept `Context` as first parameter.

For example, in order to stop receiving messages on `Stream()`:

```go
ctx, cancel := context.WithCancel(context.Background())
c.Subscribe(ctx, func(msg jetstream.Msg, err error) {
    msg.Ack()
    fmt.Printf("Received a JetStream message: %s\n", string(msg.Data()))
})

if stopCondition {
    // Calling cancel will unsubscribe the consumer from the stream. 
    cancel()
}
```

## JetStream

## Streams

`jetstream` provides methods to manage and list streams, as well as perform stream-specific operations (purging, fetching/deleting messages by sequence id)

### Stream management (CRUD)

```go
js, _ := jetstream.New(nc)

// create a stream (this is an idempotent operation)
s, _ := js.CreateStream(ctx, jetstream.StreamConfig{
    Name:     "ORDERS",
    Subjects: []string{"ORDERS.*"},
})

// update a stream
s, _ = js.UpdateStream(ctx, jetstream.StreamConfig{
    Name:        "ORDERS",
    Subjects:    []string{"ORDERS.*"},
    Description: "updated stream",
})

// get stream handle
s, _ = js.Stream(ctx, "ORDERS")

// delete a stream
js.DeleteStream(ctx, "ORDERS")
```

### Listing streams and stream names

```go
// list streams
streams := js.ListStreams(ctx)
var err error
for err != nil {
    select {
    case s := <-streams.Info():
        fmt.Println(s.Config.Name)
    case err = <-streams.Err():
    }
}
if err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
    fmt.Println("Unexpected error occured")
}

// list stream names
names := js.StreamNames(ctx)
for err != nil {
    select {
    case name := <-names.Name():
        fmt.Println(name)
    case err = <-names.Err():
    }
}
if err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
    fmt.Println("Unexpected error occured")
}
```

### Stream-specific operations

Using `Stream` interface, it is also possible to:

- Purge a stream

```go
// remove all messages from a stream
_ = s.Purge(ctx)

// remove all messages from a stream that are stored on a specific subject
_ = s.Purge(ctx, jetstream.WithSubject("ORDERS.new"))

// remove all messages up to specified sequence number
_ = s.Purge(ctx, jetstream.WithSequence(100))

// remove messages, but keep 10 newest
_ = s.Purge(ctx, jetstream.WithKeep(10))
```

- Get and messages from stream

```go
// get message from stream with sequence number == 100
msg, _ := s.GetMsg(ctx, 100)

// get last message from "ORDERS.new" subject
msg, _ = s.GetLastMsgForSubject(ctx, "ORDERS.new")

// delete a message with sequence number == 100
_ = s.DeleteMsg(ctx, 100)
```

- Get information about a stream

```go
// Fetches latest stream info from server
info, _ := s.Info(ctx)
fmt.Println(info.Config.Name)

// Returns the most recently fetched StreamInfo, without making an API call to the server
cachedInfo := s.CachedInfo()
fmt.Println(cachedInfo.Config.Name)
```

## Consumers

Currently, only pull consumers are supported in `jetstream` package. However, unlike the JetStream API in `nats` package, pull consumers allow for continous message receival (similarly to how `nats.Subscribe()` works). Because of that, push consumers can be easily replace by pull consumers for most of the use cases.

### Consumers management

CRUD operations on consumers can be achieved on 2 levels:

- on `JetStream` interface

```go
js, _ := jetstream.New(nc)

// create a consumer (this is an idempotent operation)
cons, _ := js.CreateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
    Durable: "foo",
    AckPolicy: jetstream.AckExplicitPolicy,
})

// create an ephemeral pull consumer by not providing `Durable`
ephemeral, _ := js.CreateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
    AckPolicy: jetstream.AckExplicitPolicy,
})

// update consumer
cons, _ = js.UpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
    Durable:   "foo",
    AckPolicy: jetstream.AckExplicitPolicy,
    Description: "updated consumer"
})


// get consumer handle
cons, _ = js.Consumer(ctx, "ORDERS", "foo")

// delete a consumer
js.DeleteConsumer(ctx, "ORDERS", "foo")
```

- on `Stream` interface

```go
// Create a JetStream management interface
js, _ := jetstream.New(nc)

// get stream handle
stream, _ := js.Stream(ctx, "ORDERS")

// create consumer
cons, _ := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
    Durable:   "foo",
    AckPolicy: jetstream.AckExplicitPolicy,
})

// update consumer
cons, _ = s.UpdateConsumer(ctx, jetstream.ConsumerConfig{
    Durable:   "foo",
    AckPolicy: jetstream.AckExplicitPolicy,
    Description: "updated consumer"
})

// get consumer handle
cons, _ = stream.Consumer(ctx, "ORDERS", "foo")

// delete a consumer
stream.DeleteConsumer(ctx, "foo")
```

`Consumer` interface, returned when creating/fetching consumers, allows fetching `ConsumerInfo`:

```go
// Fetches latest consumer info from server
info, _ := cons.Info(ctx)
fmt.Println(info.Config.Durable)

// Returns the most recently fetched ConsumerInfo, without making an API call to the server
cachedInfo := cons.CachedInfo()
fmt.Println(cachedInfo.Config.Durable)
```

### Listing consumers and consumer names

```go
// list consumers
consumers := s.ListConsumers(ctx)
var err error
for err != nil {
    select {
    case s := <-consumers.Info():
        fmt.Println(s.Name)
    case err = <-consumers.Err():
    }
}
if err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
    fmt.Println("Unexpected error occured")
}

// list consumer names
names := s.ConsumerNames(ctx)
for err != nil {
    select {
    case name := <-names.Name():
        fmt.Println(name)
    case err = <-names.Err():
    }
}
if err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
    fmt.Println("Unexpected error occured")
}
```

### Receiving messages from the consumer

The `Consumer` interface covers 2 patterns for receiving incoming messages from Stream - Polling Consumer pattern and Event-Driven Consumer pattern.

#### __Polling consumer__

Polling consumer pattern allows fetching messages synchronously one by one. Using context, user can decide how long the consumer should wait for a message in case none is available on the stream at the time of call.

```go
for {
    ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
    msg, _ := c.Next(ctx)
    cancel()
    // In case there are no messages, Next() returns nil
    if msg == nil {
        break
    }
    fmt.Printf("Received a JetStream message: %s\n", string(msg.Data()))
}
```

`Next()` accepts options to customize its behavior:

- `WithNoWait()` - whan used, `Next()` does not wait for the message to appear on the consumer fot the specified timeout duration, but rather return a message if it is available at the time of request (one-shot)

```go
msg, _ := c.Next(ctx, WithNoWait())
if msg != nil {
    fmt.Println(string(msg.Data()))
}
```

- `WithNextHeartbeat(time.Duration)` - when used, sets the idle heartbeat on the request to the API, veryfing whether stream is alive for the duration on the request.

```go
msg, err := c.Next(ctx, WithNextHeartbeat(1*time.Second))
if err != nil && errors.Is(err, jetstream.ErrNoHeartbeat) {
    fmt.Println("something went wrong")
}
if msg != nil {
    fmt.Println(string(msg.Data()))
}
```

#### __Event-Driven consumer__

Event-Driven consumer pattern allows for continuous, asynchronous processing of incoming messages. Its behavior is similar to how push consumers work, therefore it can be user as a drop-in replacement in most of the use cases (with the exception of ordered push consumers).

```go
c.Subscribe(ctx, func(msg jetstream.Msg, err error) {
    fmt.Printf("Received a JetStream message: %s\n", string(msg.Data()))
})
```

`Stream()` can be supplied with options to modify the behavior of a single pull request, however for most of the use cases is it using sensible defaults

- `WithBatchSize(int)` - the maximum amount of messages returned in a single pull request
- `WithExpiry(time.Duration)` - maximum amount of time a request should wait for the full batch
- `WithSubscribeHeartbeat(time.Duration)` - when used, sets the idle heartbeat on the Stream operation, veryfing whether stream is alive

## Publishing on stream

`JetStream` interface allows publishing messages on stream in 2 ways:

### __Synchronous publish__

```go
js, _ := jetstream.New(nc)

// Publish message on subject ORDERS.new
// Given subject has to belong to a stream
ack, err := js.PublishMsg(ctx, &nats.Msg{
    Data:    []byte("hello"),
    Subject: "ORDERS.new",
})
fmt.Printf("Published msg with sequence number %d on stream %q", ack.Sequence, ack.Stream)

// A helper method accepting subject and data as parameters
ack, err = js.Publish(ctx, "ORDERS.new", []byte("hello"))
```

Both `Publish()` and `PublishMsg()` can be supplied with options allowing setting various headers. Additionally, for `PublishMsg()` headers can be set directly on `nats.Msg`.

```go
// All 3 implementations are work identically 
ack, err := js.PublishMsg(ctx, &nats.Msg{
    Data:    []byte("hello"),
    Subject: "ORDERS.new",
    Header: nats.Header{
        "Nats-Msg-Id": []string{"id"},
    },
})

ack, err = js.PublishMsg(ctx, &nats.Msg{
    Data:    []byte("hello"),
    Subject: "ORDERS.new",
}, jetstream.WithMsgID("id"))

ack, err = js.Publish(ctx, "ORDERS.new", []byte("hello"), jetstream.WithMsgID("id"))
```

### __Async publish__

```go
js, _ := jetstream.New(nc)

// publish message and do not wait for ack
ackF, err := js.PublishMsgAsync(ctx, &nats.Msg{
    Data:    []byte("hello"),
    Subject: "ORDERS.new",
})

// block and wait for ack
select {
case ack := <-ackF.Ok():
    fmt.Printf("Published msg with sequence number %d on stream %q", ack.Sequence, ack.Stream)
case err := <-ackF.Err():
    fmt.Println(err)
}

// similarly to syncronous publish, there is a helper method accepting subject and data
ackF, err = js.PublishAsync(ctx, "ORDERS.new", []byte("hello"))
```

Just as for synchronous publish, `PublishAsync()` and `PublishMsgAsync()` accept options for setting headers.
