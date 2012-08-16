# Node_Nats

A Go client for the [NATS messaging system](https://github.com/derekcollison/nats).

## Installation

```bash
# Go client
go get github.com/apcera/nats
# NATS system
gem install nats
```

## Basic Usage

```go

nc := nats.Connect(nats.DefaultURL)

// Simple Publisher
nc.Publish("foo", []byte("Hello World"))

// Simple Subscriber
nc.Subscribe("foo", func(_, _ string, data []byte, _ *Subscription) {
    fmt.Printf("Received a message: %v\n", data)
})

// Unsubscribing
sub, err := nc.Subscribe("foo", nil)
sub.Unsubscribe()

// Requests
msg, err := nc.Request("help", []byte("help me"), 10*time.Millisecond)

// Replies
nc.Subscribe("help", func(_, reply string, data []byte, sub *Subscription) {
    nc.PublishMsg(&Msg{Subject:reply, Data:[]byte("I can help!")})
})

// Close connection
nc := nats.Connect("nats://localhost:4222")
nc.Close();

end
```

## Wildcard Subscriptions

```go

// "*" matches any token, at any level of the subject.
nc.Subscribe("foo.*.baz", func(subject, _ string, data []byte, sub *Subscription) {
    fmt.Printf("Msg received on [%s] : %v\n", subject, data);
})

nc.Subscribe("foo.bar.*", func(subject, _ string, data []byte, sub *Subscription) {
    fmt.Printf("Msg received on [%s] : %v\n", subject, data);
})

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.Subscribe("foo.>", func(subject, _ string, data []byte, sub *Subscription) {
    fmt.Printf("Msg received on [%s] : %v\n", subject, data);
})

// Matches all of the above
nc.Publish("foo.bar.baz", []byte("Hello World"))

```

## Queues Groups

```go
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group, queuing semantics.
// You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.QueueSubscribe("foo", "job_workers", func(_, _ string, _ []byte, _ *Subscription) {
  received += 1;
})

```

## Advanced Usage

```go

// Flush connection to server, returns  when all messages have been processed.
nc.Flush()
fmt.Println("All clear!")

// FlushTimeout specifies a timeout value as well.
err := nc.FlushTimeout(1*time.Second)
if err != nil {
    fmt.Println("All clear!")
} else {
    fmt.Println("Flushed timed out!")
}

// Auto-unsubscribe after MAX_WANTED messages received
const MAX_WANTED = 10
sub, err := nc.Subscribe("foo")
sub.AutoUnsubscribe(MAX_WANTED)

// Multiple connections
nc1 := nats.Connect("nats://host1:4222")
nc1 := nats.Connect("nats://host2:4222")

nc1.Subscribe("foo", func(_, _ string, data []byte, _ *Subscription) {
    fmt.Printf("Received a message: %v\n", data)
})

nc2.Publish("foo", []byte("Hello World!"));

```

## License

(The MIT License)

Copyright (c) 2012 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
