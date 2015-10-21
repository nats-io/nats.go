# NATS - Go Client
A [Go](http://golang.org) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/nats.svg?branch=master)](http://travis-ci.org/nats-io/nats) [![GoDoc](http://godoc.org/github.com/nats-io/nats?status.png)](http://godoc.org/github.com/nats-io/nats) [![Coverage Status](https://coveralls.io/repos/nats-io/nats/badge.svg?branch=master)](https://coveralls.io/r/nats-io/nats?branch=master)

## Installation

```bash
# Go client
go get github.com/nats-io/nats

# Servers

# gnatsd
go get github.com/nats-io/gnatsd

# nats-server (Ruby)
gem install nats
```

## Basic Encoded Usage

```go

nc, _ := nats.Connect(nats.DefaultURL)
c, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
defer c.Close()

// Simple Publisher
c.Publish("foo", "Hello World")

// Simple Async Subscriber
c.Subscribe("foo", func(s string) {
    fmt.Printf("Received a message: %s\n", s)
})

// EncodedConn can Publish any raw Go type using the registered Encoder
type person struct {
     Name     string
     Address  string
     Age      int
}

// Go type Subscriber
c.Subscribe("hello", func(p *person) {
    fmt.Printf("Received a person: %+v\n", p)
})

me := &person{Name: "derek", Age: 22, Address: "585 Howard Street, San Francisco, CA"}

// Go type Publisher
c.Publish("hello", me)

// Unsubscribing
sub, err := c.Subscribe("foo", nil)
...
sub.Unsubscribe()

// Requests
var response string
err := c.Request("help", "help me", &response, 10*time.Millisecond)
if err != nil {
    fmt.Printf("Request failed: %v\n", err)
}

// Replying
c.Subscribe("help", func(subj, reply string, msg string) {
    c.Publish(reply, "I can help!")
})

// Close connection
c.Close();
```

## Using Go Channels (netchan)

```go
nc, _ := nats.Connect(nats.DefaultURL)
ec, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
defer ec.Close()

type person struct {
     Name     string
     Address  string
     Age      int
}

recvCh := make(chan *person)
ec.BindRecvChan("hello", recvCh)

sendCh := make(chan *person)
ec.BindSendChan("hello", sendCh)

me := &person{Name: "derek", Age: 22, Address: "585 Howard Street"}

// Send via Go channels
sendCh <- me

// Receive via Go channels
who := <- recvCh
```

## Basic Usage

```go

nc, _ := nats.Connect(nats.DefaultURL)

// Simple Publisher
nc.Publish("foo", []byte("Hello World"))

// Simple Async Subscriber
nc.Subscribe("foo", func(m *Msg) {
    fmt.Printf("Received a message: %s\n", string(m.Data))
})

// Simple Sync Subscriber
sub, err := nc.SubscribeSync("foo")
m, err := sub.NextMsg(timeout)

// Unsubscribing
sub, err := nc.Subscribe("foo", nil)
sub.Unsubscribe()

// Requests
msg, err := nc.Request("help", []byte("help me"), 10*time.Millisecond)

// Replies
nc.Subscribe("help", func(m *Msg) {
    nc.Publish(m.Reply, []byte("I can help!"))
})

// Close connection
nc := nats.Connect("nats://localhost:4222")
nc.Close();
```

## Wildcard Subscriptions

```go

// "*" matches any token, at any level of the subject.
nc.Subscribe("foo.*.baz", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

nc.Subscribe("foo.bar.*", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.Subscribe("foo.>", func(m *Msg) {
    fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
})

// Matches all of the above
nc.Publish("foo.bar.baz", []byte("Hello World"))

```

## Queue Groups

```go
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group,
// using queuing semantics. You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.QueueSubscribe("foo", "job_workers", func(_ *Msg) {
  received += 1;
})

```

## Advanced Usage

```go

// Flush connection to server, returns when all messages have been processed.
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
nc2 := nats.Connect("nats://host2:4222")

nc1.Subscribe("foo", func(m *Msg) {
    fmt.Printf("Received a message: %s\n", string(m.Data))
})

nc2.Publish("foo", []byte("Hello World!"));

```

## Clustered Usage

```go

var servers = []string{
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
}

// Setup options to include all servers in the cluster
opts := nats.DefaultOptions
opts.Servers = servers

// Optionally set ReconnectWait and MaxReconnect attempts.
// This example means 10 seconds total per backend.
opts.MaxReconnect = 5
opts.ReconnectWait = (2 * time.Second)

// Optionally disable randomization of the server pool
opts.NoRandomize = true

nc, err := opts.Connect()

// Setup callbacks to be notified on disconnects and reconnects
nc.Opts.DisconnectedCB = func(_ *Conn) {
    fmt.Printf("Got disconnected!\n")
}

// See who we are connected to on reconnect.
nc.Opts.ReconnectedCB = func(nc *Conn) {
    fmt.Printf("Got reconnected to %v!\n", nc.ConnectedUrl())
}

```


## License

(The MIT License)

Copyright (c) 2012-2015 Apcera Inc.

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
