// Copyright 2012-2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// Shows different ways to create a Conn.
func ExampleConnect() {
	nc, _ := nats.Connect("demo.nats.io")
	nc.Close()

	nc, _ = nats.Connect("nats://derek:secretpassword@demo.nats.io:4222")
	nc.Close()

	nc, _ = nats.Connect("tls://derek:secretpassword@demo.nats.io:4443")
	nc.Close()

	opts := nats.Options{
		AllowReconnect: true,
		MaxReconnect:   10,
		ReconnectWait:  5 * time.Second,
		Timeout:        1 * time.Second,
	}

	nc, _ = opts.Connect()
	nc.Close()
}

// This Example shows an asynchronous subscriber.
func ExampleConn_Subscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
}

// This Example shows a synchronous subscriber.
func ExampleConn_SubscribeSync() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}
}

func ExampleSubscription_NextMsg() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}
}

func ExampleSubscription_Unsubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	// ...
	sub.Unsubscribe()
}

func ExampleConn_Publish() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Publish("foo", []byte("Hello World!"))
}

func ExampleConn_PublishMsg() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	nc.PublishMsg(msg)
}

func ExampleConn_Flush() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	err := nc.Flush()
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}
}

func ExampleConn_FlushTimeout() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	// Only wait for up to 1 second for Flush
	err := nc.FlushTimeout(1 * time.Second)
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}
}

func ExampleConn_Request() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("I will help you"))
	})
	nc.Request("foo", []byte("help"), 50*time.Millisecond)
}

func ExampleConn_QueueSubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received := 0

	nc.QueueSubscribe("foo", "worker_group", func(_ *nats.Msg) {
		received++
	})
}

func ExampleSubscription_AutoUnsubscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received, wanted, total := 0, 10, 100

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {
		received++
	})
	sub.AutoUnsubscribe(wanted)

	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()

	fmt.Printf("Received = %d", received)
}

func ExampleConn_Close() {
	nc, _ := nats.Connect(nats.DefaultURL)
	nc.Close()
}

// Shows how to wrap a Conn into an EncodedConn
func ExampleNewEncodedConn() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	c.Close()
}

// EncodedConn can publish virtually anything just
// by passing it in. The encoder will be used to properly
// encode the raw Go type
func ExampleEncodedConn_Publish() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)
}

// EncodedConn's subscribers will automatically decode the
// wire data into the requested Go type using the Decode()
// method of the registered Encoder. The callback signature
// can also vary to include additional data, such as subject
// and reply subjects.
func ExampleEncodedConn_Subscribe() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	c.Subscribe("hello", func(p *person) {
		fmt.Printf("Received a person! %+v\n", p)
	})

	c.Subscribe("hello", func(subj, reply string, p *person) {
		fmt.Printf("Received a person on subject %s! %+v\n", subj, p)
	})

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)
}

// BindSendChan() allows binding of a Go channel to a nats
// subject for publish operations. The Encoder attached to the
// EncodedConn will be used for marshaling.
func ExampleEncodedConn_BindSendChan() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindSendChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	ch <- me
}

// BindRecvChan() allows binding of a Go channel to a nats
// subject for subscribe operations. The Encoder attached to the
// EncodedConn will be used for un-marshaling.
func ExampleEncodedConn_BindRecvChan() {
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindRecvChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)

	// Receive the publish directly on a channel
	who := <-ch

	fmt.Printf("%v says hello!\n", who)
}

func ExampleJetStream() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Use the JetStream context to produce and consumer messages
	// that have been persisted.
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		log.Fatal(err)
	}

	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	js.Publish("foo", []byte("Hello JS!"))

	// Publish messages asynchronously.
	for i := 0; i < 500; i++ {
		js.PublishAsync("foo", []byte("Hello JS Async!"))
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		fmt.Println("Did not resolve in time")
	}

	// Create async consumer on subject 'foo'. Async subscribers
	// ack a message once exiting the callback.
	js.Subscribe("foo", func(msg *nats.Msg) {
		meta, _ := msg.Metadata()
		fmt.Printf("Stream Sequence  : %v\n", meta.Sequence.Stream)
		fmt.Printf("Consumer Sequence: %v\n", meta.Sequence.Consumer)
	})

	// Async subscriber with manual acks.
	js.Subscribe("foo", func(msg *nats.Msg) {
		msg.Ack()
	}, nats.ManualAck())

	// Async queue subscription where members load balance the
	// received messages together.
	// If no consumer name is specified, either with nats.Bind()
	// or nats.Durable() options, the queue name is used as the
	// durable name (that is, as if you were passing the
	// nats.Durable(<queue group name>) option.
	// It is recommended to use nats.Bind() or nats.Durable()
	// and preferably create the JetStream consumer beforehand
	// (using js.AddConsumer) so that the JS consumer is not
	// deleted on an Unsubscribe() or Drain() when the member
	// that created the consumer goes away first.
	// Check Godoc for the QueueSubscribe() API for more details.
	js.QueueSubscribe("foo", "group", func(msg *nats.Msg) {
		msg.Ack()
	}, nats.ManualAck())

	// Subscriber to consume messages synchronously.
	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsg(2 * time.Second)
	msg.Ack()

	// We can add a member to the group, with this member using
	// the synchronous version of the QueueSubscribe.
	sub, _ = js.QueueSubscribeSync("foo", "group")
	msg, _ = sub.NextMsg(2 * time.Second)
	msg.Ack()

	// ChanSubscribe
	msgCh := make(chan *nats.Msg, 8192)
	sub, _ = js.ChanSubscribe("foo", msgCh)

	select {
	case msg := <-msgCh:
		fmt.Println("[Received]", msg)
	case <-time.After(1 * time.Second):
	}

	// Create Pull based consumer with maximum 128 inflight.
	sub, _ = js.PullSubscribe("foo", "wq", nats.PullMaxWaiting(128))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, _ := sub.Fetch(10, nats.Context(ctx))
		for _, msg := range msgs {
			msg.Ack()
		}
	}
}

// A JetStream context can be configured with a default timeout using nats.MaxWait
// or with a custom API prefix in case of using an imported JetStream from another account.
func ExampleJSOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Use the JetStream context to manage streams and consumers (with nats.APIPrefix JSOpt)
	js, err := nc.JetStream(nats.APIPrefix("dlc"), nats.MaxWait(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	sub, _ := js.SubscribeSync("foo")
	js.Publish("foo", []byte("Hello JS!"))
	sub.NextMsg(2 * time.Second)
}

func ExampleJetStreamManager() {
	nc, _ := nats.Connect("localhost")

	js, _ := nc.JetStream()

	// Create a stream
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
		MaxBytes: 1024,
	})

	// Update a stream
	js.UpdateStream(&nats.StreamConfig{
		Name:     "FOO",
		MaxBytes: 2048,
	})

	// Create a durable consumer
	js.AddConsumer("FOO", &nats.ConsumerConfig{
		Durable: "BAR",
	})

	// Get information about all streams (with Context JSOpt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for info := range js.StreamsInfo(nats.Context(ctx)) {
		fmt.Println("stream name:", info.Config.Name)
	}

	// Get information about all consumers (with MaxWait JSOpt)
	for info := range js.ConsumersInfo("FOO", nats.MaxWait(10*time.Second)) {
		fmt.Println("consumer name:", info.Name)
	}

	// Delete a consumer
	js.DeleteConsumer("FOO", "BAR")

	// Delete a stream
	js.DeleteStream("FOO")
}

// A JetStreamContext is the composition of a JetStream and JetStreamManagement interfaces.
// In case of only requiring publishing/consuming messages, can create a context that
// only uses the JetStream interface.
func ExampleJetStreamContext() {
	nc, _ := nats.Connect("localhost")

	var js nats.JetStream
	var jsm nats.JetStreamManager
	var jsctx nats.JetStreamContext

	// JetStream that can publish/subscribe but cannot manage streams.
	js, _ = nc.JetStream()
	js.Publish("foo", []byte("hello"))

	// JetStream context that can manage streams/consumers but cannot produce messages.
	jsm, _ = nc.JetStream()
	jsm.AddStream(&nats.StreamConfig{Name: "FOO"})

	// JetStream context that can both manage streams/consumers
	// as well as publish/subscribe.
	jsctx, _ = nc.JetStream()
	jsctx.AddStream(&nats.StreamConfig{Name: "BAR"})
	jsctx.Publish("bar", []byte("hello world"))
}

func ExamplePubOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream context to produce/consumer messages that will be persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create stream to persist messages published on 'foo'.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Publish is synchronous by default, and waits for a PubAck response.
	js.Publish("foo", []byte("Hello JS!"))

	// Publish with a custom timeout.
	js.Publish("foo", []byte("Hello JS!"), nats.AckWait(500*time.Millisecond))

	// Publish with a context.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	js.Publish("foo", []byte("Hello JS!"), nats.Context(ctx))

	// Publish and assert the expected stream name.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectStream("FOO"))

	// Publish and assert the last sequence.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectLastSequence(5))

	// Publish and tag the message with an ID.
	js.Publish("foo", []byte("Hello JS!"), nats.MsgId("foo:6"))

	// Publish and assert the last msg ID.
	js.Publish("foo", []byte("Hello JS!"), nats.ExpectLastMsgId("foo:6"))
}

func ExampleSubOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream context to produce/consumer messages that will be persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Auto-ack each individual message.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	})

	// Auto-ack current sequence and all below.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.AckAll())

	// Auto-ack each individual message.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.AckExplicit())

	// Acks are not required.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.AckNone())

	// Manually acknowledge messages.
	js.Subscribe("foo", func(msg *nats.Msg) {
		msg.Ack()
	}, nats.ManualAck())

	// Bind to an existing stream.
	sub, _ := js.SubscribeSync("origin", nats.BindStream("m1"))
	msg, _ := sub.NextMsg(2 * time.Second)
	msg.Ack()

	// Deliver all messages from the beginning.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.DeliverAll())

	// Deliver messages starting from the last one.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.DeliverLast())

	// Deliver only new messages that arrive after subscription.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.DeliverNew())

	// Create durable consumer FOO, if it doesn't exist.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.Durable("FOO"))

	// Create consumer on Foo with flow control and heartbeats.
	js.SubscribeSync("foo",
		// Redeliver after 30s
		nats.AckWait(30*time.Second),
		// Redeliver only once
		nats.MaxDeliver(1),
		// Activate Flow control algorithm from the server.
		nats.EnableFlowControl(),
		// Track heartbeats from the server fro missed sequences.
		nats.IdleHeartbeat(500*time.Millisecond),
	)

	// Set the allowable number of outstanding acks.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.MaxAckPending(5))

	// Set the number of redeliveries for a message.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.MaxDeliver(5))

	// Set the number the max inflight pull requests.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.PullMaxWaiting(5))

	// Set the number the max inflight pull requests.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.PullMaxWaiting(5))

	// Set the rate limit on a push consumer.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.RateLimit(1024))

	// Replay messages at original speed, instead of as fast as possible.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.ReplayOriginal())

	// Start delivering messages at a given sequence.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.StartSequence(10))

	// Start delivering messages at a given time.
	js.Subscribe("foo", func(msg *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(msg.Data))
	}, nats.StartTime(time.Now().Add(-2*time.Hour)))
}

func ExampleMaxWait() {
	nc, _ := nats.Connect("localhost")

	// Set default timeout for JetStream API requests,
	// following requests will inherit this timeout.
	js, _ := nc.JetStream(nats.MaxWait(3 * time.Second))

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	}, nats.MaxWait(2*time.Second))

	sub, _ := js.PullSubscribe("foo", "my-durable-name")

	// Fetch using the default timeout of 3 seconds.
	msgs, _ := sub.Fetch(1)

	// Set custom timeout for a pull batch request.
	msgs, _ = sub.Fetch(1, nats.MaxWait(2*time.Second))

	for _, msg := range msgs {
		msg.Ack()
	}
}

func ExampleAckWait() {
	nc, _ := nats.Connect("localhost")
	js, _ := nc.JetStream()

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Wait for an ack response for 2 seconds.
	js.Publish("foo", []byte("Hello JS!"), nats.AckWait(2*time.Second))

	// Create consumer on 'foo' subject that waits for an ack for 10s,
	// after which the message will be delivered.
	sub, _ := js.SubscribeSync("foo", nats.AckWait(10*time.Second))
	msg, _ := sub.NextMsg(2 * time.Second)

	// Wait for ack of ack for 2s.
	msg.AckSync(nats.AckWait(2 * time.Second))
}

func ExampleMsg_AckSync() {
	nc, _ := nats.Connect("localhost")
	js, _ := nc.JetStream()

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsg(2 * time.Second)

	// Wait for ack of an ack.
	msg.AckSync()
}

// When a message has been delivered by JetStream, it will be possible
// to access some of its metadata such as sequence numbers.
func ExampleMsg_Metadata() {
	nc, _ := nats.Connect("localhost")
	js, _ := nc.JetStream()

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	js.Publish("foo", []byte("hello"))

	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsg(2 * time.Second)

	//
	meta, _ := msg.Metadata()

	// Stream and Consumer sequences.
	fmt.Printf("Stream seq: %s:%d, Consumer seq: %s:%d\n", meta.Stream, meta.Sequence.Stream, meta.Consumer, meta.Sequence.Consumer)
	fmt.Printf("Pending: %d\n", meta.NumPending)
	fmt.Printf("Pending: %d\n", meta.NumDelivered)
}

// AckOpt are the options that can be passed when acknowledge a message.
func ExampleAckOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream context to produce/consumer messages that will be persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create stream to persist messages published on 'foo'.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Publish is synchronous by default, and waits for a PubAck response.
	js.Publish("foo", []byte("Hello JS!"))

	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsg(2 * time.Second)

	// Ack and wait for 2 seconds
	msg.InProgress(nats.AckWait(2))

	// Using a context.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg.Ack(nats.Context(ctx))
}

func ExamplePullOpt() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	// Create JetStream context to produce/consumer messages that will be persisted.
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create stream to persist messages published on 'foo'.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	})

	// Publish is synchronous by default, and waits for a PubAck response.
	js.Publish("foo", []byte("Hello JS!"))

	sub, _ := js.PullSubscribe("foo", "wq")

	// Pull one message,
	msgs, _ := sub.Fetch(1, nats.MaxWait(2*time.Second))
	for _, msg := range msgs {
		msg.Ack()
	}

	// Using a context to timeout waiting for a message.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	msgs, _ = sub.Fetch(1, nats.Context(ctx))
	for _, msg := range msgs {
		msg.Ack()
	}
}

func ExampleContext() {
	nc, err := nats.Connect("localhost")
	if err != nil {
		log.Fatal(err)
	}

	js, _ := nc.JetStream()

	// Base context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// nats.Context option implements context.Context interface, so can be used
	// to create a new context from top level one.
	nctx := nats.Context(ctx)

	// JetStreamManager functions all can use context.
	js.AddStream(&nats.StreamConfig{
		Name:     "FOO",
		Subjects: []string{"foo"},
	}, nctx)

	// Custom context with timeout
	tctx, tcancel := context.WithTimeout(nctx, 2*time.Second)
	defer tcancel()

	// Set a timeout for publishing using context.
	deadlineCtx := nats.Context(tctx)

	js.Publish("foo", []byte("Hello JS!"), deadlineCtx)
	sub, _ := js.SubscribeSync("foo")
	msg, _ := sub.NextMsgWithContext(deadlineCtx)

	// Acks can also use a context to await for a response.
	msg.Ack(deadlineCtx)
}
