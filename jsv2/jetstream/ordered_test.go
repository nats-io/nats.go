package jetstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestOrderedConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msgs := make([]Msg, 0)
	wg := &sync.WaitGroup{}
	wg.Add(len(testMsgs))
	l, err := c.Consume(func(msg Msg) {
		msgs = append(msgs, msg)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	publishTestMsgs(t, nc)
	wg.Wait()

	name := c.CachedInfo().Name
	if err := s.DeleteConsumer(ctx, name); err != nil {
		t.Fatal(err)
	}
	wg.Add(len(testMsgs))
	publishTestMsgs(t, nc)
	wg.Wait()

	l.Stop()
	time.Sleep(10 * time.Millisecond)
	publishTestMsgs(t, nc)
	wg.Add(len(testMsgs))
	l, err = c.Consume(func(msg Msg) {
		msgs = append(msgs, msg)
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer l.Stop()
	wg.Wait()
	if len(msgs) != 3*len(testMsgs) {
		t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
	}
	for i, msg := range msgs {
		if string(msg.Data()) != testMsgs[i%5] {
			t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
		}
	}
}

func TestOrderedConsumerMessages(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msgs := make([]Msg, 0)
	it, err := c.Messages()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	publishTestMsgs(t, nc)
	for i := 0; i < 5; i++ {
		msg, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		msg.Ack()
		msgs = append(msgs, msg)
	}

	name := c.CachedInfo().Name
	if err := s.DeleteConsumer(ctx, name); err != nil {
		t.Fatal(err)
	}
	publishTestMsgs(t, nc)
	for i := 0; i < 5; i++ {
		msg, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		msg.Ack()
		msgs = append(msgs, msg)
	}

	it.Stop()
	time.Sleep(10 * time.Millisecond)
	it, err = c.Messages()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	publishTestMsgs(t, nc)
	for i := 0; i < 5; i++ {
		msg, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		msg.Ack()
		msgs = append(msgs, msg)
	}

	if len(msgs) != 3*len(testMsgs) {
		t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
	}
	for i, msg := range msgs {
		if string(msg.Data()) != testMsgs[i%5] {
			t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
		}
	}
}
