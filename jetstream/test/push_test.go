package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPushConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	t.Run("no options", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msg.Ack()
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, js)
		wg.Wait()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("consumer already consuming", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		l, err := c.Consume(func(msg jetstream.Msg) {
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		_, err = c.Consume(func(msg jetstream.Msg) {})
		if !errors.Is(err, jetstream.ErrConsumerAlreadyConsuming) {
			t.Fatalf("Expected error; got none")
		}
	})

	t.Run("missing heartbeats", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      jetstream.AckExplicitPolicy,
			IdleHeartbeat:  100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		errs := make(chan error, 1)
		l, err := c.Consume(func(msg jetstream.Msg) {},
			jetstream.ConsumeErrHandler(func(cc jetstream.ConsumeContext, err error) {
				errs <- err
			}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()
		time.Sleep(300 * time.Millisecond)
		// delete consumer to simulate missing heartbeats
		if err := s.DeleteConsumer(context.Background(), c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case <-time.After(2 * time.Second):
			t.Fatalf("Expected error; got none")
		case err := <-errs:
			if !errors.Is(err, jetstream.ErrNoHeartbeat) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoHeartbeat, err)
			}
		}
	})

	t.Run("resubscribe", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		publishTestMsgs(t, js)
		wg.Add(len(testMsgs))
		cc, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Wait()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		cc.Stop()
		publishTestMsgs(t, js)
		wg.Add(len(testMsgs))
		time.Sleep(100 * time.Millisecond)
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}

		cc, err = c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer cc.Stop()
		wg.Wait()
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}

	})

	t.Run("with server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			DeliverSubject: nats.NewInbox(),
			AckPolicy:      jetstream.AckExplicitPolicy,
			IdleHeartbeat:  time.Second,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(2 * len(testMsgs))
		msgs := make([]jetstream.Msg, 0)
		publishTestMsgs(t, js)
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()
		time.Sleep(10 * time.Millisecond)
		// restart the server
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, js)
		wg.Wait()
	})

	t.Run("drain", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
			IdleHeartbeat:  time.Second,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg := &sync.WaitGroup{}
		wg.Add(5)
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		cc.Drain()
		wg.Wait()
	})

	t.Run("wait for closed after drain", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]jetstream.Msg, 0)
		lock := sync.Mutex{}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			lock.Lock()
			msgs = append(msgs, msg)
			lock.Unlock()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		closed := cc.Closed()
		time.Sleep(100 * time.Millisecond)

		cc.Drain()

		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count after consume closed; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("wait for closed on already closed consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		cc.Stop()

		time.Sleep(100 * time.Millisecond)

		select {
		case <-cc.Closed():
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}
	})

	t.Run("empty handler", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = c.Consume(nil)
		if !errors.Is(err, jetstream.ErrHandlerRequired) {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("stop and drain idempotent", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc.Stop()
		cc.Stop()
		cc.Drain()
		cc.Drain()
	})
}

func TestPushConsumerConsume_WithQueue(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c1, err := s.CreatePushConsumer(ctx, jetstream.ConsumerConfig{
		DeliverSubject: nats.NewInbox(),
		DeliverGroup:   "workers",
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	c2, err := s.PushConsumer(ctx, c1.CachedInfo().Name)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msgs := make([]jetstream.Msg, 0)
	lock := sync.Mutex{}
	wg := &sync.WaitGroup{}
	l1, err := c1.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		lock.Lock()
		msgs = append(msgs, msg)
		lock.Unlock()
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer l1.Stop()
	l2, err := c2.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		lock.Lock()
		msgs = append(msgs, msg)
		lock.Unlock()
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer l2.Stop()

	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	wg.Add(len(testMsgs))
	for _, msg := range testMsgs {
		if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
			t.Fatalf("Unexpected error during publish: %s", err)
		}
	}

	wg.Wait()
	if len(msgs) != len(testMsgs) {
		t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
	}
}
