package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestPullConsumerFetch(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		received := make([]Msg, 0)
		var i int
		for msg := range msgs.Messages() {
			if msg == nil {
				if len(testMsgs) != len(received) {
					t.Fatalf("Invalid number of messages received; want: %d; got: %d", len(testMsgs), len(received))
				}
				return
			}
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
			received = append(received, msg)
			i++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})

	t.Run("no options, fetch single messages one by one", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		res := make([]Msg, 0)
		errs := make(chan error)
		done := make(chan struct{})
		go func() {
			for {
				if len(res) == len(testMsgs) {
					close(done)
					return
				}
				msgs, err := c.Fetch(1)
				if err != nil {
					errs <- err
					return
				}

				msg := <-msgs.Messages()
				if msg != nil {
					res = append(res, msg)
				}
				if err := msgs.Error(); err != nil {
					errs <- err
					return
				}
			}
		}()

		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)
		select {
		case err := <-errs:
			t.Fatalf("Unexpected error: %v", err)
		case <-done:
			if len(res) != len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(res))
			}
		}
		for i, msg := range res {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with no wait, no messages at the time of request", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs, err := c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		publishTestMsgs(t, nc)

		msg := <-msgs.Messages()
		if msg != nil {
			t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
		}
	})

	t.Run("with no wait, some messages available", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		time.Sleep(50 * time.Millisecond)
		msgs, err := c.FetchNoWait(10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		publishTestMsgs(t, nc)

		var msgsNum int
		for range msgs.Messages() {
			msgsNum++
		}
		if err != nil {
			t.Fatalf("Unexpected error during fetch: %v", err)
		}

		if msgsNum != len(testMsgs) {
			t.Fatalf("Expected 5 messages, got: %d", msgsNum)
		}
	})

	t.Run("with active streaming", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(_ Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = c.Fetch(5)
		if err == nil || !errors.Is(err, ErrConsumerHasActiveSubscription) {
			t.Fatalf("Expected error: %v; got: %v", ErrConsumerHasActiveSubscription, err)
		}

		_, err = c.FetchNoWait(5)
		if err == nil || !errors.Is(err, ErrConsumerHasActiveSubscription) {
			t.Fatalf("Expected error: %v; got: %v", ErrConsumerHasActiveSubscription, err)
		}
	})

	t.Run("with timeout", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs, err := c.Fetch(5, FetchMaxWait(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msg := <-msgs.Messages()
		if msg != nil {
			t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
		}
	})

	t.Run("with invalid timeout value", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Fetch(5, FetchMaxWait(-50*time.Millisecond))
		if !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", ErrInvalidOption, err)
		}
	})
}

func TestPullConsumerNext_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	name := "cluster"
	stream := StreamConfig{
		Name:     name,
		Replicas: 1,
		Subjects: []string{"FOO.*"},
	}
	t.Run("no options", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			srv := srvs[0]
			nc, err := nats.Connect(srv.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			publishTestMsgs(t, nc)
			msgs, err := c.Fetch(5)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var i int
			for msg := range msgs.Messages() {
				if string(msg.Data()) != testMsgs[i] {
					t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
				}
				i++
			}
			if msgs.Error() != nil {
				t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
			}
		})
	})

	t.Run("with no wait, no messages at the time of request", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			msgs, err := c.FetchNoWait(5)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			publishTestMsgs(t, nc)

			msg := <-msgs.Messages()
			if msg != nil {
				t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
			}
		})
	})
}

func TestPullConsumerMessages(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
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
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()

		// calling Stop() multiple times should have no effect
		it.Stop()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
		_, err = it.Next()
		if err == nil || !errors.Is(err, ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", ErrMsgIteratorClosed, err)
		}
	})

	t.Run("with custom batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages(PullMaxMessages(3))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with max fitting 1 message", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages(PullMaxBytes(60))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// with batch size set to 1, and 5 messages published on subject, there should be a total of 5 requests sent
		if requestsNum < 5 {
			t.Fatalf("Unexpected number of requests sent; want at least 5; got %d", requestsNum)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with custom max bytes", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages(PullMaxBytes(150))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if requestsNum < 3 {
			t.Fatalf("Unexpected number of requests sent; want at least 3; got %d", requestsNum)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with batch size set to 1", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages(PullMaxMessages(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// with batch size set to 1, and 5 messages published on subject, there should be a total of 5 requests sent
		if requestsNum != 5 {
			t.Fatalf("Unexpected number of requests sent; want 5; got %d", requestsNum)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("attempt iteration with active subscription twice on the same consumer", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(msg Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Messages()
		if err == nil || !errors.Is(err, ErrConsumerHasActiveSubscription) {
			t.Fatalf("Expected error: %v; got: %v", ErrConsumerHasActiveSubscription, err)
		}
	})

	t.Run("create iterator, stop, then create again", func(t *testing.T) {
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
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)

		publishTestMsgs(t, nc)
		it, err = c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		expectedMsgs := append(testMsgs, testMsgs...)
		for i, msg := range msgs {
			if string(msg.Data()) != expectedMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with invalid batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Messages(PullMaxMessages(-1))
		if err == nil || !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", ErrInvalidOption, err)
		}
	})

	t.Run("with idle heartbeat", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		// use custom function to bypass validation in test
		it, err := c.Messages(pullOptFunc(func(o *consumeOpts) error {
			o.Heartbeat = 10 * time.Millisecond
			return nil
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		done := make(chan struct{})
		errs := make(chan error)
		publishTestMsgs(t, nc)
		go func() {
			for i := 0; i < 2*len(testMsgs); i++ {
				msg, err := it.Next()
				if err != nil {
					errs <- err
					return
				}
				msg.Ack()
				msgs = append(msgs, msg)
			}
			done <- struct{}{}
		}()
		time.Sleep(10 * time.Millisecond)
		// restart the server
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)

		select {
		case <-done:
			if len(msgs) != 2*len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
			}
		case <-errs:
			t.Fatalf("Unexpected error: %s", err)
		}
	})
}

func TestPullConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
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
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
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
		defer l.Stop()

		publishTestMsgs(t, nc)
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

	t.Run("subscribe twice on the same consumer", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		l, err := c.Consume(func(msg Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		_, err = c.Consume(func(msg Msg) {})
		if err == nil || !errors.Is(err, ErrConsumerHasActiveSubscription) {
			t.Fatalf("Expected error: %v; got: %v", ErrConsumerHasActiveSubscription, err)
		}
	})

	t.Run("subscribe, cancel subscription, then subscribe again", func(t *testing.T) {
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
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(testMsgs))
		msgs := make([]Msg, 0)
		l, err := c.Consume(func(msg Msg) {
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		wg.Wait()
		l.Stop()

		time.Sleep(10 * time.Millisecond)
		wg.Add(len(testMsgs))
		l, err = c.Consume(func(msg Msg) {
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()
		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		expectedMsgs := append(testMsgs, testMsgs...)
		for i, msg := range msgs {
			if string(msg.Data()) != expectedMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with custom batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, PullMaxMessages(4))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, nc)
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

	t.Run("fetch messages one by one", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, PullMaxMessages(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, nc)
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

	t.Run("with custom max bytes", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, PullMaxBytes(150))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		wg.Wait()
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// new request should be sent after each consumed message (msg size is 57)
		if requestsNum < 3 {
			t.Fatalf("Unexpected number of requests sent; want at least 5; got %d", requestsNum)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with invalid batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(_ Msg) {
		}, PullMaxMessages(-1))
		if err == nil || !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", ErrInvalidOption, err)
		}
	})

	t.Run("with custom expiry", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, PullExpiry(2*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, nc)
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

	t.Run("with timeout on pull request, pending messages left", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(4 * len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, pullOptFunc(func(o *consumeOpts) error {
			o.Expires = 50 * time.Millisecond
			return nil
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		time.Sleep(60 * time.Millisecond)

		// publish messages in 4 batches, with pause between each batch
		// on the second pull request, we should get timeout error, which triggers another batch
		publishTestMsgs(t, nc)
		time.Sleep(40 * time.Millisecond)
		publishTestMsgs(t, nc)
		time.Sleep(40 * time.Millisecond)
		publishTestMsgs(t, nc)
		time.Sleep(40 * time.Millisecond)
		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != 4*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("with invalid expiry", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(_ Msg) {
		}, PullExpiry(-1))
		if err == nil || !errors.Is(err, ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", ErrInvalidOption, err)
		}
	})

	t.Run("with idle heartbeat", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, PullMaxBytes(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, nc)
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

	t.Run("with server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(2 * len(testMsgs))
		msgs := make([]Msg, 0)
		publishTestMsgs(t, nc)
		l, err := c.Consume(func(msg Msg) {
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
		publishTestMsgs(t, nc)
		wg.Wait()
	})
}

func TestPullConsumerStream_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	name := "cluster"
	stream := StreamConfig{
		Name:     name,
		Replicas: 1,
		Subjects: []string{"FOO.*"},
	}

	t.Run("no options", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js, err := New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
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
			defer l.Stop()

			publishTestMsgs(t, nc)
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
	})

	t.Run("subscribe, cancel subscription, then subscribe again", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js, err := New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.AddConsumer(ctx, ConsumerConfig{AckPolicy: AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			wg := sync.WaitGroup{}
			wg.Add(len(testMsgs))
			msgs := make([]Msg, 0)
			l, err := c.Consume(func(msg Msg) {
				if err := msg.Ack(); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msgs = append(msgs, msg)
				if len(msgs) == 5 {
					cancel()
				}
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			publishTestMsgs(t, nc)
			wg.Wait()
			l.Stop()

			time.Sleep(10 * time.Millisecond)
			wg.Add(len(testMsgs))
			defer cancel()
			l, err = c.Consume(func(msg Msg) {
				if err := msg.Ack(); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msgs = append(msgs, msg)
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer l.Stop()
			publishTestMsgs(t, nc)
			wg.Wait()
			if len(msgs) != 2*len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
			}
			expectedMsgs := append(testMsgs, testMsgs...)
			for i, msg := range msgs {
				if string(msg.Data()) != expectedMsgs[i] {
					t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
				}
			}
		})
	})
}
