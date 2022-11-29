// Copyright 2020-2022 The NATS Authors
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

package jetstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestMessageDetails(t *testing.T) {
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
	c, err := s.CreateConsumer(ctx, ConsumerConfig{
		Durable:     "cons",
		AckPolicy:   AckExplicitPolicy,
		Description: "test consumer",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.Publish(ctx, "FOO.1", []byte("msg"), WithMsgID("123")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msgs, err := c.Fetch(1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var msg Msg
	select {
	case msg = <-msgs.Messages():
	case err := <-msgs.Error():
		t.Fatalf("unexpected error during fetch: %v", err)
	}
	if string(msg.Data()) != "msg" {
		t.Fatalf("Invalid messge body; want: 'msg'; got: %q", string(msg.Data()))
	}
	metadata, err := msg.Metadata()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if metadata.Consumer != "cons" || metadata.Stream != "foo" {
		t.Fatalf("Invalid message metadata: %v", metadata)
	}
	if val, ok := msg.Headers()["Nats-Msg-Id"]; !ok || val[0] != "123" {
		t.Fatalf("Invalid message headers: %v", msg.Headers())
	}
	if msg.Subject() != "FOO.1" {
		t.Fatalf("Invalid message subject: %q", msg.Subject())
	}
}

func TestMessageMetadata(t *testing.T) {
	tests := []struct {
		name             string
		givenReply       string
		expectedMetadata MsgMetadata
		withError        error
	}{
		{
			name:       "valid metadata",
			givenReply: "$JS.ACK.domain.hash-123.stream.cons.5.10.20.123456789.1.token",
			expectedMetadata: MsgMetadata{
				Sequence: SequencePair{
					Consumer: 20,
					Stream:   10,
				},
				NumDelivered: 5,
				NumPending:   1,
				Timestamp:    time.Unix(0, 123456789),
				Stream:       "stream",
				Consumer:     "cons",
				Domain:       "domain",
			},
		},
		{
			name:       "no reply subject",
			givenReply: "",
			withError:  ErrMsgNoReply,
		},
		{
			name:       "not a JetStream message",
			givenReply: "ABC",
			withError:  ErrNotJSMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := &jetStreamMsg{
				msg: &nats.Msg{
					Reply: test.givenReply,
					Sub:   &nats.Subscription{},
				},
			}
			res, err := msg.Metadata()
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if *res != test.expectedMetadata {
				t.Fatalf("Invalid metadata; want: %v; got: %v", test.expectedMetadata, res)
			}
		})
	}
}

func TestAckVariants(t *testing.T) {
	setup := func(ctx context.Context, t *testing.T) (*server.Server, *nats.Conn, JetStream, Consumer) {
		srv := RunBasicJetStreamServer()

		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, ConsumerConfig{
			Durable:     "cons",
			AckPolicy:   AckExplicitPolicy,
			Description: "test consumer",
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return srv, nc, js, c
	}

	t.Run("standard ack", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.Ack(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != "+ACK" {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
	t.Run("ack twice", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if err := msg.Ack(); err == nil || !errors.Is(err, ErrMsgAlreadyAckd) {
			t.Fatalf("Expected error: %v; got: %v", ErrMsgAlreadyAckd, err)
		}
	})
	t.Run("double ack", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != "+ACK" {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
	t.Run("double ack, empty context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}

		if err := msg.DoubleAck(context.Background()); err == nil || !errors.Is(err, nats.ErrNoDeadlineContext) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrNoDeadlineContext, err)
		}
	})
	t.Run("standard nak", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.Nak(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != "-NAK" {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
	t.Run("nak with delay", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.Nak(WithNakDelay(123 * time.Nanosecond)); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != `-NAK {"delay": 123}` {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
	t.Run("term", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.Term(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != "+TERM" {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
	t.Run("in progress", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv, nc, js, c := setup(ctx, t)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		defer nc.Close()

		if _, err := js.Publish(ctx, "FOO.1", []byte("msg")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := c.Fetch(1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var msg Msg
		select {
		case msg = <-msgs.Messages():
		case err := <-msgs.Error():
			t.Fatalf("unexpected error during fetch: %v", err)
		}
		sub, err := nc.SubscribeSync(msg.Reply())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := msg.InProgress(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ack, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if string(ack.Data) != "+WPI" {
			t.Fatalf("Invalid ack body: %q", string(ack.Data))
		}
	})
}
