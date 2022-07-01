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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNewWithAPIPrefix(t *testing.T) {
	t.Run("import subject from another account", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: test_user
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: main, password: foo} ]
				exports [ { service: "$JS.API.>" },  { service: "foo" }]
			},
			U: {
				users: [ {user: test_user, password: bar} ]
				imports [
					{ service: { subject: "$JS.API.>", account: JS } , to: "main.>" }
					{ service: { subject: "foo", account: JS } }
				]
			},
		}
		`))
		defer os.Remove(conf)
		srv, _ := RunServerWithConfig(conf)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		ncMain, err := nats.Connect(srv.ClientURL(), nats.UserInfo("main", "foo"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer ncMain.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		jsMain, err := New(ncMain)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = jsMain.CreateStream(ctx, nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ncTest, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer ncTest.Close()

		jsTest, err := NewWithAPIPrefix(ncTest, "main")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = jsTest.Publish(ctx, "foo", []byte("msg"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("empty API prefix", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = NewWithAPIPrefix(nc, "")
		if err == nil || err.Error() != "API prefix cannot be empty" {
			t.Fatalf(`Expected error: "API prefix cannot be empty"; got: %v`, err)
		}
	})
}

func TestNewWithDomain(t *testing.T) {
	t.Run("jetstream account with domain", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
			listen: 127.0.0.1:-1
			jetstream: { domain: ABC }
		`))
		defer os.Remove(conf)
		srv, _ := RunServerWithConfig(conf)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("main", "foo"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		js, err := NewWithDomain(nc, "ABC")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		accInfo, err := js.AccountInfo(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if accInfo.Domain != "ABC" {
			t.Errorf("Invalid domain; want %v, got: %v", "ABC", accInfo.Domain)
		}

		_, err = js.CreateStream(ctx, nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.Publish(ctx, "foo", []byte("msg"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("empty domain", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = NewWithDomain(nc, "")
		if err == nil || err.Error() != "domain cannot be empty" {
			t.Fatalf(`Expected error: "domain cannot be empty"; got: %v`, err)
		}
	})
}

func TestWithClientTrace(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var sent, received string
	js, err := New(nc, WithClientTrace(&ClientTrace{
		RequestSent: func(subj string, _ []byte) {
			sent = fmt.Sprintf("Request sent: %s", subj)
		},
		ResponseReceived: func(subj string, _ []byte, _ nats.Header) {
			received = fmt.Sprintf("Response received: %s", subj)
		},
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if sent != "Request sent: $JS.API.STREAM.CREATE.foo" {
		t.Fatalf(`Invalid value on sent request trace; want: "Request sent: $JS.API.STREAM.CREATE.foo"; got: %s`, sent)
	}
	if received != "Response received: $JS.API.STREAM.CREATE.foo" {
		t.Fatalf(`Invalid value on response receive trace; want: "Response received: $JS.API.STREAM.CREATE.foo"; got: %s`, sent)
	}
	fmt.Println(sent)
	fmt.Println(received)
	defer nc.Close()
}

func TestCreateStream(t *testing.T) {
	tests := []struct {
		name      string
		stream    string
		subject   string
		withError error
	}{
		{
			name:    "create stream, ok",
			stream:  "foo",
			subject: "FOO.123",
		},
		{
			name:      "invalid stream name",
			stream:    "foo.123",
			subject:   "FOO.123",
			withError: ErrInvalidStreamName,
		},
		{
			name:      "stream name required",
			stream:    "",
			subject:   "FOO.123",
			withError: ErrStreamNameRequired,
		},
		{
			name:      "stream name already in use",
			stream:    "foo",
			subject:   "BAR.123",
			withError: ErrStreamNameAlreadyInUse,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err = js.CreateStream(ctx, nats.StreamConfig{Name: test.stream, Subjects: []string{test.subject}})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestUpdateStream(t *testing.T) {
	tests := []struct {
		name      string
		stream    string
		subject   string
		withError error
	}{
		{
			name:    "update existing stream",
			stream:  "foo",
			subject: "BAR.123",
		},
		{
			name:      "invalid stream name",
			stream:    "foo.123",
			subject:   "FOO.123",
			withError: ErrInvalidStreamName,
		},
		{
			name:      "stream name required",
			stream:    "",
			subject:   "FOO.123",
			withError: ErrStreamNameRequired,
		},
		{
			name:      "stream not found",
			stream:    "bar",
			subject:   "FOO.123",
			withError: ErrStreamNotFound,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	_, err = js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := js.UpdateStream(ctx, nats.StreamConfig{Name: test.stream, Subjects: []string{test.subject}})
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			info, err := s.Info(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != test.subject {
				t.Fatalf("Invalid stream subjects after update: %v", info.Config.Subjects)
			}
		})
	}
}

func TestStream(t *testing.T) {
	tests := []struct {
		name      string
		stream    string
		subject   string
		withError error
	}{
		{
			name:   "get existing stream",
			stream: "foo",
		},
		{
			name:      "invalid stream name",
			stream:    "foo.123",
			withError: ErrInvalidStreamName,
		},
		{
			name:      "stream name required",
			stream:    "",
			withError: ErrStreamNameRequired,
		},
		{
			name:      "stream not found",
			stream:    "bar",
			withError: ErrStreamNotFound,
		},
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	_, err = js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := js.Stream(ctx, test.stream)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if s.CachedInfo().Config.Name != test.stream {
				t.Fatalf("Invalid stream fetched; want: foo; got: %s", s.CachedInfo().Config.Name)
			}
		})
	}
}

func TestDeleteStream(t *testing.T) {
	tests := []struct {
		name      string
		stream    string
		subject   string
		withError error
	}{
		{
			name:   "delete existing stream",
			stream: "foo",
		},
		{
			name:      "invalid stream name",
			stream:    "foo.123",
			withError: ErrInvalidStreamName,
		},
		{
			name:      "stream name required",
			stream:    "",
			withError: ErrStreamNameRequired,
		},
		{
			name:      "stream not found",
			stream:    "foo",
			withError: ErrStreamNotFound,
		},
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	_, err = js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := js.DeleteStream(ctx, test.stream)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestPurgeStream(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 14; i++ {
		if err := nc.Publish("FOO.123", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("Before purge: %d\n", info.State.Msgs)

	err = s.Purge(ctx, WithSequence(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info, err = s.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("After purge: %d\n", info.State.Msgs)

}

func TestAccountInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js, err := New(nc)
	defer nc.Close()
	_, err = js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err := js.AccountInfo(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("Max memory: %d\n", info.Limits.MaxMemory)
	fmt.Printf("Streams: %d\n", info.Streams)
}

func TestAddConsumer(t *testing.T) {

}

// func TestPublishStream(t *testing.T) {
// 	srv := RunBasicJetStreamServer()
// 	defer shutdownJSServerAndRemoveStorage(t, srv)
// 	nc, err := nats.Connect(srv.ClientURL())
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
// 	js, err := New(nc)
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}
// 	defer nc.Close()

// 	// _, err = js.Stream(ctx, "foo")
// 	// if err == nil {
// 	// 	t.Fatalf("Expected no error: %v", err)
// 	// }

// 	s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.123"}})
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	info, err := s.Info(ctx)
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}
// 	fmt.Println(info.Config.Name)
// 	fmt.Println(info.Config.Description)

// 	info, err = s.Info(ctx)
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}
// 	fmt.Println(info.Config.Name)
// 	fmt.Println(info.Config.Description)

// 	cons, err := s.CreateConsumer(ctx, nats.ConsumerConfig{Durable: "test", AckPolicy: nats.AckExplicitPolicy})
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	for i := 0; i < 14; i++ {
// 		ack, err := js.Publish(ctx, "FOO.123", []byte(fmt.Sprintf("msg %d", i)))
// 		if err != nil {
// 			t.Fatalf("Unexpected error: %v", err)
// 		}
// 		fmt.Printf("Published msg with seq: %d on stream %s\n", ack.Sequence, ack.Stream)
// 	}

// 	time.Sleep(2 * time.Second)

// 	err = cons.Stream(ctx, func(msg JetStreamMsg, err error) {
// 		fmt.Println(string(msg.Data()))
// 	})
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	time.Sleep(2 * time.Second)

// 	for i := 0; i < 14; i++ {
// 		ack, err := js.Publish(ctx, "FOO.123", []byte(fmt.Sprintf("second batch msg %d", i)))
// 		if err != nil {
// 			t.Fatalf("Unexpected error: %v", err)
// 		}
// 		fmt.Printf("Published msg with seq: %d on stream %s\n", ack.Sequence, ack.Stream)
// 	}

// 	time.Sleep(5 * time.Second)
// }

// func BenchmarkGetMetadataFields(b *testing.B) {
// 	sub := "$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>"
// 	for n := 0; n < b.N; n++ {
// 		getMetadataFields(sub)
// 	}
// }

// func TestGetMetadataFields(t *testing.T) {
// 	sub := "$JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>"
// 	res, err := getMetadataFields(sub)
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	expected := []string{"$JS", "ACK", "", "", "<stream>", "<consumer>", "<delivered>", "<sseq>", "<cseq>", "<tm>", "<pending>"}
// 	if !reflect.DeepEqual(res, expected) {
// 		t.Fatalf("\nExpected: %v\nGot:%v", expected, res)
// 	}
// }

// func TestJetStreamPublishAsync(t *testing.T) {
// 	srv := RunBasicJetStreamServer()
// 	defer shutdownJSServerAndRemoveStorage(t, srv)
// 	nc, err := nats.Connect(srv.ClientURL())
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
// 	js, err := New(nc)
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}
// 	defer nc.Close()
// 	// Make sure we get a proper failure when no stream is present.
// 	paf, err := js.PublishAsync(ctx, "foo", []byte("Hello JS"))
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	select {
// 	case <-paf.Ok():
// 		t.Fatalf("Did not expect to get PubAck")
// 	case err := <-paf.Err():
// 		if err != nats.ErrNoResponders {
// 			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
// 		}
// 		// Should be able to get the message back to resend, etc.
// 		m := paf.Msg()
// 		if m == nil {
// 			t.Fatalf("Expected to be able to retrieve the message")
// 		}
// 		if m.Subject != "foo" || string(m.Data) != "Hello JS" {
// 			t.Fatalf("Wrong message: %+v", m)
// 		}
// 	case <-time.After(time.Second):
// 		t.Fatalf("Did not receive an error in time")
// 	}

// 	// Now create a stream and expect a PubAck from <-OK().
// 	if _, err := js.CreateStream(ctx, nats.StreamConfig{Name: "TEST"}); err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	paf, err = js.PublishAsync(ctx, "TEST", []byte("Hello JS ASYNC PUB"))
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	select {
// 	case pa := <-paf.Ok():
// 		if pa.Stream != "TEST" || pa.Sequence != 1 {
// 			t.Fatalf("Bad PubAck: %+v", pa)
// 		}
// 	case err := <-paf.Err():
// 		t.Fatalf("Did not expect to get an error: %v", err)
// 	case <-time.After(time.Second):
// 		t.Fatalf("Did not receive a PubAck in time")
// 	}

// 	errCh := make(chan error, 1)

// 	// Make sure we can register an async err handler for these.
// 	errHandler := func(js JetStream, originalMsg *nats.Msg, err error) {
// 		if originalMsg == nil {
// 			t.Fatalf("Expected non-nil original message")
// 		}
// 		errCh <- err
// 	}
// 	js, err = New(nc, WithPublishAsyncErrHandler(errHandler))
// 	if err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	if _, err = js.PublishAsync(ctx, "bar", []byte("Hello JS ASYNC PUB")); err != nil {
// 		t.Fatalf("Unexpected error: %v", err)
// 	}

// 	select {
// 	case err := <-errCh:
// 		if err != nats.ErrNoResponders {
// 			t.Fatalf("Expected a ErrNoResponders error, got %v", err)
// 		}
// 	case <-time.After(time.Second):
// 		t.Fatalf("Did not receive an async err in time")
// 	}

// 	// // Now test that we can set our window for the JetStream context to limit number of outstanding messages.
// 	// js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
// 	// if err != nil {
// 	// 	t.Fatalf("Unexpected error: %v", err)
// 	// }

// 	// for i := 0; i < 100; i++ {
// 	// 	if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
// 	// 		t.Fatalf("Unexpected error: %v", err)
// 	// 	}
// 	// 	if np := js.PublishAsyncPending(); np > 10 {
// 	// 		t.Fatalf("Expected num pending to not exceed 10, got %d", np)
// 	// 	}
// 	// }

// 	// // Now test that we can wait on all prior outstanding if we want.
// 	// js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
// 	// if err != nil {
// 	// 	t.Fatalf("Unexpected error: %v", err)
// 	// }

// 	// for i := 0; i < 500; i++ {
// 	// 	if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
// 	// 		t.Fatalf("Unexpected error: %v", err)
// 	// 	}
// 	// }

// 	// select {
// 	// case <-js.PublishAsyncComplete():
// 	// case <-time.After(5 * time.Second):
// 	// 	t.Fatalf("Did not receive completion signal")
// 	// }

// 	// // Check invalid options
// 	// _, err = js.PublishAsync("foo", []byte("Bad"), nats.StallWait(0))
// 	// expectedErr := "nats: stall wait should be more than 0"
// 	// if err == nil || err.Error() != expectedErr {
// 	// 	t.Errorf("Expected %v, got: %v", expectedErr, err)
// 	// }

// 	// _, err = js.Publish("foo", []byte("Also bad"), nats.StallWait(200*time.Millisecond))
// 	// expectedErr = "nats: stall wait cannot be set to sync publish"
// 	// if err == nil || err.Error() != expectedErr {
// 	// 	t.Errorf("Expected %v, got: %v", expectedErr, err)
// 	// }
// }

// func BenchmarkNext(b *testing.B) {
// 	srv := RunBasicJetStreamServer()
// 	defer func() {
// 		var sd string
// 		if config := srv.JetStreamConfig(); config != nil {
// 			sd = config.StoreDir
// 		}
// 		srv.Shutdown()
// 		if sd != "" {
// 			if err := os.RemoveAll(sd); err != nil {
// 				b.Fatalf("Unable to remove storage %q: %v", sd, err)
// 			}
// 		}
// 		srv.WaitForShutdown()
// 	}()
// 	nc, err := nats.Connect(srv.ClientURL())
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}

// 	js, err := New(nc)
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	defer nc.Close()
// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
// 	s, err := js.CreateStream(ctx, nats.StreamConfig{
// 		Name:     "TEST",
// 		Subjects: []string{"foo"},
// 	})
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	cons, err := s.CreateConsumer(ctx, nats.ConsumerConfig{
// 		Durable:   "dlc",
// 		AckPolicy: nats.AckExplicitPolicy,
// 	})
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	for n := 0; n < b.N; n++ {
// 		if _, err := js.Publish(ctx, "foo", []byte("test")); err != nil {
// 			b.Fatalf("Unexpected error: %v", err)
// 		}
// 	}

// 	start := time.Now()
// 	for n := 0; n < b.N; n++ {
// 		msg, err := cons.Next(ctx)
// 		if err != nil {
// 			b.Fatalf("Unexpected error: %v", err)
// 		}
// 		msg.Ack()
// 	}
// 	fmt.Printf("Execution time: %s\n Operations: %d\n", time.Since(start).String(), b.N)

// }

// func TestStreamBench(b *testing.T) {
// 	srv := RunBasicJetStreamServer()
// 	defer func() {
// 		var sd string
// 		if config := srv.JetStreamConfig(); config != nil {
// 			sd = config.StoreDir
// 		}
// 		srv.Shutdown()
// 		if sd != "" {
// 			if err := os.RemoveAll(sd); err != nil {
// 				b.Fatalf("Unable to remove storage %q: %v", sd, err)
// 			}
// 		}
// 		srv.WaitForShutdown()
// 	}()
// 	nc, err := nats.Connect(srv.ClientURL())
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}

// 	js, err := New(nc)
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	defer nc.Close()
// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()
// 	s, err := js.CreateStream(ctx, nats.StreamConfig{
// 		Name:     "TEST",
// 		Subjects: []string{"foo"},
// 	})
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	cons, err := s.CreateConsumer(ctx, nats.ConsumerConfig{
// 		Durable:   "dlc",
// 		AckPolicy: nats.AckExplicitPolicy,
// 	})
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	wg := sync.WaitGroup{}
// 	for n := 0; n < 500000; n++ {
// 		if _, err := js.Publish(ctx, "foo", []byte("test")); err != nil {
// 			b.Fatalf("Unexpected error: %v", err)
// 		}
// 		wg.Add(1)
// 	}

// 	start := time.Now()
// 	err = cons.Stream(ctx, func(msg JetStreamMsg, err error) {
// 		// fmt.Println(string(msg.Data()))
// 		msg.Ack()
// 		wg.Done()
// 	}, WithStreamHeartbeat(1*time.Second))
// 	if err != nil {
// 		b.Fatalf("Unexpected error: %v", err)
// 	}
// 	wg.Wait()
// 	fmt.Printf("Execution time: %s\n", time.Since(start).String())

// }
