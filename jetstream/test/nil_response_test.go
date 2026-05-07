// Copyright 2025 The NATS Authors
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

package test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// setupNilResponseMock starts a plain NATS server and sets up two connections:
// one used as a mock that intercepts requests on the given prefix and responds
// with the JSON returned by replyFn, and one used by the caller to build a
// JetStream client aimed at the same prefix.
//
// The returned JetStream instance uses the mock prefix so every API call is
// served by the mock subscriber. cleanup() must be called by the test.
func setupNilResponseMock(
	t *testing.T,
	replyFn func(subject string) []byte,
) (js jetstream.JetStream, cleanup func()) {
	t.Helper()
	srv := RunBasicJetStreamServer()

	// mock server connection – subscribes on "mocknilresp.>" and responds via replyFn
	ncMock, err := nats.Connect(srv.ClientURL())
	if err != nil {
		shutdownJSServerAndRemoveStorage(t, srv)
		t.Fatalf("mock connect: %v", err)
	}
	const prefix = "mocknilresp."
	_, err = ncMock.Subscribe(prefix+">", func(msg *nats.Msg) {
		// strip the prefix so replyFn sees the bare API subject
		bare := strings.TrimPrefix(msg.Subject, prefix)
		msg.Respond(replyFn(bare))
	})
	if err != nil {
		ncMock.Close()
		shutdownJSServerAndRemoveStorage(t, srv)
		t.Fatalf("mock subscribe: %v", err)
	}
	if err := ncMock.Flush(); err != nil {
		ncMock.Close()
		shutdownJSServerAndRemoveStorage(t, srv)
		t.Fatalf("mock flush: %v", err)
	}

	// client connection
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		ncMock.Close()
		shutdownJSServerAndRemoveStorage(t, srv)
		t.Fatalf("client connect: %v", err)
	}
	jsClient, err := jetstream.NewWithAPIPrefix(nc, "mocknilresp")
	if err != nil {
		nc.Close()
		ncMock.Close()
		shutdownJSServerAndRemoveStorage(t, srv)
		t.Fatalf("NewWithAPIPrefix: %v", err)
	}

	cleanup = func() {
		nc.Close()
		ncMock.Close()
		shutdownJSServerAndRemoveStorage(t, srv)
	}
	return jsClient, cleanup
}

// emptyJSON is a valid JSON object with no fields – all pointer fields in the
// response structs (StreamInfo, ConsumerInfo, storedMsg) will remain nil after
// unmarshalling this.
var emptyJSON = []byte(`{}`)

// streamInfoJSON is the minimal valid stream-info payload that a real server
// would include in a successful STREAM.CREATE / STREAM.UPDATE / STREAM.INFO
// response.  It is used by tests that need CreateStream to succeed so they can
// obtain a jetstream.Stream handle before exercising a later call.
var streamInfoJSON = []byte(`{"config":{"name":"test","num_replicas":1},"state":{}}`)

func TestNilResponseCreateStream(t *testing.T) {
	js, cleanup := setupNilResponseMock(t, func(_ string) []byte { return emptyJSON })
	defer cleanup()

	ctx := context.Background()
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"test"}})
	if !errors.Is(err, jetstream.ErrInvalidJetStreamResponse) {
		t.Fatalf("expected ErrInvalidJetStreamResponse; got: %v", err)
	}
}

func TestNilResponseUpdateStream(t *testing.T) {
	js, cleanup := setupNilResponseMock(t, func(_ string) []byte { return emptyJSON })
	defer cleanup()

	ctx := context.Background()
	_, err := js.UpdateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"test"}})
	if !errors.Is(err, jetstream.ErrInvalidJetStreamResponse) {
		t.Fatalf("expected ErrInvalidJetStreamResponse; got: %v", err)
	}
}

func TestNilResponseStream(t *testing.T) {
	js, cleanup := setupNilResponseMock(t, func(_ string) []byte { return emptyJSON })
	defer cleanup()

	ctx := context.Background()
	_, err := js.Stream(ctx, "test")
	if !errors.Is(err, jetstream.ErrInvalidJetStreamResponse) {
		t.Fatalf("expected ErrInvalidJetStreamResponse; got: %v", err)
	}
}

func TestNilResponseStreamInfo(t *testing.T) {
	// The mock returns a valid stream-info payload for the CREATE call so that
	// we can obtain a jetstream.Stream handle, then returns {} for all
	// subsequent calls (the INFO call made by stream.Info).
	called := 0
	js, cleanup := setupNilResponseMock(t, func(subj string) []byte {
		called++
		if strings.HasPrefix(subj, "STREAM.CREATE.") && called == 1 {
			return streamInfoJSON
		}
		return emptyJSON
	})
	defer cleanup()

	ctx := context.Background()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"test"}})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	_, err = s.Info(ctx)
	if !errors.Is(err, jetstream.ErrInvalidJetStreamResponse) {
	js, cleanup := setupNilResponseMock(t, func(subj string) []byte {
		if strings.HasPrefix(subj, "STREAM.CREATE.") {
			return streamInfoJSON
		}
		return emptyJSON
	})
	}
}

func TestNilResponseGetMsg(t *testing.T) {
	// The mock returns a valid stream-info payload for the CREATE call so that
	// we can obtain a jetstream.Stream handle, then returns {} for the MSG.GET
	// call.
	called := 0
	js, cleanup := setupNilResponseMock(t, func(subj string) []byte {
		called++
		if strings.HasPrefix(subj, "STREAM.CREATE.") && called == 1 {
			return streamInfoJSON
		}
		return emptyJSON
	})
	defer cleanup()

	ctx := context.Background()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"test"}})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	_, err = s.GetMsg(ctx, 1)
	if !errors.Is(err, jetstream.ErrInvalidJetStreamResponse) {
		t.Fatalf("expected ErrInvalidJetStreamResponse; got: %v", err)
	}
}
