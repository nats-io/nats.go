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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestBatchPublisher(t *testing.T) {
	t.Run("basic", func(t *testing.T) {

		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// Create a batch publisher
		batch, err := js.NewBatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add messages to the batch
		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message 1: %v", err)
		}

		if err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		}); err != nil {
			t.Fatalf("Unexpected error adding message 2: %v", err)
		}

		// Check size
		if size := batch.Size(); size != 2 {
			t.Fatalf("Expected batch size to be 2, got %d", size)
		}

		// check stream info to verify no messages yet
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 0 {
			t.Fatalf("Expected 0 messages in the stream, got %d", info.State.Msgs)
		}

		// Commit the batch
		ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		// Verify batch is closed
		if !batch.IsClosed() {
			t.Fatal("Expected batch to be closed after commit")
		}

		// Verify we can't add more messages
		if err := batch.Add("test.4", []byte("message 4")); err == nil {
			t.Fatal("Expected error adding to closed batch")
		}

		// verify we have 3 messages in the stream
		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 3 {
			t.Fatalf("Expected 3 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("too many outstanding batches", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		// create 50 batches (the default max) and add a message to each
		for i := 0; i < 50; i++ {
			batch, err := js.NewBatchPublisher()
			if err != nil {
				t.Fatalf("Unexpected error creating batch publisher: %v", err)
			}
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}
		// Now create one more batch
		batch, err := js.NewBatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}
		err = batch.Add("test.1", []byte("message 1"))
		// adding to batch will not fail, only committing
		if err != nil {
			t.Fatal("Expected error adding message to batch when too many outstanding batches")
		}
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstream.ErrBatchPublishIncomplete) {
			t.Fatal("Expected error committing batch when too many outstanding batches")
		}
	})

	t.Run("batch publish not enabled", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream without batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.>"},
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
		batch, err := js.NewBatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}
		if err := batch.Add("test.1", []byte("message 1")); err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstream.ErrBatchPublishNotEnabled) {
			t.Fatal("Expected ErrBatchPublishNotEnabled committing batch to stream without batch publishing enabled")
		}
	})

	t.Run("invalid headers", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := js.NewBatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Try to add message with MsgID header
		msg := &nats.Msg{
			Subject: "test.1",
			Data:    []byte("message 1"),
			Header:  nats.Header{},
		}
		msg.Header.Set(jetstream.MsgIDHeader, "test-msg-id")

		err = batch.AddMsg(msg)
		if err != nil {
			t.Fatalf("Unexpected error adding message: %v", err)
		}

		// reset headers, should still fail with appropriate error
		msg.Header = nats.Header{}
		_, err = batch.CommitMsg(ctx, msg)
		if !errors.Is(err, jetstream.ErrBatchPublishUnsupportedHeader) {
			t.Fatalf("Expected ErrBatchPublishUnsupportedHeader, got %v", err)
		}
	})

	t.Run("batch too large", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
		}
		_, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		batch, err := js.NewBatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add messages until we exceed the max batch size (1000 messages)
		for i := 0; i < 1000; i++ {
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}

		// commit is msg 1001
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstream.ErrBatchPublishExceedsLimit) {
			t.Fatalf("Expected ErrBatchPublishExceedsLimit, got %v", err)
		}
	})

}

func TestBatchPublisher_Discard(t *testing.T) {

	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)
	nc, js := jsClient(t, s)
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a stream with batch publishing enabled
	cfg := jetstream.StreamConfig{
		Name:               "TEST",
		Subjects:           []string{"test.>"},
		AllowAtomicPublish: true,
	}
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	batch, err := js.NewBatchPublisher()
	if err != nil {
		t.Fatalf("Unexpected error creating batch publisher: %v", err)
	}

	// Add messages to the batch
	if err := batch.Add("test.1", []byte("message 1")); err != nil {
		t.Fatalf("Unexpected error adding message 1: %v", err)
	}

	if err := batch.AddMsg(&nats.Msg{
		Subject: "test.2",
		Data:    []byte("message 2"),
	}); err != nil {
		t.Fatalf("Unexpected error adding message 2: %v", err)
	}

	// Discard the batch
	if err := batch.Discard(); err != nil {
		t.Fatalf("Unexpected error discarding batch: %v", err)
	}

	// try discarding again
	err = batch.Discard()
	if !errors.Is(err, jetstream.ErrBatchClosed) {
		t.Fatalf("Expected ErrBatchClosed discarding already closed batch, got %v", err)
	}

	// Verify batch is closed
	if !batch.IsClosed() {
		t.Fatal("Expected batch to be closed after discard")
	}

	// Verify we can't add more messages
	if err := batch.Add("test.4", []byte("message 4")); err == nil {
		t.Fatal("Expected error adding to closed batch")
	}

	// Verify we can't commit
	_, err = batch.Commit(ctx, "test.5", []byte("message 5"))
	if !errors.Is(err, jetstream.ErrBatchClosed) {
		t.Fatalf("Expected error committing closed batch, got %v", err)
	}

	// verify we have 0 messages in the stream
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error getting stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("Expected 0 messages in the stream, got %d", info.State.Msgs)
	}
}
