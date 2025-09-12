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

func TestBatchPublishLastSequence(t *testing.T) {
	t.Skip("Skipping until expected last sequence is fixed in server")
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

	// publish a message to have a last sequence
	_, err = js.Publish(ctx, "test.foo", []byte("hello"))
	if err != nil {
		t.Fatalf("Unexpected error publishing message: %v", err)
	}

	batch, err := js.BatchPublisher()
	if err != nil {
		t.Fatalf("Unexpected error creating batch publisher: %v", err)
	}

	// Add first message with ExpectLastSequence = 1
	if err := batch.Add("test.1", []byte("message 1"), jetstream.WithBatchExpectLastSequence(1)); err != nil {
		t.Fatalf("Unexpected error adding first message with ExpectLastSequence: %v", err)
	}

	// Add second message without ExpectLastSequence
	if err := batch.Add("test.2", []byte("message 2")); err != nil {
		t.Fatalf("Unexpected error adding second message: %v", err)
	}

	// Commit third message
	ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
	if err != nil {
		t.Fatalf("Unexpected error committing batch: %v", err)
	}

	if ack == nil {
		t.Fatal("Expected non-nil BatchAck")
	}

	// Verify ack contains expected stream
	if ack.Stream != "TEST" {
		t.Fatalf("Expected stream name to be TEST, got %s", ack.Stream)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error getting stream info: %v", err)
	}
	if info.State.Msgs != 4 {
		t.Fatalf("Expected 4 messages in the stream, got %d", info.State.Msgs)
	}
}

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
		batch, err := js.BatchPublisher()
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

	t.Run("with options", func(t *testing.T) {
		t.Skip("Skipping until expected last sequence is fixed in server")
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, js := jsClient(t, s)
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a stream with batch publishing and TTL enabled
		cfg := jetstream.StreamConfig{
			Name:               "TEST",
			Subjects:           []string{"test.>"},
			AllowAtomicPublish: true,
			AllowMsgTTL:        true,
		}
		stream, err := js.CreateStream(ctx, cfg)
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		for range 5 {
			if _, err := js.Publish(ctx, "test.foo", []byte("hello")); err != nil {
				t.Fatalf("Unexpected error publishing message: %v", err)
			}
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 5 {
			t.Fatalf("Expected 5 messages in the stream, got %d", info.State.Msgs)
		}
		time.Sleep(time.Second)

		batch, err := js.BatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add first message with TTL and ExpectLastSequence (allowed on first message)
		if err := batch.Add("test.1", []byte("message 1"), jetstream.WithBatchMsgTTL(5*time.Second), jetstream.WithBatchExpectLastSequence(5)); err != nil {
			t.Fatalf("Unexpected error adding first message with options: %v", err)
		}

		// Add second message with expected stream (no ExpectLastSequence)
		if err := batch.AddMsg(&nats.Msg{
			Subject: "test.2",
			Data:    []byte("message 2"),
		}, jetstream.WithBatchExpectStream("TEST")); err != nil {
			t.Fatalf("Unexpected error adding second message with expected stream: %v", err)
		}

		// Commit third message
		ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch with expected sequence: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		// Verify ack contains expected stream
		if ack.Stream != "TEST" {
			t.Fatalf("Expected stream name to be TEST, got %s", ack.Stream)
		}

		info, err = stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != 8 {
			t.Fatalf("Expected 8 messages in the stream, got %d", info.State.Msgs)
		}
	})

	t.Run("expect last sequence validation", func(t *testing.T) {
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

		batch, err := js.BatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// First message with ExpectLastSequence should work
		if err := batch.Add("test.1", []byte("message 1"), jetstream.WithBatchExpectLastSequence(0)); err != nil {
			t.Fatalf("Unexpected error adding first message with ExpectLastSequence: %v", err)
		}

		// Second message with ExpectLastSequence should fail
		if err := batch.Add("test.2", []byte("message 2"), jetstream.WithBatchExpectLastSequence(1)); err == nil {
			t.Fatal("Expected error when using ExpectLastSequence on non-first message")
		} else if !errors.Is(err, jetstream.ErrBatchExpectLastSequenceNotFirst) {
			t.Fatalf("Expected ErrBatchExpectLastSequenceNotFirst, got %v", err)
		}

		// Second message without ExpectLastSequence should work
		if err := batch.Add("test.2", []byte("message 2")); err != nil {
			t.Fatalf("Unexpected error adding second message: %v", err)
		}

		// Commit with ExpectLastSequence should fail (not first message)
		if _, err := batch.Commit(ctx, "test.3", []byte("message 3"), jetstream.WithBatchExpectLastSequence(2)); err == nil {
			t.Fatal("Expected error when using ExpectLastSequence on commit (non-first message)")
		} else if !errors.Is(err, jetstream.ErrBatchExpectLastSequenceNotFirst) {
			t.Fatalf("Expected ErrBatchExpectLastSequenceNotFirst, got %v", err)
		}

		// Commit without ExpectLastSequence should work
		ack, err := batch.Commit(ctx, "test.3", []byte("message 3"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
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
			batch, err := js.BatchPublisher()
			if err != nil {
				t.Fatalf("Unexpected error creating batch publisher: %v", err)
			}
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}
		// Now create one more batch
		batch, err := js.BatchPublisher()
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
		batch, err := js.BatchPublisher()
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

		batch, err := js.BatchPublisher()
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

		batch, err := js.BatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating batch publisher: %v", err)
		}

		// Add messages until we exceed the max batch size (1000 messages)
		for i := 0; i < 999; i++ {
			err = batch.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}

		// commit is msg 1000 (within limit)
		_, err = batch.Commit(ctx, "test.2", []byte("message 2"))
		if err != nil {
			t.Fatalf("Unexpected error committing batch: %v", err)
		}

		// Try to create another batch and add 1001 messages
		batch2, err := js.BatchPublisher()
		if err != nil {
			t.Fatalf("Unexpected error creating second batch publisher: %v", err)
		}

		for i := 0; i < 1000; i++ {
			err = batch2.Add("test.1", []byte("message 1"))
			if err != nil {
				t.Fatalf("Unexpected error adding message to batch: %v", err)
			}
		}

		// This should be message 1001 and should fail with incomplete error
		_, err = batch2.Commit(ctx, "test.2", []byte("message 2"))
		if !errors.Is(err, jetstream.ErrBatchPublishIncomplete) {
			t.Fatalf("Expected ErrBatchPublishIncomplete, got %v", err)
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

	batch, err := js.BatchPublisher()
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

func TestPublishMsgBatch(t *testing.T) {
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

		count := 100
		messages := make([]*nats.Msg, 0, count)
		for range count {
			messages = append(messages, &nats.Msg{
				Subject: "test.subject",
				Data:    []byte("message"),
				Header:  nats.Header{},
			})
		}

		ack, err := js.PublishMsgBatch(ctx, messages)
		if err != nil {
			t.Fatalf("Unexpected error publishing message batch: %v", err)
		}
		if ack == nil {
			t.Fatal("Expected non-nil BatchAck")
		}

		// verify we have 100 messages in the stream
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error getting stream info: %v", err)
		}
		if info.State.Msgs != uint64(count) {
			t.Fatalf("Expected %d messages in the stream, got %d", count, info.State.Msgs)
		}
		if ack.BatchSize != count {
			t.Fatalf("Expected BatchAck.BatchSize to be %d, got %d", count, ack.BatchSize)
		}
	})
	t.Run("too many messages", func(t *testing.T) {
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

		count := 1001
		messages := make([]*nats.Msg, 0, count)
		for range count {
			messages = append(messages, &nats.Msg{
				Subject: "test.subject",
				Data:    []byte("message"),
				Header:  nats.Header{},
			})
		}

		_, err = js.PublishMsgBatch(ctx, messages)
		if !errors.Is(err, jetstream.ErrBatchPublishIncomplete) {
			t.Fatalf("Expected ErrBatchPublishIncomplete publishing too many messages, got %v", err)
		}
	})
}
