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

package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type (
	// BatchPublisher provides methods for publishing messages to a stream in batches.
	// Messages are published immediately with batch headers, and the batch is committed
	// with the final message which includes a commit header.
	BatchPublisher interface {
		// Add publishes a message to the batch with the given subject and data.
		Add(subject string, data []byte, opts ...BatchMsgOpt) error

		// AddMsg publishes a message to the batch.
		AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error

		// Commit publishes the final message with the given subject and data,
		// and commits the batch. Returns a BatchAck containing the acknowledgment
		// from the server.
		Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error)

		// CommitMsg publishes the final message and commits the batch.
		// Returns a BatchAck containing the acknowledgment from the server.
		CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error)

		// Discard cancels the batch without committing.
		// The server will abandon the batch after a timeout.
		Discard() error

		// Size returns the number of messages added to the batch so far.
		Size() int

		// IsClosed returns true if the batch has been committed or discarded.
		IsClosed() bool
	}

	// BatchAck is the acknowledgment for a batch publish operation.
	BatchAck struct {
		// Stream is the stream name the message was published to.
		Stream string `json:"stream"`

		// Sequence is the stream sequence number of the message.
		Sequence uint64 `json:"seq"`

		// Domain is the domain the message was published to.
		Domain string `json:"domain,omitempty"`

		// Value is the counter value for the stream.
		// This is only set when publishing to a stream with [StreamConfig.AllowMsgCounter] enabled.
		Value string `json:"val,omitempty"`

		// BatchID is the unique identifier for the batch.
		BatchID string `json:"batch_id,omitempty"`

		// BatchSize is the number of messages in the batch.
		BatchSize int `json:"batch_size,omitempty"`
	}

	batchPublisher struct {
		js       *jetStream
		batchID  string
		sequence int
		closed   bool
		mu       sync.Mutex
	}

	batchAckResponse struct {
		apiResponse
		*PubAck
		BatchID   string `json:"batch,omitempty"`
		BatchSize int    `json:"count,omitempty"`
	}

	// BatchMsgOpt is an option for configuring batch message publishing.
	BatchMsgOpt func(*batchMsgOpts) error

	batchMsgOpts struct {
		ttl            time.Duration
		stream         string
		lastSeq        *uint64
		lastSubjectSeq *uint64
		lastSubject    string
	}
)

// BatchPublisher creates a new batch publisher for publishing messages in batches.
func (js *jetStream) BatchPublisher() (BatchPublisher, error) {
	return &batchPublisher{
		js:      js,
		batchID: nuid.Next(),
	}, nil
}

// Add publishes a message to the batch with the given subject and data.
func (b *batchPublisher) Add(subject string, data []byte, opts ...BatchMsgOpt) error {
	return b.AddMsg(&nats.Msg{Subject: subject, Data: data}, opts...)
}

// AddMsg publishes a message to the batch.
func (b *batchPublisher) AddMsg(msg *nats.Msg, opts ...BatchMsgOpt) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBatchClosed
	}

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	// Process batch message options
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}

	// Validate ExpectLastSequence options can only be used on first message
	if b.sequence > 0 && o.lastSeq != nil {
		return ErrBatchExpectLastSequenceNotFirst
	}

	if o.ttl > 0 {
		msg.Header.Set(MsgTTLHeader, o.ttl.String())
	}
	if o.stream != "" {
		msg.Header.Set(ExpectedStreamHeader, o.stream)
	}
	if o.lastSubjectSeq != nil {
		msg.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSubject != "" {
		msg.Header.Set(ExpectedLastSubjSeqSubjHeader, o.lastSubject)
		msg.Header.Set(ExpectedLastSubjSeqHeader, strconv.FormatUint(*o.lastSubjectSeq, 10))
	}
	if o.lastSeq != nil {
		msg.Header.Set(ExpectedLastSeqHeader, strconv.FormatUint(*o.lastSeq, 10))
	}

	b.sequence++
	msg.Header.Set(BatchIDHeader, b.batchID)
	msg.Header.Set(BatchSeqHeader, strconv.FormatUint(uint64(b.sequence), 10))

	return b.js.conn.PublishMsg(msg)
}

// Commit publishes the final message and commits the batch.
func (b *batchPublisher) Commit(ctx context.Context, subject string, data []byte, opts ...BatchMsgOpt) (*BatchAck, error) {
	return b.CommitMsg(ctx, &nats.Msg{Subject: subject, Data: data}, opts...)
}

// CommitMsg publishes the final message and commits the batch.
func (b *batchPublisher) CommitMsg(ctx context.Context, msg *nats.Msg, opts ...BatchMsgOpt) (*BatchAck, error) {
	ctx, cancel := b.js.wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBatchClosed
	}
	// Process batch message options and convert to PublishOpt
	o := batchMsgOpts{}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	// Validate ExpectLastSequence options can only be used on first message
	if b.sequence > 0 && o.lastSeq != nil {
		return nil, ErrBatchExpectLastSequenceNotFirst
	}

	// Convert batch options to publish options for commit
	var pubOpts []PublishOpt
	if o.ttl > 0 {
		pubOpts = append(pubOpts, WithMsgTTL(o.ttl))
	}
	if o.stream != "" {
		pubOpts = append(pubOpts, WithExpectStream(o.stream))
	}
	if o.lastSeq != nil {
		pubOpts = append(pubOpts, WithExpectLastSequence(*o.lastSeq))
	}
	if o.lastSubject != "" && o.lastSubjectSeq != nil {
		pubOpts = append(pubOpts, WithExpectLastSequenceForSubject(*o.lastSubjectSeq, o.lastSubject))
	} else if o.lastSubjectSeq != nil {
		pubOpts = append(pubOpts, WithExpectLastSequencePerSubject(*o.lastSubjectSeq))
	}

	b.sequence++

	if msg.Header == nil {
		msg.Header = nats.Header{}
	}

	msg.Header.Set(BatchIDHeader, b.batchID)
	msg.Header.Set(BatchSeqHeader, strconv.FormatInt(int64(b.sequence), 10))
	msg.Header.Set(BatchCommitHeader, "1")

	resp, err := b.js.publishWithOptions(ctx, msg, pubOpts)
	if err != nil {
		return nil, err
	}

	b.closed = true

	var batchResp batchAckResponse
	if err := json.Unmarshal(resp.Data, &batchResp); err != nil {
		return nil, ErrInvalidJSAck
	}
	if batchResp.Error != nil {
		return nil, batchResp.Error
	}
	if batchResp.PubAck == nil || batchResp.PubAck.Stream == "" ||
		batchResp.BatchID != b.batchID || batchResp.BatchSize != int(b.sequence) {
		return nil, ErrInvalidBatchAck
	}

	return &BatchAck{
		Stream:    batchResp.PubAck.Stream,
		Sequence:  batchResp.PubAck.Sequence,
		Domain:    batchResp.PubAck.Domain,
		Value:     batchResp.PubAck.Value,
		BatchID:   batchResp.BatchID,
		BatchSize: batchResp.BatchSize,
	}, nil
}

// Discard cancels the batch without committing.
// Server will abandon the batch after a timeout.
func (b *batchPublisher) Discard() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBatchClosed
	}

	b.closed = true
	return nil
}

// Size returns the number of messages added to the batch so far.
func (b *batchPublisher) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.sequence)
}

// IsClosed returns true if the batch has been committed or discarded.
func (b *batchPublisher) IsClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// PublishMsgBatch publishes a batch of messages to a Stream and waits for an ack for the commit.
func (js *jetStream) PublishMsgBatch(ctx context.Context, messages []*nats.Msg) (*BatchAck, error) {
	var batchAck *BatchAck
	var err error
	msgs := len(messages)

	ctx, cancel := js.wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	batchID := nuid.Next()

	for i := range messages {
		messages[i].Header.Del(BatchCommitHeader)
		messages[i].Header.Set(BatchIDHeader, batchID)
		messages[i].Header.Set(BatchSeqHeader, strconv.Itoa(i+1))

		if i < msgs-1 {
			err = js.conn.PublishMsg(messages[i])
			if err != nil {
				return nil, fmt.Errorf("publishing message in the batch: %w", err)
			}
			continue
		}

		// Commit the batch on the last message.
		messages[i].Header.Set(BatchCommitHeader, "1")
		resp, err := js.publishWithOptions(ctx, messages[i], nil)
		if err != nil {
			return nil, fmt.Errorf("committing the batch: %w", err)
		}

		var batchResp batchAckResponse
		if err := json.Unmarshal(resp.Data, &batchResp); err != nil {
			return nil, ErrInvalidJSAck
		}
		if batchResp.Error != nil {
			return nil, batchResp.Error
		}
		if batchResp.PubAck == nil || batchResp.PubAck.Stream == "" ||
			batchResp.BatchID != batchID || batchResp.BatchSize != msgs {

			return nil, ErrInvalidBatchAck
		}

		batchAck = &BatchAck{
			Stream:    batchResp.PubAck.Stream,
			Sequence:  batchResp.PubAck.Sequence,
			Domain:    batchResp.PubAck.Domain,
			Value:     batchResp.PubAck.Value,
			BatchID:   batchResp.BatchID,
			BatchSize: batchResp.BatchSize,
		}

	}
	return batchAck, nil
}
