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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	// Stream contains CRUD methods on a consumer, as well as operations on an existing stream
	Stream interface {
		streamConsumerManager

		// Info returns stream details
		Info(context.Context, ...StreamInfoOpt) (*StreamInfo, error)
		// CachedInfo returns *StreamInfo cached on a consumer struct
		CachedInfo() *StreamInfo

		// Purge removes messages from a stream
		Purge(context.Context, ...StreamPurgeOpt) error

		// GetMsg retrieves a raw stream message stored in JetStream by sequence number
		GetMsg(context.Context, uint64) (*RawStreamMsg, error)
		// GetLastMsgForSubject retrieves the last raw stream message stored in JetStream by subject
		GetLastMsgForSubject(context.Context, string) (*RawStreamMsg, error)
		// DeleteMsg deletes a message from a stream.
		// The message is marked as erased, but not overwritten
		DeleteMsg(context.Context, uint64) error

		// SecureDeleteMsg deletes a message from a stream. The deleted message is overwritten with random data
		// As a result, this operation is slower than DeleteMsg()
		SecureDeleteMsg(context.Context, uint64) error
	}

	streamConsumerManager interface {
		// CreateConsumer creates a consumer on a given stream with given config
		// This operation is idempotent - if a consumer already exists, it will be a no-op (or error if configs do not match)
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		CreateConsumer(context.Context, ConsumerConfig) (Consumer, error)
		// UpdateConsumer updates an existing consumer
		UpdateConsumer(context.Context, ConsumerConfig) (Consumer, error)
		// Consumer returns a Consumer interface for an existing consumer
		Consumer(context.Context, string) (Consumer, error)
		// DeleteConsumer removes a consumer
		DeleteConsumer(context.Context, string) error
		// ListConsumers returns ConsumerInfoLister enabling iterating over a channel of consumer infos
		ListConsumers(context.Context) ConsumerInfoLister
		// ConsumerNames returns a  ConsumerNameLister enabling iterating over a channel of consumer names
		ConsumerNames(context.Context) ConsumerNameLister
	}
	RawStreamMsg struct {
		Subject  string
		Sequence uint64
		Header   nats.Header
		Data     []byte
		Time     time.Time
	}

	stream struct {
		name      string
		info      *StreamInfo
		jetStream *jetStream
	}

	StreamInfoOpt func(*streamInfoRequest) error

	streamInfoRequest struct {
		DeletedDetails bool   `json:"deleted_details,omitempty"`
		SubjectFilter  string `json:"subjects_filter,omitempty"`
	}

	consumerInfoResponse struct {
		apiResponse
		*ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string          `json:"stream_name"`
		Config *ConsumerConfig `json:"config"`
	}

	StreamPurgeOpt func(*StreamPurgeRequest) error

	StreamPurgeRequest struct {
		// Purge up to but not including sequence.
		Sequence uint64 `json:"seq,omitempty"`
		// Subject to match against messages for the purge command.
		Subject string `json:"filter,omitempty"`
		// Number of messages to keep.
		Keep uint64 `json:"keep,omitempty"`
	}

	streamPurgeResponse struct {
		apiResponse
		Success bool   `json:"success,omitempty"`
		Purged  uint64 `json:"purged"`
	}

	consumerDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	apiMsgGetRequest struct {
		Seq     uint64 `json:"seq,omitempty"`
		LastFor string `json:"last_by_subj,omitempty"`
	}

	// apiMsgGetResponse is the response for a Stream get request.
	apiMsgGetResponse struct {
		apiResponse
		Message *storedMsg `json:"message,omitempty"`
	}

	// storedMsg is a raw message stored in JetStream.
	storedMsg struct {
		Subject  string    `json:"subject"`
		Sequence uint64    `json:"seq"`
		Header   []byte    `json:"hdrs,omitempty"`
		Data     []byte    `json:"data,omitempty"`
		Time     time.Time `json:"time"`
	}

	msgDeleteRequest struct {
		Seq     uint64 `json:"seq"`
		NoErase bool   `json:"no_erase,omitempty"`
	}

	msgDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	ConsumerInfoLister interface {
		Info() <-chan *ConsumerInfo
		Err() <-chan error
	}

	ConsumerNameLister interface {
		Name() <-chan string
		Err() <-chan error
	}

	consumerLister struct {
		js       *jetStream
		offset   int
		pageInfo *apiPaged

		consumers chan *ConsumerInfo
		names     chan string
		errs      chan error
	}

	consumerListResponse struct {
		apiResponse
		apiPaged
		Consumers []*ConsumerInfo `json:"consumers"`
	}

	consumerNamesResponse struct {
		apiResponse
		apiPaged
		Consumers []string `json:"consumers"`
	}
)

func (s *stream) CreateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	if cfg.Durable != "" {
		c, err := s.Consumer(ctx, cfg.Durable)
		if err != nil && !errors.Is(err, ErrConsumerNotFound) {
			return nil, err
		}
		if c != nil {
			if err := compareConsumerConfig(&c.CachedInfo().Config, &cfg); err != nil {
				return nil, fmt.Errorf("%w: %s", ErrConsumerNameAlreadyInUse, cfg.Durable)
			}
			return c, nil
		}
	}
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) UpdateConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	if cfg.Durable == "" {
		return nil, ErrConsumerNameRequired
	}
	_, err := s.Consumer(ctx, cfg.Durable)
	if err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) Consumer(ctx context.Context, name string) (Consumer, error) {
	return getConsumer(ctx, s.jetStream, s.name, name)
}

func (s *stream) DeleteConsumer(ctx context.Context, name string) error {
	return deleteConsumer(ctx, s.jetStream, s.name, name)
}

// Info fetches *StreamInfo from server
//
// Available options:
// WithDeletedDetails() - use to display the information about messages deleted from a stream
// WithSubjectFilter() - use to display the information about messages stored on given subjects
func (s *stream) Info(ctx context.Context, opts ...StreamInfoOpt) (*StreamInfo, error) {
	var infoReq *streamInfoRequest
	for _, opt := range opts {
		if infoReq == nil {
			infoReq = &streamInfoRequest{}
		}
		if err := opt(infoReq); err != nil {
			return nil, err
		}
	}
	var req []byte
	var err error
	if infoReq != nil {
		req, err = json.Marshal(infoReq)
		if err != nil {
			return nil, err
		}
	}

	infoSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamInfoT, s.name))
	var resp streamInfoResponse

	if _, err = s.jetStream.apiRequestJSON(ctx, infoSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}
	s.info = resp.StreamInfo

	return resp.StreamInfo, nil
}

// CachedInfo returns *StreamInfo cached on a stream struct
//
// NOTE: The returned object might not be up to date with the most recent updates on the server
// For up-to-date information, use `Info()`
func (s *stream) CachedInfo() *StreamInfo {
	return s.info
}

// Purge removes messages from a stream
//
// Available options:
// WithPurgeSubject() - can be used set a sprecific subject for which messages on a stream will be purged
// WithPurgeSequence() - can be used to set a sprecific sequence number up to which (but not including) messages will be purged from a stream
// WithPurgeKeep() - can be used to set the number of messages to be kept in the stream after purge.
func (s *stream) Purge(ctx context.Context, opts ...StreamPurgeOpt) error {
	var purgeReq StreamPurgeRequest
	for _, opt := range opts {
		if err := opt(&purgeReq); err != nil {
			return err
		}
	}
	var req []byte
	var err error
	req, err = json.Marshal(purgeReq)
	if err != nil {
		return err
	}

	purgeSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamPurgeT, s.name))

	var resp streamPurgeResponse
	if _, err = s.jetStream.apiRequestJSON(ctx, purgeSubject, &resp, req); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}

	return nil
}

func (s *stream) GetMsg(ctx context.Context, seq uint64) (*RawStreamMsg, error) {
	return s.getMsg(ctx, &apiMsgGetRequest{Seq: seq})
}

func (s *stream) GetLastMsgForSubject(ctx context.Context, subject string) (*RawStreamMsg, error) {
	return s.getMsg(ctx, &apiMsgGetRequest{LastFor: subject})
}

func (s *stream) getMsg(ctx context.Context, mreq *apiMsgGetRequest) (*RawStreamMsg, error) {
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}

	var resp apiMsgGetResponse
	dsSubj := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiMsgGetT, s.name))
	_, err = s.jetStream.apiRequestJSON(ctx, dsSubj, &resp, req)
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeMessageNotFound {
			return nil, ErrMsgNotFound
		}
		return nil, resp.Error
	}

	msg := resp.Message

	var hdr nats.Header
	if len(msg.Header) > 0 {
		hdr, err = nats.DecodeHeadersMsg(msg.Header)
		if err != nil {
			return nil, err
		}
	}

	return &RawStreamMsg{
		Subject:  msg.Subject,
		Sequence: msg.Sequence,
		Header:   hdr,
		Data:     msg.Data,
		Time:     msg.Time,
	}, nil
}

// DeleteMsg deletes a message from a stream.
// The message is marked as erased, but not overwritten
func (s *stream) DeleteMsg(ctx context.Context, seq uint64) error {
	return s.deleteMsg(ctx, &msgDeleteRequest{Seq: seq, NoErase: true})
}

// SecureDeleteMsg deletes a message from a stream. The deleted message is overwritten with random data
// As a result, this operation is slower than DeleteMsg()
func (s *stream) SecureDeleteMsg(ctx context.Context, seq uint64) error {
	return s.deleteMsg(ctx, &msgDeleteRequest{Seq: seq})
}

func (s *stream) deleteMsg(ctx context.Context, req *msgDeleteRequest) error {
	r, err := json.Marshal(req)
	if err != nil {
		return err
	}
	subj := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiMsgDeleteT, s.name))
	var resp msgDeleteResponse
	if _, err = s.jetStream.apiRequestJSON(ctx, subj, &resp, r); err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%w: %s", ErrMsgDeleteUnsuccessful, err)
	}
	return nil
}

// ListConsumers returns ConsumerInfoLister enabling iterating over a channel of consumer infos
func (s *stream) ListConsumers(ctx context.Context) ConsumerInfoLister {
	l := &consumerLister{
		js:        s.jetStream,
		consumers: make(chan *ConsumerInfo),
		errs:      make(chan error, 1),
	}
	go func() {
		defer close(l.consumers)
		for {
			page, err := l.consumerInfos(ctx, s.name)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				l.errs <- err
				return
			}
			for _, info := range page {
				select {
				case l.consumers <- info:
				case <-ctx.Done():
					l.errs <- ctx.Err()
					return
				}
			}
			if errors.Is(err, ErrEndOfData) {
				l.errs <- err
				return
			}
		}
	}()

	return l
}

func (s *consumerLister) Info() <-chan *ConsumerInfo {
	return s.consumers
}

func (s *consumerLister) Err() <-chan error {
	return s.errs
}

// ConsumerNames returns a  ConsumerNameLister enabling iterating over a channel of consumer names
func (s *stream) ConsumerNames(ctx context.Context) ConsumerNameLister {
	l := &consumerLister{
		js:    s.jetStream,
		names: make(chan string),
		errs:  make(chan error, 1),
	}
	go func() {
		defer close(l.names)
		for {
			page, err := l.consumerNames(ctx, s.name)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				l.errs <- err
				return
			}
			for _, info := range page {
				select {
				case l.names <- info:
				case <-ctx.Done():
					l.errs <- ctx.Err()
					return
				}
			}
			if errors.Is(err, ErrEndOfData) {
				l.errs <- err
				return
			}
		}
	}()

	return l
}

func (s *consumerLister) Name() <-chan string {
	return s.names
}

// consumerInfos fetches the next ConsumerInfo page
func (s *consumerLister) consumerInfos(ctx context.Context, stream string) ([]*ConsumerInfo, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req, err := json.Marshal(
		apiPagedRequest{Offset: s.offset},
	)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, fmt.Sprintf(apiConsumerListT, stream))
	var resp consumerListResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, req)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}

	s.pageInfo = &resp.apiPaged
	s.offset += len(resp.Consumers)
	return resp.Consumers, nil
}

// consumerNames fetches the next consumer names page
func (s *consumerLister) consumerNames(ctx context.Context, stream string) ([]string, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req, err := json.Marshal(
		apiPagedRequest{Offset: s.offset},
	)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, fmt.Sprintf(apiConsumerNamesT, stream))
	var resp consumerNamesResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, req)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}

	s.pageInfo = &resp.apiPaged
	s.offset += len(resp.Consumers)
	return resp.Consumers, nil
}
