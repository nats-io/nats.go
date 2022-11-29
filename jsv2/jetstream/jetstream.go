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
	"strings"

	"github.com/nats-io/nats.go"
)

type (

	// JetStream contains CRUD methods to operate on a stream
	// Create, update and get operations return 'Stream' interface,
	// allowing operations on consumers
	//
	// CreateConsumer, Consumer and DeleteConsumer are helper methods used to create/fetch/remove consumer without fetching stream (bypassing stream API)
	//
	// Client returns a JetStremClient, used to publish messages on a stream or fetch messages by sequence number
	JetStream interface {
		// Returns *AccountInfo, containing details about the account associated with this JetStream connection
		AccountInfo(ctx context.Context) (*AccountInfo, error)

		StreamConsumerManager
		StreamManager
		Publisher
	}

	Publisher interface {
		// Publish performs a synchronous publish to a stream and waits for ack from server
		// It accepts subject name (which must be bound to a stream) and message data
		Publish(context.Context, string, []byte, ...PublishOpt) (*PubAck, error)
		// PublishMsg performs a synchronous publish to a stream and waits for ack from server
		// It accepts subject name (which must be bound to a stream) and nats.Message
		PublishMsg(context.Context, *nats.Msg, ...PublishOpt) (*PubAck, error)
		// PublishAsync performs a asynchronous publish to a stream and returns [PubAckFuture] interface
		// It accepts subject name (which must be bound to a stream) and message data
		PublishAsync(context.Context, string, []byte, ...PublishOpt) (PubAckFuture, error)
		// PublishMsgAsync performs a asynchronous publish to a stream and returns [PubAckFuture] interface
		// It accepts subject name (which must be bound to a stream) and nats.Message
		PublishMsgAsync(context.Context, *nats.Msg, ...PublishOpt) (PubAckFuture, error)
		// PublishAsyncPending returns the number of async publishes outstanding for this context
		PublishAsyncPending() int
		// PublishAsyncComplete returns a channel that will be closed when all outstanding messages are ack'd
		PublishAsyncComplete() <-chan struct{}
	}

	StreamManager interface {
		// CreateStream creates a new stream with given config and returns a hook to operate on it
		CreateStream(context.Context, StreamConfig) (Stream, error)
		// UpdateStream updates an existing stream
		UpdateStream(context.Context, StreamConfig) (Stream, error)
		// Stream returns a [Stream] hook for a given stream name
		Stream(context.Context, string) (Stream, error)
		// DeleteStream removes a stream with given name
		DeleteStream(context.Context, string) error
		// ListStreams returns StreamInfoLister enabling iterating over a channel of stream infos
		ListStreams(context.Context) StreamInfoLister
		// StreamNames returns a  StreamNameLister enabling iterating over a channel of stream names
		StreamNames(context.Context) StreamNameLister
	}

	StreamConsumerManager interface {
		// CreateConsumer creates a consumer on a given stream with given config
		// This operation is idempotent - if a consumer already exists, it will be a no-op (or error if configs do not match)
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		CreateConsumer(context.Context, string, ConsumerConfig) (Consumer, error)
		// UpdateConsumer updates an existing consumer
		UpdateConsumer(context.Context, string, ConsumerConfig) (Consumer, error)
		// Consumer returns a hook to an existing consumer, allowing processing of messages
		Consumer(context.Context, string, string) (Consumer, error)
		// DeleteConsumer removes a consumer with given name from a stream
		DeleteConsumer(context.Context, string, string) error
	}

	// AccountInfo contains info about the JetStream usage from the current account.
	AccountInfo struct {
		Memory    uint64        `json:"memory"`
		Store     uint64        `json:"storage"`
		Streams   int           `json:"streams"`
		Consumers int           `json:"consumers"`
		Domain    string        `json:"domain"`
		API       APIStats      `json:"api"`
		Limits    AccountLimits `json:"limits"`
	}

	// APIStats reports on API calls to JetStream for this account.
	APIStats struct {
		Total  uint64 `json:"total"`
		Errors uint64 `json:"errors"`
	}

	// AccountLimits includes the JetStream limits of the current account.
	AccountLimits struct {
		MaxMemory    int64 `json:"max_memory"`
		MaxStore     int64 `json:"max_storage"`
		MaxStreams   int   `json:"max_streams"`
		MaxConsumers int   `json:"max_consumers"`
	}

	jetStream struct {
		conn *nats.Conn
		jsOpts

		publisher *jetStreamClient
	}

	JetStreamOpt func(*jsOpts) error

	jsOpts struct {
		publisherOpts asyncPublisherOpts
		apiPrefix     string
		clientTrace   *ClientTrace
	}

	// ClientTrace can be used to trace API interactions for the JetStream Context.
	ClientTrace struct {
		RequestSent      func(subj string, payload []byte)
		ResponseReceived func(subj string, payload []byte, hdr nats.Header)
	}
	streamInfoResponse struct {
		apiResponse
		*StreamInfo
	}

	accountInfoResponse struct {
		apiResponse
		AccountInfo
	}

	streamDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	StreamInfoLister interface {
		Info() <-chan *StreamInfo
		Err() <-chan error
	}

	StreamNameLister interface {
		Name() <-chan string
		Err() <-chan error
	}

	apiPagedRequest struct {
		Offset int `json:"offset"`
	}
	streamLister struct {
		js       *jetStream
		offset   int
		pageInfo *apiPaged

		streams chan *StreamInfo
		names   chan string
		errs    chan error
	}

	streamListResponse struct {
		apiResponse
		apiPaged
		Streams []*StreamInfo `json:"streams"`
	}

	streamNamesResponse struct {
		apiResponse
		apiPaged
		Streams []string `json:"streams"`
	}
)

// New returns a enw JetStream instance
//
// Available options:
// [WithClientTrace] - enables request/response tracing
func New(nc *nats.Conn, opts ...JetStreamOpt) (JetStream, error) {
	jsOpts := jsOpts{apiPrefix: DefaultAPIPrefix}
	for _, opt := range opts {
		if err := opt(&jsOpts); err != nil {
			return nil, err
		}
	}
	js := &jetStream{
		conn:      nc,
		jsOpts:    jsOpts,
		publisher: &jetStreamClient{asyncPublisherOpts: jsOpts.publisherOpts},
	}

	return js, nil
}

const (
	// defaultAsyncPubAckInflight is the number of async pub acks inflight.
	defaultAsyncPubAckInflight = 4000
)

// NewWithAPIPrefix returns a new JetStream instance and sets the API prefix to be used in requests to JetStream API
//
// Available options:
// [WithClientTrace] - enables request/response tracing
func NewWithAPIPrefix(nc *nats.Conn, apiPrefix string, opts ...JetStreamOpt) (JetStream, error) {
	jsOpts := jsOpts{
		publisherOpts: asyncPublisherOpts{
			maxpa: defaultAsyncPubAckInflight,
		},
	}
	for _, opt := range opts {
		if err := opt(&jsOpts); err != nil {
			return nil, err
		}
	}
	if apiPrefix == "" {
		return nil, fmt.Errorf("API prefix cannot be empty")
	}
	if !strings.HasSuffix(apiPrefix, ".") {
		jsOpts.apiPrefix = fmt.Sprintf("%s.", apiPrefix)
	}
	js := &jetStream{
		conn:      nc,
		jsOpts:    jsOpts,
		publisher: &jetStreamClient{asyncPublisherOpts: jsOpts.publisherOpts},
	}
	return js, nil
}

// NewWithDomain returns a new JetStream instance and sets the domain name token used when sending JetStream requests
//
// Available options:
// [WithClientTrace] - enables request/response tracing
func NewWithDomain(nc *nats.Conn, domain string, opts ...JetStreamOpt) (JetStream, error) {
	var jsOpts jsOpts
	for _, opt := range opts {
		if err := opt(&jsOpts); err != nil {
			return nil, err
		}
	}
	if domain == "" {
		return nil, fmt.Errorf("domain cannot be empty")
	}
	jsOpts.apiPrefix = fmt.Sprintf(jsDomainT, domain)
	js := &jetStream{
		conn:      nc,
		jsOpts:    jsOpts,
		publisher: &jetStreamClient{asyncPublisherOpts: jsOpts.publisherOpts},
	}
	return js, nil
}

// CreateStream creates a new stream with given config and returns a hook to operate on it
func (js *jetStream) CreateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	if err := validateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	createSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamCreateT, cfg.Name))
	var resp streamInfoResponse

	if _, err = js.apiRequestJSON(ctx, createSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNameInUse {
			return nil, ErrStreamNameAlreadyInUse
		}
		return nil, resp.Error
	}

	return &stream{
		jetStream: js,
		name:      cfg.Name,
		info:      resp.StreamInfo,
	}, nil
}

// UpdateStream updates an existing stream
func (js *jetStream) UpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	if err := validateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	updateSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamUpdateT, cfg.Name))
	var resp streamInfoResponse

	if _, err = js.apiRequestJSON(ctx, updateSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return &stream{
		jetStream: js,
		name:      cfg.Name,
		info:      resp.StreamInfo,
	}, nil
}

// Stream returns a [Stream] hook for a given stream name
func (js *jetStream) Stream(ctx context.Context, name string) (Stream, error) {
	if err := validateStreamName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamInfoT, name))

	var resp streamInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}
	return &stream{
		jetStream: js,
		name:      name,
		info:      resp.StreamInfo,
	}, nil
}

// DeleteStream removes a stream with given name
func (js *jetStream) DeleteStream(ctx context.Context, name string) error {
	if err := validateStreamName(name); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiStreamDeleteT, name))
	var resp streamDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return ErrStreamNotFound
		}
		return resp.Error
	}
	return nil
}

// CreateConsumer creates a consumer on a given stream with given config
// This operation is idempotent - if a consumer already exists, it will be a no-op (or error if configs do not match)
// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
func (js *jetStream) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	if cfg.Durable != "" {
		c, err := js.Consumer(ctx, stream, cfg.Durable)
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
	return upsertConsumer(ctx, js, stream, cfg)
}

// UpdateConsumer updates an existing consumer
func (js *jetStream) UpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	if cfg.Durable == "" {
		return nil, ErrConsumerNameRequired
	}
	_, err := js.Consumer(ctx, stream, cfg.Durable)
	if err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, js, stream, cfg)
}

// Consumer returns a hook to an existing consumer, allowing processing of messages
func (js *jetStream) Consumer(ctx context.Context, stream string, name string) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return getConsumer(ctx, js, stream, name)
}

// DeleteConsumer removes a consumer with given name from a stream
func (js *jetStream) DeleteConsumer(ctx context.Context, stream string, name string) error {
	if err := validateStreamName(stream); err != nil {
		return err
	}
	return deleteConsumer(ctx, js, stream, name)
}

func validateStreamName(stream string) error {
	if stream == "" {
		return ErrStreamNameRequired
	}
	if strings.Contains(stream, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidStreamName, stream)
	}
	return nil
}

func (js *jetStream) AccountInfo(ctx context.Context) (*AccountInfo, error) {
	var resp accountInfoResponse

	infoSubject := apiSubj(js.apiPrefix, apiAccountInfo)
	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		if errors.Is(err, nats.ErrNoResponders) {
			return nil, ErrJetStreamNotEnabled
		}
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeJetStreamNotEnabledForAccount {
			return nil, ErrJetStreamNotEnabledForAccount
		}
		if resp.Error.ErrorCode == JSErrCodeJetStreamNotEnabled {
			return nil, ErrJetStreamNotEnabled
		}
		return nil, resp.Error
	}

	return &resp.AccountInfo, nil
}

// ListStreams returns StreamInfoLister enabling iterating over a channel of stream infos
func (js *jetStream) ListStreams(ctx context.Context) StreamInfoLister {
	l := &streamLister{
		js:      js,
		streams: make(chan *StreamInfo),
		errs:    make(chan error, 1),
	}
	go func() {
		defer close(l.streams)
		for {
			page, err := l.streamInfos(ctx)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				l.errs <- err
				return
			}
			for _, info := range page {
				select {
				case l.streams <- info:
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

// Info returns a channel allowing retrieval of stream infos returned by [ListStreams]
func (s *streamLister) Info() <-chan *StreamInfo {
	return s.streams
}

// Err returns an error channel which will be populated with error from [ListStreams] or [StreamNames] request
func (s *streamLister) Err() <-chan error {
	return s.errs
}

// StreamNames returns a [StreamNameLister] enabling iterating over a channel of stream names
func (js *jetStream) StreamNames(ctx context.Context) StreamNameLister {
	l := &streamLister{
		js:    js,
		names: make(chan string),
		errs:  make(chan error, 1),
	}
	go func() {
		defer close(l.names)
		for {
			page, err := l.streamNames(ctx)
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

// Name returns a channel allowing retrieval of stream names returned by [StreamNames]
func (s *streamLister) Name() <-chan string {
	return s.names
}

// infos fetches the next [StreamInfo] page
func (s *streamLister) streamInfos(ctx context.Context) ([]*StreamInfo, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req, err := json.Marshal(
		apiPagedRequest{Offset: s.offset},
	)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, apiStreamListT)
	var resp streamListResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, req)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}

	s.pageInfo = &resp.apiPaged
	s.offset += len(resp.Streams)
	return resp.Streams, nil
}

// streamNames fetches the next stream names page
func (s *streamLister) streamNames(ctx context.Context) ([]string, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req, err := json.Marshal(
		apiPagedRequest{Offset: s.offset},
	)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, apiStreams)
	var resp streamNamesResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, req)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}

	s.pageInfo = &resp.apiPaged
	s.offset += len(resp.Streams)
	return resp.Streams, nil
}
