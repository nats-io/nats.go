// Copyright 2022-2023 The NATS Authors
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
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type (

	// JetStream contains CRUD methods to operate on a stream
	// Create, update and get operations return 'Stream' interface,
	// allowing operations on consumers
	//
	// CreateOrUpdateConsumer, Consumer and DeleteConsumer are helper methods used to create/fetch/remove consumer without fetching stream (bypassing stream API)
	//
	// Client returns a JetStremClient, used to publish messages on a stream or fetch messages by sequence number
	JetStream interface {
		// Returns *AccountInfo, containing details about the account associated with this JetStream connection
		AccountInfo(ctx context.Context) (*AccountInfo, error)

		StreamConsumerManager
		StreamManager
		Publisher
		KeyValueManager
	}

	Publisher interface {
		// Publish performs a synchronous publish to a stream and waits for ack from server
		// It accepts subject name (which must be bound to a stream) and message data
		Publish(ctx context.Context, subject string, payload []byte, opts ...PublishOpt) (*PubAck, error)
		// PublishMsg performs a synchronous publish to a stream and waits for ack from server
		// It accepts subject name (which must be bound to a stream) and nats.Message
		PublishMsg(ctx context.Context, msg *nats.Msg, opts ...PublishOpt) (*PubAck, error)
		// PublishAsync performs an asynchronous publish to a stream and returns [PubAckFuture] interface
		// It accepts subject name (which must be bound to a stream) and message data
		PublishAsync(subject string, payload []byte, opts ...PublishOpt) (PubAckFuture, error)
		// PublishMsgAsync performs an asynchronous publish to a stream and returns [PubAckFuture] interface
		// It accepts subject name (which must be bound to a stream) and nats.Message
		PublishMsgAsync(msg *nats.Msg, opts ...PublishOpt) (PubAckFuture, error)
		// PublishAsyncPending returns the number of async publishes outstanding for this context
		PublishAsyncPending() int
		// PublishAsyncComplete returns a channel that will be closed when all outstanding messages are ack'd
		PublishAsyncComplete() <-chan struct{}
	}

	StreamManager interface {
		// CreateStream creates a new stream with given config and returns a hook to operate on it
		CreateStream(ctx context.Context, cfg StreamConfig) (Stream, error)
		// UpdateStream updates an existing stream
		UpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error)
		// Stream returns a [Stream] hook for a given stream name
		Stream(ctx context.Context, stream string) (Stream, error)
		// StreamNameBySubject returns a stream name stream listening on given subject
		StreamNameBySubject(ctx context.Context, subject string) (string, error)
		// DeleteStream removes a stream with given name
		DeleteStream(ctx context.Context, stream string) error
		// ListStreams returns StreamInfoLister enabling iterating over a channel of stream infos
		ListStreams(context.Context, ...StreamListOpt) StreamInfoLister
		// StreamNames returns a  StreamNameLister enabling iterating over a channel of stream names
		StreamNames(context.Context, ...StreamListOpt) StreamNameLister
	}

	StreamConsumerManager interface {
		// CreateOrUpdateConsumer creates a consumer on a given stream with given config.
		// If consumer already exists, it will be updated (if possible).
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		CreateOrUpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
		// CreateConsumer creates a consumer on a given stream with given config.
		// If consumer already exists, ErrConsumerExists is returned.
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
		// UpdateConsumer updates an existing consumer.
		// If consumer does not exist, ErrConsumerDoesNotExist is returned.
		// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
		UpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error)
		// OrderedConsumer returns an OrderedConsumer instance.
		// OrderedConsumer allows fetching messages from a stream (just like standard consumer),
		// for in order delivery of messages. Underlying consumer is re-created when necessary,
		// without additional client code.
		OrderedConsumer(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error)
		// Consumer returns a hook to an existing consumer, allowing processing of messages
		Consumer(ctx context.Context, stream string, consumer string) (Consumer, error)
		// DeleteConsumer removes a consumer with given name from a stream
		DeleteConsumer(ctx context.Context, stream string, consumer string) error
	}

	StreamListOpt func(*streamsRequest) error

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
		Err() error
	}

	StreamNameLister interface {
		Name() <-chan string
		Err() error
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
		err     error
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

	streamsRequest struct {
		apiPagedRequest
		Subject string `json:"subject,omitempty"`
	}
)

// defaultAPITimeout is used if context.Background() or context.TODO() is passed to API calls.
const defaultAPITimeout = 5 * time.Second

var subjectRegexp = regexp.MustCompile(`^[^ >]*[>]?$`)

// New returns a new JetStream instance.
//
// Available options:
// [WithClientTrace] - enables request/response tracing.
// [WithPublishAsyncErrHandler] - sets error handler for async message publish.
// [WithPublishAsyncMaxPending] - sets the maximum outstanding async publishes that can be inflight at one time.
// [WithDirectGet] - specifies whether client should use direct get requests.
func New(nc *nats.Conn, opts ...JetStreamOpt) (JetStream, error) {
	jsOpts := jsOpts{
		apiPrefix: DefaultAPIPrefix,
		publisherOpts: asyncPublisherOpts{
			maxpa: defaultAsyncPubAckInflight,
		},
	}
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
// [WithPublishAsyncErrHandler] - sets error handler for async message publish
// [WithPublishAsyncMaxPending] - sets the maximum outstanding async publishes that can be inflight at one time.
// [WithDirectGet] - specifies whether client should use direct get requests.
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
// [WithPublishAsyncErrHandler] - sets error handler for async message publish
// [WithPublishAsyncMaxPending] - sets the maximum outstanding async publishes that can be inflight at one time.
// [WithDirectGet] - specifies whether client should use direct get requests.
func NewWithDomain(nc *nats.Conn, domain string, opts ...JetStreamOpt) (JetStream, error) {
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
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	ncfg := cfg
	// If we have a mirror and an external domain, convert to ext.APIPrefix.
	if ncfg.Mirror != nil && ncfg.Mirror.Domain != "" {
		// Copy so we do not change the caller's version.
		ncfg.Mirror = ncfg.Mirror.copy()
		if err := ncfg.Mirror.convertDomain(); err != nil {
			return nil, err
		}
	}

	// Check sources for the same.
	if len(ncfg.Sources) > 0 {
		ncfg.Sources = append([]*StreamSource(nil), ncfg.Sources...)
		for i, ss := range ncfg.Sources {
			if ss.Domain != "" {
				ncfg.Sources[i] = ss.copy()
				if err := ncfg.Sources[i].convertDomain(); err != nil {
					return nil, err
				}
			}
		}
	}

	req, err := json.Marshal(ncfg)
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

	// check that input subject transform (if used) is reflected in the returned StreamInfo
	if cfg.SubjectTransform != nil && resp.StreamInfo.Config.SubjectTransform == nil {
		return nil, ErrStreamSubjectTransformNotSupported
	}

	if len(cfg.Sources) != 0 {
		if len(cfg.Sources) != len(resp.Sources) {
			return nil, ErrStreamSourceNotSupported
		}
		for i := range cfg.Sources {
			if len(cfg.Sources[i].SubjectTransforms) != 0 && len(resp.Sources[i].SubjectTransforms) == 0 {
				return nil, ErrStreamSourceMultipleFilterSubjectsNotSupported
			}
		}
	}

	return &stream{
		jetStream: js,
		name:      cfg.Name,
		info:      resp.StreamInfo,
	}, nil
}

// If we have a Domain, convert to the appropriate ext.APIPrefix.
// This will change the stream source, so should be a copy passed in.
func (ss *StreamSource) convertDomain() error {
	if ss.Domain == "" {
		return nil
	}
	if ss.External != nil {
		return errors.New("nats: domain and external are both set")
	}
	ss.External = &ExternalStream{APIPrefix: fmt.Sprintf(jsExtDomainT, ss.Domain)}
	return nil
}

// Helper for copying when we do not want to change user's version.
func (ss *StreamSource) copy() *StreamSource {
	nss := *ss
	// Check pointers
	if ss.OptStartTime != nil {
		t := *ss.OptStartTime
		nss.OptStartTime = &t
	}
	if ss.External != nil {
		ext := *ss.External
		nss.External = &ext
	}
	return &nss
}

// UpdateStream updates an existing stream
func (js *jetStream) UpdateStream(ctx context.Context, cfg StreamConfig) (Stream, error) {
	if err := validateStreamName(cfg.Name); err != nil {
		return nil, err
	}
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
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

	// check that input subject transform (if used) is reflected in the returned StreamInfo
	if cfg.SubjectTransform != nil && resp.StreamInfo.Config.SubjectTransform == nil {
		return nil, ErrStreamSubjectTransformNotSupported
	}

	if len(cfg.Sources) != 0 {
		if len(cfg.Sources) != len(resp.Sources) {
			return nil, ErrStreamSourceNotSupported
		}
		for i := range cfg.Sources {
			if len(cfg.Sources[i].SubjectTransforms) != 0 && len(resp.Sources[i].SubjectTransforms) == 0 {
				return nil, ErrStreamSourceMultipleFilterSubjectsNotSupported
			}
		}
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
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
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
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
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

// CreateOrUpdateConsumer creates a consumer on a given stream with given config
// This operation is idempotent - if a consumer already exists, it will be a no-op (or error if configs do not match)
// Consumer interface is returned, serving as a hook to operate on a consumer (e.g. fetch messages)
func (js *jetStream) CreateOrUpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, js, stream, cfg, consumerActionCreateOrUpdate)
}

func (js *jetStream) CreateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, js, stream, cfg, consumerActionCreate)
}

func (js *jetStream) UpdateConsumer(ctx context.Context, stream string, cfg ConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	return upsertConsumer(ctx, js, stream, cfg, consumerActionUpdate)
}

func (js *jetStream) OrderedConsumer(ctx context.Context, stream string, cfg OrderedConsumerConfig) (Consumer, error) {
	if err := validateStreamName(stream); err != nil {
		return nil, err
	}
	oc := &orderedConsumer{
		jetStream:  js,
		cfg:        &cfg,
		stream:     stream,
		namePrefix: nuid.Next(),
		doReset:    make(chan struct{}, 1),
	}
	if cfg.OptStartSeq != 0 {
		oc.cursor.streamSeq = cfg.OptStartSeq - 1
	}
	err := oc.reset()
	if err != nil {
		return nil, err
	}

	return oc, nil
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

func validateSubject(subject string) error {
	if subject == "" {
		return fmt.Errorf("%w: %s", ErrInvalidSubject, "subject cannot be empty")
	}
	if !subjectRegexp.MatchString(subject) {
		return fmt.Errorf("%w: %s", ErrInvalidSubject, subject)
	}
	return nil
}

func (js *jetStream) AccountInfo(ctx context.Context) (*AccountInfo, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
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
//
// Available options:
// [WithStreamListSubject] - allows filtering returned streams by provided subject
func (js *jetStream) ListStreams(ctx context.Context, opts ...StreamListOpt) StreamInfoLister {
	l := &streamLister{
		js:      js,
		streams: make(chan *StreamInfo),
	}
	var streamsReq streamsRequest
	for _, opt := range opts {
		if err := opt(&streamsReq); err != nil {
			l.err = err
			close(l.streams)
			return l
		}
	}
	go func() {
		defer close(l.streams)
		ctx, cancel := wrapContextWithoutDeadline(ctx)
		if cancel != nil {
			defer cancel()
		}
		for {
			page, err := l.streamInfos(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				l.err = err
				return
			}
			for _, info := range page {
				select {
				case l.streams <- info:
				case <-ctx.Done():
					l.err = ctx.Err()
					return
				}
			}
			if errors.Is(err, ErrEndOfData) {
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
func (s *streamLister) Err() error {
	return s.err
}

// StreamNames returns a [StreamNameLister] enabling iterating over a channel of stream names
//
// Available options:
// [WithStreamListSubject] - allows filtering returned streams by provided subject
func (js *jetStream) StreamNames(ctx context.Context, opts ...StreamListOpt) StreamNameLister {
	l := &streamLister{
		js:    js,
		names: make(chan string),
	}
	var streamsReq streamsRequest
	for _, opt := range opts {
		if err := opt(&streamsReq); err != nil {
			l.err = err
			close(l.streams)
			return l
		}
	}
	go func() {
		ctx, cancel := wrapContextWithoutDeadline(ctx)
		if cancel != nil {
			defer cancel()
		}
		defer close(l.names)
		for {
			page, err := l.streamNames(ctx, streamsReq)
			if err != nil && !errors.Is(err, ErrEndOfData) {
				l.err = err
				return
			}
			for _, info := range page {
				select {
				case l.names <- info:
				case <-ctx.Done():
					l.err = ctx.Err()
					return
				}
			}
			if errors.Is(err, ErrEndOfData) {
				return
			}
		}
	}()

	return l
}

func (js *jetStream) StreamNameBySubject(ctx context.Context, subject string) (string, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateSubject(subject); err != nil {
		return "", err
	}
	streamsSubject := apiSubj(js.apiPrefix, apiStreams)

	r := &streamsRequest{Subject: subject}
	req, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	var resp streamNamesResponse
	_, err = js.apiRequestJSON(ctx, streamsSubject, &resp, req)
	if err != nil {
		return "", err
	}
	if resp.Error != nil {
		return "", resp.Error
	}
	if len(resp.Streams) == 0 {
		return "", ErrStreamNotFound
	}

	return resp.Streams[0], nil
}

// Name returns a channel allowing retrieval of stream names returned by [StreamNames]
func (s *streamLister) Name() <-chan string {
	return s.names
}

// infos fetches the next [StreamInfo] page
func (s *streamLister) streamInfos(ctx context.Context, streamsReq streamsRequest) ([]*StreamInfo, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req := streamsRequest{
		apiPagedRequest: apiPagedRequest{
			Offset: s.offset,
		},
		Subject: streamsReq.Subject,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, apiStreamListT)
	var resp streamListResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, reqJSON)
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
func (s *streamLister) streamNames(ctx context.Context, streamsReq streamsRequest) ([]string, error) {
	if s.pageInfo != nil && s.offset >= s.pageInfo.Total {
		return nil, ErrEndOfData
	}

	req := streamsRequest{
		apiPagedRequest: apiPagedRequest{
			Offset: s.offset,
		},
		Subject: streamsReq.Subject,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	slSubj := apiSubj(s.js.apiPrefix, apiStreams)
	var resp streamNamesResponse
	_, err = s.js.apiRequestJSON(ctx, slSubj, &resp, reqJSON)
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

// wrapContextWithoutDeadline wraps context without deadline with default timeout.
// If deadline is already set, it will be returned as is, and cancel() will be nil.
// Caller should check if cancel() is nil before calling it.
func wrapContextWithoutDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, nil
	}
	return context.WithTimeout(ctx, defaultAPITimeout)
}
