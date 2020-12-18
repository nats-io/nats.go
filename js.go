// Copyright 2020 The NATS Authors
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

package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// JetStream is the public interface for JetStream.
type JetStream interface {
	// Publishing messages to JetStream.
	Publish(subj string, data []byte, opts ...PubOpt) (*PubAck, error)
	PublishMsg(m *Msg, opts ...PubOpt) (*PubAck, error)

	// Subscribing to messages in JetStream.
	Subscribe(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)
	SubscribeSync(subj string, opts ...SubOpt) (*Subscription, error)

	// Channel versions.
	ChanSubscribe(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error)
	// QueueSubscribe.
	QueueSubscribe(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error)
}

// JetStreamManager is the public interface for managing JetStream streams & consumers.
type JetStreamManager interface {
	// Create a stream.
	AddStream(cfg *StreamConfig) (*StreamInfo, error)
	// Create a consumer.
	AddConsumer(stream string, cfg *ConsumerConfig) (*ConsumerInfo, error)
	// Stream information.
	StreamInfo(stream string) (*StreamInfo, error)
}

// JetStream is the public interface for the JetStream context.
type JetStreamContext interface {
	JetStream
	JetStreamManager
}

// APIError is included in all API responses if there was an error.
type APIError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// APIResponse is a standard response from the JetStream JSON API
type APIResponse struct {
	Type  string    `json:"type"`
	Error *APIError `json:"error,omitempty"`
}

type AccountInfoResponse struct {
	APIResponse
	*AccountStats
}

// AccountLimits is for the information about
type AccountLimits struct {
	MaxMemory    int64 `json:"max_memory"`
	MaxStore     int64 `json:"max_storage"`
	MaxStreams   int   `json:"max_streams"`
	MaxConsumers int   `json:"max_consumers"`
}

// AccountStats returns current statistics about the account's JetStream usage.
type AccountStats struct {
	Memory  uint64        `json:"memory"`
	Store   uint64        `json:"storage"`
	Streams int           `json:"streams"`
	Limits  AccountLimits `json:"limits"`
}

// Internal struct for jetstream
type js struct {
	nc *Conn
	// For importing JetStream from other accounts.
	pre string
	// Amount of time to wait for API requests.
	wait time.Duration
	// Signals only direct access and no API access.
	direct bool
}

// Request API subjects for JetStream.
const (
	JSDefaultAPIPrefix = "$JS.API."
	// JSApiAccountInfo is for obtaining general information about JetStream.
	JSApiAccountInfo = "INFO"
	// JSApiStreams can lookup a stream by subject.
	JSApiStreams = "STREAM.NAMES"
	// JSApiConsumerCreateT is used to create consumers.
	JSApiConsumerCreateT = "CONSUMER.CREATE.%s"
	// JSApiDurableCreateT is used to create durable consumers.
	JSApiDurableCreateT = "CONSUMER.DURABLE.CREATE.%s.%s"
	// JSApiConsumerInfoT is used to create consumers.
	JSApiConsumerInfoT = "CONSUMER.INFO.%s.%s"
	// JSApiRequestNextT is the prefix for the request next message(s) for a consumer in worker/pull mode.
	JSApiRequestNextT = "CONSUMER.MSG.NEXT.%s.%s"
	// JSApiStreamCreateT is the endpoint to create new streams.
	JSApiStreamCreateT = "STREAM.CREATE.%s"
	// JSApiStreamInfoT is the endpoint to get information on a stream.
	JSApiStreamInfoT = "STREAM.INFO.%s"
)

// JetStream returns a JetStream context for pub/sub interactions.
func (nc *Conn) JetStream(opts ...JSOpt) (JetStreamContext, error) {
	const defaultRequestWait = 5 * time.Second

	js := &js{nc: nc, pre: JSDefaultAPIPrefix, wait: defaultRequestWait}

	for _, opt := range opts {
		if err := opt.configureJSContext(js); err != nil {
			return nil, err
		}
	}

	if js.direct {
		return js, nil
	}

	resp, err := nc.Request(js.apiSubj(JSApiAccountInfo), nil, js.wait)
	if err != nil {
		if err == ErrNoResponders {
			err = ErrJetStreamNotEnabled
		}
		return nil, err
	}
	var info AccountInfoResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		return nil, err
	}
	if info.Error != nil && info.Error.Code == 503 {
		return nil, ErrJetStreamNotEnabled
	}
	return js, nil
}

// JSOpt configures a JetStream context.
type JSOpt interface {
	configureJSContext(opts *js) error
}

// jsOptFn configures an option for the JetStream context.
type jsOptFn func(opts *js) error

func (opt jsOptFn) configureJSContext(opts *js) error {
	return opt(opts)
}

func APIPrefix(pre string) JSOpt {
	return jsOptFn(func(js *js) error {
		js.pre = pre
		if !strings.HasSuffix(js.pre, ".") {
			js.pre = js.pre + "."
		}
		return nil
	})
}

func DirectOnly() JSOpt {
	return jsOptFn(func(js *js) error {
		js.direct = true
		return nil
	})
}

func (js *js) apiSubj(subj string) string {
	if js.pre == _EMPTY_ {
		return subj
	}
	var b strings.Builder
	b.WriteString(js.pre)
	b.WriteString(subj)
	return b.String()
}

// PubOpt configures options for publishing JetStream messages.
type PubOpt interface {
	configurePublish(opts *pubOpts) error
}

// pubOptFn is a function option used to configure JetStream Publish.
type pubOptFn func(opts *pubOpts) error

func (opt pubOptFn) configurePublish(opts *pubOpts) error {
	return opt(opts)
}

type pubOpts struct {
	ctx context.Context
	ttl time.Duration
	id  string
	lid string // Expected last msgId
	str string // Expected stream name
	seq uint64 // Expected last sequence
}

type PubAckResponse struct {
	APIResponse
	*PubAck
}

type PubAck struct {
	Stream    string `json:"stream"`
	Sequence  uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// Headers for published messages.
const (
	MsgIdHdr             = "Nats-Msg-Id"
	ExpectedStreamHdr    = "Nats-Expected-Stream"
	ExpectedLastSeqHdr   = "Nats-Expected-Last-Sequence"
	ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id"
)

func (js *js) PublishMsg(m *Msg, opts ...PubOpt) (*PubAck, error) {
	var o pubOpts
	if len(opts) > 0 {
		if m.Header == nil {
			m.Header = http.Header{}
		}
		for _, opt := range opts {
			if err := opt.configurePublish(&o); err != nil {
				return nil, err
			}
		}
	}
	// Check for option collisions. Right now just timeout and context.
	if o.ctx != nil && o.ttl != 0 {
		return nil, ErrContextAndTimeout
	}
	if o.ttl == 0 && o.ctx == nil {
		o.ttl = js.wait
	}

	if o.id != _EMPTY_ {
		m.Header.Set(MsgIdHdr, o.id)
	}
	if o.lid != _EMPTY_ {
		m.Header.Set(ExpectedLastMsgIdHdr, o.lid)
	}
	if o.str != _EMPTY_ {
		m.Header.Set(ExpectedStreamHdr, o.str)
	}
	if o.seq > 0 {
		m.Header.Set(ExpectedLastSeqHdr, strconv.FormatUint(o.seq, 10))
	}

	var resp *Msg
	var err error

	if o.ttl > 0 {
		resp, err = js.nc.RequestMsg(m, time.Duration(o.ttl))
	} else {
		resp, err = js.nc.RequestMsgWithContext(o.ctx, m)
	}

	if err != nil {
		if err == ErrNoResponders {
			err = ErrNoStreamResponse
		}
		return nil, err
	}
	var pa PubAckResponse
	if err := json.Unmarshal(resp.Data, &pa); err != nil {
		return nil, ErrInvalidJSAck
	}
	if pa.Error != nil {
		return nil, errors.New(pa.Error.Description)
	}
	if pa.PubAck == nil || pa.PubAck.Stream == _EMPTY_ {
		return nil, ErrInvalidJSAck
	}
	return pa.PubAck, nil
}

func (js *js) Publish(subj string, data []byte, opts ...PubOpt) (*PubAck, error) {
	return js.PublishMsg(&Msg{Subject: subj, Data: data}, opts...)
}

// Options for publishing to JetStream.

// MsgId sets the message ID used for de-duplication.
func MsgId(id string) PubOpt {
	return pubOptFn(func(opts *pubOpts) error {
		opts.id = id
		return nil
	})
}

// ExpectStream sets the expected stream to respond from the publish.
func ExpectStream(stream string) PubOpt {
	return pubOptFn(func(opts *pubOpts) error {
		opts.str = stream
		return nil
	})
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastSequence(seq uint64) PubOpt {
	return pubOptFn(func(opts *pubOpts) error {
		opts.seq = seq
		return nil
	})
}

// ExpectLastSequence sets the expected sequence in the response from the publish.
func ExpectLastMsgId(id string) PubOpt {
	return pubOptFn(func(opts *pubOpts) error {
		opts.lid = id
		return nil
	})
}

// MaxWait sets the maximum amount of time we will wait for a response.
type MaxWait time.Duration

func (ttl MaxWait) configurePublish(opts *pubOpts) error {
	opts.ttl = time.Duration(ttl)
	return nil
}

func (ttl MaxWait) configureJSContext(js *js) error {
	js.wait = time.Duration(ttl)
	return nil
}

// ContextOpt is an option used to set a context.Context.
type ContextOpt struct {
	context.Context
}

func (ctx ContextOpt) configurePublish(opts *pubOpts) error {
	opts.ctx = ctx
	return nil
}

// Context returns an option that can be used to configure a context.
func Context(ctx context.Context) ContextOpt {
	return ContextOpt{ctx}
}

// Subscribe
// We will match subjects to streams and consumers on the user's behalf.

type JSApiCreateConsumerRequest struct {
	Stream string          `json:"stream_name"`
	Config *ConsumerConfig `json:"config"`
}

type ConsumerConfig struct {
	Durable         string        `json:"durable_name,omitempty"`
	DeliverSubject  string        `json:"deliver_subject,omitempty"`
	DeliverPolicy   DeliverPolicy `json:"deliver_policy"`
	OptStartSeq     uint64        `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time    `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy     `json:"ack_policy"`
	AckWait         time.Duration `json:"ack_wait,omitempty"`
	MaxDeliver      int           `json:"max_deliver,omitempty"`
	FilterSubject   string        `json:"filter_subject,omitempty"`
	ReplayPolicy    ReplayPolicy  `json:"replay_policy"`
	RateLimit       uint64        `json:"rate_limit_bps,omitempty"` // Bits per sec
	SampleFrequency string        `json:"sample_freq,omitempty"`
	MaxWaiting      int           `json:"max_waiting,omitempty"`
	MaxAckPending   int           `json:"max_ack_pending,omitempty"`
}

type JSApiConsumerResponse struct {
	APIResponse
	*ConsumerInfo
}

type ConsumerInfo struct {
	Stream         string         `json:"stream_name"`
	Name           string         `json:"name"`
	Created        time.Time      `json:"created"`
	Config         ConsumerConfig `json:"config"`
	Delivered      SequencePair   `json:"delivered"`
	AckFloor       SequencePair   `json:"ack_floor"`
	NumAckPending  int            `json:"num_ack_pending"`
	NumRedelivered int            `json:"num_redelivered"`
	NumWaiting     int            `json:"num_waiting"`
	NumPending     uint64         `json:"num_pending"`
}

type SequencePair struct {
	Consumer uint64 `json:"consumer_seq"`
	Stream   uint64 `json:"stream_seq"`
}

// NextRequest is for getting next messages for pull based consumers.
type NextRequest struct {
	Expires *time.Time `json:"expires,omitempty"`
	Batch   int        `json:"batch,omitempty"`
	NoWait  bool       `json:"no_wait,omitempty"`
}

// SubOpt configures options for subscribing to JetStream consumers.
type SubOpt interface {
	configureSubscribe(opts *subOpts) error
}

// subOptFn is a function option used to configure a JetStream Subscribe.
type subOptFn func(opts *subOpts) error

func (opt subOptFn) configureSubscribe(opts *subOpts) error {
	return opt(opts)
}

// Subscribe will create a subscription to the appropriate stream and consumer.
func (js *js) Subscribe(subj string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
	return js.subscribe(subj, _EMPTY_, cb, nil, opts)
}

// SubscribeSync will create a sync subscription to the appropriate stream and consumer.
func (js *js) SubscribeSync(subj string, opts ...SubOpt) (*Subscription, error) {
	mch := make(chan *Msg, js.nc.Opts.SubChanLen)
	return js.subscribe(subj, _EMPTY_, nil, mch, opts)
}

// QueueSubscribe will create a subscription to the appropriate stream and consumer with queue semantics.
func (js *js) QueueSubscribe(subj, queue string, cb MsgHandler, opts ...SubOpt) (*Subscription, error) {
	return js.subscribe(subj, queue, cb, nil, opts)
}

// Subscribe will create a subscription to the appropriate stream and consumer.
func (js *js) ChanSubscribe(subj string, ch chan *Msg, opts ...SubOpt) (*Subscription, error) {
	return js.subscribe(subj, _EMPTY_, nil, ch, opts)
}

// APIPaged includes variables used to create paged responses from the JSON API
type APIPaged struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

type streamRequest struct {
	Subject string `json:"subject,omitempty"`
}

type JSApiStreamNamesResponse struct {
	APIResponse
	APIPaged
	Streams []string `json:"streams"`
}

func (js *js) subscribe(subj, queue string, cb MsgHandler, ch chan *Msg, opts []SubOpt) (*Subscription, error) {
	cfg := ConsumerConfig{AckPolicy: ackPolicyNotSet}
	o := subOpts{cfg: &cfg}
	if len(opts) > 0 {
		for _, opt := range opts {
			if err := opt.configureSubscribe(&o); err != nil {
				return nil, err
			}
		}
	}

	isPullMode := o.pull > 0
	if cb != nil && isPullMode {
		return nil, ErrPullModeNotAllowed
	}

	var err error
	var stream, deliver string
	var ccfg *ConsumerConfig

	// If we are attaching to an existing consumer.
	shouldAttach := o.stream != _EMPTY_ && o.consumer != _EMPTY_ || o.cfg.DeliverSubject != _EMPTY_
	shouldCreate := !shouldAttach

	if js.direct && shouldCreate {
		return nil, ErrDirectModeRequired
	}

	if js.direct {
		if o.cfg.DeliverSubject != _EMPTY_ {
			deliver = o.cfg.DeliverSubject
		} else {
			deliver = NewInbox()
		}
	} else if shouldAttach {
		info, err := js.getConsumerInfo(o.stream, o.consumer)
		if err != nil {
			return nil, err
		}

		ccfg = &info.Config
		// Make sure this new subject matches or is a subset.
		if ccfg.FilterSubject != _EMPTY_ && subj != ccfg.FilterSubject {
			return nil, ErrSubjectMismatch
		}
		if ccfg.DeliverSubject != _EMPTY_ {
			deliver = ccfg.DeliverSubject
		} else {
			deliver = NewInbox()
		}
	} else {
		stream, err = js.lookupStreamBySubject(subj)
		if err != nil {
			return nil, err
		}
		deliver = NewInbox()
		if !isPullMode {
			cfg.DeliverSubject = deliver
		}
		// Do filtering always, server will clear as needed.
		cfg.FilterSubject = subj
	}

	var sub *Subscription

	// Check if we are manual ack.
	if cb != nil && !o.mack {
		ocb := cb
		cb = func(m *Msg) { ocb(m); m.Ack() }
	}

	sub, err = js.nc.subscribe(deliver, queue, cb, ch, cb == nil, &jsSub{js: js})
	if err != nil {
		return nil, err
	}

	// If we are creating or updating let's process that request.
	if shouldCreate {
		// If not set default to ack explicit.
		if cfg.AckPolicy == ackPolicyNotSet {
			cfg.AckPolicy = AckExplicit
		}
		// If we have acks at all and the MaxAckPending is not set go ahead
		// and set to the internal max.
		// TODO(dlc) - We should be able to update this if client updates PendingLimits.
		if cfg.MaxAckPending == 0 && cfg.AckPolicy != AckNone {
			maxMsgs, _, _ := sub.PendingLimits()
			cfg.MaxAckPending = maxMsgs
		}

		req := &JSApiCreateConsumerRequest{
			Stream: stream,
			Config: &cfg,
		}

		j, err := json.Marshal(req)
		if err != nil {
			return nil, err
		}

		var ccSubj string
		if cfg.Durable != _EMPTY_ {
			ccSubj = fmt.Sprintf(JSApiDurableCreateT, stream, cfg.Durable)
		} else {
			ccSubj = fmt.Sprintf(JSApiConsumerCreateT, stream)
		}

		resp, err := js.nc.Request(js.apiSubj(ccSubj), j, js.wait)
		if err != nil {
			if err == ErrNoResponders {
				err = ErrJetStreamNotEnabled
			}
			sub.Unsubscribe()
			return nil, err
		}

		var info JSApiConsumerResponse
		err = json.Unmarshal(resp.Data, &info)
		if err != nil {
			sub.Unsubscribe()
			return nil, err
		}
		if info.Error != nil {
			sub.Unsubscribe()
			return nil, errors.New(info.Error.Description)
		}

		// Hold onto these for later.
		sub.jsi.stream = info.Stream
		sub.jsi.consumer = info.Name
		sub.jsi.deliver = info.Config.DeliverSubject
	} else {
		sub.jsi.stream = o.stream
		sub.jsi.consumer = o.consumer
		if js.direct {
			sub.jsi.deliver = o.cfg.DeliverSubject
		} else {
			sub.jsi.deliver = ccfg.DeliverSubject
		}
	}

	// If we are pull based go ahead and fire off the first request to populate.
	if isPullMode {
		sub.jsi.pull = o.pull
		sub.Poll()
	}

	return sub, nil
}

func (js *js) lookupStreamBySubject(subj string) (string, error) {
	var slr JSApiStreamNamesResponse
	// FIXME(dlc) - prefix
	req := &streamRequest{subj}
	j, err := json.Marshal(req)
	if err != nil {
		return _EMPTY_, err
	}
	resp, err := js.nc.Request(js.apiSubj(JSApiStreams), j, js.wait)
	if err != nil {
		if err == ErrNoResponders {
			err = ErrJetStreamNotEnabled
		}
		return _EMPTY_, err
	}
	if err := json.Unmarshal(resp.Data, &slr); err != nil {
		return _EMPTY_, err
	}
	if slr.Error != nil || len(slr.Streams) != 1 {
		return _EMPTY_, ErrNoMatchingStream
	}
	return slr.Streams[0], nil
}

type subOpts struct {
	// For attaching.
	stream, consumer string
	// For pull based consumers, batch size for pull
	pull int
	// For manual ack
	mack bool
	// For creating or updating.
	cfg *ConsumerConfig
}

func Durable(name string) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.Durable = name
		return nil
	})
}

func Attach(stream, consumer string) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.stream = stream
		opts.consumer = consumer
		return nil
	})
}

func Pull(batchSize int) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		if batchSize == 0 {
			return errors.New("nats: batch size of 0 not valid")
		}
		opts.pull = batchSize
		return nil
	})
}

func PullDirect(stream, consumer string, batchSize int) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		if batchSize == 0 {
			return errors.New("nats: batch size of 0 not valid")
		}
		opts.stream = stream
		opts.consumer = consumer
		opts.pull = batchSize
		return nil
	})
}

func PushDirect(deliverSubject string) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverSubject = deliverSubject
		return nil
	})
}

func ManualAck() SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.mack = true
		return nil
	})
}

// DeliverAll will configure a Consumer to receive all the
// messages from a Stream.
func DeliverAll() SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverPolicy = DeliverAllPolicy
		return nil
	})
}

// DeliverLast configures a Consumer to receive messages
// starting with the latest one.
func DeliverLast() SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverPolicy = DeliverLastPolicy
		return nil
	})
}

// DeliverNew configures a Consumer to receive messages
// published after the subscription.
func DeliverNew() SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverPolicy = DeliverNewPolicy
		return nil
	})
}

// StartSequence configures a Consumer to receive
// messages from a start sequence.
func StartSequence(seq uint64) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverPolicy = DeliverByStartSequencePolicy
		opts.cfg.OptStartSeq = seq
		return nil
	})
}

// DeliverFromTime configures a Consumer to receive
// messages from a start time.
func StartTime(startTime time.Time) SubOpt {
	return subOptFn(func(opts *subOpts) error {
		opts.cfg.DeliverPolicy = DeliverByStartTimePolicy
		opts.cfg.OptStartTime = &startTime
		return nil
	})
}

func (sub *Subscription) ConsumerInfo() (*ConsumerInfo, error) {
	sub.mu.Lock()
	// TODO(dlc) - Better way to mark especially if we attach.
	if sub.jsi.consumer == _EMPTY_ {
		sub.mu.Unlock()
		return nil, ErrTypeSubscription
	}

	js := sub.jsi.js
	stream, consumer := sub.jsi.stream, sub.jsi.consumer
	sub.mu.Unlock()

	return js.getConsumerInfo(stream, consumer)
}

func (sub *Subscription) Poll() error {
	sub.mu.Lock()
	if sub.jsi == nil || sub.jsi.deliver != _EMPTY_ || sub.jsi.pull == 0 {
		sub.mu.Unlock()
		return ErrTypeSubscription
	}
	batch := sub.jsi.pull
	nc, reply := sub.conn, sub.Subject
	stream, consumer := sub.jsi.stream, sub.jsi.consumer
	js := sub.jsi.js
	sub.mu.Unlock()

	req, _ := json.Marshal(&NextRequest{Batch: batch})
	reqNext := js.apiSubj(fmt.Sprintf(JSApiRequestNextT, stream, consumer))
	return nc.PublishRequest(reqNext, reply, req)
}

func (js *js) getConsumerInfo(stream, consumer string) (*ConsumerInfo, error) {
	// FIXME(dlc) - prefix
	ccInfoSubj := fmt.Sprintf(JSApiConsumerInfoT, stream, consumer)
	resp, err := js.nc.Request(js.apiSubj(ccInfoSubj), nil, js.wait)
	if err != nil {
		if err == ErrNoResponders {
			err = ErrJetStreamNotEnabled
		}
		return nil, err
	}

	var info JSApiConsumerResponse
	if err := json.Unmarshal(resp.Data, &info); err != nil {
		return nil, err
	}
	if info.Error != nil {
		return nil, errors.New(info.Error.Description)
	}
	return info.ConsumerInfo, nil
}

func (m *Msg) checkReply() (*js, bool, error) {
	if m.Reply == "" {
		return nil, false, ErrMsgNoReply
	}
	if m == nil || m.Sub == nil {
		return nil, false, ErrMsgNotBound
	}
	sub := m.Sub
	sub.mu.Lock()
	if sub.jsi == nil {
		sub.mu.Unlock()
		return nil, false, ErrNotJSMessage
	}
	js := sub.jsi.js
	isPullMode := sub.jsi.pull > 0
	sub.mu.Unlock()

	return js, isPullMode, nil
}

// ackReply handles all acks. Will do the right thing for pull and sync mode.
func (m *Msg) ackReply(ackType []byte, sync bool) error {
	js, isPullMode, err := m.checkReply()
	if err != nil {
		return err
	}
	if isPullMode {
		if bytes.Equal(ackType, AckAck) {
			err = js.nc.PublishRequest(m.Reply, m.Sub.Subject, AckNext)
		} else if bytes.Equal(ackType, AckNak) || bytes.Equal(ackType, AckTerm) {
			err = js.nc.PublishRequest(m.Reply, m.Sub.Subject, []byte("+NXT {\"batch\":1}"))
		}
		if sync && err == nil {
			_, err = js.nc.Request(m.Reply, nil, js.wait)
		}
	} else if sync {
		_, err = js.nc.Request(m.Reply, ackType, js.wait)
	} else {
		err = js.nc.Publish(m.Reply, ackType)
	}
	return err
}

// Acks for messages

// Ack a message, this will do the right thing with pull based consumers.
func (m *Msg) Ack() error {
	return m.ackReply(AckAck, false)
}

// Ack a message and wait for a response from the server.
func (m *Msg) AckSync() error {
	return m.ackReply(AckAck, true)
}

// Nak this message, indicating we can not process.
func (m *Msg) Nak() error {
	return m.ackReply(AckNak, false)
}

// Term this message from ever being delivered regardless of MaxDeliverCount.
func (m *Msg) Term() error {
	return m.ackReply(AckTerm, false)
}

// Indicate that this message is being worked on and reset redelkivery timer in the server.
func (m *Msg) InProgress() error {
	return m.ackReply(AckProgress, false)
}

// JetStream metadata associated with received messages.
type MsgMetaData struct {
	Consumer  uint64
	Stream    uint64
	Delivered uint64
	Pending   uint64
	Timestamp time.Time
}

func (m *Msg) MetaData() (*MsgMetaData, error) {
	if _, _, err := m.checkReply(); err != nil {
		return nil, err
	}

	const expectedTokens = 9
	const btsep = '.'

	tsa := [expectedTokens]string{}
	start, tokens := 0, tsa[:0]
	subject := m.Reply
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	if len(tokens) != expectedTokens || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return nil, ErrNotJSMessage
	}

	meta := &MsgMetaData{
		Delivered: uint64(parseNum(tokens[4])),
		Stream:    uint64(parseNum(tokens[5])),
		Consumer:  uint64(parseNum(tokens[6])),
		Timestamp: time.Unix(0, parseNum(tokens[7])),
		Pending:   uint64(parseNum(tokens[8])),
	}

	return meta, nil
}

// Quick parser for positive numbers in ack reply encoding.
func parseNum(d string) (n int64) {
	if len(d) == 0 {
		return -1
	}

	// Ascii numbers 0-9
	const (
		asciiZero = 48
		asciiNine = 57
	)

	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}

// Additional jetstream structures.

type AckPolicy int

const (
	AckNone AckPolicy = iota
	AckAll
	AckExplicit

	// For setting
	ackPolicyNotSet = 99
)

func jsonString(s string) string {
	return "\"" + s + "\""
}

func (p *AckPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("none"):
		*p = AckNone
	case jsonString("all"):
		*p = AckAll
	case jsonString("explicit"):
		*p = AckExplicit
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}

	return nil
}

func (p AckPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case AckNone:
		return json.Marshal("none")
	case AckAll:
		return json.Marshal("all")
	case AckExplicit:
		return json.Marshal("explicit")
	default:
		return nil, fmt.Errorf("unknown acknowlegement policy %v", p)
	}
}

func (p AckPolicy) String() string {
	switch p {
	case AckNone:
		return "AckNone"
	case AckAll:
		return "AckAll"
	case AckExplicit:
		return "AckExplicit"
	case ackPolicyNotSet:
		return "Not Initialized"
	default:
		return "Unknown AckPolicy"
	}
}

type ReplayPolicy int

const (
	ReplayInstant ReplayPolicy = iota
	ReplayOriginal
)

func (p *ReplayPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("instant"):
		*p = ReplayInstant
	case jsonString("original"):
		*p = ReplayOriginal
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}

	return nil
}

func (p ReplayPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case ReplayOriginal:
		return json.Marshal("original")
	case ReplayInstant:
		return json.Marshal("instant")
	default:
		return nil, fmt.Errorf("unknown replay policy %v", p)
	}
}

var (
	AckAck      = []byte("+ACK")
	AckNak      = []byte("-NAK")
	AckProgress = []byte("+WPI")
	AckNext     = []byte("+NXT")
	AckTerm     = []byte("+TERM")
)

// DeliverPolicy determines how the consumer should select the first message to deliver.
type DeliverPolicy int

const (
	// DeliverAllPolicy will be the default so can be omitted from the request.
	DeliverAllPolicy DeliverPolicy = iota

	// DeliverLastPolicy will start the consumer with the last sequence received.
	DeliverLastPolicy

	// DeliverNewPolicy will only deliver new messages that are sent
	// after the consumer is created.
	DeliverNewPolicy

	// DeliverByStartSequencePolicy will look for a defined starting sequence to start.
	DeliverByStartSequencePolicy

	// StartTime will select the first messsage with a timestamp >= to StartTime.
	DeliverByStartTimePolicy
)

func (p *DeliverPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("all"), jsonString("undefined"):
		*p = DeliverAllPolicy
	case jsonString("last"):
		*p = DeliverLastPolicy
	case jsonString("new"):
		*p = DeliverNewPolicy
	case jsonString("by_start_sequence"):
		*p = DeliverByStartSequencePolicy
	case jsonString("by_start_time"):
		*p = DeliverByStartTimePolicy
	}

	return nil
}

func (p DeliverPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case DeliverAllPolicy:
		return json.Marshal("all")
	case DeliverLastPolicy:
		return json.Marshal("last")
	case DeliverNewPolicy:
		return json.Marshal("new")
	case DeliverByStartSequencePolicy:
		return json.Marshal("by_start_sequence")
	case DeliverByStartTimePolicy:
		return json.Marshal("by_start_time")
	default:
		return nil, fmt.Errorf("unknown deliver policy %v", p)
	}
}

// Management for JetStream
// TODO(dlc) - Fill this out.

// AddConsumer will add a JetStream consumer.
func (js *js) AddConsumer(stream string, cfg *ConsumerConfig) (*ConsumerInfo, error) {
	if stream == _EMPTY_ {
		return nil, ErrStreamNameRequired
	}
	req, err := json.Marshal(&JSApiCreateConsumerRequest{Stream: stream, Config: cfg})
	if err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.Durable != _EMPTY_ {
		ccSubj = fmt.Sprintf(JSApiDurableCreateT, stream, cfg.Durable)
	} else {
		ccSubj = fmt.Sprintf(JSApiConsumerCreateT, stream)
	}

	resp, err := js.nc.Request(js.apiSubj(ccSubj), req, js.wait)
	if err != nil {
		if err == ErrNoResponders {
			err = ErrJetStreamNotEnabled
		}
		return nil, err
	}
	var info JSApiConsumerResponse
	err = json.Unmarshal(resp.Data, &info)
	if err != nil {
		return nil, err
	}
	if info.Error != nil {
		return nil, errors.New(info.Error.Description)
	}
	return info.ConsumerInfo, nil
}

// StreamConfig will determine the properties for a stream.
// There are sensible defaults for most. If no subjects are
// given the name will be used as the only subject.
type StreamConfig struct {
	Name         string          `json:"name"`
	Subjects     []string        `json:"subjects,omitempty"`
	Retention    RetentionPolicy `json:"retention"`
	MaxConsumers int             `json:"max_consumers"`
	MaxMsgs      int64           `json:"max_msgs"`
	MaxBytes     int64           `json:"max_bytes"`
	Discard      DiscardPolicy   `json:"discard"`
	MaxAge       time.Duration   `json:"max_age"`
	MaxMsgSize   int32           `json:"max_msg_size,omitempty"`
	Storage      StorageType     `json:"storage"`
	Replicas     int             `json:"num_replicas"`
	NoAck        bool            `json:"no_ack,omitempty"`
	Template     string          `json:"template_owner,omitempty"`
	Duplicates   time.Duration   `json:"duplicate_window,omitempty"`
}

// JSApiStreamCreateResponse stream creation.
type JSApiStreamCreateResponse struct {
	APIResponse
	*StreamInfo
}

func (js *js) AddStream(cfg *StreamConfig) (*StreamInfo, error) {
	if cfg == nil || cfg.Name == _EMPTY_ {
		return nil, ErrStreamNameRequired
	}

	req, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	csSubj := js.apiSubj(fmt.Sprintf(JSApiStreamCreateT, cfg.Name))
	r, err := js.nc.Request(csSubj, req, js.wait)
	if err != nil {
		return nil, err
	}
	var resp JSApiStreamCreateResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Description)
	}
	return resp.StreamInfo, nil
}

type JSApiStreamInfoResponse = JSApiStreamCreateResponse

func (js *js) StreamInfo(stream string) (*StreamInfo, error) {
	csSubj := js.apiSubj(fmt.Sprintf(JSApiStreamInfoT, stream))
	r, err := js.nc.Request(csSubj, nil, js.wait)
	if err != nil {
		return nil, err
	}
	var resp JSApiStreamInfoResponse
	if err := json.Unmarshal(r.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, errors.New(resp.Error.Description)
	}
	return resp.StreamInfo, nil
}

// StreamInfo shows config and current state for this stream.
type StreamInfo struct {
	Config  StreamConfig `json:"config"`
	Created time.Time    `json:"created"`
	State   StreamState  `json:"state"`
}

// StreamStats is information about the given stream.
type StreamState struct {
	Msgs      uint64    `json:"messages"`
	Bytes     uint64    `json:"bytes"`
	FirstSeq  uint64    `json:"first_seq"`
	FirstTime time.Time `json:"first_ts"`
	LastSeq   uint64    `json:"last_seq"`
	LastTime  time.Time `json:"last_ts"`
	Consumers int       `json:"consumer_count"`
}

// RetentionPolicy determines how messages in a set are retained.
type RetentionPolicy int

const (
	// LimitsPolicy (default) means that messages are retained until any given limit is reached.
	// This could be one of MaxMsgs, MaxBytes, or MaxAge.
	LimitsPolicy RetentionPolicy = iota
	// InterestPolicy specifies that when all known observables have acknowledged a message it can be removed.
	InterestPolicy
	// WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message it can be removed.
	WorkQueuePolicy
)

// Discard Policy determines how we proceed when limits of messages or bytes are hit. The default, DicscardOld will
// remove older messages. DiscardNew will fail to store the new message.
type DiscardPolicy int

const (
	// DiscardOld will remove older messages to return to the limits.
	DiscardOld = iota
	//DiscardNew will error on a StoreMsg call
	DiscardNew
)

const (
	limitsPolicyString    = "limits"
	interestPolicyString  = "interest"
	workQueuePolicyString = "workqueue"
)

func (rp RetentionPolicy) String() string {
	switch rp {
	case LimitsPolicy:
		return "Limits"
	case InterestPolicy:
		return "Interest"
	case WorkQueuePolicy:
		return "WorkQueue"
	default:
		return "Unknown Retention Policy"
	}
}

func (rp RetentionPolicy) MarshalJSON() ([]byte, error) {
	switch rp {
	case LimitsPolicy:
		return json.Marshal(limitsPolicyString)
	case InterestPolicy:
		return json.Marshal(interestPolicyString)
	case WorkQueuePolicy:
		return json.Marshal(workQueuePolicyString)
	default:
		return nil, fmt.Errorf("can not marshal %v", rp)
	}
}

func (rp *RetentionPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(limitsPolicyString):
		*rp = LimitsPolicy
	case jsonString(interestPolicyString):
		*rp = InterestPolicy
	case jsonString(workQueuePolicyString):
		*rp = WorkQueuePolicy
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

func (dp DiscardPolicy) String() string {
	switch dp {
	case DiscardOld:
		return "DiscardOld"
	case DiscardNew:
		return "DiscardNew"
	default:
		return "Unknown Discard Policy"
	}
}

func (dp DiscardPolicy) MarshalJSON() ([]byte, error) {
	switch dp {
	case DiscardOld:
		return json.Marshal("old")
	case DiscardNew:
		return json.Marshal("new")
	default:
		return nil, fmt.Errorf("can not marshal %v", dp)
	}
}

func (dp *DiscardPolicy) UnmarshalJSON(data []byte) error {
	switch strings.ToLower(string(data)) {
	case jsonString("old"):
		*dp = DiscardOld
	case jsonString("new"):
		*dp = DiscardNew
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}

// StorageType determines how messages are stored for retention.
type StorageType int

const (
	// FileStorage specifies on disk storage. It's the default.
	FileStorage StorageType = iota
	// MemoryStorage specifies in memory only.
	MemoryStorage
)

const (
	memoryStorageString = "memory"
	fileStorageString   = "file"
)

func (st StorageType) String() string {
	switch st {
	case MemoryStorage:
		return strings.Title(memoryStorageString)
	case FileStorage:
		return strings.Title(fileStorageString)
	default:
		return "Unknown Storage Type"
	}
}

func (st StorageType) MarshalJSON() ([]byte, error) {
	switch st {
	case MemoryStorage:
		return json.Marshal(memoryStorageString)
	case FileStorage:
		return json.Marshal(fileStorageString)
	default:
		return nil, fmt.Errorf("can not marshal %v", st)
	}
}

func (st *StorageType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString(memoryStorageString):
		*st = MemoryStorage
	case jsonString(fileStorageString):
		*st = FileStorage
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}
	return nil
}
