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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"errors"

	"github.com/nats-io/nats.go"
)

type (

	// Consumer contains methods for fetching/processing messages from a stream, as well as fetching consumer info
	Consumer interface {
		// Fetch is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and wait until either all messages are retreived
		// or context reaches its deadline.
		Fetch(context.Context, int) ([]Msg, error)
		// FetchNoWait is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and immediately return up to a provided number of messages
		FetchNoWait(batch int) ([]Msg, error)
		// Subscribe can be used to continuously receive messages and handle them with the provided callback function
		Subscribe(context.Context, MessageHandler, ...ConsumerSubscribeOpt) error
		// Messages returns MsgIterator allowing continously iterating over messages on a stream.
		Messages(...ConsumerMessagesOpt) (MsgIterator, error)

		// Info returns Consumer details
		Info(context.Context) (*ConsumerInfo, error)
		// CachedInfo returns *ConsumerInfo cached on a consumer struct
		CachedInfo() *ConsumerInfo
	}

	// MsgIterator supports iterating over a messages on a stream.
	MsgIterator interface {
		// Next retreives nest message on a stream. It will block until the next message is available.
		Next() (Msg, error)
		// Stop closes the iterator and cancels subscription.
		Stop()
	}

	// ConsumerSubscribeOpt represent additional options used in `Subscribe()` for pull consumers
	ConsumerSubscribeOpt func(*pullRequest) error

	// ConsumerMessagesOpt represent additional options used in `Messages()` for pull consumers
	ConsumerMessagesOpt func(*pullRequest) error

	// MessageHandler is a handler function used as callback in `Subscribe()`
	MessageHandler func(msg Msg, err error)

	consumer struct {
		jetStream    *jetStream
		stream       string
		durable      bool
		name         string
		subscription *nats.Subscription
		info         *ConsumerInfo
		sync.Mutex
	}
	pullConsumer struct {
		consumer
		isSubscribed uint32
		errs         chan error
		pendingMsgs  int64
	}

	pullRequest struct {
		Expires   time.Duration `json:"expires,omitempty"`
		Batch     int           `json:"batch,omitempty"`
		MaxBytes  int           `json:"max_bytes,omitempty"`
		NoWait    bool          `json:"no_wait,omitempty"`
		Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
	}

	messagesIter struct {
		sync.Mutex
		consumer        *pullConsumer
		req             *pullRequest
		msgs            chan *nats.Msg
		fetchNext       chan struct{}
		fetchInProgress bool
		reconnected     chan struct{}
		done            chan struct{}
		closed          uint32
		hbTimer         *time.Timer
	}
)

// Messages returns MsgIterator allowing continously iterating over messages on a stream.
//
// Available options:
// WithMessagesBatchSize() - sets a single batch request messages limit, default is set to 100.
// WithMessagesHeartbeat() - sets an idle heartbeat setting for a pull request, default value is 5 seconds.
func (p *pullConsumer) Messages(opts ...ConsumerMessagesOpt) (MsgIterator, error) {
	if atomic.LoadUint32(&p.isSubscribed) == 1 {
		return nil, ErrConsumerHasActiveSubscription
	}
	atomic.StoreUint32(&p.isSubscribed, 1)
	defaultTimeout := 10 * time.Second
	req := &pullRequest{
		Batch:     100,
		Expires:   defaultTimeout,
		Heartbeat: 5 * time.Second,
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return nil, err
		}
	}
	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))
	p.errs = make(chan error, 1)
	p.pendingMsgs = 0
	atomic.StoreUint32(&p.isSubscribed, 1)

	msgs := make(chan *nats.Msg, 2*req.Batch)
	if err := p.setupSubscription(msgs); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	reconnected := make(chan struct{})
	reconnectHandler := p.jetStream.conn.Opts.ReconnectedCB
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if reconnectHandler != nil {
			reconnectHandler(p.jetStream.conn)
		}
		reconnected <- struct{}{}
	})
	it := &messagesIter{
		consumer:    p,
		req:         req,
		done:        make(chan struct{}, 1),
		msgs:        msgs,
		fetchNext:   make(chan struct{}, 1),
		reconnected: reconnected,
	}

	go func() {
		<-it.done
		cancel()
		p.cleanupSubscriptionAndRestoreConnHandler(reconnectHandler)
	}()

	if err := p.pull(ctx, *req, subject); err != nil {
		p.errs <- err
	}
	atomic.StoreInt64(&p.pendingMsgs, int64(req.Batch))
	it.hbTimer = scheduleHeartbeatCheck(req.Heartbeat, p.errs)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-it.fetchNext:
				if err := p.pull(ctx, *req, subject); err != nil {
					p.errs <- err
				}
				atomic.AddInt64(&p.pendingMsgs, int64(req.Batch))
				it.fetchInProgress = false

			case <-it.done:
				cancel()
				p.cleanupSubscriptionAndRestoreConnHandler(reconnectHandler)
			}
		}
	}()

	return it, nil
}

func (it *messagesIter) Next() (Msg, error) {
	it.Lock()
	defer it.Unlock()
	if atomic.LoadUint32(&it.closed) == 1 {
		return nil, ErrMsgIteratorClosed
	}

	for {
		pennding := atomic.LoadInt64(&it.consumer.pendingMsgs)
		if pennding <= int64(it.req.Batch)/2 && !it.fetchInProgress {
			it.fetchInProgress = true
			it.fetchNext <- struct{}{}
		}
		select {
		case msg := <-it.msgs:
			if it.hbTimer != nil {
				it.hbTimer.Reset(2 * it.req.Heartbeat)
			}
			userMsg, err := checkMsg(msg)
			if err != nil {
				if !errors.Is(err, nats.ErrTimeout) {
					return nil, err
				}
				if atomic.LoadInt64(&it.consumer.pendingMsgs) < int64(it.req.Batch) {
					atomic.StoreInt64(&it.consumer.pendingMsgs, 0)
					continue
				}
				atomic.AddInt64(&it.consumer.pendingMsgs, -int64(it.req.Batch))
				continue
			}
			if !userMsg {
				continue
			}
			atomic.AddInt64(&it.consumer.pendingMsgs, -1)
			return it.consumer.jetStream.toJSMsg(msg), nil
		case <-it.reconnected:
			_, err := it.consumer.Info(context.Background())
			if err != nil {
				it.Stop()
				return nil, err
			}
			if atomic.LoadInt64(&it.consumer.pendingMsgs) < int64(it.req.Batch) {
				atomic.StoreInt64(&it.consumer.pendingMsgs, 0)
				continue
			}
			atomic.AddInt64(&it.consumer.pendingMsgs, -int64(it.req.Batch))
		case err := <-it.consumer.errs:
			if errors.Is(err, ErrNoHeartbeat) {
				it.Stop()
			}
			return nil, err
		}
	}
}

func (it *messagesIter) Stop() {
	if atomic.LoadUint32(&it.closed) == 1 {
		return
	}
	close(it.done)
	atomic.StoreUint32(&it.consumer.isSubscribed, 0)
	atomic.StoreUint32(&it.closed, 1)
}

// Next fetches an individual message from a consumer.
// Timeout for this operation is handled using `context.Deadline()`, so it should always be set to avoid getting stuck
func (p *pullConsumer) Fetch(ctx context.Context, batch int) ([]Msg, error) {
	p.Lock()
	if atomic.LoadUint32(&p.isSubscribed) == 1 {
		p.Unlock()
		return nil, ErrConsumerHasActiveSubscription
	}
	timeout := 30 * time.Second
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		timeout = time.Until(deadline)
	}
	req := &pullRequest{
		Batch: batch,
	}
	// Make expiry a little bit shorter than timeout
	if timeout >= 20*time.Millisecond {
		req.Expires = timeout - 10*time.Millisecond
	}
	// for longer pulls, set heartbeat value
	if timeout >= 10*time.Second {
		req.Heartbeat = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	p.Unlock()

	return p.fetch(ctx, req)

}

func (p *pullConsumer) FetchNoWait(batch int) ([]Msg, error) {
	p.Lock()
	if atomic.LoadUint32(&p.isSubscribed) == 1 {
		p.Unlock()
		return nil, ErrConsumerHasActiveSubscription
	}
	req := &pullRequest{
		Batch:  batch,
		NoWait: true,
	}
	p.Unlock()

	return p.fetch(context.Background(), req)
}

func (p *pullConsumer) fetch(ctx context.Context, req *pullRequest) ([]Msg, error) {
	msgs := make(chan *nats.Msg, 2*req.Batch)
	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))

	if err := p.setupSubscription(msgs); err != nil {
		return nil, err
	}
	defer p.subscription.Unsubscribe()
	if err := p.pull(ctx, *req, subject); err != nil {
		return nil, err
	}

	errs := make(chan error, 1)
	jsMsgs := make([]Msg, 0)
	hbTimer := scheduleHeartbeatCheck(req.Heartbeat, p.errs)
	for {
		select {
		case msg := <-msgs:
			if hbTimer != nil {
				hbTimer.Reset(2 * req.Heartbeat)
			}
			userMsg, err := checkMsg(msg)
			if err != nil {
				if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, ErrNoMessages) {
					return nil, err
				}
				return jsMsgs, nil
			}
			if !userMsg {
				continue
			}
			jsMsgs = append(jsMsgs, p.jetStream.toJSMsg(msg))
			if len(jsMsgs) == req.Batch {
				return jsMsgs, nil
			}
		case err := <-errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Subscribe continuously receives messages from a consumer and handles them with the provided callback function
// ctx is used to handle the whole operation, not individual messages batch, so to avoid cancellation, a context without Deadline should be provided
//
// Available options:
// WithSubscribeBatchSize() - sets a single batch request messages limit, default is set to 100
// WitSubscribehExpiry() - sets a timeout for individual batch request, default is set to 30 seconds
// WithSubscribeHeartbeat() - sets an idle heartbeat setting for a pull request, default is set to 5s
func (p *pullConsumer) Subscribe(ctx context.Context, handler MessageHandler, opts ...ConsumerSubscribeOpt) error {
	if atomic.LoadUint32(&p.isSubscribed) == 1 {
		return ErrConsumerHasActiveSubscription
	}
	if handler == nil {
		return ErrHandlerRequired
	}
	defaultTimeout := 30 * time.Second
	req := &pullRequest{
		Batch:     100,
		Expires:   defaultTimeout,
		Heartbeat: 5 * time.Second,
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return err
		}
	}

	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))
	p.errs = make(chan error, 1)
	p.pendingMsgs = 0
	atomic.StoreUint32(&p.isSubscribed, 1)

	msgs := make(chan *nats.Msg, 2*req.Batch)
	if err := p.setupSubscription(msgs); err != nil {
		return err
	}

	fetchNext := make(chan struct{}, 1)
	fetchComplete := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(ctx)
	reconnected := make(chan struct{})
	reconnectHandler := p.jetStream.conn.Opts.ReconnectedCB
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if reconnectHandler != nil {
			reconnectHandler(p.jetStream.conn)
		}
		reconnected <- struct{}{}
	})
	hbTimer := scheduleHeartbeatCheck(req.Heartbeat, p.errs)
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.cleanupSubscriptionAndRestoreConnHandler(reconnectHandler)
				return
			case <-fetchNext:
				if err := p.pull(ctx, *req, subject); err != nil {
					p.errs <- err
				}
				atomic.AddInt64(&p.pendingMsgs, int64(req.Batch))
				fetchComplete <- struct{}{}
			}
		}
	}()

	go func() {
		var fetchInProgress bool
		for {
			if atomic.LoadInt64(&p.pendingMsgs) <= int64(req.Batch)/2 && !fetchInProgress {
				fetchInProgress = true
				fetchNext <- struct{}{}
			}
			select {
			case msg := <-msgs:
				if hbTimer != nil {
					hbTimer.Reset(2 * req.Heartbeat)
				}
				userMsg, err := checkMsg(msg)
				if err != nil {
					if !errors.Is(err, nats.ErrTimeout) {
						handler(nil, err)
						continue
					}
					if atomic.LoadInt64(&p.pendingMsgs) < int64(req.Batch) {
						atomic.StoreInt64(&p.pendingMsgs, 0)
						continue
					}
					atomic.AddInt64(&p.pendingMsgs, -int64(req.Batch))
					continue
				}
				if !userMsg {
					continue
				}
				handler(p.jetStream.toJSMsg(msg), nil)
				atomic.AddInt64(&p.pendingMsgs, -1)
			case <-reconnected:
				_, err := p.Info(ctx)
				if err != nil {
					cancel()
					p.cleanupSubscriptionAndRestoreConnHandler(reconnectHandler)
					handler(nil, err)
					return
				}
				if atomic.LoadInt64(&p.pendingMsgs) < int64(req.Batch) {
					atomic.StoreInt64(&p.pendingMsgs, 0)
					continue
				}
				atomic.AddInt64(&p.pendingMsgs, -int64(req.Batch))
			case <-fetchComplete:
				fetchInProgress = false
			case err := <-p.errs:
				if errors.Is(err, ErrNoHeartbeat) {
					cancel()
					p.cleanupSubscriptionAndRestoreConnHandler(reconnectHandler)
					handler(nil, err)
					return
				}
				handler(nil, err)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func scheduleHeartbeatCheck(dur time.Duration, errCh chan error) *time.Timer {
	if dur == 0 {
		return nil
	}
	return time.AfterFunc(2*dur, func() {
		errCh <- ErrNoHeartbeat
	})
}

func (p *pullConsumer) cleanupSubscriptionAndRestoreConnHandler(reconnectHandler nats.ConnHandler) {
	p.Lock()
	p.subscription.Unsubscribe()
	p.subscription = nil
	p.Unlock()
	atomic.StoreUint32(&p.isSubscribed, 0)
	p.jetStream.conn.SetReconnectHandler(reconnectHandler)
}

func (p *pullConsumer) setupSubscription(msgs chan *nats.Msg) error {
	inbox := nats.NewInbox()
	sub, err := p.jetStream.conn.ChanSubscribe(inbox, msgs)
	if err != nil {
		return err
	}
	p.subscription = sub
	return nil
}

// pull sends a pull request to the server and waits for messages using a subscription from `pullConsumer`.
// Messages will be fetched up to given batch_size or until there are no more messages or timeout is returned
func (c *pullConsumer) pull(ctx context.Context, req pullRequest, subject string) error {
	if req.Batch < 1 {
		return fmt.Errorf("%w: batch size must be at least 1", nats.ErrInvalidArg)
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return err
	}

	if err := c.jetStream.conn.PublishRequest(subject, c.subscription.Subject, reqJSON); err != nil {
		return err
	}
	return nil
}

// Info returns ConsumerInfo for a given consumer
func (p *pullConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	infoSubject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, p.stream, p.name))
	var resp consumerInfoResponse

	if _, err := p.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	p.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// CachedInfo returns ConsumerInfo fetched when initializing/updating a consumer
//
// NOTE: The returned object might not be up to date with the most recent updates on the server
// For up-to-date information, use `Info()`
func (p *pullConsumer) CachedInfo() *ConsumerInfo {
	return p.info
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig) (Consumer, error) {
	req := createConsumerRequest{
		Stream: stream,
		Config: &cfg,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.Durable != "" {
		if err := validateDurableName(cfg.Durable); err != nil {
			return nil, err
		}
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiDurableCreateT, stream, cfg.Durable))
	} else {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateT, stream))
	}
	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, ccSubj, &resp, reqJSON); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		consumer: consumer{
			jetStream: js,
			stream:    stream,
			name:      resp.Name,
			durable:   cfg.Durable != "",
			info:      resp.ConsumerInfo,
		},
	}, nil
}

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	if err := validateDurableName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerInfoT, stream, name))

	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	return &pullConsumer{
		consumer: consumer{
			jetStream: js,
			stream:    stream,
			name:      name,
			durable:   resp.Config.Durable != "",
			info:      resp.ConsumerInfo,
		},
	}, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	if err := validateDurableName(consumer); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerDeleteT, stream, consumer))

	var resp consumerDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return ErrConsumerNotFound
		}
		return resp.Error
	}
	return nil
}

func validateDurableName(dur string) error {
	if strings.Contains(dur, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidConsumerName, dur)
	}
	return nil
}

func compareConsumerConfig(s, u *ConsumerConfig) error {
	makeErr := func(fieldName string, usrVal, srvVal interface{}) error {
		return fmt.Errorf("configuration requests %s to be %v, but consumer's value is %v", fieldName, usrVal, srvVal)
	}

	if u.Durable != s.Durable {
		return makeErr("durable", u.Durable, s.Durable)
	}
	if u.Description != s.Description {
		return makeErr("description", u.Description, s.Description)
	}
	if u.DeliverPolicy != s.DeliverPolicy {
		return makeErr("deliver policy", u.DeliverPolicy, s.DeliverPolicy)
	}
	if u.OptStartSeq != s.OptStartSeq {
		return makeErr("optional start sequence", u.OptStartSeq, s.OptStartSeq)
	}
	if u.OptStartTime != nil && !u.OptStartTime.IsZero() && !(*u.OptStartTime).Equal(*s.OptStartTime) {
		return makeErr("optional start time", u.OptStartTime, s.OptStartTime)
	}
	if u.AckPolicy != s.AckPolicy {
		return makeErr("ack policy", u.AckPolicy, s.AckPolicy)
	}
	if u.AckWait != 0 && u.AckWait != s.AckWait {
		return makeErr("ack wait", u.AckWait.String(), s.AckWait.String())
	}
	if !(u.MaxDeliver == 0 && s.MaxDeliver == -1) && u.MaxDeliver != s.MaxDeliver {
		return makeErr("max deliver", u.MaxDeliver, s.MaxDeliver)
	}
	if len(u.BackOff) != len(s.BackOff) {
		return makeErr("backoff", u.BackOff, s.BackOff)
	}
	for i, val := range u.BackOff {
		if val != s.BackOff[i] {
			return makeErr("backoff", u.BackOff, s.BackOff)
		}
	}
	if u.FilterSubject != s.FilterSubject {
		return makeErr("filter subject", u.FilterSubject, s.FilterSubject)
	}
	if u.ReplayPolicy != s.ReplayPolicy {
		return makeErr("replay policy", u.ReplayPolicy, s.ReplayPolicy)
	}
	if u.RateLimit != s.RateLimit {
		return makeErr("rate limit", u.RateLimit, s.RateLimit)
	}
	if u.SampleFrequency != s.SampleFrequency {
		return makeErr("sample frequency", u.SampleFrequency, s.SampleFrequency)
	}
	if u.MaxWaiting != 0 && u.MaxWaiting != s.MaxWaiting {
		return makeErr("max waiting", u.MaxWaiting, s.MaxWaiting)
	}
	if u.MaxAckPending != 0 && u.MaxAckPending != s.MaxAckPending {
		return makeErr("max ack pending", u.MaxAckPending, s.MaxAckPending)
	}
	if u.FlowControl != s.FlowControl {
		return makeErr("flow control", u.FlowControl, s.FlowControl)
	}
	if u.Heartbeat != s.Heartbeat {
		return makeErr("heartbeat", u.Heartbeat, s.Heartbeat)
	}
	if u.HeadersOnly != s.HeadersOnly {
		return makeErr("headers only", u.HeadersOnly, s.HeadersOnly)
	}
	if u.MaxRequestBatch != s.MaxRequestBatch {
		return makeErr("max request batch", u.MaxRequestBatch, s.MaxRequestBatch)
	}
	if u.MaxRequestExpires != s.MaxRequestExpires {
		return makeErr("max request expires", u.MaxRequestExpires.String(), s.MaxRequestExpires.String())
	}
	if u.DeliverSubject != s.DeliverSubject {
		return makeErr("deliver subject", u.DeliverSubject, s.DeliverSubject)
	}
	if u.DeliverGroup != s.DeliverGroup {
		return makeErr("deliver group", u.DeliverSubject, s.DeliverSubject)
	}
	if u.InactiveThreshold != s.InactiveThreshold {
		return makeErr("inactive threshhold", u.InactiveThreshold.String(), s.InactiveThreshold.String())
	}
	if u.Replicas != s.Replicas {
		return makeErr("replicas", u.Replicas, s.Replicas)
	}
	if u.MemoryStorage != s.MemoryStorage {
		return makeErr("memory storage", u.MemoryStorage, s.MemoryStorage)
	}
	return nil
}
