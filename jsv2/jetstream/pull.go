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
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	// ConsumerReader supports iterating over a messages on a stream.
	ConsumerReader interface {
		// Next retreives nest message on a stream. It will block until the next message is available.
		Next() (Msg, error)
		// Stop closes the iterator and cancels subscription.
		Stop()
	}

	ConsumerListener interface {
		Stop()
	}

	// MessageHandler is a handler function used as callback in `Listener()`
	MessageHandler func(msg Msg, err error)

	// ConsumerListenerOpts represent additional options used in `Listener()` for pull consumers
	ConsumerListenerOpts func(*pullRequest) error

	// ConsumerReaderOpts represent additional options used in `Messages()` for pull consumers
	ConsumerReaderOpts func(*pullRequest) error

	pullConsumer struct {
		sync.Mutex
		jetStream    *jetStream
		stream       string
		durable      bool
		name         string
		info         *ConsumerInfo
		isSubscribed uint32
	}

	pullRequest struct {
		Expires   time.Duration `json:"expires,omitempty"`
		Batch     int           `json:"batch,omitempty"`
		MaxBytes  int           `json:"max_bytes,omitempty"`
		NoWait    bool          `json:"no_wait,omitempty"`
		Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
	}

	pullSubscription struct {
		sync.Mutex
		consumer         *pullConsumer
		subscription     *nats.Subscription
		req              *pullRequest
		msgs             chan *nats.Msg
		errs             chan error
		pending          pendingMsgs
		hbTimer          *time.Timer
		fetchInProgress  bool
		closed           uint32
		done             chan struct{}
		reconnected      chan struct{}
		fetchNext        chan struct{}
		fetchComplete    chan struct{}
		reconnectHandler nats.ConnHandler
	}

	pendingMsgs struct {
		msgCount  int
		byteCount int
	}
)

// Reader returns ConsumerReader, allowing continously iterating over messages on a stream.
//
// Available options:
// WithReaderBatchSize() - sets a single batch request messages limit, default is set to 100.
// WithReaderHeartbeat() - sets an idle heartbeat setting for a pull request, default value is 5 seconds.
func (p *pullConsumer) Reader(opts ...ConsumerReaderOpts) (ConsumerReader, error) {
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

	msgs := make(chan *nats.Msg, 2*req.Batch)

	sub := &pullSubscription{
		consumer:         p,
		req:              req,
		done:             make(chan struct{}, 1),
		msgs:             msgs,
		errs:             make(chan error, 1),
		fetchNext:        make(chan struct{}, 1),
		reconnected:      make(chan struct{}),
		fetchComplete:    make(chan struct{}, 1),
		reconnectHandler: p.jetStream.conn.Opts.ReconnectedCB,
	}
	if err := sub.setupSubscription(); err != nil {
		return nil, err
	}
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if sub.reconnectHandler != nil {
			sub.reconnectHandler(p.jetStream.conn)
		}
		sub.reconnected <- struct{}{}
	})

	sub.hbTimer = sub.scheduleHeartbeatCheck(req.Heartbeat)
	go func() {
		<-sub.done
		sub.cleanupSubscriptionAndRestoreConnHandler()
	}()

	if err := sub.pull(*req, subject); err != nil {
		sub.errs <- err
	}
	sub.pending.msgCount = req.Batch
	sub.pending.byteCount = req.MaxBytes
	go sub.pullMessages(subject)

	return sub, nil
}

func (s *pullSubscription) Next() (Msg, error) {
	s.Lock()
	defer s.Unlock()
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, ErrMsgIteratorClosed
	}

	for {
		if s.pending.msgCount <= s.req.Batch/2 ||
			(s.pending.byteCount <= s.req.MaxBytes/2 && s.req.MaxBytes != 0) &&
				!s.fetchInProgress {

			s.fetchInProgress = true
			s.fetchNext <- struct{}{}
		}
		select {
		case msg := <-s.msgs:
			if s.hbTimer != nil {
				s.hbTimer.Reset(2 * s.req.Heartbeat)
			}
			userMsg, err := checkMsg(msg)
			if err != nil {
				if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, ErrMaxBytesExceeded) {
					return nil, err
				}
				s.pending.msgCount -= s.req.Batch
				if s.pending.msgCount < 0 {
					s.pending.msgCount = 0
				}
				if s.req.MaxBytes > 0 {
					s.pending.byteCount -= s.req.MaxBytes
					if s.pending.byteCount < 0 {
						s.pending.byteCount = 0
					}
				}
				continue
			}
			if !userMsg {
				continue
			}
			s.pending.msgCount--
			if s.req.MaxBytes > 0 {
				s.pending.byteCount -= msgSize(msg)
			}
			return s.consumer.jetStream.toJSMsg(msg), nil
		case <-s.reconnected:
			_, err := s.consumer.Info(context.Background())
			if err != nil {
				s.Stop()
				return nil, err
			}
			s.pending.msgCount -= s.req.Batch
			if s.pending.msgCount < 0 {
				s.pending.msgCount = 0
				continue
			}
			if s.req.MaxBytes > 0 {
				s.pending.byteCount -= s.req.MaxBytes
				if s.pending.byteCount < 0 {
					s.pending.byteCount = 0
				}
			}
		case <-s.fetchComplete:
			s.fetchInProgress = false
			s.pending.msgCount += s.req.Batch
			if s.req.MaxBytes > 0 {
				s.pending.byteCount += s.req.MaxBytes
			}
		case err := <-s.errs:
			if errors.Is(err, ErrNoHeartbeat) {
				s.Stop()
			}
			return nil, err
		}
	}
}

func (s *pullSubscription) Stop() {
	if atomic.LoadUint32(&s.closed) == 1 {
		return
	}
	close(s.done)
	atomic.StoreUint32(&s.consumer.isSubscribed, 0)
	atomic.StoreUint32(&s.closed, 1)
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

	sub := &pullSubscription{
		consumer:      p,
		req:           req,
		done:          make(chan struct{}, 1),
		msgs:          msgs,
		errs:          make(chan error, 1),
		fetchNext:     make(chan struct{}, 1),
		reconnected:   make(chan struct{}),
		fetchComplete: make(chan struct{}, 1),
	}
	if err := sub.setupSubscription(); err != nil {
		return nil, err
	}
	defer sub.subscription.Unsubscribe()
	if err := sub.pull(*req, subject); err != nil {
		return nil, err
	}

	errs := make(chan error, 1)
	jsMsgs := make([]Msg, 0)
	hbTimer := sub.scheduleHeartbeatCheck(req.Heartbeat)
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

// Listener returns a ConsumerListener, allowing for processing incoming messages from a stream in a given callback function.
//
// Available options:
// WithListenerBatchSize() - sets a single batch request messages limit, default is set to 100
// WitListenerExpiry() - sets a timeout for individual batch request, default is set to 30 seconds
// WithListenerHeartbeat() - sets an idle heartbeat setting for a pull request, default is set to 5s
func (p *pullConsumer) Listener(handler MessageHandler, opts ...ConsumerListenerOpts) (ConsumerListener, error) {
	if atomic.LoadUint32(&p.isSubscribed) == 1 {
		return nil, ErrConsumerHasActiveSubscription
	}
	if handler == nil {
		return nil, ErrHandlerRequired
	}
	defaultTimeout := 30 * time.Second
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

	msgs := make(chan *nats.Msg, 2*req.Batch)

	atomic.StoreUint32(&p.isSubscribed, 1)
	sub := &pullSubscription{
		consumer:         p,
		req:              req,
		msgs:             msgs,
		errs:             make(chan error, 1),
		done:             make(chan struct{}, 1),
		fetchNext:        make(chan struct{}, 1),
		reconnected:      make(chan struct{}),
		fetchComplete:    make(chan struct{}, 1),
		reconnectHandler: p.jetStream.conn.Opts.ReconnectedCB,
	}
	if err := sub.setupSubscription(); err != nil {
		return nil, err
	}
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if sub.reconnectHandler != nil {
			sub.reconnectHandler(p.jetStream.conn)
		}
		sub.reconnected <- struct{}{}
	})
	sub.hbTimer = sub.scheduleHeartbeatCheck(req.Heartbeat)
	go func() {
		<-sub.done
		sub.cleanupSubscriptionAndRestoreConnHandler()
	}()
	go sub.pullMessages(subject)

	go func() {
		for {
			if atomic.LoadUint32(&sub.closed) == 1 {
				return
			}
			if sub.pending.msgCount <= sub.req.Batch/2 ||
				(sub.pending.byteCount <= sub.req.MaxBytes/2 && sub.req.MaxBytes != 0) &&
					!sub.fetchInProgress {

				sub.fetchInProgress = true
				sub.fetchNext <- struct{}{}
			}
			select {
			case msg := <-msgs:
				if sub.hbTimer != nil {
					sub.hbTimer.Reset(2 * req.Heartbeat)
				}
				userMsg, err := checkMsg(msg)
				if err != nil {
					if !errors.Is(err, nats.ErrTimeout) {
						handler(nil, err)
						continue
					}
					sub.pending.msgCount -= req.Batch
					if sub.pending.msgCount < 0 {
						sub.pending.msgCount = 0
					}
					continue
				}
				if !userMsg {
					continue
				}
				handler(p.jetStream.toJSMsg(msg), nil)
				sub.pending.msgCount--
			case <-sub.reconnected:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := p.Info(ctx)
				cancel()
				if err != nil {
					sub.cleanupSubscriptionAndRestoreConnHandler()
					handler(nil, err)
					return
				}
				sub.pending.msgCount -= req.Batch
				if sub.pending.msgCount < 0 {
					sub.pending.msgCount = 0
				}
			case <-sub.fetchComplete:
				sub.fetchInProgress = false
				sub.pending.msgCount += req.Batch
			case err := <-sub.errs:
				if errors.Is(err, ErrNoHeartbeat) {
					sub.cleanupSubscriptionAndRestoreConnHandler()
					handler(nil, err)
					return
				}
				handler(nil, err)
			}
		}
	}()

	return sub, nil
}

func (s *pullSubscription) pullMessages(subject string) {
	for {
		select {
		case <-s.fetchNext:
			if err := s.pull(*s.req, subject); err != nil {
				if errors.Is(err, ErrMsgIteratorClosed) {
					s.cleanupSubscriptionAndRestoreConnHandler()
					return
				}
				s.errs <- err
			}
			s.fetchComplete <- struct{}{}
		case <-s.done:
			s.cleanupSubscriptionAndRestoreConnHandler()
			return
		}
	}
}

func (s *pullSubscription) scheduleHeartbeatCheck(dur time.Duration) *time.Timer {
	if dur == 0 {
		return nil
	}
	return time.AfterFunc(2*dur, func() {
		s.errs <- ErrNoHeartbeat
	})
}

func (s *pullSubscription) cleanupSubscriptionAndRestoreConnHandler() {
	s.consumer.Lock()
	defer s.consumer.Unlock()
	if s.hbTimer != nil {
		s.hbTimer.Stop()
	}
	s.subscription.Unsubscribe()
	s.subscription = nil
	atomic.StoreUint32(&s.consumer.isSubscribed, 0)
	s.consumer.jetStream.conn.SetReconnectHandler(s.reconnectHandler)
}

func (s *pullSubscription) setupSubscription() error {
	inbox := nats.NewInbox()
	sub, err := s.consumer.jetStream.conn.ChanSubscribe(inbox, s.msgs)
	if err != nil {
		return err
	}
	s.subscription = sub
	return nil
}

func msgSize(msg *nats.Msg) int {
	if msg == nil {
		return 0
	}
	return len(msg.Subject) + len(msg.Reply) + len(msg.Data)
}

// pull sends a pull request to the server and waits for messages using a subscription from `pullSubscription`.
// Messages will be fetched up to given batch_size or until there are no more messages or timeout is returned
func (s *pullSubscription) pull(req pullRequest, subject string) error {
	if atomic.LoadUint32(&s.closed) == 1 {
		return ErrMsgIteratorClosed
	}
	if req.Batch < 1 {
		return fmt.Errorf("%w: batch size must be at least 1", nats.ErrInvalidArg)
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return err
	}

	reply := s.subscription.Subject
	if err := s.consumer.jetStream.conn.PublishRequest(subject, reply, reqJSON); err != nil {
		return err
	}
	return nil
}
