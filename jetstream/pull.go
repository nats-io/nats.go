// Copyright 2022-2024 The NATS Authors
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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type (
	// MessagesContext supports iterating over a messages on a stream.
	// It is returned by [Consumer.Messages] method.
	MessagesContext interface {
		// Next retrieves next message on a stream. It will block until the next
		// message is available. If the context is canceled, Next will return
		// ErrMsgIteratorClosed error.
		Next() (Msg, error)

		// Stop unsubscribes from the stream and cancels subscription. Calling
		// Next after calling Stop will return ErrMsgIteratorClosed error.
		// All messages that are already in the buffer are discarded.
		Stop()

		// Drain unsubscribes from the stream and cancels subscription. All
		// messages that are already in the buffer will be available on
		// subsequent calls to Next. After the buffer is drained, Next will
		// return ErrMsgIteratorClosed error.
		Drain()
	}

	// ConsumeContext supports processing incoming messages from a stream.
	// It is returned by [Consumer.Consume] method.
	ConsumeContext interface {
		// Stop unsubscribes from the stream and cancels subscription.
		// No more messages will be received after calling this method.
		// All messages that are already in the buffer are discarded.
		Stop()

		// Drain unsubscribes from the stream and cancels subscription.
		// All messages that are already in the buffer will be processed in callback function.
		Drain()
	}

	// MessageHandler is a handler function used as callback in [Consume].
	MessageHandler func(msg Msg)

	// PullConsumeOpt represent additional options used in [Consume] for pull consumers.
	PullConsumeOpt interface {
		configureConsume(*consumeOpts) error
	}

	// PullMessagesOpt represent additional options used in [Messages] for pull consumers.
	PullMessagesOpt interface {
		configureMessages(*consumeOpts) error
	}

	pullConsumer struct {
		sync.Mutex
		jetStream     *jetStream
		stream        string
		durable       bool
		name          string
		info          *ConsumerInfo
		subscriptions map[string]*pullSubscription
	}

	pullRequest struct {
		Expires   time.Duration `json:"expires,omitempty"`
		Batch     int           `json:"batch,omitempty"`
		MaxBytes  int           `json:"max_bytes,omitempty"`
		NoWait    bool          `json:"no_wait,omitempty"`
		Heartbeat time.Duration `json:"idle_heartbeat,omitempty"`
	}

	consumeOpts struct {
		Expires                 time.Duration
		MaxMessages             int
		MaxBytes                int
		Heartbeat               time.Duration
		ErrHandler              ConsumeErrHandlerFunc
		ReportMissingHeartbeats bool
		ThresholdMessages       int
		ThresholdBytes          int
		StopAfter               int
		stopAfterMsgsLeft       chan int
	}

	ConsumeErrHandlerFunc func(consumeCtx ConsumeContext, err error)

	pullSubscription struct {
		sync.Mutex
		id                string
		consumer          *pullConsumer
		subscription      *nats.Subscription
		msgs              chan *nats.Msg
		errs              chan error
		pending           pendingMsgs
		hbMonitor         *hbMonitor
		fetchInProgress   uint32
		closed            uint32
		draining          uint32
		done              chan struct{}
		connStatusChanged chan nats.Status
		fetchNext         chan *pullRequest
		consumeOpts       *consumeOpts
		delivered         int
	}

	pendingMsgs struct {
		msgCount  int
		byteCount int
	}

	MessageBatch interface {
		Messages() <-chan Msg
		Error() error
	}

	fetchResult struct {
		msgs chan Msg
		err  error
		done bool
		sseq uint64
	}

	FetchOpt func(*pullRequest) error

	hbMonitor struct {
		timer *time.Timer
		sync.Mutex
	}
)

const (
	DefaultMaxMessages = 500
	DefaultExpires     = 30 * time.Second
	unset              = -1
)

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Consume can be used to continuously receive messages and handle them
// with the provided callback function. Consume cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Consume] for more details.
func (p *pullConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	if handler == nil {
		return nil, ErrHandlerRequired
	}
	consumeOpts, err := parseConsumeOpts(false, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	p.Lock()

	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))

	// for single consume, use empty string as id
	// this is useful for ordered consumer, where only a single subscription is valid
	var consumeID string
	if len(p.subscriptions) > 0 {
		consumeID = nuid.Next()
	}
	sub := &pullSubscription{
		id:          consumeID,
		consumer:    p,
		errs:        make(chan error, 1),
		done:        make(chan struct{}, 1),
		fetchNext:   make(chan *pullRequest, 1),
		consumeOpts: consumeOpts,
	}
	sub.connStatusChanged = p.jetStream.conn.StatusChanged(nats.CONNECTED, nats.RECONNECTING)

	sub.hbMonitor = sub.scheduleHeartbeatCheck(consumeOpts.Heartbeat)

	p.subscriptions[sub.id] = sub
	p.Unlock()

	internalHandler := func(msg *nats.Msg) {
		if sub.hbMonitor != nil {
			sub.hbMonitor.Stop()
		}
		userMsg, msgErr := checkMsg(msg)
		if !userMsg && msgErr == nil {
			if sub.hbMonitor != nil {
				sub.hbMonitor.Reset(2 * consumeOpts.Heartbeat)
			}
			return
		}
		defer func() {
			sub.Lock()
			sub.checkPending()
			if sub.hbMonitor != nil {
				sub.hbMonitor.Reset(2 * consumeOpts.Heartbeat)
			}
			sub.Unlock()
		}()
		if !userMsg {
			// heartbeat message
			if msgErr == nil {
				return
			}

			sub.Lock()
			err := sub.handleStatusMsg(msg, msgErr)
			sub.Unlock()

			if err != nil {
				if atomic.LoadUint32(&sub.closed) == 1 {
					return
				}
				if sub.consumeOpts.ErrHandler != nil {
					sub.consumeOpts.ErrHandler(sub, err)
				}
				sub.Stop()
			}
			return
		}
		handler(p.jetStream.toJSMsg(msg))
		sub.Lock()
		sub.decrementPendingMsgs(msg)
		sub.incrementDeliveredMsgs()
		sub.Unlock()

		if sub.consumeOpts.StopAfter > 0 && sub.consumeOpts.StopAfter == sub.delivered {
			sub.Stop()
		}
	}
	inbox := p.jetStream.conn.NewInbox()
	sub.subscription, err = p.jetStream.conn.Subscribe(inbox, internalHandler)
	if err != nil {
		return nil, err
	}
	sub.subscription.SetClosedHandler(func(sid string) func(string) {
		return func(subject string) {
			p.Lock()
			defer p.Unlock()
			delete(p.subscriptions, sid)
			atomic.CompareAndSwapUint32(&sub.draining, 1, 0)
		}
	}(sub.id))

	sub.Lock()
	// initial pull
	sub.resetPendingMsgs()
	batchSize := sub.consumeOpts.MaxMessages
	if sub.consumeOpts.StopAfter > 0 {
		batchSize = min(batchSize, sub.consumeOpts.StopAfter-sub.delivered)
	}
	if err := sub.pull(&pullRequest{
		Expires:   consumeOpts.Expires,
		Batch:     batchSize,
		MaxBytes:  consumeOpts.MaxBytes,
		Heartbeat: consumeOpts.Heartbeat,
	}, subject); err != nil {
		sub.errs <- err
	}
	sub.Unlock()

	go func() {
		isConnected := true
		for {
			if atomic.LoadUint32(&sub.closed) == 1 {
				return
			}
			select {
			case status, ok := <-sub.connStatusChanged:
				if !ok {
					continue
				}
				if status == nats.RECONNECTING {
					if sub.hbMonitor != nil {
						sub.hbMonitor.Stop()
					}
					isConnected = false
				}
				if status == nats.CONNECTED {
					sub.Lock()
					if !isConnected {
						isConnected = true
						// try fetching consumer info several times to make sure consumer is available after reconnect
						backoffOpts := backoffOpts{
							attempts:                10,
							initialInterval:         1 * time.Second,
							disableInitialExecution: true,
							factor:                  2,
							maxInterval:             10 * time.Second,
							cancel:                  sub.done,
						}
						err = retryWithBackoff(func(attempt int) (bool, error) {
							isClosed := atomic.LoadUint32(&sub.closed) == 1
							if isClosed {
								return false, nil
							}
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							_, err := p.Info(ctx)
							if err != nil {
								if sub.consumeOpts.ErrHandler != nil {
									err = fmt.Errorf("[%d] attempting to fetch consumer info after reconnect: %w", attempt, err)
									if attempt == backoffOpts.attempts-1 {
										err = errors.Join(err, fmt.Errorf("maximum retry attempts reached"))
									}
									sub.consumeOpts.ErrHandler(sub, err)
								}
								return true, err
							}
							return false, nil
						}, backoffOpts)
						if err != nil {
							if sub.consumeOpts.ErrHandler != nil {
								sub.consumeOpts.ErrHandler(sub, err)
							}
							sub.Unlock()
							sub.cleanup()
							return
						}

						sub.fetchNext <- &pullRequest{
							Expires:   sub.consumeOpts.Expires,
							Batch:     sub.consumeOpts.MaxMessages,
							MaxBytes:  sub.consumeOpts.MaxBytes,
							Heartbeat: sub.consumeOpts.Heartbeat,
						}
						if sub.hbMonitor != nil {
							sub.hbMonitor.Reset(2 * sub.consumeOpts.Heartbeat)
						}
						sub.resetPendingMsgs()
					}
					sub.Unlock()
				}
			case err := <-sub.errs:
				sub.Lock()
				if sub.consumeOpts.ErrHandler != nil {
					sub.consumeOpts.ErrHandler(sub, err)
				}
				if errors.Is(err, ErrNoHeartbeat) {
					batchSize := sub.consumeOpts.MaxMessages
					if sub.consumeOpts.StopAfter > 0 {
						batchSize = min(batchSize, sub.consumeOpts.StopAfter-sub.delivered)
					}
					sub.fetchNext <- &pullRequest{
						Expires:   sub.consumeOpts.Expires,
						Batch:     batchSize,
						MaxBytes:  sub.consumeOpts.MaxBytes,
						Heartbeat: sub.consumeOpts.Heartbeat,
					}
					if sub.hbMonitor != nil {
						sub.hbMonitor.Reset(2 * sub.consumeOpts.Heartbeat)
					}
					sub.resetPendingMsgs()
				}
				sub.Unlock()
			case <-sub.done:
				return
			}
		}
	}()

	go sub.pullMessages(subject)

	return sub, nil
}

// resetPendingMsgs resets pending message count and byte count
// to the values set in consumeOpts
// lock should be held before calling this method
func (s *pullSubscription) resetPendingMsgs() {
	s.pending.msgCount = s.consumeOpts.MaxMessages
	s.pending.byteCount = s.consumeOpts.MaxBytes
}

// decrementPendingMsgs decrements pending message count and byte count
// lock should be held before calling this method
func (s *pullSubscription) decrementPendingMsgs(msg *nats.Msg) {
	s.pending.msgCount--
	if s.consumeOpts.MaxBytes != 0 {
		s.pending.byteCount -= msg.Size()
	}
}

// incrementDeliveredMsgs increments delivered message count
// lock should be held before calling this method
func (s *pullSubscription) incrementDeliveredMsgs() {
	s.delivered++
}

// checkPending verifies whether there are enough messages in
// the buffer to trigger a new pull request.
// lock should be held before calling this method
func (s *pullSubscription) checkPending() {
	if (s.pending.msgCount < s.consumeOpts.ThresholdMessages ||
		(s.pending.byteCount < s.consumeOpts.ThresholdBytes && s.consumeOpts.MaxBytes != 0)) &&
		atomic.LoadUint32(&s.fetchInProgress) == 0 {

		var batchSize, maxBytes int
		if s.consumeOpts.MaxBytes == 0 {
			// if using messages, calculate appropriate batch size
			batchSize = s.consumeOpts.MaxMessages - s.pending.msgCount
		} else {
			// if using bytes, use the max value
			batchSize = s.consumeOpts.MaxMessages
			maxBytes = s.consumeOpts.MaxBytes - s.pending.byteCount
		}
		if s.consumeOpts.StopAfter > 0 {
			batchSize = min(batchSize, s.consumeOpts.StopAfter-s.delivered-s.pending.msgCount)
		}
		if batchSize > 0 {
			s.fetchNext <- &pullRequest{
				Expires:   s.consumeOpts.Expires,
				Batch:     batchSize,
				MaxBytes:  maxBytes,
				Heartbeat: s.consumeOpts.Heartbeat,
			}

			s.pending.msgCount = s.consumeOpts.MaxMessages
			s.pending.byteCount = s.consumeOpts.MaxBytes
		}
	}
}

// Messages returns MessagesContext, allowing continuously iterating
// over messages on a stream. Messages cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Messages] for more details.
func (p *pullConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	consumeOpts, err := parseMessagesOpts(false, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}

	p.Lock()
	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))

	msgs := make(chan *nats.Msg, consumeOpts.MaxMessages)

	// for single consume, use empty string as id
	// this is useful for ordered consumer, where only a single subscription is valid
	var consumeID string
	if len(p.subscriptions) > 0 {
		consumeID = nuid.Next()
	}
	sub := &pullSubscription{
		id:          consumeID,
		consumer:    p,
		done:        make(chan struct{}, 1),
		msgs:        msgs,
		errs:        make(chan error, 1),
		fetchNext:   make(chan *pullRequest, 1),
		consumeOpts: consumeOpts,
	}
	sub.connStatusChanged = p.jetStream.conn.StatusChanged(nats.CONNECTED, nats.RECONNECTING)
	inbox := p.jetStream.conn.NewInbox()
	sub.subscription, err = p.jetStream.conn.ChanSubscribe(inbox, sub.msgs)
	if err != nil {
		p.Unlock()
		return nil, err
	}
	sub.subscription.SetClosedHandler(func(sid string) func(string) {
		return func(subject string) {
			p.Lock()
			defer p.Unlock()
			if atomic.LoadUint32(&sub.draining) != 1 {
				// if we're not draining, subscription can be closed as soon
				// as closed handler is called
				// otherwise, we need to wait until all messages are drained
				// in Next
				delete(p.subscriptions, sid)
			}
			close(msgs)
		}
	}(sub.id))

	p.subscriptions[sub.id] = sub
	p.Unlock()

	go sub.pullMessages(subject)

	go func() {
		for {
			select {
			case status, ok := <-sub.connStatusChanged:
				if !ok {
					return
				}
				if status == nats.CONNECTED {
					sub.errs <- errConnected
				}
				if status == nats.RECONNECTING {
					sub.errs <- errDisconnected
				}
			case <-sub.done:
				return
			}
		}
	}()

	return sub, nil
}

var (
	errConnected    = errors.New("connected")
	errDisconnected = errors.New("disconnected")
)

// Next retrieves next message on a stream. It will block until the next
// message is available. If the context is canceled, Next will return
// ErrMsgIteratorClosed error.
func (s *pullSubscription) Next() (Msg, error) {
	s.Lock()
	defer s.Unlock()
	drainMode := atomic.LoadUint32(&s.draining) == 1
	closed := atomic.LoadUint32(&s.closed) == 1
	if closed && !drainMode {
		return nil, ErrMsgIteratorClosed
	}
	hbMonitor := s.scheduleHeartbeatCheck(2 * s.consumeOpts.Heartbeat)
	defer func() {
		if hbMonitor != nil {
			hbMonitor.Stop()
		}
	}()

	isConnected := true
	if s.consumeOpts.StopAfter > 0 && s.delivered >= s.consumeOpts.StopAfter {
		s.Stop()
		return nil, ErrMsgIteratorClosed
	}

	for {
		s.checkPending()
		select {
		case msg, ok := <-s.msgs:
			if !ok {
				// if msgs channel is closed, it means that subscription was either drained or stopped
				delete(s.consumer.subscriptions, s.id)
				atomic.CompareAndSwapUint32(&s.draining, 1, 0)
				return nil, ErrMsgIteratorClosed
			}
			if hbMonitor != nil {
				hbMonitor.Reset(2 * s.consumeOpts.Heartbeat)
			}
			userMsg, msgErr := checkMsg(msg)
			if !userMsg {
				// heartbeat message
				if msgErr == nil {
					continue
				}
				if err := s.handleStatusMsg(msg, msgErr); err != nil {
					s.Stop()
					return nil, err
				}
				continue
			}
			s.decrementPendingMsgs(msg)
			s.incrementDeliveredMsgs()
			return s.consumer.jetStream.toJSMsg(msg), nil
		case err := <-s.errs:
			if errors.Is(err, ErrNoHeartbeat) {
				s.pending.msgCount = 0
				s.pending.byteCount = 0
				if s.consumeOpts.ReportMissingHeartbeats {
					return nil, err
				}
				if hbMonitor != nil {
					hbMonitor.Reset(2 * s.consumeOpts.Heartbeat)
				}
			}
			if errors.Is(err, errConnected) {
				if !isConnected {
					isConnected = true
					// try fetching consumer info several times to make sure consumer is available after reconnect
					backoffOpts := backoffOpts{
						attempts:                10,
						initialInterval:         1 * time.Second,
						disableInitialExecution: true,
						factor:                  2,
						maxInterval:             10 * time.Second,
						cancel:                  s.done,
					}
					err = retryWithBackoff(func(attempt int) (bool, error) {
						isClosed := atomic.LoadUint32(&s.closed) == 1
						if isClosed {
							return false, nil
						}
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						_, err := s.consumer.Info(ctx)
						if err != nil {
							if errors.Is(err, ErrConsumerNotFound) {
								return false, err
							}
							if attempt == backoffOpts.attempts-1 {
								return true, fmt.Errorf("could not get consumer info after server reconnect: %w", err)
							}
							return true, err
						}
						return false, nil
					}, backoffOpts)
					if err != nil {
						s.Stop()
						return nil, err
					}

					s.pending.msgCount = 0
					s.pending.byteCount = 0
					if hbMonitor != nil {
						hbMonitor.Reset(2 * s.consumeOpts.Heartbeat)
					}
				}
			}
			if errors.Is(err, errDisconnected) {
				if hbMonitor != nil {
					hbMonitor.Reset(2 * s.consumeOpts.Heartbeat)
				}
				isConnected = false
			}
		}
	}
}

func (s *pullSubscription) handleStatusMsg(msg *nats.Msg, msgErr error) error {
	if !errors.Is(msgErr, nats.ErrTimeout) && !errors.Is(msgErr, ErrMaxBytesExceeded) {
		if errors.Is(msgErr, ErrConsumerDeleted) || errors.Is(msgErr, ErrBadRequest) {
			return msgErr
		}
		if s.consumeOpts.ErrHandler != nil {
			s.consumeOpts.ErrHandler(s, msgErr)
		}
		if errors.Is(msgErr, ErrConsumerLeadershipChanged) {
			s.pending.msgCount = 0
			s.pending.byteCount = 0
		}
		return nil
	}
	msgsLeft, bytesLeft, err := parsePending(msg)
	if err != nil {
		return err
	}
	s.pending.msgCount -= msgsLeft
	if s.pending.msgCount < 0 {
		s.pending.msgCount = 0
	}
	if s.consumeOpts.MaxBytes > 0 {
		s.pending.byteCount -= bytesLeft
		if s.pending.byteCount < 0 {
			s.pending.byteCount = 0
		}
	}
	return nil
}

func (hb *hbMonitor) Stop() {
	hb.Mutex.Lock()
	hb.timer.Stop()
	hb.Mutex.Unlock()
}

func (hb *hbMonitor) Reset(dur time.Duration) {
	hb.Mutex.Lock()
	hb.timer.Reset(dur)
	hb.Mutex.Unlock()
}

// Stop unsubscribes from the stream and cancels subscription. Calling
// Next after calling Stop will return ErrMsgIteratorClosed error.
// All messages that are already in the buffer are discarded.
func (s *pullSubscription) Stop() {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return
	}
	close(s.done)
	if s.consumeOpts.stopAfterMsgsLeft != nil {
		if s.delivered >= s.consumeOpts.StopAfter {
			close(s.consumeOpts.stopAfterMsgsLeft)
		} else {
			s.consumeOpts.stopAfterMsgsLeft <- s.consumeOpts.StopAfter - s.delivered
		}
	}
}

// Drain unsubscribes from the stream and cancels subscription. All
// messages that are already in the buffer will be available on
// subsequent calls to Next. After the buffer is drained, Next will
// return ErrMsgIteratorClosed error.
func (s *pullSubscription) Drain() {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return
	}
	atomic.StoreUint32(&s.draining, 1)
	close(s.done)
	if s.consumeOpts.stopAfterMsgsLeft != nil {
		if s.delivered >= s.consumeOpts.StopAfter {
			close(s.consumeOpts.stopAfterMsgsLeft)
		} else {
			s.consumeOpts.stopAfterMsgsLeft <- s.consumeOpts.StopAfter - s.delivered
		}
	}
}

// Fetch sends a single request to retrieve given number of messages.
// It will wait up to provided expiry time if not all messages are available.
func (p *pullConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	req := &pullRequest{
		Batch:     batch,
		Expires:   DefaultExpires,
		Heartbeat: unset,
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return nil, err
		}
	}
	// if heartbeat was not explicitly set, set it to 5 seconds for longer pulls
	// and disable it for shorter pulls
	if req.Heartbeat == unset {
		if req.Expires >= 10*time.Second {
			req.Heartbeat = 5 * time.Second
		} else {
			req.Heartbeat = 0
		}
	}
	if req.Expires < 2*req.Heartbeat {
		return nil, fmt.Errorf("%w: expiry time should be at least 2 times the heartbeat", ErrInvalidOption)
	}

	return p.fetch(req)
}

// FetchBytes is used to retrieve up to a provided bytes from the stream.
func (p *pullConsumer) FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error) {
	req := &pullRequest{
		Batch:     1000000,
		MaxBytes:  maxBytes,
		Expires:   DefaultExpires,
		Heartbeat: unset,
	}
	for _, opt := range opts {
		if err := opt(req); err != nil {
			return nil, err
		}
	}
	// if heartbeat was not explicitly set, set it to 5 seconds for longer pulls
	// and disable it for shorter pulls
	if req.Heartbeat == unset {
		if req.Expires >= 10*time.Second {
			req.Heartbeat = 5 * time.Second
		} else {
			req.Heartbeat = 0
		}
	}
	if req.Expires < 2*req.Heartbeat {
		return nil, fmt.Errorf("%w: expiry time should be at least 2 times the heartbeat", ErrInvalidOption)
	}

	return p.fetch(req)
}

// FetchNoWait sends a single request to retrieve given number of messages.
// FetchNoWait will only return messages that are available at the time of the
// request. It will not wait for more messages to arrive.
func (p *pullConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	req := &pullRequest{
		Batch:  batch,
		NoWait: true,
	}

	return p.fetch(req)
}

func (p *pullConsumer) fetch(req *pullRequest) (MessageBatch, error) {
	res := &fetchResult{
		msgs: make(chan Msg, req.Batch),
	}
	msgs := make(chan *nats.Msg, 2*req.Batch)
	subject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiRequestNextT, p.stream, p.name))

	sub := &pullSubscription{
		consumer: p,
		done:     make(chan struct{}, 1),
		msgs:     msgs,
		errs:     make(chan error, 1),
	}
	inbox := p.jetStream.conn.NewInbox()
	var err error
	sub.subscription, err = p.jetStream.conn.ChanSubscribe(inbox, sub.msgs)
	if err != nil {
		return nil, err
	}
	if err := sub.pull(req, subject); err != nil {
		return nil, err
	}

	var receivedMsgs, receivedBytes int
	hbTimer := sub.scheduleHeartbeatCheck(req.Heartbeat)
	go func(res *fetchResult) {
		defer sub.subscription.Unsubscribe()
		defer close(res.msgs)
		for {
			select {
			case msg := <-msgs:
				p.Lock()
				if hbTimer != nil {
					hbTimer.Reset(2 * req.Heartbeat)
				}
				userMsg, err := checkMsg(msg)
				if err != nil {
					errNotTimeoutOrNoMsgs := !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, ErrNoMessages)
					if errNotTimeoutOrNoMsgs && !errors.Is(err, ErrMaxBytesExceeded) {
						res.err = err
					}
					res.done = true
					p.Unlock()
					return
				}
				if !userMsg {
					p.Unlock()
					continue
				}
				res.msgs <- p.jetStream.toJSMsg(msg)
				meta, err := msg.Metadata()
				if err != nil {
					res.err = fmt.Errorf("parsing message metadata: %s", err)
				}
				res.sseq = meta.Sequence.Stream
				receivedMsgs++
				if req.MaxBytes != 0 {
					receivedBytes += msg.Size()
				}
				if receivedMsgs == req.Batch || (req.MaxBytes != 0 && receivedBytes >= req.MaxBytes) {
					res.done = true
					p.Unlock()
					return
				}
				p.Unlock()
			case err := <-sub.errs:
				res.err = err
				res.done = true
				return
			case <-time.After(req.Expires + 1*time.Second):
				res.done = true
				return
			}
		}
	}(res)
	return res, nil
}

func (fr *fetchResult) Messages() <-chan Msg {
	return fr.msgs
}

func (fr *fetchResult) Error() error {
	return fr.err
}

// Next is used to retrieve the next message from the stream. This
// method will block until the message is retrieved or timeout is
// reached.
func (p *pullConsumer) Next(opts ...FetchOpt) (Msg, error) {
	res, err := p.Fetch(1, opts...)
	if err != nil {
		return nil, err
	}
	msg := <-res.Messages()
	if msg != nil {
		return msg, nil
	}
	if res.Error() == nil {
		return nil, nats.ErrTimeout
	}
	return nil, res.Error()
}

func (s *pullSubscription) pullMessages(subject string) {
	for {
		select {
		case req := <-s.fetchNext:
			atomic.StoreUint32(&s.fetchInProgress, 1)

			if err := s.pull(req, subject); err != nil {
				if errors.Is(err, ErrMsgIteratorClosed) {
					s.cleanup()
					return
				}
				s.errs <- err
			}
			atomic.StoreUint32(&s.fetchInProgress, 0)
		case <-s.done:
			s.cleanup()
			return
		}
	}
}

func (s *pullSubscription) scheduleHeartbeatCheck(dur time.Duration) *hbMonitor {
	if dur == 0 {
		return nil
	}
	return &hbMonitor{
		timer: time.AfterFunc(2*dur, func() {
			s.errs <- ErrNoHeartbeat
		}),
	}
}

func (s *pullSubscription) cleanup() {
	// For now this function does not need to hold the lock.
	// Holding the lock here might cause a deadlock if Next()
	// is already holding the lock and waiting.
	// The fields that are read (subscription, hbMonitor)
	// are read only (Only written on creation of pullSubscription).
	if s.subscription == nil || !s.subscription.IsValid() {
		return
	}
	if s.hbMonitor != nil {
		s.hbMonitor.Stop()
	}
	drainMode := atomic.LoadUint32(&s.draining) == 1
	if drainMode {
		s.subscription.Drain()
	} else {
		s.subscription.Unsubscribe()
	}
	atomic.StoreUint32(&s.closed, 1)
}

// pull sends a pull request to the server and waits for messages using a subscription from [pullSubscription].
// Messages will be fetched up to given batch_size or until there are no more messages or timeout is returned
func (s *pullSubscription) pull(req *pullRequest, subject string) error {
	s.consumer.Lock()
	defer s.consumer.Unlock()
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

func parseConsumeOpts(ordered bool, opts ...PullConsumeOpt) (*consumeOpts, error) {
	consumeOpts := &consumeOpts{
		MaxMessages:             unset,
		MaxBytes:                unset,
		Expires:                 DefaultExpires,
		Heartbeat:               unset,
		ReportMissingHeartbeats: true,
		StopAfter:               unset,
	}
	for _, opt := range opts {
		if err := opt.configureConsume(consumeOpts); err != nil {
			return nil, err
		}
	}
	if err := consumeOpts.setDefaults(ordered); err != nil {
		return nil, err
	}
	return consumeOpts, nil
}

func parseMessagesOpts(ordered bool, opts ...PullMessagesOpt) (*consumeOpts, error) {
	consumeOpts := &consumeOpts{
		MaxMessages:             unset,
		MaxBytes:                unset,
		Expires:                 DefaultExpires,
		Heartbeat:               unset,
		ReportMissingHeartbeats: true,
		StopAfter:               unset,
	}
	for _, opt := range opts {
		if err := opt.configureMessages(consumeOpts); err != nil {
			return nil, err
		}
	}
	if err := consumeOpts.setDefaults(ordered); err != nil {
		return nil, err
	}
	return consumeOpts, nil
}

func (consumeOpts *consumeOpts) setDefaults(ordered bool) error {
	if consumeOpts.MaxBytes != unset && consumeOpts.MaxMessages != unset {
		return fmt.Errorf("only one of MaxMessages and MaxBytes can be specified")
	}
	if consumeOpts.MaxBytes != unset {
		// when max_bytes is used, set batch size to a very large number
		consumeOpts.MaxMessages = 1000000
	} else if consumeOpts.MaxMessages != unset {
		consumeOpts.MaxBytes = 0
	} else {
		if consumeOpts.MaxBytes == unset {
			consumeOpts.MaxBytes = 0
		}
		if consumeOpts.MaxMessages == unset {
			consumeOpts.MaxMessages = DefaultMaxMessages
		}
	}

	if consumeOpts.ThresholdMessages == 0 {
		consumeOpts.ThresholdMessages = int(math.Ceil(float64(consumeOpts.MaxMessages) / 2))
	}
	if consumeOpts.ThresholdBytes == 0 {
		consumeOpts.ThresholdBytes = int(math.Ceil(float64(consumeOpts.MaxBytes) / 2))
	}
	if consumeOpts.Heartbeat == unset {
		if ordered {
			consumeOpts.Heartbeat = 5 * time.Second
			if consumeOpts.Expires < 10*time.Second {
				consumeOpts.Heartbeat = consumeOpts.Expires / 2
			}
		} else {
			consumeOpts.Heartbeat = consumeOpts.Expires / 2
			if consumeOpts.Heartbeat > 30*time.Second {
				consumeOpts.Heartbeat = 30 * time.Second
			}
		}
	}
	if consumeOpts.Heartbeat > consumeOpts.Expires/2 {
		return fmt.Errorf("the value of Heartbeat must be less than 50%% of expiry")
	}
	return nil
}

type backoffOpts struct {
	// total retry attempts
	// -1 for unlimited
	attempts int
	// initial interval after which first retry will be performed
	// defaults to 1s
	initialInterval time.Duration
	// determines whether first function execution should be performed immediately
	disableInitialExecution bool
	// multiplier on each attempt
	// defaults to 2
	factor float64
	// max interval between retries
	// after reaching this value, all subsequent
	// retries will be performed with this interval
	// defaults to 1 minute
	maxInterval time.Duration
	// custom backoff intervals
	// if set, overrides all other options except attempts
	// if attempts are set, then the last interval will be used
	// for all subsequent retries after reaching the limit
	customBackoff []time.Duration
	// cancel channel
	// if set, retry will be canceled when this channel is closed
	cancel <-chan struct{}
}

func retryWithBackoff(f func(int) (bool, error), opts backoffOpts) error {
	var err error
	var shouldContinue bool
	// if custom backoff is set, use it instead of other options
	if len(opts.customBackoff) > 0 {
		if opts.attempts != 0 {
			return fmt.Errorf("cannot use custom backoff intervals when attempts are set")
		}
		for i, interval := range opts.customBackoff {
			select {
			case <-opts.cancel:
				return nil
			case <-time.After(interval):
			}
			shouldContinue, err = f(i)
			if !shouldContinue {
				return err
			}
		}
		return err
	}

	// set default options
	if opts.initialInterval == 0 {
		opts.initialInterval = 1 * time.Second
	}
	if opts.factor == 0 {
		opts.factor = 2
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = 1 * time.Minute
	}
	if opts.attempts == 0 {
		return fmt.Errorf("retry attempts have to be set when not using custom backoff intervals")
	}
	interval := opts.initialInterval
	for i := 0; ; i++ {
		if i == 0 && opts.disableInitialExecution {
			time.Sleep(interval)
			continue
		}
		shouldContinue, err = f(i)
		if !shouldContinue {
			return err
		}
		if opts.attempts > 0 && i >= opts.attempts-1 {
			break
		}
		select {
		case <-opts.cancel:
			return nil
		case <-time.After(interval):
		}
		interval = time.Duration(float64(interval) * opts.factor)
		if interval >= opts.maxInterval {
			interval = opts.maxInterval
		}
	}
	return err
}

func (c *pullConsumer) getSubscription(id string) (*pullSubscription, bool) {
	c.Lock()
	defer c.Unlock()
	sub, ok := c.subscriptions[id]
	return sub, ok
}
