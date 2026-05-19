// Copyright 2026 The NATS Authors
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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// defaultOrderedPushHeartbeat mirrors the legacy nats.OrderedConsumer() SubOpt
// default (orderedHeartbeatsInterval in js.go).
const defaultOrderedPushHeartbeat = 5 * time.Second

type orderedPushConsumer struct {
	sync.Mutex
	js             *jetStream
	cfg            *OrderedPushConsumerConfig
	stream         string
	namePrefix     string
	deliverSubject string
	idleHeartbeat  time.Duration

	// info reflects the currently active underlying server consumer.
	info *ConsumerInfo

	cursor  cursor
	serial  int
	started atomic.Bool

	// sub is created once on first Consume and reused across resets.
	sub *orderedPushSubscription
}

type orderedPushSubscription struct {
	sync.Mutex
	consumer     *orderedPushConsumer
	natsSub      *nats.Subscription
	userHandler  MessageHandler
	consumeOpts  *pushConsumeOpts
	hbMonitor    *hbMonitor
	errs         chan error
	doReset      chan struct{}
	done         chan struct{}
	closedCh     chan struct{}
	closedChOnce sync.Once
	closed       atomic.Bool
	connStatusCh chan nats.Status
}

// newOrderedPushConsumer constructs an orderedPushConsumer. The underlying
// server-side consumer is not created until Consume is called.
func newOrderedPushConsumer(js *jetStream, stream string, cfg OrderedPushConsumerConfig) *orderedPushConsumer {
	namePrefix := cfg.NamePrefix
	if namePrefix == "" {
		namePrefix = nuid.Next()
	}
	hb := cfg.IdleHeartbeat
	if hb == 0 {
		hb = defaultOrderedPushHeartbeat
	}
	return &orderedPushConsumer{
		js:            js,
		cfg:           &cfg,
		stream:        stream,
		namePrefix:    namePrefix,
		idleHeartbeat: hb,
	}
}

// consumerConfig builds the underlying ConsumerConfig for the next server
// consumer. It increments the serial counter (so callers see the bumped
// value via c.serial) and uses c.cursor.streamSeq + 1 as the start sequence
// once delivery has begun.
//
// This intentionally mirrors orderedConsumer.getConsumerConfig in ordered.go
// for parity. Caller MUST hold c (the wrapper's mutex).
func (c *orderedPushConsumer) consumerConfig() *ConsumerConfig {
	c.serial++
	// Reset the delivery-sequence cursor atomically with the serial bump.
	// The new server consumer's deliveries restart at consumer-seq 1, so the
	// expected next deliverSeq must be 0 by the time any of its messages
	// reach handleMessage. Doing this after upsertConsumer returns creates a
	// race where the server's first delivery arrives before the cursor is
	// reset and gets dropped as "out of order".
	c.cursor.deliverSeq = 0

	var nextSeq uint64
	if c.cursor.streamSeq == 0 {
		// no message delivered yet
		if c.cfg.OptStartSeq != 0 {
			nextSeq = c.cfg.OptStartSeq
		} else {
			nextSeq = 1
		}
	} else {
		nextSeq = c.cursor.streamSeq + 1
	}

	inactive := c.cfg.InactiveThreshold
	if inactive == 0 {
		inactive = 5 * time.Minute
	}

	cfg := &ConsumerConfig{
		Name:              fmt.Sprintf("%s_%d", c.namePrefix, c.serial),
		DeliverPolicy:     DeliverByStartSequencePolicy,
		OptStartSeq:       nextSeq,
		AckPolicy:         AckNonePolicy,
		MaxDeliver:        1,
		AckWait:           22 * time.Hour, // matches legacy ordered SubOpt
		Replicas:          1,
		MemoryStorage:     true,
		FlowControl:       true,
		IdleHeartbeat:     c.idleHeartbeat,
		HeadersOnly:       c.cfg.HeadersOnly,
		Metadata:          c.cfg.Metadata,
		InactiveThreshold: inactive,
		ReplayPolicy:      c.cfg.ReplayPolicy,
		DeliverSubject:    c.deliverSubject,
	}

	if len(c.cfg.FilterSubjects) == 1 {
		cfg.FilterSubject = c.cfg.FilterSubjects[0]
	} else {
		cfg.FilterSubjects = c.cfg.FilterSubjects
	}

	// On the very first create, honor the user's DeliverPolicy.
	if c.cursor.streamSeq != 0 {
		return cfg
	}
	cfg.DeliverPolicy = c.cfg.DeliverPolicy
	switch c.cfg.DeliverPolicy {
	case DeliverLastPerSubjectPolicy, DeliverLastPolicy, DeliverNewPolicy, DeliverAllPolicy:
		cfg.OptStartSeq = 0
	case DeliverByStartTimePolicy:
		cfg.OptStartSeq = 0
		cfg.OptStartTime = c.cfg.OptStartTime
	default:
		cfg.OptStartSeq = c.cfg.OptStartSeq
	}
	if cfg.DeliverPolicy == DeliverLastPerSubjectPolicy && len(c.cfg.FilterSubjects) == 0 {
		cfg.FilterSubjects = []string{">"}
	}
	return cfg
}

// Info fetches the current ConsumerInfo from the server for the currently
// active underlying consumer. Returns ErrOrderedConsumerNotCreated if
// Consume has not yet been called.
func (c *orderedPushConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	c.Lock()
	name := ""
	if c.info != nil {
		name = c.info.Name
	}
	c.Unlock()
	if name == "" {
		return nil, ErrOrderedConsumerNotCreated
	}

	ctx, cancel := c.js.wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	infoSubject := fmt.Sprintf(apiConsumerInfoT, c.stream, name)
	var resp consumerInfoResponse
	if _, err := c.js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	if resp.ConsumerInfo == nil {
		return nil, ErrConsumerNotFound
	}
	c.Lock()
	c.info = resp.ConsumerInfo
	c.Unlock()
	return resp.ConsumerInfo, nil
}

// CachedInfo returns the most recent ConsumerInfo for the underlying server
// consumer. Returns nil before the first Consume call.
func (c *orderedPushConsumer) CachedInfo() *ConsumerInfo {
	c.Lock()
	defer c.Unlock()
	return c.info
}

// Consume starts delivering messages from the stream in order to the
// provided handler. Concurrent calls return ErrOrderedConsumerConcurrentRequests.
// The first call creates the underlying server consumer and attaches a NATS
// subscription on a deliver subject; subsequent resets recreate only the
// server consumer.
func (c *orderedPushConsumer) Consume(handler MessageHandler, opts ...PushConsumeOpt) (ConsumeContext, error) {
	if handler == nil {
		return nil, ErrHandlerRequired
	}
	consumeOpts := &pushConsumeOpts{}
	for _, opt := range opts {
		if err := opt.configurePushConsume(consumeOpts); err != nil {
			return nil, err
		}
	}

	if err := c.validateConfig(); err != nil {
		return nil, err
	}

	if !c.started.CompareAndSwap(false, true) {
		return nil, ErrOrderedConsumerConcurrentRequests
	}

	c.Lock()
	if c.deliverSubject == "" {
		if c.cfg.DeliverSubject != "" {
			c.deliverSubject = c.cfg.DeliverSubject
		} else {
			c.deliverSubject = c.js.conn.NewInbox()
		}
	}

	sub := &orderedPushSubscription{
		consumer:     c,
		userHandler:  handler,
		consumeOpts:  consumeOpts,
		errs:         make(chan error, 1),
		doReset:      make(chan struct{}, 1),
		done:         make(chan struct{}),
		connStatusCh: c.js.conn.StatusChanged(nats.CONNECTED, nats.RECONNECTING),
	}
	c.sub = sub
	c.Unlock()

	// Create the first underlying server consumer.
	if err := c.createUnderlying(); err != nil {
		c.started.Store(false)
		c.Lock()
		c.sub = nil
		c.Unlock()
		return nil, err
	}

	// Attach the NATS subscription on the deliver subject once. The
	// subscription persists across resets — only the server-side consumer
	// is recreated. Late stragglers from the previous server consumer that
	// land on this subscription are filtered out by the serial check in
	// handleMessage.
	natsSub, err := c.js.conn.Subscribe(c.deliverSubject, sub.handleMessage(handler))
	if err != nil {
		c.Lock()
		name := ""
		if c.info != nil {
			name = c.info.Name
		}
		c.Unlock()
		if name != "" {
			delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = deleteConsumer(delCtx, c.js, c.stream, name)
			cancel()
		}
		c.started.Store(false)
		c.Lock()
		c.sub = nil
		c.Unlock()
		return nil, err
	}

	sub.Lock()
	sub.natsSub = natsSub
	sub.hbMonitor = sub.scheduleHeartbeatCheck(c.idleHeartbeat)
	sub.Unlock()

	natsSub.SetClosedHandler(func(string) {
		sub.Lock()
		defer sub.Unlock()
		if sub.closedCh == nil {
			sub.closedCh = make(chan struct{})
		}
		sub.closedChOnce.Do(func() { close(sub.closedCh) })
	})

	go sub.manage()

	return sub, nil
}

// createUnderlying creates a fresh server-side consumer matching the current
// cursor + serial state, stores it on c.info, and returns the result.
func (c *orderedPushConsumer) createUnderlying() error {
	c.Lock()
	cfg := c.consumerConfig()
	c.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := upsertConsumer(ctx, c.js, c.stream, *cfg, consumerActionCreate)
	if err != nil {
		return err
	}
	c.Lock()
	c.info = resp.ConsumerInfo
	c.Unlock()
	return nil
}

// validateConfig rejects configurations incompatible with ordered push
// semantics. Returns errors wrapped in ErrInvalidOption.
func (c *orderedPushConsumer) validateConfig() error {
	if c.cfg.OptStartSeq != 0 && c.cfg.OptStartTime != nil {
		return fmt.Errorf("%w: OptStartSeq and OptStartTime are mutually exclusive", ErrInvalidOption)
	}
	return nil
}

// reset creates a fresh deliver subject, a fresh server consumer at
// cursor.streamSeq+1, and a fresh NATS subscription, then unsubscribes from
// the old subject. The previous server consumer is left to be cleaned up by
// its InactiveThreshold (matching the pull-side ordered consumer's
// behavior — synchronous delete-on-reset piles up concurrent CONSUMER.DELETE
// calls under high-frequency resets and slows CONSUMER.CREATE handling).
// Stop()/Drain() still delete the current consumer best-effort.
func (c *orderedPushConsumer) reset() error {
	maxAttempts := c.cfg.MaxResetAttempts
	if maxAttempts == 0 {
		maxAttempts = -1
	}

	backoff := backoffOpts{
		attempts:        maxAttempts,
		initialInterval: time.Second,
		factor:          2,
		maxInterval:     10 * time.Second,
		cancel:          c.sub.done,
	}
	return retryWithBackoff(func(_ int) (bool, error) {
		if c.sub.closed.Load() {
			return false, errOrderedConsumerClosed
		}
		if err := c.createUnderlying(); err != nil {
			return true, err
		}
		return false, nil
	}, backoff)
}

// handleMessage returns the nats.MsgHandler for the deliver subject. It
// performs serial + sequence checks, replies to flow-control, treats 409
// consumer-deleted as a reset trigger, and ignores other status messages.
func (s *orderedPushSubscription) handleMessage(userHandler MessageHandler) nats.MsgHandler {
	return func(msg *nats.Msg) {
		s.Lock()
		if s.hbMonitor != nil {
			s.hbMonitor.Stop()
		}
		s.Unlock()
		defer func() {
			s.Lock()
			if s.hbMonitor != nil {
				s.hbMonitor.Reset(2 * s.consumer.idleHeartbeat)
			}
			s.Unlock()
		}()

		status := msg.Header.Get("Status")
		descr := msg.Header.Get("Description")
		if status != "" {
			s.handleStatusMsg(msg, status, descr)
			return
		}

		meta, err := msg.Metadata()
		if err != nil {
			// Cannot determine sequence; surface via err handler but do not reset.
			if s.consumeOpts.ErrHandler != nil {
				s.consumeOpts.ErrHandler(s, err)
			}
			return
		}

		// Drop late stragglers from a previous server consumer (after reset).
		// The serial is parsed from the consumer name suffix; cur serial is
		// bumped inside consumerConfig() atomically with cursor reset.
		msgSerial := serialNumberFromConsumer(meta.Consumer)
		s.consumer.Lock()
		curSerial := s.consumer.serial
		s.consumer.Unlock()
		if msgSerial != curSerial {
			return
		}

		dseq := meta.Sequence.Consumer
		s.consumer.Lock()
		expected := s.consumer.cursor.deliverSeq + 1
		s.consumer.Unlock()
		if dseq != expected {
			s.triggerReset()
			return
		}

		s.consumer.Lock()
		s.consumer.cursor.deliverSeq = dseq
		s.consumer.cursor.streamSeq = meta.Sequence.Stream
		s.consumer.Unlock()

		userHandler(s.consumer.js.toJSMsg(msg))
	}
}

// handleStatusMsg reacts to server control / status messages. Flow-control
// requests are replied to, idle heartbeats are no-ops (the deferred timer
// reset in handleMessage already extends the deadline), and 409
// consumer-deleted triggers a reset.
func (s *orderedPushSubscription) handleStatusMsg(msg *nats.Msg, status, description string) {
	switch status {
	case statusControlMsg:
		switch strings.ToLower(description) {
		case idleHeartbeatDescr:
			return
		case fcRequestDescr:
			if err := msg.Respond(nil); err != nil && s.consumeOpts.ErrHandler != nil {
				s.consumeOpts.ErrHandler(s, err)
			}
			return
		}
	case statusConflict:
		if strings.Contains(strings.ToLower(description), consumerDeleted) {
			s.triggerReset()
			return
		}
		if strings.Contains(strings.ToLower(description), leadershipChange) {
			s.triggerReset()
			return
		}
	}
}

// triggerReset signals manage() to recreate the underlying server consumer.
// Coalescing is done by the buffered (size-1) doReset channel itself: if a
// signal is already queued, additional triggers are dropped silently. A
// stale flag-based "resetting" guard caused a race where triggers fired
// during the brief window between reset() returning and the guard being
// cleared were lost, leaving the consumer wedged on a sequence gap with
// no further triggers coming. The channel-only approach may produce one
// extra (no-op) reset after the real recovery, which self-stabilizes.
func (s *orderedPushSubscription) triggerReset() {
	if s.closed.Load() {
		return
	}
	select {
	case s.doReset <- struct{}{}:
	default:
		// already queued
	}
}

// manage runs the reset / heartbeat / reconnect loop until Stop or Drain.
func (s *orderedPushSubscription) manage() {
	isConnected := true
	for {
		if s.closed.Load() {
			return
		}
		select {
		case <-s.doReset:
			if err := s.consumer.reset(); err != nil {
				if errors.Is(err, errOrderedConsumerClosed) {
					return
				}
				if s.consumeOpts.ErrHandler != nil {
					s.consumeOpts.ErrHandler(s, err)
				}
			}
		case status, ok := <-s.connStatusCh:
			if !ok {
				continue
			}
			switch status {
			case nats.RECONNECTING:
				s.Lock()
				if s.hbMonitor != nil {
					s.hbMonitor.Stop()
				}
				s.Unlock()
				isConnected = false
			case nats.CONNECTED:
				if !isConnected {
					isConnected = true
					s.Lock()
					if s.hbMonitor != nil {
						s.hbMonitor.Reset(2 * s.consumer.idleHeartbeat)
					}
					s.Unlock()
					// On reconnect, recreate the server consumer to recover any
					// messages potentially missed during the disconnect.
					s.triggerReset()
				}
			}
		case err := <-s.errs:
			if errors.Is(err, ErrNoHeartbeat) {
				// Mirror legacy js.go:2363 — only react if currently connected.
				// A racing RECONNECTING transition would otherwise drive a doomed
				// reset attempt against a disconnected NATS connection.
				if s.consumer.js.conn.Status() != nats.CONNECTED {
					continue
				}
				s.triggerReset()
				continue
			}
			if s.consumeOpts.ErrHandler != nil {
				s.consumeOpts.ErrHandler(s, err)
			}
		case <-s.done:
			return
		}
	}
}

// scheduleHeartbeatCheck arms a timer that pushes ErrNoHeartbeat onto s.errs
// when no heartbeat / message arrives within 2*dur.
func (s *orderedPushSubscription) scheduleHeartbeatCheck(dur time.Duration) *hbMonitor {
	if dur == 0 {
		return nil
	}
	return &hbMonitor{
		timer: time.AfterFunc(2*dur, func() {
			select {
			case s.errs <- ErrNoHeartbeat:
			default:
			}
		}),
	}
}

// Stop unsubscribes from the deliver subject, deletes the underlying server
// consumer (best effort), and signals Closed.
func (s *orderedPushSubscription) Stop() {
	s.shutdown(false)
}

// Drain processes any messages already in the NATS subscription buffer, then
// performs the same teardown as Stop.
func (s *orderedPushSubscription) Drain() {
	s.shutdown(true)
}

func (s *orderedPushSubscription) shutdown(drain bool) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.consumer.started.Store(false)
	close(s.done)

	s.Lock()
	natsSub := s.natsSub
	if s.hbMonitor != nil {
		s.hbMonitor.Stop()
	}
	s.Unlock()

	if natsSub != nil {
		if drain {
			_ = natsSub.Drain()
		} else {
			_ = natsSub.Unsubscribe()
		}
	}

	s.consumer.Lock()
	name := ""
	if s.consumer.info != nil {
		name = s.consumer.info.Name
	}
	s.consumer.Unlock()
	if name != "" {
		go func() {
			delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = deleteConsumer(delCtx, s.consumer.js, s.consumer.stream, name)
		}()
	}
}

// Closed returns a channel that is closed when the subscription has fully
// stopped or drained. Idempotent: repeated calls return the same channel,
// and once closed it stays closed.
func (s *orderedPushSubscription) Closed() <-chan struct{} {
	s.Lock()
	defer s.Unlock()
	if s.closedCh == nil {
		s.closedCh = make(chan struct{})
	}
	if s.natsSub != nil && !s.natsSub.IsValid() {
		s.closedChOnce.Do(func() { close(s.closedCh) })
	}
	return s.closedCh
}
