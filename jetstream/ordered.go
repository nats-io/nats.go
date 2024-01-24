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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	orderedConsumer struct {
		jetStream         *jetStream
		cfg               *OrderedConsumerConfig
		stream            string
		currentConsumer   *pullConsumer
		cursor            cursor
		namePrefix        string
		serial            int
		consumerType      consumerType
		doReset           chan struct{}
		resetInProgress   uint32
		userErrHandler    ConsumeErrHandlerFunc
		stopAfter         int
		stopAfterMsgsLeft chan int
		withStopAfter     bool
		runningFetch      *fetchResult
		sync.Mutex
	}

	orderedSubscription struct {
		consumer *orderedConsumer
		opts     []PullMessagesOpt
		done     chan struct{}
		closed   uint32
	}

	cursor struct {
		streamSeq  uint64
		deliverSeq uint64
	}

	consumerType int
)

const (
	consumerTypeNotSet consumerType = iota
	consumerTypeConsume
	consumerTypeFetch
)

var errOrderedSequenceMismatch = errors.New("sequence mismatch")

// Consume can be used to continuously receive messages and handle them
// with the provided callback function. Consume cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Consume] for more details.
func (c *orderedConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	if (c.consumerType == consumerTypeNotSet || c.consumerType == consumerTypeConsume) && c.currentConsumer == nil {
		err := c.reset()
		if err != nil {
			return nil, err
		}
	} else if c.consumerType == consumerTypeConsume && c.currentConsumer != nil {
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	if c.consumerType == consumerTypeFetch {
		return nil, ErrOrderConsumerUsedAsFetch
	}
	c.consumerType = consumerTypeConsume
	consumeOpts, err := parseConsumeOpts(true, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	c.userErrHandler = consumeOpts.ErrHandler
	opts = append(opts, ConsumeErrHandler(c.errHandler(c.serial)))
	if consumeOpts.StopAfter > 0 {
		c.withStopAfter = true
		c.stopAfter = consumeOpts.StopAfter
	}
	c.stopAfterMsgsLeft = make(chan int, 1)
	if c.stopAfter > 0 {
		opts = append(opts, consumeStopAfterNotify(c.stopAfter, c.stopAfterMsgsLeft))
	}
	sub := &orderedSubscription{
		consumer: c,
		done:     make(chan struct{}, 1),
	}
	internalHandler := func(serial int) func(msg Msg) {
		return func(msg Msg) {
			// handler is a noop if message was delivered for a consumer with different serial
			if serial != c.serial {
				return
			}
			meta, err := msg.Metadata()
			if err != nil {
				sub, ok := c.currentConsumer.getSubscription("")
				if !ok {
					return
				}
				c.errHandler(serial)(sub, err)
				return
			}
			dseq := meta.Sequence.Consumer
			if dseq != c.cursor.deliverSeq+1 {
				sub, ok := c.currentConsumer.getSubscription("")
				if !ok {
					return
				}
				c.errHandler(serial)(sub, errOrderedSequenceMismatch)
				return
			}
			c.cursor.deliverSeq = dseq
			c.cursor.streamSeq = meta.Sequence.Stream
			handler(msg)
		}
	}

	_, err = c.currentConsumer.Consume(internalHandler(c.serial), opts...)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-c.doReset:
				if err := c.reset(); err != nil {
					sub, ok := c.currentConsumer.getSubscription("")
					if !ok {
						return
					}
					c.errHandler(c.serial)(sub, err)
				}
				if c.withStopAfter {
					select {
					case c.stopAfter = <-c.stopAfterMsgsLeft:
					default:
					}
					if c.stopAfter <= 0 {
						sub.Stop()
						return
					}
				}
				if c.stopAfter > 0 {
					opts = opts[:len(opts)-2]
				} else {
					opts = opts[:len(opts)-1]
				}

				// overwrite the previous err handler to use the new serial
				opts = append(opts, ConsumeErrHandler(c.errHandler(c.serial)))
				if c.withStopAfter {
					opts = append(opts, consumeStopAfterNotify(c.stopAfter, c.stopAfterMsgsLeft))
				}
				if _, err := c.currentConsumer.Consume(internalHandler(c.serial), opts...); err != nil {
					sub, ok := c.currentConsumer.getSubscription("")
					if !ok {
						return
					}
					c.errHandler(c.serial)(sub, err)
				}
			case <-sub.done:
				return
			case msgsLeft, ok := <-c.stopAfterMsgsLeft:
				if !ok {
					close(sub.done)
				}
				c.stopAfter = msgsLeft
				return
			}
		}
	}()
	return sub, nil
}

func (c *orderedConsumer) errHandler(serial int) func(cc ConsumeContext, err error) {
	return func(cc ConsumeContext, err error) {
		c.Lock()
		defer c.Unlock()
		if c.userErrHandler != nil && !errors.Is(err, errOrderedSequenceMismatch) {
			c.userErrHandler(cc, err)
		}
		if errors.Is(err, ErrNoHeartbeat) ||
			errors.Is(err, errOrderedSequenceMismatch) ||
			errors.Is(err, ErrConsumerDeleted) ||
			errors.Is(err, ErrConsumerNotFound) {
			// only reset if serial matches the current consumer serial and there is no reset in progress
			if serial == c.serial && atomic.LoadUint32(&c.resetInProgress) == 0 {
				atomic.StoreUint32(&c.resetInProgress, 1)
				c.doReset <- struct{}{}
			}
		}
	}
}

// Messages returns MessagesContext, allowing continuously iterating
// over messages on a stream. Messages cannot be used concurrently
// when using ordered consumer.
//
// See [Consumer.Messages] for more details.
func (c *orderedConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	if (c.consumerType == consumerTypeNotSet || c.consumerType == consumerTypeConsume) && c.currentConsumer == nil {
		err := c.reset()
		if err != nil {
			return nil, err
		}
	} else if c.consumerType == consumerTypeConsume && c.currentConsumer != nil {
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	if c.consumerType == consumerTypeFetch {
		return nil, ErrOrderConsumerUsedAsFetch
	}
	c.consumerType = consumerTypeConsume
	consumeOpts, err := parseMessagesOpts(true, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	opts = append(opts, WithMessagesErrOnMissingHeartbeat(true))
	c.stopAfterMsgsLeft = make(chan int, 1)
	if consumeOpts.StopAfter > 0 {
		c.withStopAfter = true
		c.stopAfter = consumeOpts.StopAfter
	}
	c.userErrHandler = consumeOpts.ErrHandler
	if c.stopAfter > 0 {
		opts = append(opts, messagesStopAfterNotify(c.stopAfter, c.stopAfterMsgsLeft))
	}
	_, err = c.currentConsumer.Messages(opts...)
	if err != nil {
		return nil, err
	}

	sub := &orderedSubscription{
		consumer: c,
		opts:     opts,
		done:     make(chan struct{}, 1),
	}

	return sub, nil
}

func (s *orderedSubscription) Next() (Msg, error) {
	for {
		currentConsumer := s.consumer.currentConsumer
		sub, ok := currentConsumer.getSubscription("")
		if !ok {
			return nil, ErrMsgIteratorClosed
		}
		msg, err := sub.Next()
		if err != nil {
			if errors.Is(err, ErrMsgIteratorClosed) {
				s.Stop()
				return nil, err
			}
			if s.consumer.withStopAfter {
				select {
				case s.consumer.stopAfter = <-s.consumer.stopAfterMsgsLeft:
				default:
				}
				if s.consumer.stopAfter <= 0 {
					s.Stop()
					return nil, ErrMsgIteratorClosed
				}
				s.opts[len(s.opts)-1] = StopAfter(s.consumer.stopAfter)
			}
			if err := s.consumer.reset(); err != nil {
				return nil, err
			}
			_, err := s.consumer.currentConsumer.Messages(s.opts...)
			if err != nil {
				return nil, err
			}
			continue
		}
		meta, err := msg.Metadata()
		if err != nil {
			s.consumer.errHandler(s.consumer.serial)(sub, err)
			continue
		}
		serial := serialNumberFromConsumer(meta.Consumer)
		dseq := meta.Sequence.Consumer
		if dseq != s.consumer.cursor.deliverSeq+1 {
			s.consumer.errHandler(serial)(sub, errOrderedSequenceMismatch)
			continue
		}
		s.consumer.cursor.deliverSeq = dseq
		s.consumer.cursor.streamSeq = meta.Sequence.Stream
		return msg, nil
	}
}

func (s *orderedSubscription) Stop() {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return
	}
	sub, ok := s.consumer.currentConsumer.getSubscription("")
	if !ok {
		return
	}
	s.consumer.currentConsumer.Lock()
	defer s.consumer.currentConsumer.Unlock()
	sub.Stop()
	close(s.done)
}

func (s *orderedSubscription) Drain() {
	if !atomic.CompareAndSwapUint32(&s.closed, 0, 1) {
		return
	}
	sub, ok := s.consumer.currentConsumer.getSubscription("")
	if !ok {
		return
	}
	s.consumer.currentConsumer.Lock()
	defer s.consumer.currentConsumer.Unlock()
	sub.Drain()
	close(s.done)
}

// Fetch is used to retrieve up to a provided number of messages from a
// stream. This method will always send a single request and wait until
// either all messages are retrieved or request times out.
//
// It is not efficient to use Fetch with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	if c.consumerType == consumerTypeConsume {
		return nil, ErrOrderConsumerUsedAsConsume
	}
	c.currentConsumer.Lock()
	if c.runningFetch != nil {
		if !c.runningFetch.done {
			c.currentConsumer.Unlock()
			return nil, ErrOrderedConsumerConcurrentRequests
		}
		c.cursor.streamSeq = c.runningFetch.sseq
	}
	c.currentConsumer.Unlock()
	c.consumerType = consumerTypeFetch
	err := c.reset()
	if err != nil {
		return nil, err
	}
	msgs, err := c.currentConsumer.Fetch(batch, opts...)
	if err != nil {
		return nil, err
	}
	c.runningFetch = msgs.(*fetchResult)
	return msgs, nil
}

// FetchBytes is used to retrieve up to a provided bytes from the
// stream. This method will always send a single request and wait until
// provided number of bytes is exceeded or request times out.
//
// It is not efficient to use FetchBytes with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error) {
	if c.consumerType == consumerTypeConsume {
		return nil, ErrOrderConsumerUsedAsConsume
	}
	if c.runningFetch != nil {
		if !c.runningFetch.done {
			return nil, ErrOrderedConsumerConcurrentRequests
		}
		c.cursor.streamSeq = c.runningFetch.sseq
	}
	c.consumerType = consumerTypeFetch
	err := c.reset()
	if err != nil {
		return nil, err
	}
	msgs, err := c.currentConsumer.FetchBytes(maxBytes, opts...)
	if err != nil {
		return nil, err
	}
	c.runningFetch = msgs.(*fetchResult)
	return msgs, nil
}

// FetchNoWait is used to retrieve up to a provided number of messages
// from a stream. This method will always send a single request and
// immediately return up to a provided number of messages or wait until
// at least one message is available or request times out.
//
// It is not efficient to use FetchNoWait with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	if c.consumerType == consumerTypeConsume {
		return nil, ErrOrderConsumerUsedAsConsume
	}
	if c.runningFetch != nil && !c.runningFetch.done {
		return nil, ErrOrderedConsumerConcurrentRequests
	}
	c.consumerType = consumerTypeFetch
	err := c.reset()
	if err != nil {
		return nil, err
	}
	return c.currentConsumer.FetchNoWait(batch)
}

// Next is used to retrieve the next message from the stream. This
// method will block until the message is retrieved or timeout is
// reached.
//
// It is not efficient to use Next with on an ordered consumer, as it will
// reset the consumer for each subsequent Fetch call.
// Consider using [Consumer.Consume] or [Consumer.Messages] instead.
func (c *orderedConsumer) Next(opts ...FetchOpt) (Msg, error) {
	res, err := c.Fetch(1, opts...)
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

func serialNumberFromConsumer(name string) int {
	if len(name) == 0 {
		return 0
	}
	serial, err := strconv.Atoi(name[len(name)-1:])
	if err != nil {
		return 0
	}
	return serial
}

func (c *orderedConsumer) reset() error {
	c.Lock()
	defer c.Unlock()
	defer atomic.StoreUint32(&c.resetInProgress, 0)
	if c.currentConsumer != nil {
		sub, ok := c.currentConsumer.getSubscription("")
		c.currentConsumer.Lock()
		if ok {
			sub.Stop()
		}
		consName := c.currentConsumer.CachedInfo().Name
		c.currentConsumer.Unlock()
		var err error
		for i := 0; ; i++ {
			if c.cfg.MaxResetAttempts > 0 && i == c.cfg.MaxResetAttempts {
				return fmt.Errorf("%w: maximum number of delete attempts reached: %s", ErrOrderedConsumerReset, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = c.jetStream.DeleteConsumer(ctx, c.stream, consName)
			cancel()
			if err != nil {
				if errors.Is(err, ErrConsumerNotFound) {
					break
				}
				if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return err
			}
			break
		}
	}
	seq := c.cursor.streamSeq + 1
	c.cursor.deliverSeq = 0
	consumerConfig := c.getConsumerConfigForSeq(seq)

	var err error
	var cons Consumer
	for i := 0; ; i++ {
		if c.cfg.MaxResetAttempts > 0 && i == c.cfg.MaxResetAttempts {
			return fmt.Errorf("%w: maximum number of create consumer attempts reached: %s", ErrOrderedConsumerReset, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cons, err = c.jetStream.CreateOrUpdateConsumer(ctx, c.stream, *consumerConfig)
		if err != nil {
			if errors.Is(err, ErrConsumerNotFound) {
				cancel()
				break
			}
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				cancel()
				continue
			}
			cancel()
			return err
		}
		cancel()
		break
	}
	c.currentConsumer = cons.(*pullConsumer)
	return nil
}

func (c *orderedConsumer) getConsumerConfigForSeq(seq uint64) *ConsumerConfig {
	c.serial++
	name := fmt.Sprintf("%s_%d", c.namePrefix, c.serial)
	cfg := &ConsumerConfig{
		Name:              name,
		DeliverPolicy:     DeliverByStartSequencePolicy,
		OptStartSeq:       seq,
		AckPolicy:         AckNonePolicy,
		InactiveThreshold: 5 * time.Minute,
		Replicas:          1,
		HeadersOnly:       c.cfg.HeadersOnly,
		MemoryStorage:     true,
	}
	if len(c.cfg.FilterSubjects) == 1 {
		cfg.FilterSubject = c.cfg.FilterSubjects[0]
	} else {
		cfg.FilterSubjects = c.cfg.FilterSubjects
	}

	if seq != c.cfg.OptStartSeq+1 {
		return cfg
	}

	// initial request, some options may be modified at that point
	cfg.DeliverPolicy = c.cfg.DeliverPolicy
	if c.cfg.DeliverPolicy == DeliverLastPerSubjectPolicy ||
		c.cfg.DeliverPolicy == DeliverLastPolicy ||
		c.cfg.DeliverPolicy == DeliverNewPolicy ||
		c.cfg.DeliverPolicy == DeliverAllPolicy {

		cfg.OptStartSeq = 0
	}

	if cfg.DeliverPolicy == DeliverLastPerSubjectPolicy && len(c.cfg.FilterSubjects) == 0 {
		cfg.FilterSubjects = []string{">"}
	}
	if c.cfg.OptStartTime != nil {
		cfg.OptStartSeq = 0
		cfg.DeliverPolicy = DeliverByStartTimePolicy
		cfg.OptStartTime = c.cfg.OptStartTime
	}
	if c.cfg.InactiveThreshold != 0 {
		cfg.InactiveThreshold = c.cfg.InactiveThreshold
	}

	return cfg
}

func consumeStopAfterNotify(numMsgs int, msgsLeftAfterStop chan int) PullConsumeOpt {
	return pullOptFunc(func(opts *consumeOpts) error {
		opts.StopAfter = numMsgs
		opts.stopAfterMsgsLeft = msgsLeftAfterStop
		return nil
	})
}

func messagesStopAfterNotify(numMsgs int, msgsLeftAfterStop chan int) PullMessagesOpt {
	return pullOptFunc(func(opts *consumeOpts) error {
		opts.StopAfter = numMsgs
		opts.stopAfterMsgsLeft = msgsLeftAfterStop
		return nil
	})
}

// Info returns information about the ordered consumer.
// Note that this method will fetch the latest instance of the
// consumer from the server, which can be deleted by the library at any time.
func (c *orderedConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	c.Lock()
	defer c.Unlock()
	if c.currentConsumer == nil {
		return nil, ErrOrderedConsumerNotCreated
	}
	infoSubject := apiSubj(c.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, c.stream, c.currentConsumer.name))
	var resp consumerInfoResponse

	if _, err := c.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}
	if resp.Error == nil && resp.ConsumerInfo == nil {
		return nil, ErrConsumerNotFound
	}

	c.currentConsumer.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// CachedInfo returns cached information about the consumer currently
// used by the ordered consumer. Cached info will be updated on every call
// to [Consumer.Info] or on consumer reset.
func (c *orderedConsumer) CachedInfo() *ConsumerInfo {
	c.Lock()
	defer c.Unlock()
	if c.currentConsumer == nil {
		return nil
	}
	return c.currentConsumer.info
}
