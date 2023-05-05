package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	orderedConsumer struct {
		jetStream       *jetStream
		cfg             *OrderedConsumerConfig
		stream          string
		currentConsumer *pullConsumer
		cursor          cursor
		namePrefix      string
		serial          int
		consumerType    consumerType
		doReset         chan struct{}
		resetInProgress uint32
		userErrHandler  ConsumeErrHandlerFunc
		runningFetch    *fetchResult
	}

	orderedSubscription struct {
		consumer *orderedConsumer
		opts     []PullMessagesOpt
		done     chan struct{}
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

// Consume can be used to continuously receive messages and handle them with the provided callback function
func (c *orderedConsumer) Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error) {
	if c.consumerType == consumerTypeNotSet || c.consumerType == consumerTypeConsume && c.currentConsumer == nil {
		c.consumerType = consumerTypeConsume
		err := c.reset()
		if err != nil {
			return nil, err
		}
	}
	if c.consumerType == consumerTypeFetch {
		return nil, fmt.Errorf("ordered consumer initialized as fetch")
	}
	consumeOpts, err := parseConsumeOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	c.userErrHandler = consumeOpts.ErrHandler
	opts = append(opts, ConsumeErrHandler(c.errHandler(c.serial)))
	internalHandler := func(serial int) func(msg Msg) {
		return func(msg Msg) {
			// handler is a noop if message was delivered for a consumer with different serial
			if serial != c.serial {
				return
			}
			meta, err := msg.Metadata()
			if err != nil {
				c.errHandler(serial)(c.currentConsumer.subscription, err)
				return
			}
			dseq := meta.Sequence.Consumer
			if dseq != c.cursor.deliverSeq+1 {
				c.errHandler(serial)(c.currentConsumer.subscription, ErrOrderedSequenceMismatch)
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

	sub := &orderedSubscription{
		consumer: c,
		done:     make(chan struct{}, 1),
	}
	go func() {
		for {
			select {
			case <-c.doReset:
				if err := c.reset(); err != nil {
					c.errHandler(c.serial)(c.currentConsumer.subscription, err)
				}
				// overwrite the previous err handler to use the new serial
				opts[len(opts)-1] = ConsumeErrHandler(c.errHandler(c.serial))
				c.currentConsumer.Consume(internalHandler(c.serial), opts...)
			case <-sub.done:
				return
			}
		}
	}()
	return sub, nil
}

func (c *orderedConsumer) errHandler(serial int) func(cc ConsumeContext, err error) {
	return func(cc ConsumeContext, err error) {
		if c.userErrHandler != nil {
			c.userErrHandler(cc, err)
		}
		if errors.Is(err, ErrNoHeartbeat) ||
			errors.Is(err, ErrOrderedSequenceMismatch) ||
			errors.Is(err, ErrConsumerDeleted) {
			// only reset if serial matches the currect consumer serial and there is no reset in progress
			if serial == c.serial && atomic.LoadUint32(&c.resetInProgress) == 0 {
				atomic.StoreUint32(&c.resetInProgress, 1)
				c.doReset <- struct{}{}
			}

		}
	}
}

// Messages returns [MessagesContext], allowing continuously iterating over messages on a stream.
func (c *orderedConsumer) Messages(opts ...PullMessagesOpt) (MessagesContext, error) {
	if c.consumerType == consumerTypeNotSet {
		c.consumerType = consumerTypeConsume
		err := c.reset()
		if err != nil {
			return nil, err
		}
	}
	if c.consumerType == consumerTypeFetch {
		return nil, fmt.Errorf("ordered consumer initialized as fetch")
	}
	consumeOpts, err := parseMessagesOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidOption, err)
	}
	c.userErrHandler = consumeOpts.ErrHandler
	opts = append(opts, WithMessagesErrOnMissingHeartbeat(true))
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
	next := func() (Msg, error) {
		for {
			currentConsumer := s.consumer.currentConsumer
			msg, err := currentConsumer.subscription.Next()
			if err != nil {
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
				s.consumer.errHandler(s.consumer.serial)(currentConsumer.subscription, err)
				continue
			}
			serial := serialNumberFromConsumer(meta.Consumer)
			dseq := meta.Sequence.Consumer
			if dseq != s.consumer.cursor.deliverSeq+1 {
				s.consumer.errHandler(serial)(currentConsumer.subscription, ErrOrderedSequenceMismatch)
				continue
			}
			s.consumer.cursor.deliverSeq = dseq
			s.consumer.cursor.streamSeq = meta.Sequence.Stream
			return msg, nil
		}
	}
	return next()
}

func (s *orderedSubscription) Stop() {
	if s.consumer.currentConsumer == nil || s.consumer.currentConsumer.subscription == nil {
		return
	}
	s.consumer.currentConsumer.subscription.Stop()
	close(s.done)
}

// Fetch is used to retrieve up to a provided number of messages from a stream.
// This method will always send a single request and wait until either all messages are retreived
// or context reaches its deadline.
func (c *orderedConsumer) Fetch(batch int, opts ...FetchOpt) (MessageBatch, error) {
	if c.consumerType == consumerTypeConsume {
		return nil, fmt.Errorf("ordered consumer initialized as consume")
	}
	if c.runningFetch != nil && !c.runningFetch.done {
		return nil, fmt.Errorf("cannot run concurrent ordered Fetch requests")
	}
	c.consumerType = consumerTypeFetch
	err := c.reset()
	if err != nil {
		return nil, err
	}
	return c.currentConsumer.Fetch(batch, opts...)
}

// FetchNoWait is used to retrieve up to a provided number of messages from a stream.
// This method will always send a single request and immediately return up to a provided number of messages
func (c *orderedConsumer) FetchNoWait(batch int) (MessageBatch, error) {
	if c.consumerType == consumerTypeConsume {
		return nil, fmt.Errorf("ordered consumer initialized as consume")
	}
	if c.runningFetch != nil && !c.runningFetch.done {
		return nil, fmt.Errorf("cannot run concurrent ordered Fetch requests")
	}
	c.consumerType = consumerTypeFetch
	err := c.reset()
	if err != nil {
		return nil, err
	}
	return c.currentConsumer.FetchNoWait(batch)
}

func (c *orderedConsumer) Next(opts ...FetchOpt) (Msg, error) {
	res, err := c.Fetch(1, opts...)
	if err != nil {
		return nil, err
	}
	msg := <-res.Messages()
	if msg != nil {
		return msg, nil
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
	defer atomic.StoreUint32(&c.resetInProgress, 0)
	if c.currentConsumer != nil {
		// c.currentConsumer.subscription.Stop()
		var err error
		for i := 0; ; i++ {
			if c.cfg.MaxResetAttempts > 0 && i == c.cfg.MaxResetAttempts {
				return fmt.Errorf("%w: maximum number of delete attempts reached: %s", ErrOrderedConsumerReset, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = c.jetStream.DeleteConsumer(ctx, c.stream, c.currentConsumer.CachedInfo().Name)
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
		cons, err = c.jetStream.CreateConsumer(ctx, c.stream, *consumerConfig)
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
		FilterSubjects:    c.cfg.FilterSubjects,
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

func (c *orderedConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
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

	c.currentConsumer.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

func (c *orderedConsumer) CachedInfo() *ConsumerInfo {
	return c.currentConsumer.info
}
