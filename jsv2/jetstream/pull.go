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
	// MsgIterator supports iterating over a messages on a stream.
	MsgIterator interface {
		// Next retreives nest message on a stream. It will block until the next message is available.
		Next() (Msg, error)
		// Stop closes the iterator and cancels subscription.
		Stop()
	}

	ConsumerListener interface {
		Stop()
	}

	// MessageHandler is a handler function used as callback in `Subscribe()`
	MessageHandler func(msg Msg, err error)

	// ConsumerSubscribeOpt represent additional options used in `Subscribe()` for pull consumers
	ConsumerSubscribeOpt func(*pullRequest) error

	// ConsumerMessagesOpt represent additional options used in `Messages()` for pull consumers
	ConsumerMessagesOpt func(*pullRequest) error

	consumer struct {
		jetStream *jetStream
		stream    string
		durable   bool
		name      string
		info      *ConsumerInfo
		sync.Mutex
	}

	pullConsumer struct {
		consumer
		isSubscribed uint32
		errs         chan error
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

	msgs := make(chan *nats.Msg, 2*req.Batch)

	it := &messagesIter{
		consumer:         p,
		req:              req,
		done:             make(chan struct{}, 1),
		msgs:             msgs,
		fetchNext:        make(chan struct{}, 1),
		reconnected:      make(chan struct{}),
		fetchComplete:    make(chan struct{}, 1),
		reconnectHandler: p.jetStream.conn.Opts.ReconnectedCB,
	}
	if err := it.setupSubscription(msgs); err != nil {
		return nil, err
	}
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if it.reconnectHandler != nil {
			it.reconnectHandler(p.jetStream.conn)
		}
		it.reconnected <- struct{}{}
	})

	it.hbTimer = scheduleHeartbeatCheck(req.Heartbeat, p.errs)
	go func() {
		<-it.done
		it.cleanupSubscriptionAndRestoreConnHandler()
	}()

	if err := it.pull(*req, subject); err != nil {
		p.errs <- err
	}
	it.pending.msgCount = req.Batch
	it.pending.byteCount = req.MaxBytes
	go it.pullMessages(subject)

	return it, nil
}

func (it *messagesIter) Next() (Msg, error) {
	it.Lock()
	defer it.Unlock()
	if atomic.LoadUint32(&it.closed) == 1 {
		return nil, ErrMsgIteratorClosed
	}

	for {
		if it.pending.msgCount <= it.req.Batch/2 ||
			(it.pending.byteCount <= it.req.MaxBytes/2 && it.req.MaxBytes != 0) &&
				!it.fetchInProgress {

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
				if !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, ErrMaxBytesExceeded) {
					return nil, err
				}
				it.pending.msgCount -= it.req.Batch
				if it.pending.msgCount < 0 {
					it.pending.msgCount = 0
				}
				if it.req.MaxBytes > 0 {
					it.pending.byteCount -= it.req.MaxBytes
					if it.pending.byteCount < 0 {
						it.pending.byteCount = 0
					}
				}
				continue
			}
			if !userMsg {
				continue
			}
			it.pending.msgCount--
			if it.req.MaxBytes > 0 {
				it.pending.byteCount -= msgSize(msg)
			}
			return it.consumer.jetStream.toJSMsg(msg), nil
		case <-it.reconnected:
			_, err := it.consumer.Info(context.Background())
			if err != nil {
				it.Stop()
				return nil, err
			}
			it.pending.msgCount -= it.req.Batch
			if it.pending.msgCount < 0 {
				it.pending.msgCount = 0
				continue
			}
			if it.req.MaxBytes > 0 {
				it.pending.byteCount -= it.req.MaxBytes
				if it.pending.byteCount < 0 {
					it.pending.byteCount = 0
				}
			}
		case <-it.fetchComplete:
			it.fetchInProgress = false
			it.pending.msgCount += it.req.Batch
			if it.req.MaxBytes > 0 {
				it.pending.byteCount += it.req.MaxBytes
			}
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
	p.errs = make(chan error, 1)

	it := &messagesIter{
		consumer:      p,
		req:           req,
		done:          make(chan struct{}, 1),
		msgs:          msgs,
		fetchNext:     make(chan struct{}, 1),
		reconnected:   make(chan struct{}),
		fetchComplete: make(chan struct{}, 1),
	}
	if err := it.setupSubscription(msgs); err != nil {
		return nil, err
	}
	defer it.subscription.Unsubscribe()
	if err := it.pull(*req, subject); err != nil {
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
func (p *pullConsumer) Subscribe(handler MessageHandler, opts ...ConsumerSubscribeOpt) (ConsumerListener, error) {
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
	p.errs = make(chan error, 1)

	msgs := make(chan *nats.Msg, 2*req.Batch)

	atomic.StoreUint32(&p.isSubscribed, 1)
	it := &messagesIter{
		consumer:         p,
		req:              req,
		msgs:             msgs,
		done:             make(chan struct{}, 1),
		fetchNext:        make(chan struct{}, 1),
		reconnected:      make(chan struct{}),
		fetchComplete:    make(chan struct{}, 1),
		reconnectHandler: p.jetStream.conn.Opts.ReconnectedCB,
	}
	if err := it.setupSubscription(msgs); err != nil {
		return nil, err
	}
	p.jetStream.conn.SetReconnectHandler(func(c *nats.Conn) {
		if it.reconnectHandler != nil {
			it.reconnectHandler(p.jetStream.conn)
		}
		it.reconnected <- struct{}{}
	})
	it.hbTimer = scheduleHeartbeatCheck(req.Heartbeat, p.errs)
	go func() {
		<-it.done
		it.cleanupSubscriptionAndRestoreConnHandler()
	}()
	go it.pullMessages(subject)

	go func() {
		for {
			if atomic.LoadUint32(&it.closed) == 1 {
				return
			}
			if it.pending.msgCount <= it.req.Batch/2 ||
				(it.pending.byteCount <= it.req.MaxBytes/2 && it.req.MaxBytes != 0) &&
					!it.fetchInProgress {

				it.fetchInProgress = true
				it.fetchNext <- struct{}{}
			}
			select {
			case msg := <-msgs:
				if it.hbTimer != nil {
					it.hbTimer.Reset(2 * req.Heartbeat)
				}
				userMsg, err := checkMsg(msg)
				if err != nil {
					if !errors.Is(err, nats.ErrTimeout) {
						handler(nil, err)
						continue
					}
					it.pending.msgCount -= req.Batch
					if it.pending.msgCount < 0 {
						it.pending.msgCount = 0
					}
					continue
				}
				if !userMsg {
					continue
				}
				handler(p.jetStream.toJSMsg(msg), nil)
				it.pending.msgCount--
			case <-it.reconnected:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := p.Info(ctx)
				cancel()
				if err != nil {
					it.cleanupSubscriptionAndRestoreConnHandler()
					handler(nil, err)
					return
				}
				it.pending.msgCount -= req.Batch
				if it.pending.msgCount < 0 {
					it.pending.msgCount = 0
				}
			case <-it.fetchComplete:
				it.fetchInProgress = false
				it.pending.msgCount += req.Batch
			case err := <-p.errs:
				if errors.Is(err, ErrNoHeartbeat) {
					it.cleanupSubscriptionAndRestoreConnHandler()
					handler(nil, err)
					return
				}
				handler(nil, err)
			}
		}
	}()

	return it, nil
}

func (it *messagesIter) pullMessages(subject string) {
	for {
		select {
		case <-it.fetchNext:
			if err := it.pull(*it.req, subject); err != nil {
				if errors.Is(err, ErrMsgIteratorClosed) {
					it.cleanupSubscriptionAndRestoreConnHandler()
					return
				}
				it.consumer.errs <- err
			}
			it.fetchComplete <- struct{}{}
		case <-it.done:
			it.cleanupSubscriptionAndRestoreConnHandler()
			return
		}
	}
}

func scheduleHeartbeatCheck(dur time.Duration, errCh chan error) *time.Timer {
	if dur == 0 {
		return nil
	}
	return time.AfterFunc(2*dur, func() {
		errCh <- ErrNoHeartbeat
	})
}

func (it *messagesIter) cleanupSubscriptionAndRestoreConnHandler() {
	it.consumer.Lock()
	defer it.consumer.Unlock()
	if it.hbTimer != nil {
		it.hbTimer.Stop()
	}
	it.subscription.Unsubscribe()
	it.subscription = nil
	atomic.StoreUint32(&it.consumer.isSubscribed, 0)
	it.consumer.jetStream.conn.SetReconnectHandler(it.reconnectHandler)
}

func (it *messagesIter) setupSubscription(msgs chan *nats.Msg) error {
	inbox := nats.NewInbox()
	sub, err := it.consumer.jetStream.conn.ChanSubscribe(inbox, msgs)
	if err != nil {
		return err
	}
	it.subscription = sub
	return nil
}

func msgSize(msg *nats.Msg) int {
	if msg == nil {
		return 0
	}
	return len(msg.Subject) + len(msg.Reply) + len(msg.Data)
}

// pull sends a pull request to the server and waits for messages using a subscription from `pullConsumer`.
// Messages will be fetched up to given batch_size or until there are no more messages or timeout is returned
func (it *messagesIter) pull(req pullRequest, subject string) error {
	if atomic.LoadUint32(&it.closed) == 1 {
		return ErrMsgIteratorClosed
	}
	if req.Batch < 1 {
		return fmt.Errorf("%w: batch size must be at least 1", nats.ErrInvalidArg)
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return err
	}

	reply := it.subscription.Subject
	if err := it.consumer.jetStream.conn.PublishRequest(subject, reply, reqJSON); err != nil {
		return err
	}
	return nil
}
