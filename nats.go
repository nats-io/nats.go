// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"time"
	"net/url"
)

const (
	Version              = "0.1"
	DefaultURL           = "nats://localhost:4222"
	DefaultPort          = 4222
	DefaultMaxReconnect  = 10
	DefaultReconnectWait = 2 * time.Second
	DefaultTimeout       = 2 * time.Second
)

var (
	DefaultOptions = Options {
		AllowReconnect : true,
		MaxReconnect   : DefaultMaxReconnect,
		ReconnectWait  : DefaultReconnectWait,
		Timeout        : DefaultTimeout,
	}
)

// Options can be used to create a customized Connection.
type Options struct {
	Url            string
	Verbose        bool
	Pedantic       bool
	AllowReconnect bool
	MaxReconnect   uint
	ReconnectWait  time.Duration
	Timeout        time.Duration
}

// Msg is a structure used by Subscribers and PublishMsg().
type Msg struct {
	Subject string
	Reply   string
	Data    []byte
	Sub     *Subscription
}

type Connection interface {

	// Publish publishes the data argument to the given subject.
	Publish(subject string, data []byte) error

	// PublishMsg publishes the Msg structure, which includes the
	// Subject, and optional Reply, and Optional Data fields.
	PublishMsg(msg *Msg) error

	// Subscribe will express interest in a given subject. The subject
	// can have wildcards (partial:*, full:>). Messages will be delivered
	// to the associated MsgHandler. If no MsgHandler is given, the
	// subscription is a synchronous subscription and get be polled via
	// Subscription.NextMsg()
	Subscribe(subject string, cb MsgHandler) (*Subscription, error)

	// SubscribeSync is syntactic sugar for Subscribe(subject, nil)
	SubscribeSync(subj string) (*Subscription, error)

	// QueueSubscribe creates a queue subscriber on the given subject. All
	// subscribers with the same queue name will form the queue group, and
	// only one member of the group will be selected to receive any given
	// message.
	QueueSubscribe(subject, queue string, cb MsgHandler) (*Subscription, error)

	// Request will perform and Publish() call with an Inbox reply and return
	// the first reply received.
	Request(subj string, data []byte, timeout time.Duration) (*Msg, error)

	// Flush will perform a round trip to the server and return when it
	// receives the internal reply.
	Flush() error

	// FlushTimeout allows a Flush operation to have an associated timeout.
	FlushTimeout(timeout time.Duration) error

	// LastError reports the last error encountered via the Connection.
	LastError() error

	// Close will close the Connection to the server.
	Close()


	// unsubscribe performs the low level unsubscribe to the server.
	// Use Subscription.Unsubscribe()
	unsubscribe(sub *Subscription, max int, timeout time.Duration) error

//	Subscriptions() []*Subscriptions ?
}


// ErrHandler is a place holder for receiving asynchronous callbacks for
// protocol errors.
type ErrHandler func(Connection, error)

// MsgHandler is a callback function that processes messages delivered to
// asynchronous subscribers.
//type MsgHandler func(subj, reply string, data []byte, sub *Subscription)

type MsgHandler func(msg *Msg)

//type TimeoutHandler func(sub *Subscription)

// Connect will attempt to connect to the NATS server.
// The url can contain username/password semantics.
func Connect(url string) (Connection, error) {
	opts := DefaultOptions
	opts.Url = url
	return opts.Connect()
}

// DefaultConnection returns a default connection.
func DefaultConnection() (Connection, error) {
	return Connect(DefaultURL)
}

// Connect will attempt to connect to a NATS server with multiple options.
func (o Options) Connect() (Connection, error) {
	nc := &conn{opts:o}
	var err error
	nc.url, err = url.Parse(o.Url)
	if err != nil {
		return nil, err
	}
	err = nc.connect()
	if err != nil {
		return nil, err
	}
	return nc, nil
}
