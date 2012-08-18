// Copyright 2012 Apcera Inc. All rights reserved.

/*
	A Go client for the NATS messaging system (https://github.com/derekcollison/nats).
*/
package nats

import (
	"fmt"
	"net"
	"net/url"
	"io"
	"bufio"
	"strings"
	"sync/atomic"
	"encoding/json"
	"time"
	"runtime"
	"errors"
	"math/rand"
	"sync"
	"strconv"
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
	ErrConnectionClosed = errors.New("Connection closed")
	ErrBadSubscription  = errors.New("Invalid Subscription")
	ErrSlowConsumer     = errors.New("Slow consumer, messages dropped")
	ErrTimeout          = errors.New("Timeout")
)

var DefaultOptions = Options {
	AllowReconnect : true,
	MaxReconnect   : DefaultMaxReconnect,
	ReconnectWait  : DefaultReconnectWait,
	Timeout        : DefaultTimeout,
}

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

// ErrHandler is a place holder for receiving asynchronous callbacks for
// protocol errors.
type ErrHandler func(*Conn, error)

// MsgHandler is a callback function that processes messages delivered to
// asynchronous subscribers.
type MsgHandler func(msg *Msg)

// Connect will attempt to connect to the NATS server.
// The url can contain username/password semantics.
func Connect(url string) (*Conn, error) {
	opts := DefaultOptions
	opts.Url = url
	return opts.Connect()
}

// Connect will attempt to connect to a NATS server with multiple options.
func (o Options) Connect() (*Conn, error) {
	nc := &Conn{opts:o}
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


// Internal implementation for a Connection
type Conn struct {
	url     *url.URL
	opts    Options
	conn    net.Conn
	bwl     sync.Mutex
	bw      *bufio.Writer
	br      *bufio.Reader
	fch     chan bool
	info    serverInfo
	stats   stats
	ssid    uint64
	subs    map[uint64]*Subscription
	mch     chan *Msg
	pongs   []chan bool
	sc      bool
	err     error
	closed bool
}

type stats struct {
	inMsgs, outMsgs, inBytes, outBytes uint64
}

type serverInfo struct {
	Id           string `json:"server_id"`
	Host         string `json:"host"`
	Port         uint   `json:"port"`
	Version      string `json:"version"`
	AuthRequired bool   `json:"auth_required"`
	SslRequired  bool   `json:"ssl_required"`
	MaxPayload   int64  `json:"max_payload"`
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
	Ssl      bool   `json:"ssl_required"`
}

const (
	_CRLF_    = "\r\n"
	_EMPTY_   = ""
	_SPC_     = " "
)

const (
	_OK_OP_   = "+OK"
	_ERR_OP_  = "-ERR"
	_MSG_OP_  = "MSG"
	_PING_OP_ = "PING"
	_PONG_OP_ = "PONG"
	_INFO_OP_ = "INFO"
)

const (
	conProto   = "CONNECT %s"   + _CRLF_
	pingProto  = "PING"         + _CRLF_
	pongProto  = "PONG"         + _CRLF_
	pubProto   = "PUB %s %s %d" + _CRLF_
	subProto   = "SUB %s %s %d" + _CRLF_
	unsubProto = "UNSUB %d %s"  + _CRLF_
)

const maxChanLen     = 8192
const defaultBufSize = 32768

func (nc *Conn) connect() (err error) {

	// FIXME: Check for 0 Timeout
	nc.conn, err = net.DialTimeout("tcp", nc.url.Host, nc.opts.Timeout)
	if err != nil {
		return
	}

	nc.subs  = make(map[uint64]*Subscription)
	nc.mch   = make(chan *Msg, maxChanLen)
	nc.pongs = make([] chan bool, 0, 8)

	nc.bw = bufio.NewWriterSize(nc.conn, defaultBufSize)
	nc.br = bufio.NewReaderSize(nc.conn, defaultBufSize)

	nc.fch = make(chan bool, 512)

	go nc.readLoop()
	go nc.deliverMsgs()
	go nc.flusher()

	runtime.SetFinalizer(nc, fin)
	return nc.sendConnect()
}

// TODO(derek) Do flusher go routine to allow coalescing
// Cant allow split writes to bw unless protected

func (nc *Conn) sendMsgProto(proto string, data []byte) {
	nc.bwl.Lock()
	nc.bw.WriteString(proto)
	nc.bw.Write(data)
	nc.bw.WriteString(_CRLF_)
	nc.bwl.Unlock()
	nc.fch <- true
}

func (nc *Conn) sendProto(proto string) {
	nc.bwl.Lock()
	nc.bw.WriteString(proto)
	nc.bwl.Unlock()
	nc.fch <- true
}

func (nc *Conn) sendConnect() error {
	o := nc.opts

	var user, pass string
	u := nc.url.User
	if u != nil {
		user = u.Username()
		pass, _ = u.Password()
	}
	cCmd := connectInfo{o.Verbose, o.Pedantic, user, pass, false} // FIXME ssl
	b, err := json.Marshal(cCmd)
	if err != nil {
		return errors.New("Can't create connection message, json failed") // FIXME, standardize
	}

	nc.sendProto(fmt.Sprintf(conProto, b))

	err = nc.FlushTimeout(DefaultTimeout)
	if err != nil {
		return err
	} else if nc.closed {
		return nc.err
	}
	return nil
}

type control struct {
	op, args string
}

func parseControl(line string, c *control) {
	toks := strings.SplitN(string(line), _SPC_, 2)
	if len(toks) == 1 {
		c.op   = strings.TrimSpace(toks[0])
		c.args = _EMPTY_
	} else if len(toks) == 2 {
		c.op, c.args = strings.TrimSpace(toks[0]), strings.TrimSpace(toks[1])
	} else {
		c.op = _EMPTY_
	}
}

func (nc *Conn) readOp(c *control) error {
	if nc.closed {
		return ErrConnectionClosed
	}
	line, pre, err := nc.br.ReadLine()
	if err != nil {
		return err
	}
	if pre {
		return errors.New("Line too long")
	}
	parseControl(string(line), c)
	return nil
}

func (nc *Conn) readLoop() {
	c := &control{}
	for !nc.closed {
		err := nc.readOp(c)
		if err != nil {
			nc.Close()
			break
		}
		switch c.op {
		case _MSG_OP_:
			nc.processMsg(c.args)
		case _OK_OP_:
			processOK()
		case _PING_OP_:
			nc.processPing()
		case _PONG_OP_:
			nc.processPong()
		case _INFO_OP_:
			nc.processInfo(c.args)
		case _ERR_OP_:
			nc.processErr(c.args)
		}
	}
}

func (nc *Conn) deliverMsgs() {
	for !nc.closed {
		m, ok := <- nc.mch
		if !ok { break }
		s := m.Sub
		if (!s.IsValid() || s.mcb == nil) { continue }
		// Fixme, race on compare
		s.delivered = atomic.AddUint64(&s.delivered, 1)
		if s.max <= 0 || s.delivered <= s.max {
			s.mcb(m)
		}
	}
}

func (nc *Conn) processMsg(args string) {
	var subj  string
	var reply string
	var sid   uint64
	var blen  int
	var n     int
	var err   error

	num := strings.Count(args, _SPC_) + 1

	switch num {
	case 3:
		n, err = fmt.Sscanf(args, "%s %d %d", &subj, &sid, &blen)
	case 4:
		n, err = fmt.Sscanf(args, "%s %d %s %d", &subj, &sid, &reply, &blen)
	}
	if err != nil {
		println("Failed to parse control for message")
	}
	if (n != num) {
		println("Failed to parse control for message, not enough elements")
	}

	// Grab payload here.
	buf := make([]byte, blen)
	n, err = io.ReadFull(nc.br, buf)

	// FIXME - Properly handle errors
	if err != nil || n != blen {
		return
	}

	sub := nc.subs[sid]
	if (sub == nil || (sub.max > 0 && sub.msgs > sub.max)) {
		return
	}
	sub.msgs += 1

	m := &Msg{Data:buf, Subject:subj, Reply:reply, Sub:sub}

	if sub.mcb != nil {
		if len(nc.mch) >= maxChanLen {
			nc.sc = true
		} else {
			nc.mch <- m
		}
	} else if sub.mch != nil {
		if len(sub.mch) >= maxChanLen {
			sub.sc = true
		} else {
			sub.mch <- m
		}
	}
}

func (nc *Conn) flusher() {
	var b int
	for !nc.closed {

		_, ok := <- nc.fch
		if !ok { continue }

		nc.bwl.Lock()
		b = nc.bw.Buffered()
		if b > 0 {
			nc.bw.Flush()
		}
		nc.bwl.Unlock()
	}
}

func (nc *Conn) processPing() {
	nc.sendProto(pongProto)
}

func (nc *Conn) processPong() {
	ch := nc.pongs[0]
	nc.pongs = nc.pongs[1:]
	if ch != nil {
		ch <- true
	}
}

func processOK() {
}

func (nc *Conn) processInfo(info string) {
	if info == _EMPTY_ { return }
	err := json.Unmarshal([]byte(info), &nc.info)
	if err != nil {
		// ?
	}
}

// LastError reports the last error encountered via the Connection.
func (nc *Conn) LastError() error {
	return nc.err
}

func (nc *Conn) processErr(e string) {
	nc.err = errors.New(e)
	nc.Close()
}

func (nc *Conn) publish(subj, reply string, data []byte) error {
	nc.sendMsgProto(fmt.Sprintf(pubProto, subj, reply, len(data)), data)
	return nil
}

// Publish publishes the data argument to the given subject.
func (nc *Conn) Publish(subj string, data []byte) error {
	return nc.publish(subj, _EMPTY_, data)
}

// PublishMsg publishes the Msg structure, which includes the
// Subject, and optional Reply, and Optional Data fields.
func (nc *Conn) PublishMsg(m *Msg) error {
	return nc.publish(m.Subject, m.Reply, m.Data)
}

// Request will perform and Publish() call with an Inbox reply and return
// the first reply received.
func (nc *Conn) Request(subj string, data []byte, timeout time.Duration) (*Msg, error) {
	inbox := NewInbox()
	s, err := nc.SubscribeSync(inbox)
	if err != nil { return nil, err }
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()
	err = nc.publish(subj, inbox, data)
	if err != nil { return nil, err }
	return s.NextMsg(timeout)
}

// A Subscription represents interest in a given subject.
type Subscription struct {
	sid           uint64
	Subject       string
	Queue         string
	msgs          uint64
	delivered     uint64
	bytes         uint64
	max           uint64
	conn          *Conn
	mcb           MsgHandler
	mch           chan *Msg
	sc            bool
}

// NewInbox will return an inbox string which can be used for directed replies from
// subscribers.
func NewInbox() (inbox string) {
	inbox = fmt.Sprintf("_INBOX.%04x%04x%04x%04x%04x%06x",
		rand.Int31n(0x0010000),
		rand.Int31n(0x0010000),
		rand.Int31n(0x0010000),
		rand.Int31n(0x0010000),
		rand.Int31n(0x0010000),
		rand.Int31n(0x1000000))
	return
}

func (nc *Conn) subscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	sub := &Subscription{Subject: subj, mcb: cb, conn:nc}
	if cb == nil {
		// Indicates a sync subscription
		sub.mch = make(chan *Msg, maxChanLen)
	}
	sub.sid = atomic.AddUint64(&nc.ssid, 1)
	nc.subs[sub.sid] = sub
	nc.sendProto(fmt.Sprintf(subProto, subj, queue, sub.sid))
	return sub, nil
}

// Subscribe will express interest in a given subject. The subject
// can have wildcards (partial:*, full:>). Messages will be delivered
// to the associated MsgHandler. If no MsgHandler is given, the
// subscription is a synchronous subscription and get be polled via
// Subscription.NextMsg()
func (nc *Conn) Subscribe(subj string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, cb)
}

// SubscribeSync is syntactic sugar for Subscribe(subject, nil)
func (nc *Conn) SubscribeSync(subj string) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, nil)
}

// QueueSubscribe creates a queue subscriber on the given subject. All
// subscribers with the same queue name will form the queue group, and
// only one member of the group will be selected to receive any given
// message.
func (nc *Conn) QueueSubscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, queue, cb)
}

// unsubscribe performs the low level unsubscribe to the server.
// Use Subscription.Unsubscribe()
func (nc *Conn) unsubscribe(sub *Subscription, max int, timeout time.Duration) error {
	s := nc.subs[sub.sid]
	// Already unsubscribed
	if s == nil { return nil }
	maxStr := _EMPTY_
	if max > 0 {
		s.max = uint64(max)
		maxStr = strconv.Itoa(max)
	} else {
		delete(nc.subs, s.sid)
		if s.mch != nil {
			close(s.mch)
			s.mch = nil
		}
		// Mark as invalid
		s.conn = nil
	}
	nc.sendProto(fmt.Sprintf(unsubProto, s.sid, maxStr))
	return nil
}

// IsValid returns a boolean indidcating whether the subscription
// is still active.
func (s *Subscription) IsValid() bool {
	return s.conn != nil
}

// Unsubscribe will remove interest in a given subject.
func (s *Subscription) Unsubscribe() error {
	return s.conn.unsubscribe(s, 0, 0)
}

// AutoUnsubscribe will issue an automatic Unsubscribe that is
// processed by the server when max messages have been received.
// This can be useful when sending a request to an unknown number
// of subscribers. Request() uses this functionality.
func (s *Subscription) AutoUnsubscribe(max int) error {
	return s.conn.unsubscribe(s, max, 0)
}

// NextMsg() will return the next message available to a synchrnous subscriber,
// or block until one is available. A timeout can be used to return when no
// message has been delivered.
func (s *Subscription) NextMsg(timeout time.Duration) (msg *Msg, err error) {
	if s.mcb != nil {
		return nil, errors.New("Illegal to call NextMsg on async Subscription")
	}
	if !s.IsValid() {
		return nil, ErrBadSubscription
	}
	if s.sc {
		s.sc = false
		return nil, ErrSlowConsumer
	}

	var ok bool
	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case msg, ok = <- s.mch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		s.delivered = atomic.AddUint64(&s.delivered, 1)
		if s.max > 0 && s.delivered > s.max {
			return nil, errors.New("Max messages delivered")
		}
	case <-t.C:
		return nil, ErrTimeout
	}
	return
}

// FIXME: This is a hack
func (nc *Conn) removeFlushEntry(ch chan bool) bool {
	if nc.pongs == nil { return false }
	for i,c := range nc.pongs {
		if c == ch {
			nc.pongs[i] = nil
			return true
		}
	}
	return false
}

// FlushTimeout allows a Flush operation to have an associated timeout.
func (nc *Conn) FlushTimeout(timeout time.Duration) (err error) {
	if (timeout <= 0) {
		return errors.New("Bad timeout value")
	}
	if nc.closed {
		return ErrConnectionClosed
	}
	t := time.NewTimer(timeout)
	defer t.Stop()

	ch := make(chan bool) // Inefficient?
	defer close(ch)

	// Lock? Race?
	nc.pongs = append(nc.pongs, ch)
	nc.sendProto(pingProto)

	select {
	case _, ok := <-ch:
		if !ok {
			// println("FLUSH:Received error")
			err = ErrConnectionClosed
		}
		if nc.sc {
			err = ErrSlowConsumer
		}
	case <-t.C:
		err = ErrTimeout
	}

	if err != nil {
		nc.removeFlushEntry(ch)
	}
	return
}

// Flush will perform a round trip to the server and return when it
// receives the internal reply.
func (nc *Conn) Flush() error {
	return nc.FlushTimeout(60*time.Second)
}

// Close will close the connection to the server.
func (nc *Conn) Close() {
	if nc.closed { return }
	nc.closed = true

	// Kick the go routines
	close(nc.fch)
	close(nc.mch)
	defer func() { nc.fch, nc.mch = nil, nil }()

	for _, ch := range nc.pongs {
		if ch != nil {
			ch <- true
		}
	}
	nc.pongs = nil
	for _, s := range nc.subs {
		if s.mch != nil {
			close(s.mch)
			s.mch = nil
		}
	}
	nc.subs = nil
	nc.bwl.Lock()
	nc.bw.Flush()
	nc.conn.Close()
	nc.bwl.Unlock()
}

func fin(nc *Conn) {
	nc.Close()
}
