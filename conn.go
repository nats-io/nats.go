// Copyright 2012 Apcera Inc. All rights reserved.

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

// Internal implementation for a Connection
type conn struct {
	url     *url.URL
	opts    Options
	conn    net.Conn
	bwl     sync.Mutex
	bw      *bufio.Writer
	br      *bufio.Reader
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

const maxChanLen = 1024

func (nc *conn) connect() (err error) {

	// FIXME: Check for 0 Timeout
	nc.conn, err = net.DialTimeout("tcp", nc.url.Host, nc.opts.Timeout)
	if err != nil {
		return
	}

	nc.subs  = make(map[uint64]*Subscription)
	nc.mch   = make(chan *Msg, maxChanLen)
	nc.pongs = make([] chan bool, 0, 8)

	nc.bw = bufio.NewWriter(nc.conn)
	nc.br = bufio.NewReader(nc.conn)

	go nc.readLoop()
	go nc.deliverMsgs()

	runtime.SetFinalizer(nc, fin)
	return nc.sendConnect()
}

func (nc *conn) sendConnect() error {
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

	nc.bwl.Lock()
	nc.bw.WriteString(fmt.Sprintf(conProto, string(b)))
	nc.bw.Flush()
	nc.bwl.Unlock()

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

func (nc *conn) readOp(c *control) error {
	if nc.closed {
		return errors.New("Connection closed")
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

func (nc *conn) readLoop() {
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

func (nc *conn) deliverMsgs() {
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

func (nc *conn) processMsg(args string) {
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

func (nc *conn) processPing() {
	nc.bwl.Lock()
	nc.bw.WriteString(pongProto)
	nc.bw.Flush()
	nc.bwl.Unlock()
}

func (nc *conn) processPong() {
	ch := nc.pongs[0]
	nc.pongs = nc.pongs[1:]
	if ch != nil {
		ch <- true
	}
}

func processOK() {
}

func (nc *conn) processInfo(info string) {
	if info == _EMPTY_ { return }
	err := json.Unmarshal([]byte(info), &nc.info)
	if err != nil {
		// ?
	}
}

func (nc *conn) LastError() error {
	return nc.err
}

func (nc *conn) processErr(e string) {
	nc.err = errors.New(e)
	nc.Close()
}

func (nc *conn) publish(subj, reply string, data []byte) error {
	// TODO(derek) Do flusher go routine to allow coalescing
	// Cant allow split writes to bw unless protected
	nc.bwl.Lock()
	nc.bw.WriteString(fmt.Sprintf(pubProto, subj, reply, len(data)))
	nc.bw.Write(data)
	nc.bw.WriteString(_CRLF_)
	nc.bw.Flush()
	nc.bwl.Unlock()
	return nil
}

// Publish will send data to the given subject
func (nc *conn) Publish(subj string, data []byte) error {
	return nc.publish(subj, _EMPTY_, data)
}

func (nc *conn) PublishMsg(m *Msg) error {
	return nc.publish(m.Subject, m.Reply, m.Data)
}

func (nc *conn) Request(subj string, data []byte, timeout time.Duration) (*Msg, error) {
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
	conn          Connection
	mcb           MsgHandler
//	tcb           TimeoutCB
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

func (nc *conn) subscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	sub := &Subscription{Subject: subj, mcb: cb, conn:nc}
	if cb == nil {
		// Indicates a sync subscription
		sub.mch = make(chan *Msg, maxChanLen) //FIXME, is this a blocker?
	}
	sub.sid = atomic.AddUint64(&nc.ssid, 1)
	nc.bwl.Lock()
	nc.bw.WriteString(fmt.Sprintf(subProto, subj, queue, sub.sid))
	nc.bw.Flush()
	nc.bwl.Unlock()
	nc.subs[sub.sid] = sub
	return sub, nil
}

func (nc *conn) Subscribe(subj string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, cb)
}

func (nc *conn) SubscribeSync(subj string) (*Subscription, error) {
	return nc.subscribe(subj, _EMPTY_, nil)
}

func (nc *conn) QueueSubscribe(subj, queue string, cb MsgHandler) (*Subscription, error) {
	return nc.subscribe(subj, queue, cb)
}

func (nc *conn) unsubscribe(sub *Subscription, max int, timeout time.Duration) error {
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
	nc.bwl.Lock()
	nc.bw.WriteString(fmt.Sprintf(unsubProto, s.sid, maxStr))
	nc.bw.Flush()
	nc.bwl.Unlock()

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
		return nil, errors.New("Invalid Subscription")
	}
	if s.sc {
		s.sc = false
		return nil, errors.New("Slow consumer, messages have been dropped")
	}

	var ok bool
	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case msg, ok = <- s.mch:
		if !ok {
			return nil, errors.New("Connection closed")
		}
		s.delivered = atomic.AddUint64(&s.delivered, 1)
		if s.max > 0 && s.delivered > s.max {
			return nil, errors.New("Max messages delivered")
		}
	case <-t.C:
		return nil, errors.New("Timeout")
	}
	return
}

// FIXME: This is a hack
func (nc *conn) removeFlushEntry(ch chan bool) bool {
	if nc.pongs == nil { return false }
	for i,c := range nc.pongs {
		if c == ch {
			nc.pongs[i] = nil
			return true
		}
	}
	return false
}

func (nc *conn) FlushTimeout(timeout time.Duration) (err error) {
	if (timeout <= 0) {
		return errors.New("Bad timeout value")
	}
	if nc.closed {
		return errors.New("Connection closed")
	}
	t := time.NewTimer(timeout)
	defer t.Stop()

	ch := make(chan bool) // Inefficient?
	defer close(ch)

	// Lock? Race?
	nc.pongs = append(nc.pongs, ch)

	nc.bwl.Lock()
	nc.bw.WriteString(pingProto)
	nc.bw.Flush()
	nc.bwl.Unlock()

	select {
	case _, ok := <-ch:
		if !ok {
			// println("FLUSH:Received error")
			err = errors.New("Connection closed")
		}
		if nc.sc {
			err = errors.New("Slow consumer, messages have been dropped")
		}
	case <-t.C:
		err = errors.New("Timeout")
	}

	if err != nil {
		nc.removeFlushEntry(ch)
	}
	return
}

func (nc *conn) Flush() error {
	return nc.FlushTimeout(60*time.Second)
}

func (nc *conn) Close() {
	if nc.closed { return }
	nc.closed = true
	close(nc.mch)
	nc.mch = nil
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
	nc.bw.Flush()
	nc.conn.Close()
}

func fin(nc *conn) {
	nc.Close()
}
