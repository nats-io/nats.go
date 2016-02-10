package nats

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	gnatsd "github.com/nats-io/gnatsd/test"
)

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

////////////////////////////////////////////////////////////////////////////////
// Reconnect tests
////////////////////////////////////////////////////////////////////////////////

const TEST_PORT = 8368

var reconnectOpts = Options{
	Url:            fmt.Sprintf("nats://localhost:%d", TEST_PORT),
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        DefaultTimeout,
}

func RunServerOnPort(port int) *server.Server {
	opts := gnatsd.DefaultTestOptions
	opts.Port = port
	return RunServerWithOptions(opts)
}

func RunServerWithOptions(opts server.Options) *server.Server {
	return gnatsd.RunServer(&opts)
}

func TestReconnectServerStats(t *testing.T) {
	ts := RunServerOnPort(TEST_PORT)

	opts := reconnectOpts
	nc, _ := opts.Connect()
	defer nc.Close()
	nc.Flush()

	ts.Shutdown()
	// server is stopped here...

	ts = RunServerOnPort(TEST_PORT)
	defer ts.Shutdown()

	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	// Make sure the server who is reconnected has the reconnects stats reset.
	nc.mu.Lock()
	_, cur := nc.currentServer()
	nc.mu.Unlock()

	if cur.reconnects != 0 {
		t.Fatalf("Current Server's reconnects should be 0 vs %d\n", cur.reconnects)
	}
}

func TestParseStateReconnectFunctionality(t *testing.T) {
	ts := RunServerOnPort(TEST_PORT)
	ch := make(chan bool)

	opts := reconnectOpts
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}

	nc, errc := opts.Connect()
	if errc != nil {
		t.Fatalf("Failed to create a connection: %v\n", errc)
	}
	ec, errec := NewEncodedConn(nc, DEFAULT_ENCODER)
	if errec != nil {
		nc.Close()
		t.Fatalf("Failed to create an encoded connection: %v\n", errec)
	}
	defer ec.Close()

	testString := "bar"
	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String doesn't match")
		}
		ch <- true
	})
	ec.Flush()

	// Got a RACE condition with Travis build. The locking below does not
	// really help because the parser running in the readLoop accesses
	// nc.ps without the connection lock. Sleeping may help better since
	// it would make the memory write in parse.go (when processing the
	// pong) further away from the modification below.
	time.Sleep(1 * time.Second)

	// Simulate partialState, this needs to be cleared
	nc.mu.Lock()
	nc.ps.state = OP_PON
	nc.mu.Unlock()

	ts.Shutdown()
	// server is stopped here...

	if err := Wait(dch); err != nil {
		t.Fatal("Did not get the DisconnectedCB")
	}

	if err := ec.Publish("foo", testString); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = RunServerOnPort(TEST_PORT)
	defer ts.Shutdown()

	if err := ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	reconnectedCount := ec.Conn.Stats().Reconnects

	if reconnectedCount != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			reconnectedCount, expectedReconnectCount)
	}
}

////////////////////////////////////////////////////////////////////////////////
// ServerPool tests
////////////////////////////////////////////////////////////////////////////////

var testServers = []string{
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
	"nats://localhost:1225",
	"nats://localhost:1226",
	"nats://localhost:1227",
	"nats://localhost:1228",
}

func TestServersRandomize(t *testing.T) {
	opts := DefaultOptions
	opts.Servers = testServers
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers := []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	// In theory this could happen..
	if reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list not randomized\n")
	}

	// Now test that we do not randomize if proper flag is set.
	opts = DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	nc = &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers = []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	if !reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list should not be randomized\n")
	}
}

func TestSelectNextServer(t *testing.T) {
	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	if nc.url != nc.srvPool[0].url {
		t.Fatalf("Wrong default selection: %v\n", nc.url)
	}

	sel, err := nc.selectNextServer()
	if err != nil {
		t.Fatalf("Got an err: %v\n", err)
	}
	// Check that we are now looking at #2, and current is now last.
	if len(nc.srvPool) != len(testServers) {
		t.Fatalf("List is incorrect size: %d vs %d\n", len(nc.srvPool), len(testServers))
	}
	if nc.url.String() != testServers[1] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.url, testServers[1])
	}
	if nc.srvPool[len(nc.srvPool)-1].url.String() != testServers[0] {
		t.Fatalf("Did not push old to last position\n")
	}
	if sel != nc.srvPool[0] {
		t.Fatalf("Did not return correct server: %v vs %v\n", sel.url, nc.srvPool[0].url)
	}

	// Test that we do not keep servers where we have tried to reconnect past our limit.
	nc.srvPool[0].reconnects = int(opts.MaxReconnect)
	if _, err := nc.selectNextServer(); err != nil {
		t.Fatalf("Got an err: %v\n", err)
	}
	// Check that we are now looking at #3, and current is not in the list.
	if len(nc.srvPool) != len(testServers)-1 {
		t.Fatalf("List is incorrect size: %d vs %d\n", len(nc.srvPool), len(testServers)-1)
	}
	if nc.url.String() != testServers[2] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.url, testServers[2])
	}
	if nc.srvPool[len(nc.srvPool)-1].url.String() == testServers[1] {
		t.Fatalf("Did not throw away the last server correctly\n")
	}
}

// This will test that comma separated url strings work properly for
// the Connect() command.
func TestUrlArgument(t *testing.T) {
	check := func(url string, expected []string) {
		if !reflect.DeepEqual(processUrlString(url), expected) {
			t.Fatalf("Got wrong response processing URL: %q, RES: %#v\n", url, processUrlString(url))
		}
	}
	// This is normal case
	oneExpected := []string{"nats://localhost:1222"}

	check("nats://localhost:1222", oneExpected)
	check("nats://localhost:1222 ", oneExpected)
	check(" nats://localhost:1222", oneExpected)
	check(" nats://localhost:1222 ", oneExpected)

	var multiExpected = []string{
		"nats://localhost:1222",
		"nats://localhost:1223",
		"nats://localhost:1224",
	}

	check("nats://localhost:1222,nats://localhost:1223,nats://localhost:1224", multiExpected)
	check("nats://localhost:1222, nats://localhost:1223, nats://localhost:1224", multiExpected)
	check(" nats://localhost:1222, nats://localhost:1223, nats://localhost:1224 ", multiExpected)
	check("nats://localhost:1222,   nats://localhost:1223  ,nats://localhost:1224", multiExpected)
}

func TestParserPing(t *testing.T) {
	c := &Conn{}
	fake := &bytes.Buffer{}
	c.bw = bufio.NewWriterSize(fake, c.Opts.ReconnectBufSize)

	c.ps = &parseState{}

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	ping := []byte("PING\r\n")
	err := c.parse(ping[:1])
	if err != nil || c.ps.state != OP_P {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[1:2])
	if err != nil || c.ps.state != OP_PI {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[2:3])
	if err != nil || c.ps.state != OP_PIN {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[3:4])
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[4:5])
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[5:6])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Should tolerate spaces
	ping = []byte("PING  \r")
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	c.ps.state = OP_START
	ping = []byte("PING  \r  \n")
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserErr(t *testing.T) {
	c := &Conn{}
	c.status = CLOSED
	fake := &bytes.Buffer{}
	c.bw = bufio.NewWriterSize(fake, c.Opts.ReconnectBufSize)

	c.ps = &parseState{}

	// This test focuses on the parser only, not how the error is
	// actually processed by the upper layer.

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}

	expectedError := "'Any kind of error'"
	errProto := []byte("-ERR  " + expectedError + "\r\n")
	err := c.parse(errProto[:1])
	if err != nil || c.ps.state != OP_MINUS {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[1:2])
	if err != nil || c.ps.state != OP_MINUS_E {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[2:3])
	if err != nil || c.ps.state != OP_MINUS_ER {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[3:4])
	if err != nil || c.ps.state != OP_MINUS_ERR {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[4:5])
	if err != nil || c.ps.state != OP_MINUS_ERR_SPC {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[5:6])
	if err != nil || c.ps.state != OP_MINUS_ERR_SPC {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// Check with split arg buffer
	err = c.parse(errProto[6:7])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[7:10])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[10 : len(errProto)-2])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	if c.ps.argBuf == nil {
		t.Fatal("ArgBuf should not be nil")
	}
	s := string(c.ps.argBuf)
	if s != expectedError {
		t.Fatalf("Expected %v, got %v", expectedError, s)
	}
	err = c.parse(errProto[len(errProto)-2:])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// Check without split arg buffer
	errProto = []byte("-ERR 'Any error'\r\n")
	err = c.parse(errProto)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserOK(t *testing.T) {
	c := &Conn{}
	c.ps = &parseState{}

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	errProto := []byte("+OKay\r\n")
	err := c.parse(errProto[:1])
	if err != nil || c.ps.state != OP_PLUS {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[1:2])
	if err != nil || c.ps.state != OP_PLUS_O {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[2:3])
	if err != nil || c.ps.state != OP_PLUS_OK {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[3:])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserShouldFail(t *testing.T) {
	c := &Conn{}
	c.ps = &parseState{}

	if err := c.parse([]byte(" PING")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("POO")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("Px")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PIx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PINx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	// Stop here because 'PING' protos are tolerant for anything between PING and \n

	c.ps.state = OP_START
	if err := c.parse([]byte("POx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PONx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	// Stop here because 'PONG' protos are tolerant for anything between PONG and \n

	c.ps.state = OP_START
	if err := c.parse([]byte("ZOO")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("Mx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSGx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG  foo\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo 1\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo bar 1\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo bar 1 baz\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo 1 bar baz\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("+x\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("+Ox\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-x\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-Ex\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-ERx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-ERRx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
}

func TestParserSplitMsg(t *testing.T) {

	nc := &Conn{}
	nc.ps = &parseState{}

	buf := []byte("MSG a\r\n")
	err := nc.parse(buf)
	if err == nil {
		t.Fatal("Expected an error")
	}
	nc.ps = &parseState{}

	buf = []byte("MSG a b c\r\n")
	err = nc.parse(buf)
	if err == nil {
		t.Fatal("Expected an error")
	}
	nc.ps = &parseState{}

	expectedCount := uint64(1)
	expectedSize := uint64(3)

	buf = []byte("MSG a")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}

	buf = []byte(" 1 3\r\nf")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 3 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	buf = []byte("oo\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	buf = []byte("MSG a 1 3\r\nfo")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 3 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	expectedCount++
	expectedSize += 3

	buf = []byte("o\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	buf = []byte("MSG a 1 6\r\nfo")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 6 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	buf = []byte("ob")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}

	expectedCount++
	expectedSize += 6

	buf = []byte("ar\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	// Let's have a msg that is bigger than the parser's scratch size.
	// Since we prepopulate the msg with 'foo', adding 3 to the size.
	msgSize := cap(nc.ps.scratch) + 100 + 3
	buf = []byte(fmt.Sprintf("MSG a 1 b %d\r\nfoo", msgSize))
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != msgSize {
		t.Fatalf("Wrong msg size: %d instead of %d", nc.ps.ma.size, msgSize)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if string(nc.ps.ma.reply) != "b" {
		t.Fatalf("Wrong reply: '%s' instead of 'b'", string(nc.ps.ma.reply))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	expectedCount++
	expectedSize += uint64(msgSize)

	bufSize := msgSize - 3

	buf = make([]byte, bufSize)
	for i := 0; i < bufSize; i++ {
		buf[i] = byte('a' + (i % 26))
	}

	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.state != MSG_PAYLOAD {
		t.Fatalf("Wrong state: %v instead of %v", nc.ps.state, MSG_PAYLOAD)
	}
	if nc.ps.ma.size != msgSize {
		t.Fatalf("Wrong (ma) msg size: %d instead of %d", nc.ps.ma.size, msgSize)
	}
	if len(nc.ps.msgBuf) != msgSize {
		t.Fatalf("Wrong msg size: %d instead of %d", len(nc.ps.msgBuf), msgSize)
	}
	// Check content:
	if string(nc.ps.msgBuf[0:3]) != "foo" {
		t.Fatalf("Wrong msg content: %s", string(nc.ps.msgBuf))
	}
	for k := 3; k < nc.ps.ma.size; k++ {
		if nc.ps.msgBuf[k] != byte('a'+((k-3)%26)) {
			t.Fatalf("Wrong msg content: %s", string(nc.ps.msgBuf))
		}
	}

	buf = []byte("\r\n")
	err = nc.parse(buf)
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}
	if nc.ps.state != OP_START {
		t.Fatalf("Wrong state: %v", nc.ps.state)
	}
}

func TestNormalizeError(t *testing.T) {
	received := "Typical Error"
	expected := strings.ToLower(received)
	if s := normalizeErr("-ERR '" + received + "'"); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	received = "Trim Surrounding Spaces"
	expected = strings.ToLower(received)
	if s := normalizeErr("-ERR    '" + received + "'   "); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	received = "Trim Surrounding Spaces Without Quotes"
	expected = strings.ToLower(received)
	if s := normalizeErr("-ERR    " + received + "   "); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	received = "Error Without Quotes"
	expected = strings.ToLower(received)
	if s := normalizeErr("-ERR " + received); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	received = "Error With Quote Only On Left"
	expected = strings.ToLower(received)
	if s := normalizeErr("-ERR '" + received); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	received = "Error With Quote Only On Right"
	expected = strings.ToLower(received)
	if s := normalizeErr("-ERR " + received + "'"); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}
}
