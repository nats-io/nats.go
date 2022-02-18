// Copyright 2021 The NATS Authors
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

package nats

import (
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	serverTest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nuid"
)

func testWSGetDefaultOptions(t *testing.T, tls bool) *server.Options {
	t.Helper()
	sopts := serverTest.DefaultTestOptions
	sopts.Host = "127.0.0.1"
	sopts.Port = -1
	sopts.Websocket.Host = "127.0.0.1"
	sopts.Websocket.Port = -1
	sopts.Websocket.NoTLS = !tls
	if tls {
		tc := &server.TLSConfigOpts{
			CertFile: "./test/configs/certs/server.pem",
			KeyFile:  "./test/configs/certs/key.pem",
			CaFile:   "./test/configs/certs/ca.pem",
		}
		tlsConfig, err := server.GenTLSConfig(tc)
		if err != nil {
			t.Fatalf("Can't build TLCConfig: %v", err)
		}
		sopts.Websocket.TLSConfig = tlsConfig
	}
	return &sopts
}

type fakeReader struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	ch     chan []byte
	closed bool
}

func (f *fakeReader) Read(p []byte) (int, error) {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return 0, io.EOF
	}
	for {
		if f.buf.Len() > 0 {
			n, err := f.buf.Read(p)
			return n, err
		}
		buf, ok := <-f.ch
		if !ok {
			return 0, io.EOF
		}
		f.buf.Write(buf)
	}
}

func (f *fakeReader) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	close(f.ch)
}

func TestWSReader(t *testing.T) {
	mr := &fakeReader{ch: make(chan []byte, 1)}
	defer mr.close()
	r := wsNewReader(mr)

	p := make([]byte, 100)
	checkRead := func(limit int, expected []byte, lenPending int) {
		t.Helper()
		n, err := r.Read(p[:limit])
		if err != nil {
			t.Fatalf("Error reading: %v", err)
		}
		if !bytes.Equal(p[:n], expected) {
			t.Fatalf("Expected %q, got %q", expected, p[:n])
		}
		if len(r.pending) != lenPending {
			t.Fatalf("Expected len(r.pending) to be %v, got %v", lenPending, len(r.pending))
		}
	}

	// Test with a buffer that contains a single pending with all data that
	// fits in the read buffer.
	mr.buf.Write([]byte{130, 10})
	mr.buf.WriteString("ABCDEFGHIJ")
	checkRead(100, []byte("ABCDEFGHIJ"), 0)

	// Write 2 frames in the buffer. Since we will call with a read buffer
	// that can fit both, we will create 2 pending and consume them at once.
	mr.buf.Write([]byte{130, 5})
	mr.buf.WriteString("ABCDE")
	mr.buf.Write([]byte{130, 5})
	mr.buf.WriteString("FGHIJ")
	checkRead(100, []byte("ABCDEFGHIJ"), 0)

	// We also write 2 frames, but this time we will call the first read
	// with a read buffer that can accommodate only the first frame.
	// So internally only a single frame is going to be read in pending.
	mr.buf.Write([]byte{130, 5})
	mr.buf.WriteString("ABCDE")
	mr.buf.Write([]byte{130, 5})
	mr.buf.WriteString("FGHIJ")
	checkRead(6, []byte("ABCDE"), 0)
	checkRead(100, []byte("FGHIJ"), 0)

	// To test partials, we need to directly set the pending buffers.
	r.pending = append(r.pending, []byte("ABCDE"))
	r.pending = append(r.pending, []byte("FGHIJ"))
	// Now check that the first read cannot get the full first pending
	// buffer and gets only a partial.
	checkRead(3, []byte("ABC"), 2)
	// Since the read buffer is big enough to get everything else, after
	// this call we should have no pending.
	checkRead(7, []byte("DEFGHIJ"), 0)

	// Similar to above but with both partials.
	r.pending = append(r.pending, []byte("ABCDE"))
	r.pending = append(r.pending, []byte("FGHIJ"))
	checkRead(3, []byte("ABC"), 2)
	// Exact amount of the partial of 1st pending
	checkRead(2, []byte("DE"), 1)
	checkRead(3, []byte("FGH"), 1)
	// More space in read buffer than last partial
	checkRead(10, []byte("IJ"), 0)

	// This test the fact that read will return only when a frame is complete.
	mr.buf.Write([]byte{130, 5})
	mr.buf.WriteString("AB")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		mr.ch <- []byte{'C', 'D', 'E', 130, 2, 'F', 'G'}
		wg.Done()
	}()
	// Read() will get "load" only the first frame, so after this call there
	// should be no pending.
	checkRead(100, []byte("ABCDE"), 0)
	// This will load the second frame.
	checkRead(100, []byte("FG"), 0)
	wg.Wait()

	// Set the buffer that may be populated during the init handshake.
	// Make sure that we process that one first.
	r.ib = []byte{130, 4, 'A', 'B'}
	mr.buf.WriteString("CD")
	mr.buf.Write([]byte{130, 2})
	mr.buf.WriteString("EF")
	// This will only read up to ABCD and have no pending after the call.
	checkRead(100, []byte("ABCD"), 0)
	// We need another Read() call to read/load the second frame.
	checkRead(100, []byte("EF"), 0)

	// Close the underlying reader while reading.
	mr.buf.Write([]byte{130, 4, 'A', 'B'})
	wg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		mr.close()
		wg.Done()
	}()
	if _, err := r.Read(p); err != io.EOF {
		t.Fatalf("Expected EOF, got %v", err)
	}
	wg.Wait()
}

func TestWSParseControlFrames(t *testing.T) {
	mr := &fakeReader{ch: make(chan []byte, 1)}
	defer mr.close()
	r := wsNewReader(mr)

	p := make([]byte, 100)

	// Write a PING
	mr.buf.Write([]byte{137, 0})
	n, err := r.Read(p)
	if err != nil || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// Write a PONG
	mr.buf.Write([]byte{138, 0})
	n, err = r.Read(p)
	if err != nil || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// Write a CLOSE
	mr.buf.Write([]byte{136, 6, 3, 232, 't', 'e', 's', 't'})
	n, err = r.Read(p)
	if err != io.EOF || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// Write a CLOSE without payload
	mr.buf.Write([]byte{136, 2, 3, 232})
	n, err = r.Read(p)
	if err != io.EOF || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// Write a CLOSE with invalid status
	mr.buf.Write([]byte{136, 1, 100})
	n, err = r.Read(p)
	if err != io.EOF || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// Write CLOSE with valid status and payload but call with a read buffer
	// that has capacity of 1.
	mr.buf.Write([]byte{136, 6, 3, 232, 't', 'e', 's', 't'})
	pl := []byte{136}
	n, err = r.Read(pl[:])
	if err != io.EOF || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}
}

func TestWSParseInvalidFrames(t *testing.T) {

	newReader := func() (*fakeReader, *websocketReader) {
		mr := &fakeReader{}
		r := wsNewReader(mr)
		return mr, r
	}

	p := make([]byte, 100)

	// Invalid utf-8 of close message
	mr, r := newReader()
	mr.buf.Write([]byte{136, 6, 3, 232, 't', 'e', 0xF1, 't'})
	n, err := r.Read(p)
	if err != io.EOF || n != 0 {
		t.Fatalf("Error on read: n=%v err=%v", n, err)
	}

	// control frame length too long
	mr, r = newReader()
	mr.buf.Write([]byte{137, 126, 0, wsMaxControlPayloadSize + 10})
	for i := 0; i < wsMaxControlPayloadSize+10; i++ {
		mr.buf.WriteByte('a')
	}
	n, err = r.Read(p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// Not a final frame
	mr, r = newReader()
	mr.buf.Write([]byte{byte(wsPingMessage), 0})
	n, err = r.Read(p[:2])
	if n != 0 || err == nil || !strings.Contains(err.Error(), "final") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// Marked as compressed
	mr, r = newReader()
	mr.buf.Write([]byte{byte(wsPingMessage) | wsRsv1Bit, 0})
	n, err = r.Read(p[:2])
	if n != 0 || err == nil || !strings.Contains(err.Error(), "compressed") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// Continuation frame marked as compressed
	mr, r = newReader()
	mr.buf.Write([]byte{2, 3})
	mr.buf.WriteString("ABC")
	mr.buf.Write([]byte{0 | wsRsv1Bit, 3})
	mr.buf.WriteString("DEF")
	n, err = r.Read(p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "invalid continuation frame") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// Continuation frame after a final frame
	mr, r = newReader()
	mr.buf.Write([]byte{130, 3})
	mr.buf.WriteString("ABC")
	mr.buf.Write([]byte{0, 3})
	mr.buf.WriteString("DEF")
	n, err = r.Read(p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "invalid continuation frame") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// New message received before previous ended
	mr, r = newReader()
	mr.buf.Write([]byte{2, 3})
	mr.buf.WriteString("ABC")
	mr.buf.Write([]byte{0, 3})
	mr.buf.WriteString("DEF")
	mr.buf.Write([]byte{130, 3})
	mr.buf.WriteString("GHI")
	n, err = r.Read(p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "started before final frame") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}

	// Unknown frame type
	mr, r = newReader()
	mr.buf.Write([]byte{99, 3})
	mr.buf.WriteString("ABC")
	n, err = r.Read(p)
	if n != 0 || err == nil || !strings.Contains(err.Error(), "unknown opcode") {
		t.Fatalf("Unexpected error: n=%v err=%v", n, err)
	}
}

func TestWSControlFrameBetweenDataFrames(t *testing.T) {
	mr := &fakeReader{ch: make(chan []byte, 1)}
	defer mr.close()
	r := wsNewReader(mr)

	p := make([]byte, 100)

	// Write a frame that will continue after the PONG
	mr.buf.Write([]byte{2, 3})
	mr.buf.WriteString("ABC")
	// Write a PONG
	mr.buf.Write([]byte{138, 0})
	// Continuation of the frame
	mr.buf.Write([]byte{0, 3})
	mr.buf.WriteString("DEF")
	// Another PONG
	mr.buf.Write([]byte{138, 0})
	// End of frame
	mr.buf.Write([]byte{128, 3})
	mr.buf.WriteString("GHI")

	n, err := r.Read(p)
	if err != nil {
		t.Fatalf("Error on read: %v", err)
	}
	if string(p[:n]) != "ABCDEFGHI" {
		t.Fatalf("Unexpected result: %q", p[:n])
	}
}

func TestWSDecompressor(t *testing.T) {
	var br *wsDecompressor

	p := make([]byte, 100)
	checkRead := func(limit int, expected []byte) {
		t.Helper()
		n, err := br.Read(p[:limit])
		if err != nil {
			t.Fatalf("Error on read: %v", err)
		}
		if got := p[:n]; !bytes.Equal(expected, got) {
			t.Fatalf("Expected %v, got %v", expected, got)
		}
	}
	checkEOF := func() {
		t.Helper()
		n, err := br.Read(p)
		if err != io.EOF || n > 0 {
			t.Fatalf("Unexpected result: n=%v err=%v", n, err)
		}
	}
	checkReadByte := func(expected byte) {
		t.Helper()
		b, err := br.ReadByte()
		if err != nil {
			t.Fatalf("Error on read: %v", err)
		}
		if b != expected {
			t.Fatalf("Expected %c, got %c", expected, b)
		}
	}
	checkEOFWithReadByte := func() {
		t.Helper()
		n, err := br.ReadByte()
		if err != io.EOF || n > 0 {
			t.Fatalf("Unexpected result: n=%v err=%v", n, err)
		}
	}

	newDecompressor := func(str string) *wsDecompressor {
		d := &wsDecompressor{}
		d.addBuf([]byte(str))
		return d
	}

	// Read with enough room
	br = newDecompressor("ABCDE")
	checkRead(100, []byte("ABCDE"))
	checkEOF()
	checkEOFWithReadByte()

	// Read with a partial from our buffer
	br = newDecompressor("FGHIJ")
	checkRead(2, []byte("FG"))
	// Call with more than the end of our buffer.
	checkRead(10, []byte("HIJ"))
	checkEOF()
	checkEOFWithReadByte()

	// Read with a partial from our buffer
	br = newDecompressor("KLMNO")
	checkRead(2, []byte("KL"))
	// Call with exact number of bytes left for our buffer.
	checkRead(3, []byte("MNO"))
	checkEOF()
	checkEOFWithReadByte()

	// Finally, check ReadByte.
	br = newDecompressor("UVWXYZ")
	checkRead(4, []byte("UVWX"))
	checkReadByte('Y')
	checkReadByte('Z')
	checkEOFWithReadByte()
	checkEOF()

	br = newDecompressor("ABC")
	buf := make([]byte, 0)
	n, err := br.Read(buf)
	if n != 0 || err != nil {
		t.Fatalf("Unexpected n=%v err=%v", n, err)
	}
}

func TestWSNoMixingScheme(t *testing.T) {
	// Check opts.Connect() first
	for _, test := range []struct {
		url     string
		servers []string
	}{
		{"ws://127.0.0.1:1234", []string{"nats://127.0.0.1:1235"}},
		{"ws://127.0.0.1:1234", []string{"ws://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
		{"ws://127.0.0.1:1234", []string{"wss://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
		{"wss://127.0.0.1:1234", []string{"nats://127.0.0.1:1235"}},
		{"wss://127.0.0.1:1234", []string{"wss://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
		{"wss://127.0.0.1:1234", []string{"ws://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
	} {
		t.Run("Options", func(t *testing.T) {
			opts := GetDefaultOptions()
			opts.Url = test.url
			opts.Servers = test.servers
			nc, err := opts.Connect()
			if err == nil || !strings.Contains(err.Error(), "mixing") {
				if nc != nil {
					nc.Close()
				}
				t.Fatalf("Expected error about mixing, got %v", err)
			}
		})
	}
	// Check Connect() now.
	for _, test := range []struct {
		urls    string
		servers []string
	}{
		{"ws://127.0.0.1:1234,nats://127.0.0.1:1235", nil},
		{"ws://127.0.0.1:1234,tcp://127.0.0.1:1235", nil},
		{"ws://127.0.0.1:1234,tls://127.0.0.1:1235", nil},
		{"nats://127.0.0.1:1234,ws://127.0.0.1:1235", nil},
		{"nats://127.0.0.1:1234,wss://127.0.0.1:1235", nil},
		{"nats://127.0.0.1:1234,tls://127.0.0.1:1235,ws://127.0.0.1:1236", nil},
		{"nats://127.0.0.1:1234,tls://127.0.0.1:1235,wss://127.0.0.1:1236", nil},
		// In Connect(), the URL is ignored when Servers() is provided.
		{"", []string{"nats://127.0.0.1:1235", "ws://127.0.0.1:1236"}},
		{"", []string{"nats://127.0.0.1:1235", "wss://127.0.0.1:1236"}},
		{"", []string{"ws://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
		{"", []string{"wss://127.0.0.1:1235", "nats://127.0.0.1:1236"}},
	} {
		t.Run("Connect", func(t *testing.T) {
			var opt Option
			if len(test.servers) > 0 {
				opt = func(o *Options) error {
					o.Servers = test.servers
					return nil
				}
			}
			nc, err := Connect(test.urls, opt)
			if err == nil || !strings.Contains(err.Error(), "mixing") {
				if nc != nil {
					nc.Close()
				}
				t.Fatalf("Expected error about mixing, got %v", err)
			}
		})
	}
}

func TestWSBasic(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
	nc, err := Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	msgs := make([][]byte, 100)
	for i := 0; i < len(msgs); i++ {
		msg := make([]byte, rand.Intn(70000))
		for j := 0; j < len(msg); j++ {
			msg[j] = 'A' + byte(rand.Intn(26))
		}
		msgs[i] = msg
	}
	for i, msg := range msgs {
		if err := nc.Publish("foo", msg); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		// Make sure that masking does not overwrite user data
		if !bytes.Equal(msgs[i], msg) {
			t.Fatalf("User content has been changed: %v, got %v", msgs[i], msg)
		}
	}

	for i := 0; i < len(msgs); i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}
		if !bytes.Equal(msgs[i], msg.Data) {
			t.Fatalf("Expected message: %v, got %v", msgs[i], msg)
		}
	}
}

func TestWSControlFrames(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	rch := make(chan bool, 10)
	ncSub, err := Connect(s.ClientURL(),
		ReconnectWait(50*time.Millisecond),
		ReconnectHandler(func(_ *Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncSub.Close()

	sub, err := ncSub.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := ncSub.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	dch := make(chan error, 10)
	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
	nc, err := Connect(url,
		ReconnectWait(50*time.Millisecond),
		DisconnectErrHandler(func(_ *Conn, err error) { dch <- err }),
		ReconnectHandler(func(_ *Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Enqueue a PING and make sure that we don't break
	nc.wsEnqueueControlMsg(wsPingMessage, []byte("this is a ping payload"))
	select {
	case e := <-dch:
		t.Fatal(e)
	case <-time.After(250 * time.Millisecond):
		// OK
	}

	// Shutdown the server, which should send a close message, which by
	// spec the client will try to echo back.
	s.Shutdown()

	select {
	case <-dch:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Should have been disconnected")
	}

	s = RunServerWithOptions(sopts)
	defer s.Shutdown()

	// Wait for both connections to reconnect
	if err := Wait(rch); err != nil {
		t.Fatalf("Should have reconnected: %v", err)
	}
	if err := Wait(rch); err != nil {
		t.Fatalf("Should have reconnected: %v", err)
	}
	// Even if the first connection reconnects, there is no guarantee
	// that the resend of SUB has yet been processed by the server.
	// Doing a flush here will give us the guarantee.
	if err := ncSub.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// Publish and close connection.
	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	nc.Close()

	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
}

func TestWSConcurrentConns(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)

	total := 50
	errCh := make(chan error, total)
	wg := sync.WaitGroup{}
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()

			nc, err := Connect(url)
			if err != nil {
				errCh <- fmt.Errorf("Error on connect: %v", err)
				return
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync(nuid.Next())
			if err != nil {
				errCh <- fmt.Errorf("Error on subscribe: %v", err)
				return
			}
			nc.Publish(sub.Subject, []byte("here"))
			if _, err := sub.NextMsg(time.Second); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	default:
	}
}

func TestWSCompression(t *testing.T) {
	msgSize := rand.Intn(40000)
	for _, test := range []struct {
		name           string
		srvCompression bool
		cliCompression bool
	}{
		{"srv_off_cli_off", false, false},
		{"srv_off_cli_on", false, true},
		{"srv_on_cli_off", true, false},
		{"srv_on_cli_on", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, false)
			sopts.Websocket.Compression = test.srvCompression
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
			var opts []Option
			if test.cliCompression {
				opts = append(opts, Compression(true))
			}
			nc, err := Connect(url, opts...)
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}

			msgs := make([][]byte, 100)
			for i := 0; i < len(msgs); i++ {
				msg := make([]byte, msgSize)
				for j := 0; j < len(msg); j++ {
					msg[j] = 'A'
				}
				msgs[i] = msg
			}
			for i, msg := range msgs {
				if err := nc.Publish("foo", msg); err != nil {
					t.Fatalf("Error on publish: %v", err)
				}
				// Make sure that compression/masking does not touch user data
				if !bytes.Equal(msgs[i], msg) {
					t.Fatalf("User content has been changed: %v, got %v", msgs[i], msg)
				}
			}

			for i := 0; i < len(msgs); i++ {
				msg, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting next message (%d): %v", i+1, err)
				}
				if !bytes.Equal(msgs[i], msg.Data) {
					t.Fatalf("Expected message (%d): %v, got %v", i+1, msgs[i], msg)
				}
			}
		})
	}
}

func TestWSCompressionWithContinuationFrames(t *testing.T) {
	uncompressed := []byte("this is an uncompressed message with AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	buf := &bytes.Buffer{}
	compressor, _ := flate.NewWriter(buf, flate.BestSpeed)
	compressor.Write(uncompressed)
	compressor.Close()
	b := buf.Bytes()
	if len(b) < 30 {
		panic("revisit test so that compressed buffer is more than 30 bytes long")
	}

	srbuf := &bytes.Buffer{}
	// We are going to split this in several frames.
	fh := []byte{66, 10}
	srbuf.Write(fh)
	srbuf.Write(b[:10])
	fh = []byte{0, 10}
	srbuf.Write(fh)
	srbuf.Write(b[10:20])
	fh = []byte{wsFinalBit, 0}
	fh[1] = byte(len(b) - 20)
	srbuf.Write(fh)
	srbuf.Write(b[20:])

	r := wsNewReader(srbuf)
	rbuf := make([]byte, 100)
	n, err := r.Read(rbuf[:15])
	// Since we have a partial of compressed message, the library keeps track
	// of buffer, but it can't return anything at this point, so n==0 err==nil
	// is the expected result.
	if n != 0 || err != nil {
		t.Fatalf("Error reading: n=%v err=%v", n, err)
	}
	n, err = r.Read(rbuf)
	if n != len(uncompressed) || err != nil {
		t.Fatalf("Error reading: n=%v err=%v", n, err)
	}
	if !reflect.DeepEqual(uncompressed, rbuf[:n]) {
		t.Fatalf("Unexpected uncompressed data: %v", rbuf[:n])
	}
}

func TestWSWithTLS(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression bool
	}{
		{"without compression", false},
		{"with compression", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, true)
			sopts.Websocket.Compression = test.compression
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			var copts []Option
			if test.compression {
				copts = append(copts, Compression(true))
			}

			// Check that we fail to connect without proper TLS configuration.
			nc, err := Connect(fmt.Sprintf("ws://localhost:%d", sopts.Websocket.Port), copts...)
			if err == nil {
				if nc != nil {
					nc.Close()
				}
				t.Fatal("Expected error, got none")
			}

			// Same but with wss protocol, which should translate to TLS, however,
			// since we used self signed certificates, this should fail without
			// asking to skip server cert verification.
			nc, err = Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
			if err == nil || !strings.Contains(err.Error(), "authority") {
				if nc != nil {
					nc.Close()
				}
				t.Fatalf("Expected error about unknown authority: %v", err)
			}

			// Skip server verification and we should be good.
			copts = append(copts, Secure(&tls.Config{InsecureSkipVerify: true}))
			nc, err = Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := nc.Publish("foo", []byte("hello")); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
			if msg, err := sub.NextMsg(time.Second); err != nil {
				t.Fatalf("Did not get message: %v", err)
			} else if got := string(msg.Data); got != "hello" {
				t.Fatalf("Expected %q, got %q", "hello", got)
			}
		})
	}
}

func TestWSTlsNoConfig(t *testing.T) {
	opts := GetDefaultOptions()
	opts.Servers = []string{"wss://localhost:443"}

	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Error setting up pool: %v", err)
	}
	// Verify that this has set Secure/TLSConfig
	nc.mu.Lock()
	ok := nc.Opts.Secure && nc.Opts.TLSConfig != nil
	nc.mu.Unlock()
	if !ok {
		t.Fatal("Secure and TLSConfig were not set")
	}
	// Now try to add a bare host:ip to the pool and verify
	// that the wss:// scheme is added.
	if err := nc.addURLToPool("1.2.3.4:443", true, false); err != nil {
		t.Fatalf("Error adding to pool: %v", err)
	}
	nc.mu.Lock()
	for _, srv := range nc.srvPool {
		if srv.url.Scheme != wsSchemeTLS {
			nc.mu.Unlock()
			t.Fatalf("Expected scheme to be %q, got url: %s", wsSchemeTLS, srv.url)
		}
	}
	nc.mu.Unlock()
}

func TestWSGossipAndReconnect(t *testing.T) {
	o1 := testWSGetDefaultOptions(t, false)
	o1.ServerName = "A"
	o1.Cluster.Host = "127.0.0.1"
	o1.Cluster.Name = "abc"
	o1.Cluster.Port = -1
	s1 := RunServerWithOptions(o1)
	defer s1.Shutdown()

	o2 := testWSGetDefaultOptions(t, false)
	o2.ServerName = "B"
	o2.Cluster.Host = "127.0.0.1"
	o2.Cluster.Name = "abc"
	o2.Cluster.Port = -1
	o2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServerWithOptions(o2)
	defer s2.Shutdown()

	rch := make(chan bool, 10)
	url := fmt.Sprintf("ws://127.0.0.1:%d", o1.Websocket.Port)
	nc, err := Connect(url,
		ReconnectWait(50*time.Millisecond),
		ReconnectHandler(func(_ *Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	timeout := time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		if len(nc.Servers()) > 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if len(nc.Servers()) == 1 {
		t.Fatal("Did not discover server 2")
	}
	s1.Shutdown()

	// Wait for reconnect
	if err := Wait(rch); err != nil {
		t.Fatalf("Did not reconnect: %v", err)
	}

	// Now check that connection is still WS
	nc.mu.Lock()
	isWS := nc.ws
	_, ok := nc.bw.w.(*websocketWriter)
	nc.mu.Unlock()

	if !isWS {
		t.Fatal("Connection is not marked as websocket")
	}
	if !ok {
		t.Fatal("Connection writer is not websocket")
	}
}

func TestWSStress(t *testing.T) {
	// Enable this test only when wanting to stress test the system, say after
	// some changes in the library or if a bug is found. Also, don't run it
	// with the `-race` flag!
	t.SkipNow()
	// Total producers (there will be 2 per subject)
	prods := 4
	// Total messages sent
	total := int64(1000000)
	// Total messages received, there is 2 consumer per subject
	totalRecv := 2 * total
	// We will create a "golden" slice from which sent messages
	// will be a subset of. Receivers will check that the content
	// match the expected content.
	maxPayloadSize := 100000
	mainPayload := make([]byte, maxPayloadSize)
	for i := 0; i < len(mainPayload); i++ {
		mainPayload[i] = 'A' + byte(rand.Intn(26))
	}
	for _, test := range []struct {
		name     string
		compress bool
	}{
		{"no_compression", false},
		{"with_compression", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, false)
			sopts.Websocket.Compression = test.compress
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			var count int64
			consDoneCh := make(chan struct{}, 1)
			errCh := make(chan error, 1)
			prodDoneCh := make(chan struct{}, prods)

			pushErr := func(e error) {
				select {
				case errCh <- e:
				default:
				}
			}

			createConn := func() *Conn {
				t.Helper()
				nc, err := Connect(fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port),
					Compression(test.compress),
					ErrorHandler(func(_ *Conn, sub *Subscription, err error) {
						if sub != nil {
							err = fmt.Errorf("Subscription on %q - err=%v", sub.Subject, err)
						}
						pushErr(err)
					}))
				if err != nil {
					t.Fatalf("Error connecting: %v", err)
				}
				return nc
			}

			cb := func(m *Msg) {
				if len(m.Data) < 4 {
					pushErr(fmt.Errorf("Message payload too small: %+v", m.Data))
					return
				}
				ps := int(binary.BigEndian.Uint32(m.Data[:4]))
				if ps > maxPayloadSize {
					pushErr(fmt.Errorf("Invalid message size: %v", ps))
					return
				}
				if !bytes.Equal(m.Data[4:4+ps], mainPayload[:ps]) {
					pushErr(fmt.Errorf("invalid content"))
					return
				}
				if atomic.AddInt64(&count, 1) == totalRecv {
					consDoneCh <- struct{}{}
				}
			}

			subjects := []string{"foo", "bar"}
			for _, subj := range subjects {
				for i := 0; i < 2; i++ {
					nc := createConn()
					defer nc.Close()
					sub, err := nc.Subscribe(subj, cb)
					if err != nil {
						t.Fatalf("Error on subscribe: %v", err)
					}
					sub.SetPendingLimits(-1, -1)
					if err := nc.Flush(); err != nil {
						t.Fatalf("Error on flush: %v", err)
					}
				}
			}

			msgsPerProd := int(total / int64(prods))
			prodPerSubj := prods / len(subjects)
			for _, subj := range subjects {
				for i := 0; i < prodPerSubj; i++ {
					go func(subj string) {
						defer func() { prodDoneCh <- struct{}{} }()

						nc := createConn()
						defer nc.Close()

						for i := 0; i < msgsPerProd; i++ {
							// Have 80% of messages being rather small (<=1024)
							maxSize := 1024
							if rand.Intn(100) > 80 {
								maxSize = maxPayloadSize
							}
							ps := rand.Intn(maxSize)
							msg := make([]byte, 4+ps)
							binary.BigEndian.PutUint32(msg, uint32(ps))
							copy(msg[4:], mainPayload[:ps])
							if err := nc.Publish(subj, msg); err != nil {
								pushErr(err)
								return
							}
						}
						nc.Flush()
					}(subj)
				}
			}

			for i := 0; i < prods; i++ {
				select {
				case <-prodDoneCh:
				case e := <-errCh:
					t.Fatal(e)
				}
			}
			// Now wait for all consumers to be done.
			<-consDoneCh
		})
	}
}
