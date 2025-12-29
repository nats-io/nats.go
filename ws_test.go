// Copyright 2021-2023 The NATS Authors
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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/flate"
)

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

func TestWSProxyPath(t *testing.T) {
	const proxyPath = "proxy1"

	// Listen to a random port
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Error in listen: %v", err)
	}
	defer l.Close()

	proxyPort := l.Addr().(*net.TCPAddr).Port

	ch := make(chan struct{}, 1)
	proxySrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/"+proxyPath {
				ch <- struct{}{}
			}
		}),
	}
	defer proxySrv.Shutdown(context.Background())
	go proxySrv.Serve(l)

	for _, test := range []struct {
		name string
		path string
	}{
		{"without slash", proxyPath},
		{"with slash", "/" + proxyPath},
	} {
		t.Run(test.name, func(t *testing.T) {
			url := fmt.Sprintf("ws://127.0.0.1:%d", proxyPort)
			nc, err := Connect(url, ProxyPath(test.path))
			if err == nil {
				nc.Close()
				t.Fatal("Did not expect to connect")
			}
			select {
			case <-ch:
				// OK:
			case <-time.After(time.Second):
				t.Fatal("Proxy was not reached")
			}
		})
	}
}

// --- helpers ---

func startHeaderCatcher(t *testing.T) (addr string, got chan []string, closer func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	got = make(chan []string, 1)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			// surface nothing; test will timeout
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		var lines []string
		for {
			s, err := r.ReadString('\n')
			if err != nil {
				break
			}
			s = strings.TrimRight(s, "\r\n")
			if s == "" { // end of HTTP headers
				break
			}
			lines = append(lines, s)
		}
		got <- lines
	}()

	return ln.Addr().String(), got, func() { _ = ln.Close() }
}

func hasHeaderValue(headers []string, name, want string) bool {
	prefix := strings.ToLower(name) + ":"
	for _, h := range headers {
		if !strings.HasPrefix(strings.ToLower(h), prefix) {
			continue
		}
		val := strings.TrimSpace(strings.SplitN(h, ":", 2)[1])
		for _, part := range strings.Split(val, ",") {
			if strings.EqualFold(strings.TrimSpace(part), want) {
				return true
			}
		}
	}
	return false
}

func TestWSHeaders_StaticAppliedOnHandshake(t *testing.T) {
	addr, got, closeLn := startHeaderCatcher(t)
	defer closeLn()

	static := make(http.Header)
	static.Set("Authorization", "Bearer Random Token")
	static.Add("X-Multi", "v1")
	static.Add("X-Multi", "v2")

	// Intentionally connect to our fake server; it won't complete the upgrade.
	opts := GetDefaultOptions()
	opts.WebSocketConnectionHeaders = static
	opts.Url = "ws://" + addr
	_, err := opts.Connect()
	if err == nil {
		t.Fatalf("expected connect to fail because server does not reply")
	}

	var headers []string
	select {
	case headers = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("did not capture headers in time")
	}

	if !hasHeaderValue(headers, "Authorization", "Bearer Random Token") {
		t.Fatalf("Authorization header missing: %v", headers)
	}
	if !hasHeaderValue(headers, "X-Multi", "v1") || !hasHeaderValue(headers, "X-Multi", "v2") {
		t.Fatalf("X-Multi headers missing/combined incorrectly: %v", headers)
	}
}

func TestWSHeaders_HandlerAppliedOnHandshake(t *testing.T) {
	addr, got, closeLn := startHeaderCatcher(t)
	defer closeLn()

	provider := func() (http.Header, error) {
		h := make(http.Header)
		h.Set("Authorization", "Bearer FromHandler")
		h.Add("X-Multi", "h1")
		h.Add("X-Multi", "h2")
		return h, nil
	}

	opts := GetDefaultOptions()
	opts.WebSocketConnectionHeadersHandler = provider
	opts.Url = "ws://" + addr
	_, err := opts.Connect()
	if err == nil {
		t.Fatalf("expected connect to fail because server does not reply")
	}

	var headers []string
	select {
	case headers = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("did not capture headers in time")
	}

	if !hasHeaderValue(headers, "Authorization", "Bearer FromHandler") {
		t.Fatalf("Authorization header missing: %v", headers)
	}
	if !hasHeaderValue(headers, "X-Multi", "h1") || !hasHeaderValue(headers, "X-Multi", "h2") {
		t.Fatalf("X-Multi headers missing/combined incorrectly: %v", headers)
	}
}
