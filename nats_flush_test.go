package nats

import (
	"bytes"
	"errors"
	"testing"
)

// A writer that accepts only a bounded number of bytes on its first call and
// fails that call with a timeout, then behaves normally — the exact sequence
// from #2158: a socket write timeout after the kernel accepted a partial
// buffer.
type partialWriteWriter struct {
	data       bytes.Buffer
	firstLimit int
	calls      int
}

func (w *partialWriteWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		n := w.firstLimit
		if n > len(p) {
			n = len(p)
		}
		w.data.Write(p[:n])
		return n, errPartialWrite
	}
	n, _ := w.data.Write(p)
	return n, nil
}

var errPartialWrite = errors.New("i/o timeout")

func TestFlushRetriesPartialWrite(t *testing.T) {
	calls := &partialWriteWriter{firstLimit: 10}
	w := &natsWriter{w: calls}

	frame := []byte("PUB foo 5\r\nhello\r\n")
	w.bufs = append(w.bufs, frame...)

	// First flush: the write times out after the kernel accepted the first 10
	// bytes.  The unsent remainder must be preserved (not dropped) so the
	// protocol frame can be completed.
	if err := w.flush(); err == nil {
		t.Fatal("expected the partial write to surface the write error")
	}
	if got := calls.data.String(); got != string(frame[:10]) {
		t.Fatalf("expected only the accepted prefix on the wire, got %q", got)
	}

	// Second flush: the remainder completes the protocol frame.
	if err := w.flush(); err != nil {
		t.Fatalf("flush() returned an error while completing the frame: %v", err)
	}
	if got := calls.data.String(); got != string(frame) {
		t.Fatalf("protocol frame is torn: got %q, want %q", got, string(frame))
	}
	if len(w.bufs) != 0 {
		t.Fatalf("flush() left %d buffered bytes behind", len(w.bufs))
	}
}
