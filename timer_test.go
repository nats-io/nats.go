package nats

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestTimerPool(t *testing.T) {
	// This test modifies runtime behavior and must not be run concurrently
	// with other tests so we can't call t.Parallel

	// Force a single P to avoid the current goroutine from being scheduled
	// onto another P. this is needed as sync.Pool will keep one value on a
	// per-P cache.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	// Disable GC to prevent the pool from being emptied while testing
	defer debug.SetGCPercent(debug.SetGCPercent(0))

	var tp timerPool

	t1 := time.NewTimer(time.Hour)
	tp.Put(t1)

	t2 := tp.Get(time.Millisecond * 25)

	if t1 != t2 {
		t.Errorf("Got new timer")
	}

	select {
	case <-t2.C:
	case <-time.After(time.Millisecond * 100):
		t.Errorf("Timer from pool didn't expire in time")
	}

	t3 := tp.Get(time.Millisecond * 25)

	if t2 == t3 {
		t.Errorf("Got old timer")
	}

	select {
	case <-t3.C:
	case <-time.After(time.Millisecond * 100):
		t.Errorf("New timer didn't expire in time")
	}
}
