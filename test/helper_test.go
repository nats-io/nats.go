// Copyright 2015-2026 The NATS Authors
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

package test

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

// Wait for a chan with a timeout.
func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func WaitOnChannel[T comparable](t *testing.T, ch <-chan T, expected T) {
	t.Helper()
	select {
	case s := <-ch:
		if s != expected {
			t.Fatalf("Expected result: %v; got: %v", expected, s)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for result %v", expected)
	}
}

func stackFatalf(t testing.TB, f string, args ...any) {
	t.Helper()
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers: Skip us and verify* frames.
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}
	t.Fatalf("%s", strings.Join(lines, "\n"))
}

// getStableNumGoroutine returns runtime.NumGoroutine() once the count is
// observed stable for several consecutive samples. Used by goroutine-leak
// tests to take a stable baseline.
func getStableNumGoroutine(t *testing.T) int {
	t.Helper()
	timeout := time.Now().Add(2 * time.Second)
	var base, old, same int
	for time.Now().Before(timeout) {
		base = runtime.NumGoroutine()
		if old == base {
			same++
			if same == 5 {
				return base
			}
		} else {
			same = 0
		}
		old = base
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Unable to get stable number of go routines")
	return 0
}

// checkNoGoroutineLeak asserts the goroutine count returns to base within
// 2 seconds; otherwise reports the delta.
func checkNoGoroutineLeak(t *testing.T, base int, action string) {
	t.Helper()
	waitFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		delta := (runtime.NumGoroutine() - base)
		if delta > 0 {
			return fmt.Errorf("%d Go routines still exist after %s", delta, action)
		}
		return nil
	})
}

// checkErrChannel polls the error channel non-blockingly; if an error is
// present, fails the test with it. Supports patterns where the producer may
// send nil to signal absence-of-error.
func checkErrChannel(t *testing.T, errCh chan error) {
	t.Helper()
	select {
	case e := <-errCh:
		if e != nil {
			t.Fatal(e.Error())
		}
	default:
	}
}

// waitFor retries f every sleepDur up to totalWait, failing the test with the
// last non-nil error if f never returns nil.
func waitFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

// checkFor is an alias for waitFor used by JetStream-flavored tests; both
// signatures exist in the historical code base, kept for callers'
// convenience.
func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	waitFor(t, totalWait, sleepDur, f)
}
