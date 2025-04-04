// Copyright 2012-2022 The NATS Authors
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
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestContextRequestWithNilConnection(t *testing.T) {
	var nc *nats.Conn

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	_, err := nc.RequestWithContext(ctx, "fast", []byte(""))
	if err == nil {
		t.Fatal("Expected request with context and nil connection to fail")
	}
	if err != nats.ErrInvalidConnection {
		t.Fatalf("Expected nats.ErrInvalidConnection, got %v\n", err)
	}
}

func testContextRequestWithTimeout(t *testing.T, nc *nats.Conn) {
	nc.Subscribe("slow", func(m *nats.Msg) {
		// Simulates latency into the client so that timeout is hit.
		time.Sleep(200 * time.Millisecond)
		nc.Publish(m.Reply, []byte("NG"))
	})
	nc.Subscribe("fast", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("OK"))
	})
	nc.Subscribe("hdrs", func(m *nats.Msg) {
		if m.Header.Get("Hdr-Test") != "1" {
			m.Respond([]byte("-ERR"))
		}

		r := nats.NewMsg(m.Reply)
		r.Header = m.Header
		r.Data = []byte("+OK")
		m.RespondMsg(r)
	})

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	// Fast request should not fail at this point.
	resp, err := nc.RequestWithContext(ctx, "fast", []byte(""))
	if err != nil {
		t.Fatalf("Expected request with context to not fail on fast response: %s", err)
	}
	got := string(resp.Data)
	expected := "OK"
	if got != expected {
		t.Errorf("Expected to receive %s, got: %s", expected, got)
	}

	// Slow request hits timeout so expected to fail.
	_, err = nc.RequestWithContext(ctx, "slow", []byte("world"))
	if err == nil {
		t.Fatal("Expected request with timeout context to fail")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Error("Expected to have a timeout error")
	}
	expected = `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}

	// 2nd request should fail again even if they would be fast because context
	// has already timed out.
	_, err = nc.RequestWithContext(ctx, "fast", []byte("world"))
	if err == nil {
		t.Fatal("Expected request with context to fail")
	}

	// now test headers make it all the way back
	msg := nats.NewMsg("hdrs")
	msg.Header.Add("Hdr-Test", "1")
	resp, err = nc.RequestMsgWithContext(context.Background(), msg)
	if err != nil {
		t.Fatalf("Expected request to be published: %v", err)
	}
	if string(resp.Data) != "+OK" {
		t.Fatalf("Headers were not published to the requestor")
	}
	if resp.Header.Get("Hdr-Test") != "1" {
		t.Fatalf("Did not receive header in response")
	}
}

func TestContextRequestWithTimeout(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	testContextRequestWithTimeout(t, nc)
}

func TestOldContextRequestWithTimeout(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	testContextRequestWithTimeout(t, nc)
}

func testContextRequestWithTimeoutCanceled(t *testing.T, nc *nats.Conn) {
	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB()

	nc.Subscribe("fast", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("OK"))
	})

	// Fast request should not fail
	resp, err := nc.RequestWithContext(ctx, "fast", []byte(""))
	if err != nil {
		t.Fatalf("Expected request with context to not fail on fast response: %s", err)
	}
	got := string(resp.Data)
	expected := "OK"
	if got != expected {
		t.Errorf("Expected to receive %s, got: %s", expected, got)
	}

	// Cancel the context already so that rest of requests fail.
	cancelCB()

	// Context is already canceled so requests should immediately fail.
	_, err = nc.RequestWithContext(ctx, "fast", []byte("world"))
	if err == nil {
		t.Fatal("Expected request with timeout context to fail")
	}

	// Reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected = `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}

	// 2nd request should fail again even if fast because context has already been canceled
	_, err = nc.RequestWithContext(ctx, "fast", []byte("world"))
	if err == nil {
		t.Fatal("Expected request with context to fail")
	}
}

func TestContextRequestWithTimeoutCanceled(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	testContextRequestWithTimeoutCanceled(t, nc)
}

func TestOldContextRequestWithTimeoutCanceled(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	testContextRequestWithTimeoutCanceled(t, nc)
}

func testContextRequestWithCancel(t *testing.T, nc *nats.Conn) {
	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	// timer which cancels the context though can also be arbitrarily extended
	expirationTimer := time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	sub1, err := nc.Subscribe("slow", func(m *nats.Msg) {
		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		nc.Publish(m.Reply, []byte("OK"))
	})
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}
	defer sub1.Unsubscribe()
	sub2, err := nc.Subscribe("slower", func(m *nats.Msg) {
		// we know this request will take longer so extend the timeout
		expirationTimer.Reset(100 * time.Millisecond)

		// slower reply which would have hit original timeout
		time.Sleep(70 * time.Millisecond)

		nc.Publish(m.Reply, []byte("Also OK"))
	})
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}
	defer sub2.Unsubscribe()

	for i := 0; i < 2; i++ {
		resp, err := nc.RequestWithContext(ctx, "slow", []byte(""))
		if err != nil {
			t.Fatalf("Expected request with context to not fail: %s", err)
		}
		got := string(resp.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// A third request with latency would make the context
	// get canceled, but these reset the timer so deadline
	// gets extended:
	for i := 0; i < 10; i++ {
		resp, err := nc.RequestWithContext(ctx, "slower", []byte(""))
		if err != nil {
			t.Fatalf("Expected request with context to not fail: %s", err)
		}
		got := string(resp.Data)
		expected := "Also OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// One more slow request will expire the timer and cause an error...
	_, err = nc.RequestWithContext(ctx, "slow", []byte(""))
	if err == nil {
		t.Fatal("Expected request with cancellation context to fail")
	}

	// ...though reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected := `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextOldRequestClosed(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	ctx, cancelCB := context.WithTimeout(context.Background(), time.Second)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	errCh := make(chan error, 1)
	start := time.Now()
	go func() {
		sub, _ := nc.SubscribeSync("checkClose")
		defer sub.Unsubscribe()
		_, err = nc.RequestWithContext(ctx, "checkClose", []byte("should be kicked out on close"))
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	nc.Close()
	if e := <-errCh; e != nats.ErrConnectionClosed {
		t.Fatalf("Unexpected error: %v", e)
	}
	if dur := time.Since(start); dur >= time.Second {
		t.Fatalf("Request took too long to bail out: %v", dur)
	}
}

func TestContextRequestWithCancel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	testContextRequestWithCancel(t, nc)
}

func TestOldContextRequestWithCancel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	testContextRequestWithCancel(t, nc)
}

func testContextRequestWithDeadline(t *testing.T, nc *nats.Conn) {
	deadline := time.Now().Add(100 * time.Millisecond)
	ctx, cancelCB := context.WithDeadline(context.Background(), deadline)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	nc.Subscribe("slow", func(m *nats.Msg) {
		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		nc.Publish(m.Reply, []byte("OK"))
	})

	for i := 0; i < 2; i++ {
		resp, err := nc.RequestWithContext(ctx, "slow", []byte(""))
		if err != nil {
			t.Fatalf("Expected request with context to not fail: %s", err)
		}
		got := string(resp.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// A third request with latency would make the context
	// reach the deadline.
	_, err := nc.RequestWithContext(ctx, "slow", []byte(""))
	if err == nil {
		t.Fatal("Expected request with context to reach deadline")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error Timeout interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected := `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextRequestWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	testContextRequestWithDeadline(t, nc)
}

func TestOldContextRequestWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(nats.DefaultURL, nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	testContextRequestWithDeadline(t, nc)
}

func TestContextSubNextMsgWithTimeout(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	sub, err := nc.SubscribeSync("slow")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	for i := 0; i < 2; i++ {
		err := nc.Publish("slow", []byte("OK"))
		if err != nil {
			t.Fatalf("Expected publish to not fail: %s", err)
		}
		// Enough time to get a couple of messages
		time.Sleep(40 * time.Millisecond)

		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Expected to receive message: %s", err)
		}
		got := string(msg.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// Third message will fail because the context will be canceled by now
	_, err = sub.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("Expected to fail receiving a message")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error Timeout interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected := `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextSubNextMsgWithTimeoutCanceled(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	sub, err := nc.SubscribeSync("fast")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	for i := 0; i < 2; i++ {
		err := nc.Publish("fast", []byte("OK"))
		if err != nil {
			t.Fatalf("Expected publish to not fail: %s", err)
		}
		// Enough time to get a couple of messages
		time.Sleep(40 * time.Millisecond)

		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Expected to receive message: %s", err)
		}
		got := string(msg.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// Cancel the context already so that rest of NextMsg calls fail.
	cancelCB()

	_, err = sub.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("Expected request with timeout context to fail")
	}

	// Reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected := `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextSubNextMsgWithCancel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	// timer which cancels the context though can also be arbitrarily extended
	time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	sub1, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}
	sub2, err := nc.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	for i := 0; i < 2; i++ {
		err := nc.Publish("foo", []byte("OK"))
		if err != nil {
			t.Fatalf("Expected publish to not fail: %s", err)
		}
		resp, err := sub1.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Expected request with context to not fail: %s", err)
		}
		got := string(resp.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}
	err = nc.Publish("bar", []byte("Also OK"))
	if err != nil {
		t.Fatalf("Expected publish to not fail: %s", err)
	}

	resp, err := sub2.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatalf("Expected request with context to not fail: %s", err)
	}
	got := string(resp.Data)
	expected := "Also OK"
	if got != expected {
		t.Errorf("Expected to receive %s, got: %s", expected, got)
	}

	// We do not have another message pending so timer will
	// cancel the context.
	_, err = sub2.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("Expected request with context to fail")
	}

	// Reported error is "context canceled" from Context package,
	// which is not a timeout error.
	type timeoutError interface {
		Timeout() bool
	}
	if _, ok := err.(timeoutError); ok {
		t.Errorf("Expected to not have a timeout error")
	}
	expected = `context canceled`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextSubNextMsgWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	deadline := time.Now().Add(100 * time.Millisecond)
	ctx, cancelCB := context.WithDeadline(context.Background(), deadline)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	sub, err := nc.SubscribeSync("slow")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	for i := 0; i < 2; i++ {
		err := nc.Publish("slow", []byte("OK"))
		if err != nil {
			t.Fatalf("Expected publish to not fail: %s", err)
		}
		// Enough time to get a couple of messages
		time.Sleep(40 * time.Millisecond)

		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			t.Fatalf("Expected to receive message: %s", err)
		}
		got := string(msg.Data)
		expected := "OK"
		if got != expected {
			t.Errorf("Expected to receive %s, got: %s", expected, got)
		}
	}

	// Third message will fail because the context will be canceled by now
	_, err = sub.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("Expected to fail receiving a message")
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error Timeout interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected := `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}
}

func TestContextRequestConnClosed(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB()

	time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	nc.Close()
	_, err := nc.RequestWithContext(ctx, "foo", []byte(""))
	if err == nil {
		t.Fatal("Expected request to fail with error")
	}
	if err != nats.ErrConnectionClosed {
		t.Errorf("Expected request to fail with connection closed error: %s", err)
	}
}

func TestContextBadSubscription(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB()
	time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	err = sub.Unsubscribe()
	if err != nil {
		t.Fatalf("Expected to be able to unsubscribe: %s", err)
	}

	_, err = sub.NextMsgWithContext(ctx)
	if err == nil {
		t.Fatal("Expected to fail getting next message with context")
	}

	if err != nats.ErrBadSubscription {
		t.Errorf("Expected request to fail with connection closed error: %s", err)
	}
}

func TestFlushWithContext(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx := context.Background()

	// No context should error.
	//lint:ignore SA1012 testing that passing nil fails
	if err := nc.FlushWithContext(nil); err != nats.ErrInvalidContext {
		t.Fatalf("Expected '%v', got '%v'", nats.ErrInvalidContext, err)
	}
	// A context with no deadline set should error also.
	if err := nc.FlushWithContext(ctx); err != nats.ErrNoDeadlineContext {
		t.Fatalf("Expected '%v', got '%v'", nats.ErrNoDeadlineContext, err)
	}

	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	cancel()

	// A closed context should error.
	if err := nc.FlushWithContext(dctx); err != context.Canceled {
		t.Fatalf("Expected '%v', got '%v'", context.Canceled, err)
	}
}

func TestUnsubscribeAndNextMsgWithContext(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}
	sub.Unsubscribe()
	if _, err = sub.NextMsgWithContext(ctx); err != nats.ErrBadSubscription {
		t.Fatalf("Expected '%v', but got: '%v'", nats.ErrBadSubscription, err)
	}

	ctx, cancelCB = context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	sub, err = nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	// Now make sure we get same error when unsubscribing from separate routine
	// while in the call.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		sub.Unsubscribe()
		wg.Done()
	}()

	if _, err = sub.NextMsgWithContext(ctx); err != nats.ErrBadSubscription {
		t.Fatalf("Expected '%v', but got: '%v'", nats.ErrBadSubscription, err)
	}
	wg.Wait()
}

func TestContextInvalid(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	//lint:ignore SA1012 testing that passing nil fails
	_, err := nc.RequestWithContext(nil, "foo", []byte(""))
	if err == nil {
		t.Fatal("Expected request to fail with error")
	}
	if err != nats.ErrInvalidContext {
		t.Errorf("Expected request to fail with connection closed error: %s", err)
	}

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
	if err != nil {
		t.Fatalf("Expected to be able to subscribe: %s", err)
	}

	//lint:ignore SA1012 testing that passing nil fails
	_, err = sub.NextMsgWithContext(nil)
	if err == nil {
		t.Fatal("Expected request to fail with error")
	}
	if err != nats.ErrInvalidContext {
		t.Errorf("Expected request to fail with connection closed error: %s", err)
	}
}
