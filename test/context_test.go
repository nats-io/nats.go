// Copyright 2012-2017 Apcera Inc. All rights reserved.

// +build go1.7

package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

func TestContextRequestWithTimeout(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	nc.Subscribe("slow", func(m *nats.Msg) {
		// Simulates latency into the client so that timeout is hit.
		time.Sleep(200 * time.Millisecond)
		nc.Publish(m.Reply, []byte("NG"))
	})
	nc.Subscribe("fast", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("OK"))
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
		t.Fatalf("Expected request with timeout context to fail: %s", err)
	}

	// Reported error is "context deadline exceeded" from Context package,
	// which implements net.Error interface.
	type timeoutError interface {
		Timeout() bool
	}
	timeoutErr, ok := err.(timeoutError)
	if !ok || !timeoutErr.Timeout() {
		t.Errorf("Expected to have a timeout error")
	}
	expected = `context deadline exceeded`
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected %q error, got: %q", expected, err.Error())
	}

	// 2nd request should fail again even if they would be fast because context
	// has already timed out.
	_, err = nc.RequestWithContext(ctx, "fast", []byte("world"))
	if err == nil {
		t.Fatalf("Expected request with context to fail: %s", err)
	}
}

func TestContextRequestWithTimeoutCancelled(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	nc.Subscribe("fast", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("OK"))
	})

	ctx, cancelCB := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelCB()

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

	_, err = nc.RequestWithContext(ctx, "fast", []byte("world"))
	if err == nil {
		t.Fatalf("Expected request with timeout context to fail: %s", err)
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
		t.Fatalf("Expected request with context to fail: %s", err)
	}
}

func TestContextRequestWithCancel(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ctx, cancelCB := context.WithCancel(context.Background())
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	// timer which cancels the context though can also be arbitrarily extended
	expirationTimer := time.AfterFunc(100*time.Millisecond, func() {
		cancelCB()
	})

	nc.Subscribe("slow", func(m *nats.Msg) {
		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		nc.Publish(m.Reply, []byte("OK"))
	})
	nc.Subscribe("slower", func(m *nats.Msg) {
		// we know this request will take longer so extend the timeout
		expirationTimer.Reset(100 * time.Millisecond)

		// slower reply which would have hit original timeout
		time.Sleep(90 * time.Millisecond)

		nc.Publish(m.Reply, []byte("Also OK"))
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
	// get cancelled, but these reset the timer so deadline
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
	_, err := nc.RequestWithContext(ctx, "slow", []byte(""))
	if err == nil {
		t.Fatalf("Expected request with cancellation context to fail: %s", err)
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

func TestContextRequestWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

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
		t.Fatalf("Expected request with context to reach deadline: %s", err)
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
		t.Fatalf("Expected to fail receiving a message: %s", err)
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

func TestContextEncodedRequestWithDeadline(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Unable to create encoded connection: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(100 * time.Millisecond)
	ctx, cancelCB := context.WithDeadline(context.Background(), deadline)
	defer cancelCB() // should always be called, not discarded, to prevent context leak

	type request struct {
		Message string `json:"message"`
	}
	type response struct {
		Code int `json:"code"`
	}
	c.Subscribe("slow", func(_, reply string, req *request) {
		got := req.Message
		expected := "Hello"
		if got != expected {
			t.Errorf("Expected to receive request with %q, got %q", got, expected)
		}

		// simulates latency into the client so that timeout is hit.
		time.Sleep(40 * time.Millisecond)
		c.Publish(reply, &response{Code: 200})
	})

	for i := 0; i < 2; i++ {
		req := &request{Message: "Hello"}
		resp := &response{}
		err := c.RequestWithContext(ctx, "slow", req, resp)
		if err != nil {
			t.Fatalf("Expected encoded request with context to not fail: %s", err)
		}
		got := resp.Code
		expected := 200
		if got != expected {
			t.Errorf("Expected to receive %v, got: %v", expected, got)
		}
	}

	// A third request with latency would make the context
	// reach the deadline.
	req := &request{Message: "Hello"}
	resp := &response{}
	err = c.RequestWithContext(ctx, "slow", req, resp)
	if err == nil {
		t.Fatalf("Expected request with context to reach deadline: %s", err)
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
