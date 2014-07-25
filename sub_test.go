package nats

import (
	"sync/atomic"
	"testing"
	"time"
)

// More advanced tests on subscriptions

func TestServerAutoUnsub(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	received := 0
	max := 10
	sub, err := nc.Subscribe("foo", func(_ *Msg) {
		received += 1
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	sub.AutoUnsubscribe(max)
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()
	if received != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
}

func TestClientSyncAutoUnsub(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	received := 0
	max := 10
	sub, _ := nc.SubscribeSync("foo")
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	sub.AutoUnsubscribe(max)
	nc.Flush()
	for {
		_, err := sub.NextMsg(1 * time.Millisecond)
		if err != nil {
			break
		}
		received += 1
	}
	if received != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
}

func TestClientASyncAutoUnsub(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	received := 0
	max := 10
	sub, err := nc.Subscribe("foo", func(_ *Msg) {
		received += 1
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	sub.AutoUnsubscribe(max)
	nc.Flush()
	if received != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
}

func TestCloseSubRelease(t *testing.T) {
	nc := newConnection(t)
	sub, _ := nc.SubscribeSync("foo")
	start := time.Now()
	go func() {
		time.Sleep(5 * time.Millisecond)
		nc.Close()
	}()
	_, err := sub.NextMsg(50 * time.Millisecond)
	if err == nil {
		t.Fatalf("Expected an error from NextMsg")
	}
	elapsed := time.Since(start)
	if elapsed > 10*time.Millisecond {
		t.Fatalf("Too much time has elapsed to release NextMsg: %dms",
			(elapsed / time.Millisecond))
	}
}

func TestIsValidSubscriber(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	if !sub.IsValid() {
		t.Fatalf("Subscription should be valid")
	}
	for i := 0; i < 10; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	_, err = sub.NextMsg(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("NextMsg returned an error")
	}
	sub.Unsubscribe()
	_, err = sub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("NextMsg should have returned an error")
	}
}

func TestSlowSubscriber(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	for i := 0; i < (maxChanLen + 100); i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	timeout := 5 * time.Second
	start := time.Now()
	nc.FlushTimeout(timeout)
	elapsed := time.Since(start)
	if elapsed >= timeout {
		t.Fatalf("Flush did not return before timeout: %d > %d", elapsed, timeout)
	}
	// Make sure NextMsg returns an error to indicate slow consumer
	_, err := sub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("NextMsg did not return an error")
	}
}

func TestSlowAsyncSubscriber(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	nc.Subscribe("foo", func(_ *Msg) {
		time.Sleep(100 * time.Second)
	})
	for i := 0; i < (maxChanLen + 100); i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	timeout := 5 * time.Second
	start := time.Now()
	err := nc.FlushTimeout(timeout)
	elapsed := time.Since(start)
	if elapsed >= timeout {
		t.Fatalf("Flush did not return before timeout")
	}
	if err == nil {
		t.Fatal("Expected an error indicating slow consumer")
	}
}

func TestAsyncErrHandler(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	ch := make(chan bool)
	subj := "async_test"

	sub, err := nc.Subscribe(subj, func(_ *Msg) {
		time.Sleep(100 * time.Second)
	})
	if err != nil {
		t.Fatalf("Could not subscribe: %v\n", err)
	}

	nc.Opts.AsyncErrorCB = func(c *Conn, s *Subscription, e error) {
		if s != sub {
			t.Fatal("Did not receive proper subscription")
		}
		if e != ErrSlowConsumer {
			t.Fatalf("Did not receive proper error: %v vs %v\n", e, ErrSlowConsumer)
		}
		ch <- true
	}

	b := []byte("Hello World!")
	for i := 0; i < (maxChanLen + 100); i++ {
		nc.Publish(subj, b)
	}

	nc.Flush()

	if e := wait(ch); e != nil {
		t.Fatal("Failed to call async err handler")
	}
}

// Test to make sure that we can send and async receive messages on
// different subjects within a callback.
func TestAsyncSubscriberStarvation(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	// Helper
	nc.Subscribe("helper", func(m *Msg) {
		nc.Publish(m.Reply, []byte("Hello"))
	})

	ch := make(chan bool)

	// Kickoff
	nc.Subscribe("start", func(m *Msg) {
		// Helper Response
		response := NewInbox()
		nc.Subscribe(response, func(_ *Msg) {
			ch <- true
		})
		nc.PublishRequest("helper", response, []byte("Help Me!"))
	})

	nc.Publish("start", []byte("Begin"))
	nc.Flush()

	if e := wait(ch); e != nil {
		t.Fatal("Was stalled inside of callback waiting on another callback")
	}
}

func TestAsyncSubscribersOnClose(t *testing.T) {
	nc := newConnection(t)

	toSend := 10
	callbacks := int64(0)
	ch := make(chan bool, toSend)

	nc.Subscribe("foo", func(_ *Msg) {
		atomic.AddInt64(&callbacks, 1)
		<-ch
	})

	for i := 0; i < toSend; i++ {
		nc.Publish("foo", []byte("Hello World!"))
	}
	nc.Flush()
	nc.Close()

	// Release callbacks
	for i := 1; i < toSend; i++ {
		ch <- true
	}

	// Wait for some time.
	time.Sleep(10 * time.Millisecond)
	seen := atomic.LoadInt64(&callbacks)
	if seen != 1 {
		t.Fatalf("Expected only one callback, received %d callbacks\n", seen)
	}
}

// FIXME: Hack, make this better
func TestStopServer(t *testing.T) {
	s.stopServer()
}
