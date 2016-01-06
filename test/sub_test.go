package test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

// More advanced tests on subscriptions

func TestServerAutoUnsub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	received := int32(0)
	max := int32(10)

	// Call this to make sure that we have everything setup connection wise
	nc.Flush()

	base := runtime.NumGoroutine()

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		atomic.AddInt32(&received, 1)
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	sub.AutoUnsubscribe(int(max))
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&received) != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
	if sub.IsValid() {
		t.Fatal("Expected subscription to be invalid after hitting max")
	}
	delta := (runtime.NumGoroutine() - base)
	if delta > 0 {
		t.Fatalf("%d Go routines still exist post max subscriptions hit", delta)
	}
}

func TestClientSyncAutoUnsub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	received := 0
	max := 10
	sub, _ := nc.SubscribeSync("foo")
	sub.AutoUnsubscribe(max)
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()
	for {
		_, err := sub.NextMsg(10 * time.Millisecond)
		if err != nil {
			if err != nats.ErrMaxMessages {
				t.Fatalf("Expected '%v', but got: '%v'\n", nats.ErrBadSubscription, err.Error())
			}
			break
		}
		received += 1
	}
	if received != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
	if sub.IsValid() {
		t.Fatal("Expected subscription to be invalid after hitting max")
	}
}

func TestClientASyncAutoUnsub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	received := int32(0)
	max := int32(10)
	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		atomic.AddInt32(&received, 1)
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	sub.AutoUnsubscribe(int(max))
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&received) != max {
		t.Fatalf("Received %d msgs, wanted only %d\n", received, max)
	}
}

func TestCloseSubRelease(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

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
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	if !sub.IsValid() {
		t.Fatalf("Subscription should be valid")
	}
	for i := 0; i < 10; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()
	_, err = sub.NextMsg(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("NextMsg returned an error")
	}
	sub.Unsubscribe()
	_, err = sub.NextMsg(200 * time.Millisecond)
	if err == nil {
		t.Fatalf("NextMsg should have returned an error")
	}
}

func TestSlowSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Make the sub channel length small so that it reduces the risk of failure
	// when running with GOMAXPROCS=1 (the server would disconnect the client as
	// a slow consumer). Alternatively, leave SubChanLen alone, but uncomment the
	// line in the publish loop).
	nc.Opts.SubChanLen = 100

	sub, _ := nc.SubscribeSync("foo")

	for i := 0; i < (nc.Opts.SubChanLen + 100); i++ {
		nc.Publish("foo", []byte("Hello"))
		//		runtime.Gosched()
	}
	timeout := 5 * time.Second
	start := time.Now()
	nc.FlushTimeout(timeout)
	elapsed := time.Since(start)
	if elapsed >= timeout {
		t.Fatalf("Flush did not return before timeout: %d > %d", elapsed, timeout)
	}
	// Make sure NextMsg returns an error to indicate slow consumer
	_, err := sub.NextMsg(200 * time.Millisecond)
	if err == nil {
		t.Fatalf("NextMsg did not return an error")
	}
}

func TestSlowAsyncSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	bch := make(chan bool)

	// Make the sub channel length small so that it reduces the risk of failure
	// when running with GOMAXPROCS=1 (the server would disconnect the client as
	// a slow consumer). Alternatively, leave SubChanLen alone, but uncomment the
	// line in the publish loop).
	nc.Opts.SubChanLen = 100

	nc.Subscribe("foo", func(_ *nats.Msg) {
		// block to back us up..
		<-bch
	})
	for i := 0; i < (nc.Opts.SubChanLen + 100); i++ {
		nc.Publish("foo", []byte("Hello"))
		//		runtime.Gosched()
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
	// release the sub
	bch <- true
}

func TestAsyncErrHandler(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	// Limit internal subchan length to trip condition easier.
	opts := nats.DefaultOptions
	opts.SubChanLen = 10

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect to server: %v\n", err)
	}
	defer nc.Close()

	subj := "async_test"
	bch := make(chan bool)

	sub, err := nc.Subscribe(subj, func(_ *nats.Msg) {
		// block to back us up..
		<-bch
	})
	if err != nil {
		t.Fatalf("Could not subscribe: %v\n", err)
	}

	ch := make(chan bool)

	aeCalled := int64(0)

	nc.Opts.AsyncErrorCB = func(c *nats.Conn, s *nats.Subscription, e error) {
		// Suppress additional calls
		if atomic.LoadInt64(&aeCalled) == 1 {
			return
		}
		atomic.AddInt64(&aeCalled, 1)

		if s != sub {
			t.Fatal("Did not receive proper subscription")
		}
		if e != nats.ErrSlowConsumer {
			t.Fatalf("Did not receive proper error: %v vs %v\n", e, nats.ErrSlowConsumer)
		}
		// release the sub
		bch <- true

		// release the test
		ch <- true
	}

	b := []byte("Hello World!")
	for i := 0; i < (nc.Opts.SubChanLen + 100); i++ {
		nc.Publish(subj, b)
	}
	nc.Flush()

	if e := Wait(ch); e != nil {
		t.Fatal("Failed to call async err handler")
	}
}

// Test to make sure that we can send and async receive messages on
// different subjects within a callback.
func TestAsyncSubscriberStarvation(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Helper
	nc.Subscribe("helper", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("Hello"))
	})

	ch := make(chan bool)

	// Kickoff
	nc.Subscribe("start", func(m *nats.Msg) {
		// Helper Response
		response := nats.NewInbox()
		nc.Subscribe(response, func(_ *nats.Msg) {
			ch <- true
		})
		nc.PublishRequest("helper", response, []byte("Help Me!"))
	})

	nc.Publish("start", []byte("Begin"))
	nc.Flush()

	if e := Wait(ch); e != nil {
		t.Fatal("Was stalled inside of callback waiting on another callback")
	}
}

func TestAsyncSubscribersOnClose(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	toSend := 10
	callbacks := int32(0)
	ch := make(chan bool, toSend)

	nc.Subscribe("foo", func(_ *nats.Msg) {
		atomic.AddInt32(&callbacks, 1)
		<-ch
	})

	for i := 0; i < toSend; i++ {
		nc.Publish("foo", []byte("Hello World!"))
	}
	nc.Flush()
	time.Sleep(10 * time.Millisecond)
	nc.Close()

	// Release callbacks
	for i := 1; i < toSend; i++ {
		ch <- true
	}

	// Wait for some time.
	time.Sleep(10 * time.Millisecond)
	seen := atomic.LoadInt32(&callbacks)
	if seen != 1 {
		t.Fatalf("Expected only one callback, received %d callbacks\n", seen)
	}
}

func TestNextMsgCallOnAsyncSub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err == nil {
		t.Fatal("Expected an error call NextMsg() on AsyncSubscriber")
	}
}

func TestNextMsgCallOnClosedSub(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {

		t.Fatal("Failed to subscribe: ", err)
	}

	if err = sub.Unsubscribe(); err != nil {
		t.Fatal("Unsubscribe failed with err:", err)
	}

	_, err = sub.NextMsg(time.Second)
	if err == nil {
		t.Fatal("Expected an error calling NextMsg() on closed subscription")
	} else if err != nats.ErrBadSubscription {
		t.Fatalf("Expected '%v', but got: '%v'\n", nats.ErrBadSubscription, err.Error())
	}
}
