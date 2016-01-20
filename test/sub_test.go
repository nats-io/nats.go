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
		received++
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

	sub, _ := nc.SubscribeSync("foo")
	sub.SetPendingLimits(100, 1024)

	for i := 0; i < 200; i++ {
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
	_, err := sub.NextMsg(200 * time.Millisecond)
	if err == nil {
		t.Fatalf("NextMsg did not return an error")
	}
}

func TestSlowChanSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	ch := make(chan *nats.Msg, 64)
	sub, _ := nc.ChanSubscribe("foo", ch)
	sub.SetPendingLimits(100, 1024)

	for i := 0; i < 200; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	timeout := 5 * time.Second
	start := time.Now()
	nc.FlushTimeout(timeout)
	elapsed := time.Since(start)
	if elapsed >= timeout {
		t.Fatalf("Flush did not return before timeout: %d > %d", elapsed, timeout)
	}
}

func TestSlowAsyncSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	bch := make(chan bool)

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {
		// block to back us up..
		<-bch
	})
	// Make sure these are the defaults
	pm, pb, _ := sub.PendingLimits()
	if pm != nats.DefaultSubPendingMsgsLimit {
		t.Fatalf("Pending limit for number of msgs incorrect, expected %d, got %d\n", nats.DefaultSubPendingMsgsLimit, pm)
	}
	if pb != nats.DefaultSubPendingBytesLimit {
		t.Fatalf("Pending limit for number of bytes incorrect, expected %d, got %d\n", nats.DefaultSubPendingBytesLimit, pb)
	}

	// Set new limits
	pml := 100
	pbl := 1024 * 1024

	sub.SetPendingLimits(pml, pbl)

	// Make sure the set is correct
	pm, pb, _ = sub.PendingLimits()
	if pm != pml {
		t.Fatalf("Pending limit for number of msgs incorrect, expected %d, got %d\n", pml, pm)
	}
	if pb != pbl {
		t.Fatalf("Pending limit for number of bytes incorrect, expected %d, got %d\n", pbl, pb)
	}

	for i := 0; i < (int(pml) + 100); i++ {
		nc.Publish("foo", []byte("Hello"))
	}

	timeout := 5 * time.Second
	start := time.Now()
	err := nc.FlushTimeout(timeout)
	elapsed := time.Since(start)
	if elapsed >= timeout {
		t.Fatalf("Flush did not return before timeout")
	}
	// We want flush to work, so expect no error for it.
	if err != nil {
		t.Fatalf("Expected no error from Flush()\n")
	}
	if nc.LastError() != nats.ErrSlowConsumer {
		t.Fatal("Expected LastError to indicate slow consumer")
	}
	// release the sub
	bch <- true
}

func TestAsyncErrHandler(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	opts := nats.DefaultOptions

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

	limit := 10
	toSend := 100

	// Limit internal subchan length to trip condition easier.
	sub.SetPendingLimits(limit, 1024)

	ch := make(chan bool)

	aeCalled := int64(0)

	nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, e error) {
		atomic.AddInt64(&aeCalled, 1)

		if s != sub {
			t.Fatal("Did not receive proper subscription")
		}
		if e != nats.ErrSlowConsumer {
			t.Fatalf("Did not receive proper error: %v vs %v\n", e, nats.ErrSlowConsumer)
		}
		// Suppress additional calls
		if atomic.LoadInt64(&aeCalled) == 1 {
			// release the sub
			defer close(bch)
			// release the test
			ch <- true
		}
	})

	b := []byte("Hello World!")
	// First one trips the ch wait in subscription callback.
	nc.Publish(subj, b)
	nc.Flush()
	for i := 0; i < toSend; i++ {
		nc.Publish(subj, b)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Got an error on Flush:%v\n", err)
	}

	if e := Wait(ch); e != nil {
		t.Fatal("Failed to call async err handler")
	}
	// Make sure dropped stats is correct.
	if d, _ := sub.Dropped(); d != toSend-limit {
		t.Fatalf("Expected Dropped to be %d, got %d\n", toSend-limit, d)
	}
	if ae := atomic.LoadInt64(&aeCalled); ae != 1 {
		t.Fatalf("Expected err handler to be called once, got %d\n", ae)
	}
}

func TestAsyncErrHandlerChanSubscription(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	opts := nats.DefaultOptions

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect to server: %v\n", err)
	}
	defer nc.Close()

	subj := "chan_test"

	limit := 10
	toSend := 100

	// Create our own channel.
	mch := make(chan *nats.Msg, limit)
	sub, err := nc.ChanSubscribe(subj, mch)
	if err != nil {
		t.Fatalf("Could not subscribe: %v\n", err)
	}
	ch := make(chan bool)
	aeCalled := int64(0)

	nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, e error) {
		atomic.AddInt64(&aeCalled, 1)
		if e != nats.ErrSlowConsumer {
			t.Fatalf("Did not receive proper error: %v vs %v\n",
				e, nats.ErrSlowConsumer)
		}
		// Suppress additional calls
		if atomic.LoadInt64(&aeCalled) == 1 {
			// release the test
			ch <- true
		}
	})

	b := []byte("Hello World!")
	for i := 0; i < toSend; i++ {
		nc.Publish(subj, b)
	}
	nc.Flush()

	if e := Wait(ch); e != nil {
		t.Fatal("Failed to call async err handler")
	}
	// Make sure dropped stats is correct.
	if d, _ := sub.Dropped(); d != toSend-limit {
		t.Fatalf("Expected Dropped to be %d, go %d\n", toSend-limit, d)
	}
	if ae := atomic.LoadInt64(&aeCalled); ae != 1 {
		t.Fatalf("Expected err handler to be called once, got %d\n", ae)
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

func TestChanSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Create our own channel.
	ch := make(chan *nats.Msg, 128)

	_, err := nc.ChanSubscribe("foo", ch)
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}

	// Send some messages to ourselves.
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}

	received := 0
	tm := time.NewTimer(5 * time.Second)
	defer tm.Stop()

	// Go ahead and receive
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatalf("Got an error reading from channel")
			}
		case <-tm.C:
			t.Fatalf("Timed out waiting on messages")
		}
		received++
		if received >= total {
			return
		}
	}
}

func TestChanQueueSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Create our own channel.
	ch1 := make(chan *nats.Msg, 64)
	ch2 := make(chan *nats.Msg, 64)

	nc.ChanQueueSubscribe("foo", "bar", ch1)
	nc.ChanQueueSubscribe("foo", "bar", ch2)

	// Send some messages to ourselves.
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}

	received := 0
	tm := time.NewTimer(5 * time.Second)
	defer tm.Stop()

	chk := func(ok bool) {
		if !ok {
			t.Fatalf("Got an error reading from channel")
		} else {
			received++
		}
	}

	// Go ahead and receive
	for {
		select {
		case _, ok := <-ch1:
			chk(ok)
		case _, ok := <-ch2:
			chk(ok)
		case <-tm.C:
			t.Fatalf("Timed out waiting on messages")
		}
		if received >= total {
			return
		}
	}
}

func TestCloseChanOnSubscriber(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Create our own channel.
	ch := make(chan *nats.Msg, 8)
	sub, _ := nc.ChanSubscribe("foo", ch)

	// Send some messages to ourselves.
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}

	sub.Unsubscribe()
	for len(ch) > 0 {
		<-ch
	}
	// Make sure we can send to the channel still.
	// Test that we do not close it.
	ch <- &nats.Msg{}
}

func TestAsyncSubscriptionPending(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Send some messages to ourselves.
	total := 100
	msg := []byte("0123456789")

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {
		time.Sleep(1 * time.Second)
	})
	defer sub.Unsubscribe()

	for i := 0; i < total; i++ {
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Test old way
	q, _ := sub.QueuedMsgs()
	if q != total && q != total-1 {
		t.Fatalf("Expected %d or %d, got %d\n", total, total-1, q)
	}

	// New way, make sure the same and check bytes.
	m, b, _ := sub.Pending()
	mlen := len(msg)

	if m != total && m != total-1 {
		t.Fatalf("Expected msgs of %d or %d, got %d\n", total, total-1, m)
	}
	if b != total*mlen && b != (total-1)*mlen {
		t.Fatalf("Expected bytes of %d or %d, got %d\n",
			total*mlen, (total-1)*mlen, b)
	}
}

func TestAsyncSubscriptionPendingDrain(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Send some messages to ourselves.
	total := 100
	msg := []byte("0123456789")

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {})
	defer sub.Unsubscribe()

	for i := 0; i < total; i++ {
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Wait for all delivered.
	for d, _ := sub.Delivered(); d != int64(total); d, _ = sub.Delivered() {
		time.Sleep(10 * time.Millisecond)
	}

	m, b, _ := sub.Pending()
	if m != 0 {
		t.Fatalf("Expected msgs of 0, got %d\n", m)
	}
	if b != 0 {
		t.Fatalf("Expected bytes of 0, got %d\n", b)
	}
}

func TestSyncSubscriptionPendingDrain(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	// Send some messages to ourselves.
	total := 100
	msg := []byte("0123456789")

	sub, _ := nc.SubscribeSync("foo")
	defer sub.Unsubscribe()

	for i := 0; i < total; i++ {
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Wait for all delivered.
	for d, _ := sub.Delivered(); d != int64(total); d, _ = sub.Delivered() {
		sub.NextMsg(10 * time.Millisecond)
	}

	m, b, _ := sub.Pending()
	if m != 0 {
		t.Fatalf("Expected msgs of 0, got %d\n", m)
	}
	if b != 0 {
		t.Fatalf("Expected bytes of 0, got %d\n", b)
	}
}

func TestSyncSubscriptionPending(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	defer sub.Unsubscribe()

	// Send some messages to ourselves.
	total := 100
	msg := []byte("0123456789")
	for i := 0; i < total; i++ {
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Test old way
	q, _ := sub.QueuedMsgs()
	if q != total && q != total-1 {
		t.Fatalf("Expected %d or %d, got %d\n", total, total-1, q)
	}

	// New way, make sure the same and check bytes.
	m, b, _ := sub.Pending()
	mlen := len(msg)

	if m != total {
		t.Fatalf("Expected msgs of %d, got %d\n", total, m)
	}
	if b != total*mlen {
		t.Fatalf("Expected bytes of %d, got %d\n", total*mlen, b)
	}

	// Now drain some down and make sure pending is correct
	for i := 0; i < total-1; i++ {
		sub.NextMsg(10 * time.Millisecond)
	}
	m, b, _ = sub.Pending()
	if m != 1 {
		t.Fatalf("Expected msgs of 1, got %d\n", m)
	}
	if b != mlen {
		t.Fatalf("Expected bytes of %d, got %d\n", mlen, b)
	}
}

func TestSubscriptionTypes(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {})
	defer sub.Unsubscribe()
	if st := sub.Type(); st != nats.AsyncSubscription {
		t.Fatalf("Expected AsyncSubscription, got %v\n", st)
	}
	// Check Pending
	if err := sub.SetPendingLimits(1, 100); err != nil {
		t.Fatalf("We should be able to SetPendingLimits()")
	}
	if _, _, err := sub.Pending(); err != nil {
		t.Fatalf("We should be able to call Pending()")
	}

	sub, _ = nc.SubscribeSync("foo")
	defer sub.Unsubscribe()
	if st := sub.Type(); st != nats.SyncSubscription {
		t.Fatalf("Expected SyncSubscription, got %v\n", st)
	}
	// Check Pending
	if err := sub.SetPendingLimits(1, 100); err != nil {
		t.Fatalf("We should be able to SetPendingLimits()")
	}
	if _, _, err := sub.Pending(); err != nil {
		t.Fatalf("We should be able to call Pending()")
	}

	sub, _ = nc.ChanSubscribe("foo", make(chan *nats.Msg))
	defer sub.Unsubscribe()
	if st := sub.Type(); st != nats.ChanSubscription {
		t.Fatalf("Expected ChanSubscription, got %v\n", st)
	}
	// Check Pending
	if err := sub.SetPendingLimits(1, 100); err == nil {
		t.Fatalf("We should NOT be able to SetPendingLimits() on ChanSubscriber")
	}
	if _, _, err := sub.Pending(); err == nil {
		t.Fatalf("We should NOT be able to call Pending() on ChanSubscriber")
	}
}
