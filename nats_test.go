package nats

import (
	"bytes"
	"math"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCloseLeakingGoRoutines(t *testing.T) {
	base := runtime.NumGoroutine()
	nc := newConnection(t)
	time.Sleep(10 * time.Millisecond)
	nc.Close()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 0 {
		t.Fatalf("%d Go routines still exists post Close()", delta)
	}
	// Make sure we can call Close() multiple times
	nc.Close()
}

func TestMultipleClose(t *testing.T) {
	nc := newConnection(t)
	var wg sync.WaitGroup
	for i:=0 ; i < 10 ; i++ {
		wg.Add(1)
		go func() {
			nc.Close()
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestSimplePublish(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	err := nc.Publish("foo", []byte("Hello World"))
	if err != nil {
		t.Fatal("Failed to publish string message: ", err)
	}
}

func TestSimplePublishNoData(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	err := nc.Publish("foo", nil)
	if err != nil {
		t.Fatal("Failed to publish empty message: ", err)
	}
}

func TestAsyncSubscribe(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	omsg := []byte("Hello World")
	received := 0
	_, err := nc.Subscribe("foo", func(m *Msg) {
		received += 1
		if !bytes.Equal(m.Data, omsg) {
			t.Fatal("Message received does not match")
		}
		if m.Sub == nil {
			t.Fatal("Callback does not have a valid Subsription")
		}
	})
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	nc.Publish("foo", omsg)
	nc.Flush()
	if (received != 1) {
		t.Fatal("Message not received for subscription")
	}
}

func TestSyncSubscribe(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	s, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	omsg := []byte("Hello World")
	nc.Publish("foo", omsg)
	msg, err := s.NextMsg(10 * time.Second)
	if err != nil || !bytes.Equal(msg.Data, omsg) {
		t.Fatal("Message received does not match")
	}
}

func TestPubSubWithReply(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	s, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	omsg := []byte("Hello World")
	nc.PublishMsg(&Msg{Subject: "foo", Reply: "bar", Data: omsg})
	msg, err := s.NextMsg(10 * time.Second)
	if err != nil || !bytes.Equal(msg.Data, omsg) {
		t.Fatal("Message received does not match")
	}
}

func TestFlush(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	omsg := []byte("Hello World")
	received := 0
	nc.Subscribe("flush", func(_ *Msg) {
		received += 1
	})
	total := 10000
	for i := 0; i < total; i++ {
		nc.Publish("flush", omsg)
	}
	err := nc.Flush()
	if err != nil {
		t.Fatalf("Received error from flush: %s\n", err)
	}
	if received != total {
		t.Fatalf("All messages not received: %d != %d\n", received, total)
	}
}

func TestQueueSubscriber(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	r1 := 0
	r2 := 0
	nc.QueueSubscribe("foo", "bar", func(_ *Msg) {
		r1 += 1
	})
	nc.QueueSubscribe("foo", "bar", func(_ *Msg) {
		r2 += 1
	})
	omsg := []byte("Hello World")
	nc.Publish("foo", omsg)
	nc.Flush()
	if (r1 + r2) != 1 {
		t.Fatal("Received too many messages for multiple queue subscribers")
	}
	r1, r2 = 0, 0
	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", omsg)
	}
	nc.Flush()
	v := uint(float32(total) * 0.15)
	if r1 + r2 != total {
		t.Fatalf("Incorrect number of messages: %d vs %d", (r1 + r2), total)
	}
	expected := total / 2
	d1 := uint(math.Abs(float64(expected - r1)))
	d2 := uint(math.Abs(float64(expected - r2)))
	if (d1 > v || d2 > v) {
		t.Fatalf("Too much variance in totals: %d, %d > %d", d1, d2, v)
	}
}

func TestQueueSyncSubscriber(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	s1, _ := nc.QueueSubscribeSync("foo", "bar")
	s2, _ := nc.QueueSubscribeSync("foo", "bar")
	omsg := []byte("Hello World")
	nc.Publish("foo", omsg)
	nc.Flush()
	r1, r2 := len(s1.mch), len(s2.mch)
	if (r1 + r2) != 1 {
		t.Fatal("Received too many messages for multiple queue subscribers")
	}
	// Drain message
	s1.NextMsg(0)
	s2.NextMsg(0)

	total := 100
	for i := 0; i < total; i++ {
		nc.Publish("foo", omsg)
	}
	nc.Flush()
	v := uint(float32(total) * 0.15)
	r1, r2 = len(s1.mch), len(s2.mch)
	if r1+r2 != total {
		t.Fatalf("Incorrect number of messages: %d vs %d", (r1 + r2), total)
	}
	expected := total / 2
	d1 := uint(math.Abs(float64(expected - r1)))
	d2 := uint(math.Abs(float64(expected - r2)))
	if d1 > v || d2 > v {
		t.Fatalf("Too much variance in totals: %d, %d > %d", d1, d2, v)
	}
}

func TestReplyArg(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	replyExpected := "bar"
	cbReceived := false
	nc.Subscribe("foo", func(m *Msg) {
		cbReceived = true
		if (m.Reply != replyExpected) {
			t.Fatalf("Did not receive correct reply arg in callback: " +
				     "('%s' vs '%s')", m.Reply, replyExpected)
		}
	})
	nc.PublishMsg(&Msg{Subject:"foo", Reply:replyExpected, Data:[]byte("Hello")})
	nc.Flush()
	if !cbReceived {
		t.Fatal("Did not receive callback")
	}
}

func TestSyncReplyArg(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	replyExpected := "bar"
	s, _ := nc.SubscribeSync("foo")
	nc.PublishMsg(&Msg{Subject: "foo", Reply: replyExpected, Data: []byte("Hello")})
	nc.Flush()
	msg, err := s.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal("Received and err on NextMsg()")
	}
	if msg.Reply != replyExpected {
		t.Fatalf("Did not receive correct reply arg in callback: "+
			"('%s' vs '%s')", msg.Reply, replyExpected)
	}
}

func TestUnsubscribe(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	received := 0
	max := 10
	nc.Subscribe("foo", func(m *Msg) {
		received += 1
		if received == max {
			err := m.Sub.Unsubscribe()
			if err != nil {
				t.Fatal("Unsubscribe failed with err:", err)
			}
		}
	})
	send := 20
	for i := 0; i < send; i++ {
		nc.Publish("foo", []byte("hello"))
	}
	nc.Flush()
	if received != max {
		t.Fatalf("Received wrong # of messages after unsubscribe: %d vs %d",
			     received, max)
	}
}

func TestDoubleUnsubscribe(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	s, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal("Failed to subscribe: ", err)
	}
	err = s.Unsubscribe()
	if err != nil {
		t.Fatal("Unsubscribe failed with err:", err)
	}
	err = s.Unsubscribe()
	if err == nil {
		t.Fatal("Unsubscribe should have reported an error")
	}
}

func TestRequestTimeout(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	_, err := nc.Request("foo", []byte("help"), 10*time.Millisecond)
	if err == nil {
		t.Fatalf("Expected to receive a timeout error")
	}
}

func TestRequest(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	response := []byte("I will help you")
	nc.Subscribe("foo", func(m *Msg) {
		nc.Publish(m.Reply, response)
	})
	msg, err := nc.Request("foo", []byte("help"), 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Received an error on Request test: %s", err)
	}
	if !bytes.Equal(msg.Data, response) {
		t.Fatalf("Received invalid response")
	}
}

func TestFlushInCB(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()
	nc.Subscribe("foo", func(_ *Msg) {
		nc.Flush()
	})
	nc.Publish("foo", []byte("Hello"))
}

func TestReleaseFlush(t *testing.T) {
	nc := newConnection(t)
	for i := 0; i < 1000; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	go nc.Close()
	nc.Flush()
}

func TestInbox(t *testing.T) {
	inbox := NewInbox()
	matched, _ := regexp.Match(`_INBOX.\S`, []byte(inbox))
	if !matched {
		t.Fatal("Bad INBOX format")
	}
}

func TestStats(t *testing.T) {
	nc := newConnection(t)
	defer nc.Close()

	data := []byte("The quick brown fox jumped over the lazy dog")
	iter := 10

	for i := 0; i < iter; i++ {
		nc.Publish("foo", data)
	}

	if nc.OutMsgs != uint64(iter) {
		t.Fatalf("Not properly tracking OutMsgs: got %d wanted %d\n", nc.OutMsgs, iter)
	}
	obb := uint64(iter * len(data))
	if nc.OutBytes != obb {
		t.Fatalf("Not properly tracking OutBytes: got %d wanted %d\n", nc.OutBytes, obb)
	}

	// Clear outbound
	nc.OutMsgs, nc.OutBytes = 0, 0

	// Test both sync and async versions of subscribe.
	nc.Subscribe("foo", func(_ *Msg) {})
	nc.SubscribeSync("foo")

	for i:=0; i<iter ; i++ {
		nc.Publish("foo", data)
	}
	nc.Flush()

	if nc.InMsgs != uint64(2*iter) {
		t.Fatalf("Not properly tracking InMsgs: got %d wanted %d\n", nc.InMsgs, 2*iter)
	}

	ibb := 2 * obb
	if nc.InBytes != ibb {
		t.Fatalf("Not properly tracking InBytes: got %d wanted %d\n", nc.InBytes, ibb)
	}
}
