package nats

import (
	"testing"
	"fmt"
//	"os"
	"os/exec"
	"net"
	"time"
	"math"
	"runtime"
	"regexp"
	"bytes"
	"strings"
)

const natsServer = "nats-server"

type server struct {
	args []string
	cmd  *exec.Cmd
}

var s *server

func startNatsServer(t *testing.T, port uint, other string) *server {
	var s server
	args := fmt.Sprintf("-p %d %s", port, other)
	s.args = strings.Split(args, " ")
	s.cmd = exec.Command(natsServer, s.args...)
	err := s.cmd.Start()
	if err != nil {
		s.cmd = nil
		t.Errorf("Could not start %s, is NATS installed and in path?", natsServer)
		return &s
	}
	// Give it time to start up
	start := time.Now()
	for {
		addr := fmt.Sprintf("localhost:%d", port)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			if time.Since(start) > (5 * time.Second) {
				t.Fatalf("Timed out trying to connect to %s", natsServer)
				return nil
			}
		} else {
			c.Close()
			break
		}
	}
	return &s
}

func (s *server) stopServer() {
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
}

func newConnection(t *testing.T) *Conn {
	nc, err := Connect(DefaultURL)
	if err != nil {
		t.Fatal("Failed to create default connection", err)
		return nil
	}
	return nc
}

func TestDefaultConnection(t *testing.T) {
	s = startNatsServer(t, DefaultPort, "")
	nc := newConnection(t)
	nc.Close()
}

func TestClose(t *testing.T) {
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
		if (m.Sub == nil) {
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
	nc.PublishMsg(&Msg{Subject:"foo", Reply:"bar", Data:omsg})
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
	nc.PublishMsg(&Msg{Subject:"foo", Reply:replyExpected, Data:[]byte("Hello")})
	nc.Flush()
	msg, err := s.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal("Received and err on NextMsg()")
	}
	if (msg.Reply != replyExpected) {
		t.Fatalf("Did not receive correct reply arg in callback: " +
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
