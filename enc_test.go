package nats_test

import (
	"testing"
	"time"

	. "github.com/nats-io/nats"
	"github.com/nats-io/nats/encoders/protobuf"
	"github.com/nats-io/nats/encoders/protobuf/testdata"
)

var options = Options{
	Url:            "nats://localhost:22222",
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        DefaultTimeout,
}

////////////////////////////////////////////////////////////////////////////////
// Encoded connection tests
////////////////////////////////////////////////////////////////////////////////

func TestPublishErrorAfterSubscribeDecodeError(t *testing.T) {
	ts := RunServerOnPort(22222)
	defer ts.Shutdown()
	opts := options
	nc, _ := opts.Connect()
	defer nc.Close()
	c, _ := NewEncodedConn(nc, JSON_ENCODER)

	type Message struct {
		Message string
	}
	const testSubj = "test"

	c.Subscribe(testSubj, func(msg *Message) {})

	//Sending invalid json message
	c.Publish(testSubj, `foo`)

	time.Sleep(100 * time.Millisecond)

	if err := c.Publish(testSubj, Message{"2"}); err != nil {
		t.Error("Fail to send correct json message after decede error in subscription")
	}
}

func TestPublishErrorAfterInvalidPublishMessage(t *testing.T) {
	ts := RunServerOnPort(22222)
	defer ts.Shutdown()
	opts := options
	nc, _ := opts.Connect()
	defer nc.Close()
	c, _ := NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)

	const testSubj = "test"

	c.Publish(testSubj, testdata.Person{Name: "Anatolii"})

	//Sending invalid protobuf message
	c.Publish(testSubj, "foo")
	if err := c.Publish(testSubj, testdata.Person{Name: "Anatolii"}); err != nil {
		t.Error("Fail to send correct json message after invalid message publishing")
	}
}
