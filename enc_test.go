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

	//Test message type
	type Message struct {
		Message string
	}
	const testSubj = "test"

	c.Subscribe(testSubj, func(msg *Message) {})

	//Publish invalid json to catch decode error in subscription callback
	c.Publish(testSubj, `foo`)
	c.Flush()

	//Next publish should be successful
	if err := c.Publish(testSubj, Message{"2"}); err != nil {
		t.Error("Fail to send correct json message after decode error in subscription")
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

	c.Publish(testSubj, &testdata.Person{Name: "Anatolii"})

	//Publish invalid protobuff message to catch decode error
	c.Publish(testSubj, "foo")

	//Next publish with valid protobuf message should be successful
	if err := c.Publish(testSubj, &testdata.Person{Name: "Anatolii"}); err != nil {
		t.Error("Fail to send correct json message after invalid message publishing", err)
	}
}
