package protobuf

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats"
	pb "github.com/nats-io/nats/encoders/protobuf/testdata"
)

// Dumb wait program to sync on callbacks, etc... Will timeout
func wait(ch chan bool) error {
	return waitTime(ch, 500*time.Millisecond)
}

func waitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func newConnection(t *testing.T) *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

func NewProtoEncodedConn(t *testing.T) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(newConnection(t), PROTOBUF_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestProtoMarshalStruct(t *testing.T) {
	ec := NewProtoEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	me := &pb.Person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*pb.Person)

	me.Children["sam"] = &pb.Person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &pb.Person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	ec.Subscribe("protobuf_test", func(p *pb.Person) {
		ch <- true
		if !reflect.DeepEqual(p, me) {
			t.Fatalf("Did not receive the correct protobuf response")
		}
		ch <- true
	})

	ec.Publish("protobuf_test", me)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}
