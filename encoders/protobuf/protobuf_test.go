package protobuf_test

import (
	"reflect"
	"testing"

	"github.com/nats-io/nats"
	"github.com/nats-io/nats/test"

	"github.com/nats-io/nats/encoders/protobuf"
	pb "github.com/nats-io/nats/encoders/protobuf/testdata"
)

func NewProtoEncodedConn(t *testing.T) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(test.NewDefaultConnection(t), protobuf.PROTOBUF_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestProtoMarshalStruct(t *testing.T) {
	s := test.RunDefaultServer()
	defer s.Shutdown()

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
	if e := test.Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}
