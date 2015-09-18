package nats

import (
	"reflect"
	"testing"

	pb "github.com/nats-io/nats/testdata"
)

func NewProtoEncodedConn(t *testing.T) *EncodedConn {
	ec, err := NewEncodedConn(newConnection(t), PROTOBUF_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestProtoMarshalStruct(t *testing.T) {
	ec := NewProtoEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	me := &pb.Person{Name: "derek", Age: 22, Address: "85 Second St"}
	me.Children = make(map[string]*pb.Person)

	me.Children["sam"] = &pb.Person{Name: "sam", Age: 16, Address: "85 Second St"}
	me.Children["meg"] = &pb.Person{Name: "meg", Age: 14, Address: "85 Second St"}

	ec.Subscribe("proto_struct", func(p *pb.Person) {
		ch <- true
		if !reflect.DeepEqual(p, me) {
			t.Fatalf("Did not receive the correct struct response")
		}
		ch <- true
	})

	ec.Publish("proto_struct", me)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}
