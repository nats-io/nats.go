// Copyright 2015-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats.go/encoders/protobuf"
	pb "github.com/nats-io/nats.go/encoders/protobuf/testdata"
)

//lint:file-ignore SA1019 Ignore deprecation warnings for EncodedConn

// newProtoEncodedConn wraps an existing *nats.Conn as a Protobuf-encoded connection.
func newProtoEncodedConn(tl testing.TB, nc *nats.Conn) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(nc, protobuf.PROTOBUF_ENCODER)
	if err != nil {
		tl.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestEncProtoMarshalStruct(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newProtoEncodedConn(t, nc)

		me := &pb.Person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
		me.Children = make(map[string]*pb.Person)

		me.Children["sam"] = &pb.Person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
		me.Children["meg"] = &pb.Person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

		ch := make(chan error, 1)
		ec.Subscribe("protobuf_test", func(p *pb.Person) {
			var err error
			if !reflect.DeepEqual(p.ProtoReflect(), me.ProtoReflect()) {
				err = errors.New("Did not receive the correct protobuf response")
			}
			ch <- err
		})

		ec.Publish("protobuf_test", me)
		select {
		case e := <-ch:
			if e != nil {
				t.Fatal(e.Error())
			}
		case <-time.After(time.Second):
			t.Fatal("Failed to receive message")
		}
	})
}

func TestEncProtoNilRequest(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newProtoEncodedConn(t, nc)

		testPerson := &pb.Person{Name: "Anatolii", Age: 25, Address: "Ukraine, Nikolaev"}

		// Subscribe with empty interface shouldn't fail on empty message
		ec.Subscribe("nil_test", func(_, reply string, _ any) {
			ec.Publish(reply, testPerson)
		})

		resp := new(pb.Person)

		// Request with nil argument shouldn't fail
		err := ec.Request("nil_test", nil, resp, 100*time.Millisecond)
		ec.Flush()

		if err != nil {
			t.Error("Fail to send empty message via encoded proto connection")
		}

		if !reflect.DeepEqual(testPerson.ProtoReflect(), resp.ProtoReflect()) {
			t.Error("Fail to receive encoded response")
		}
	})
}

func BenchmarkProtobufMarshalStruct(b *testing.B) {
	me := &pb.Person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*pb.Person)

	me.Children["sam"] = &pb.Person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &pb.Person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	encoder := &protobuf.ProtobufEncoder{}
	for n := 0; n < b.N; n++ {
		if _, err := encoder.Encode("protobuf_test", me); err != nil {
			b.Fatal("Couldn't serialize object", err)
		}
	}
}

// BenchmarkPublishProtobufStruct (original protobuf_test.go) deferred to task 4.13 —
// testservice helpers take *testing.T, not *testing.B.
