package test

import (
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/encoders/builtin"
)

func NewMsgpackEncodedConn(tl TestLogger) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(NewConnection(tl, TEST_PORT), nats.MSGPACK_ENCODER)
	if err != nil {
		tl.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestEncBuiltinMsgpackmarshalString(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testString := "Hello World!"

	ec.Subscribe("msgpack_string", func(s string) {
		if s != testString {
			t.Fatalf("Received test string pf '%s', wanted '%s'\n", s, testString)
		}
		ch <- true
	})
	ec.Publish("msgpack_string", testString)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalEmptyString(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	ec.Subscribe("msgpack_empty_string", func(s string) {
		if s != "" {
			t.Fatalf("Received test of '%v', wanted empty string\n", s)
		}
		ch <- true
	})
	ec.Publish("msgpack_empty_string", "")
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalInt(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("msgpack_int", func(n int) {
		if n != testN {
			t.Fatalf("Received test int of '%d', wanted '%d'\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("msgpack_int", testN)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalBool(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	ec.Subscribe("msgpack_bool", func(b bool) {
		if !b {
			t.Fatalf("Received test of '%v', wanted 'true'\n", b)
		}
		ch <- true
	})
	ec.Publish("msgpack_bool", true)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalNull(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()

	type TestType struct{}
	ch := make(chan bool)

	var testValue *TestType

	ec.Subscribe("msgpack_null", func(i any) {
		if i != nil {
			t.Fatalf("Received test of '%v', wanted 'nil'\n", i)
		}
		ch <- true
	})
	ec.Publish("msgpack_null", testValue)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMgspackMarshalArray(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()

	ch := make(chan bool)

	a := []string{"a", "b", "c"}

	ec.Subscribe("msgpack_array", func(v []string) {
		if !reflect.DeepEqual(v, a) {
			t.Fatalf("Received test of '%v', wanted '%v'\n", v, a)
		}
		ch <- true
	})
	ec.Publish("msgpack_array", a)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalEmptyArray(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()

	ch := make(chan bool)

	var a []string

	ec.Subscribe("msgpack_empty_array", func(v []string) {
		if !reflect.DeepEqual(v, a) {
			t.Fatalf("Received test of '%v', wanted '%v'\n", v, a)
		}
		ch <- true
	})
	ec.Publish("msgpack_empty_array", a)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestEncBuiltinMsgpackMarshalStruct(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	me.Assets = make(map[string]uint)
	me.Assets["house"] = 1000
	me.Assets["car"] = 100

	ec.Subscribe("msgpack_struct", func(p *person) {
		if !reflect.DeepEqual(p, me) {
			t.Fatal("Did not receive the correct struct response")
		}
		ch <- true
	})

	ec.Publish("msgpack_struct", me)
	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func BenchmarkMsgpackMarshalStruct(b *testing.B) {
	me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	encoder := &builtin.MsgpackEncoder{}
	for n := 0; n < b.N; n++ {
		if _, err := encoder.Encode("msgpack_benchmark_struct_marshal", me); err != nil {
			b.Fatal("Couldn't serialize object", err)
		}
	}
}

func BenchmarkPublishMsgpackStruct(b *testing.B) {
	// stop benchmark for set-up
	b.StopTimer()

	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(b)
	defer ec.Close()
	ch := make(chan bool)

	me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	ec.Subscribe("msgpack_benchmark_struct_publish", func(p *person) {
		if !reflect.DeepEqual(p, me) {
			b.Fatalf("Did not receive the correct struct response")
		}
		ch <- true
	})

	// resume benchmark
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		ec.Publish("msgpack_struct", me)
		if e := Wait(ch); e != nil {
			b.Fatal("Did not receive the message")
		}
	}
}

func TestEncBuiltinNotMarshableToMsgpack(t *testing.T) {
	je := &builtin.MsgpackEncoder{}
	ch := make(chan bool)
	_, err := je.Encode("foo", ch)
	if err == nil {
		t.Fatal("Expected an error when failing encoding")
	}
}

func TestEncBuiltinMsgpackFailedEncodedPublish(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ec := NewMsgpackEncodedConn(t)
	defer ec.Close()

	ch := make(chan bool)
	err := ec.Publish("foo", ch)
	if err == nil {
		t.Fatal("Expected an error trying to publish a channel")
	}
	err = ec.PublishRequest("foo", "bar", ch)
	if err == nil {
		t.Fatal("Expected an error trying to publish a channel")
	}
	var cr chan bool
	err = ec.Request("foo", ch, &cr, 1*time.Second)
	if err == nil {
		t.Fatal("Expected an error trying to publish a channel")
	}
	err = ec.LastError()
	if err != nil {
		t.Fatalf("Expected LastError to be nil: %q ", err)
	}
}
