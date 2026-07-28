// Copyright 2012-2026 The NATS Authors
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
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/encoders/builtin"
)

//lint:file-ignore SA1019 Ignore deprecation warnings for EncodedConn

// newJSONEncodedConn wraps an existing *nats.Conn as a JSON-encoded connection.
// The wrapped conn's lifetime is managed by the helper that produced it.
func newJSONEncodedConn(tl testing.TB, nc *nats.Conn) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		tl.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestEncBuiltinJsonMarshalString(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		testString := "Hello World!"

		ec.Subscribe("json_string", func(s string) {
			if s != testString {
				t.Fatalf("Received test string of '%s', wanted '%s'", s, testString)
			}
			ch <- true
		})
		ec.Publish("json_string", testString)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalEmptyString(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		ec.Subscribe("json_empty_string", func(s string) {
			if s != "" {
				t.Fatalf("Received test of '%v', wanted empty string", s)
			}
			ch <- true
		})
		ec.Publish("json_empty_string", "")
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalInt(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		testN := 22

		ec.Subscribe("json_int", func(n int) {
			if n != testN {
				t.Fatalf("Received test int of '%d', wanted '%d'", n, testN)
			}
			ch <- true
		})
		ec.Publish("json_int", testN)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalBool(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		ec.Subscribe("json_bool", func(b bool) {
			if !b {
				t.Fatalf("Received test of '%v', wanted 'true'", b)
			}
			ch <- true
		})
		ec.Publish("json_bool", true)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalNull(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)

		type TestType struct{}
		ch := make(chan bool)

		var testValue *TestType

		ec.Subscribe("json_null", func(i any) {
			if i != nil {
				t.Fatalf("Received test of '%v', wanted 'nil'", i)
			}
			ch <- true
		})
		ec.Publish("json_null", testValue)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalArray(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		a := []string{"a", "b", "c"}

		ec.Subscribe("json_array", func(v []string) {
			if !reflect.DeepEqual(v, a) {
				t.Fatalf("Received test of '%v', wanted '%v'", v, a)
			}
			ch <- true
		})
		ec.Publish("json_array", a)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func TestEncBuiltinJsonMarshalEmptyArray(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		var a []string

		ec.Subscribe("json_empty_array", func(v []string) {
			if !reflect.DeepEqual(v, a) {
				t.Fatalf("Received test of '%v', wanted '%v'", v, a)
			}
			ch <- true
		})
		ec.Publish("json_empty_array", a)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

type person struct {
	Name     string
	Address  string
	Age      int
	Children map[string]*person
	Assets   map[string]uint
}

func TestEncBuiltinJsonMarshalStruct(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)
		ch := make(chan bool)

		me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
		me.Children = make(map[string]*person)

		me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
		me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

		me.Assets = make(map[string]uint)
		me.Assets["house"] = 1000
		me.Assets["car"] = 100

		ec.Subscribe("json_struct", func(p *person) {
			if !reflect.DeepEqual(p, me) {
				t.Fatal("Did not receive the correct struct response")
			}
			ch <- true
		})

		ec.Publish("json_struct", me)
		if e := Wait(ch); e != nil {
			t.Fatal("Did not receive the message")
		}
	})
}

func BenchmarkJsonMarshalStruct(b *testing.B) {
	me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
	me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

	encoder := &builtin.JsonEncoder{}
	for n := 0; n < b.N; n++ {
		if _, err := encoder.Encode("json_benchmark_struct_marshal", me); err != nil {
			b.Fatal("Couldn't serialize object", err)
		}
	}
}

func BenchmarkPublishJsonStruct(b *testing.B) {
	b.StopTimer()
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		ec := newJSONEncodedConn(b, nc)
		defer ec.Close()
		ch := make(chan bool)

		me := &person{Name: "derek", Age: 22, Address: "140 New Montgomery St"}
		me.Children = make(map[string]*person)
		me.Children["sam"] = &person{Name: "sam", Age: 19, Address: "140 New Montgomery St"}
		me.Children["meg"] = &person{Name: "meg", Age: 17, Address: "140 New Montgomery St"}

		ec.Subscribe("json_benchmark_struct_publish", func(p *person) {
			if !reflect.DeepEqual(p, me) {
				b.Fatalf("Did not receive the correct struct response")
			}
			ch <- true
		})

		b.StartTimer()
		for range b.N {
			ec.Publish("json_benchmark_struct_publish", me)
			if e := Wait(ch); e != nil {
				b.Fatal("Did not receive the message")
			}
		}
	})
}

func TestEncBuiltinNotMarshableToJson(t *testing.T) {
	je := &builtin.JsonEncoder{}
	ch := make(chan bool)
	_, err := je.Encode("foo", ch)
	if err == nil {
		t.Fatal("Expected an error when failing encoding")
	}
}

func TestEncBuiltinFailedEncodedPublish(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newJSONEncodedConn(t, nc)

		ch := make(chan bool)
		if err := ec.Publish("foo", ch); err == nil {
			t.Fatal("Expected an error trying to publish a channel")
		}
		if err := ec.PublishRequest("foo", "bar", ch); err == nil {
			t.Fatal("Expected an error trying to publish a channel")
		}
		var cr chan bool
		if err := ec.Request("foo", ch, &cr, 1*time.Second); err == nil {
			t.Fatal("Expected an error trying to publish a channel")
		}
		if err := ec.LastError(); err != nil {
			t.Fatalf("Expected LastError to be nil: %q ", err)
		}
	})
}

func TestEncBuiltinDecodeConditionals(t *testing.T) {
	je := &builtin.JsonEncoder{}

	b, err := je.Encode("foo", 22)
	if err != nil {
		t.Fatalf("Expected no error when encoding, got %v\n", err)
	}
	var foo string
	var bar []byte
	err = je.Decode("foo", b, &foo)
	if err != nil {
		t.Fatalf("Expected no error when decoding, got %v\n", err)
	}
	err = je.Decode("foo", b, &bar)
	if err != nil {
		t.Fatalf("Expected no error when decoding, got %v\n", err)
	}
}
