// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"reflect"
	"testing"
)

func NewJsonEncodedConn(t *testing.T) *EncodedConn {
	ec, err := NewEncodedConn(newConnection(t), "json")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestJsonMarshalString(t *testing.T) {
	ec := NewJsonEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testString := "Hello World!"

	ec.Subscribe("json_string", func(s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		ch <- true
	})
	ec.Publish("json_string", testString)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestJsonMarshalInt(t *testing.T) {
	ec := NewJsonEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("json_int", func(n int) {
		if n != testN {
			t.Fatalf("Received test int of '%d', wanted '%d'\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("json_int", testN)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

type person struct {
	Name     string
	Address  string
	Age      int
	Children map[string]*person
	Assets   map[string]uint
}

func TestJsonMarshalStruct(t *testing.T) {
	ec := NewJsonEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 16, Address: "85 Second St"}
	me.Children["meg"] = &person{Name: "meg", Age: 14, Address: "85 Second St"}

	me.Assets = make(map[string]uint)
	me.Assets["house"] = 1000
	me.Assets["car"] = 100

	ec.Subscribe("json_struct", func(p *person) {
		ch <- true
		if !reflect.DeepEqual(p, me) {
			t.Fatalf("Did not receive the correct struct response")
		}
		ch <- true
	})

	ec.Publish("json_struct", me)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}
