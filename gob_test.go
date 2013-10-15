// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"reflect"
	"testing"
)

func NewGobEncodedConn(t *testing.T) *EncodedConn {
	ec, err := NewEncodedConn(newConnection(t), "gob")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestGobMarshalString(t *testing.T) {
	ec := NewGobEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testString := "Hello World!"

	ec.Subscribe("gob_string", func(s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		ch <- true
	})
	ec.Publish("gob_string", testString)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestGobMarshalInt(t *testing.T) {
	ec := NewGobEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("gob_int", func(n int) {
		if n != testN {
			t.Fatalf("Received test int of '%d', wanted '%d'\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("gob_int", testN)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}

func TestGobMarshalStruct(t *testing.T) {
	ec := NewGobEncodedConn(t)
	defer ec.Close()
	ch := make(chan bool)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	me.Children = make(map[string]*person)

	me.Children["sam"] = &person{Name: "sam", Age: 16, Address: "85 Second St"}
	me.Children["meg"] = &person{Name: "meg", Age: 14, Address: "85 Second St"}

	me.Assets = make(map[string]uint)
	me.Assets["house"] = 1000
	me.Assets["car"] = 100

	ec.Subscribe("gob_struct", func(p *person) {
		ch <- true
		if !reflect.DeepEqual(p, me) {
			t.Fatalf("Did not receive the correct struct response")
		}
		ch <- true
	})

	ec.Publish("gob_struct", me)
	if e := wait(ch); e != nil {
		t.Fatal("Did not receive the message")
	}
}
