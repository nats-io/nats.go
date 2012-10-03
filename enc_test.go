// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"bytes"
	"testing"
)

func NewEConn(t *testing.T) *EncodedConn {
	ec, err := NewEncodedConn(newConnection(t), "default")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestMarshalString(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testString := "Hello World!"

	ec.Subscribe("enc_string", func(s string) {
		if s != testString {
			t.Fatalf("Got test string of '%s', wanted '%s'\n", s, testString)
		}
		ch <- true
	})
	ec.Publish("enc_string", testString)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalBytes(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testBytes := []byte("Hello World!")

	ec.Subscribe("enc_bytes", func(b []byte) {
		if !bytes.Equal(b, testBytes) {
			t.Fatalf("Got test bytes of '%s', wanted '%s'\n", b, testBytes)
		}
		ch <- true
	})
	ec.Publish("enc_bytes", testBytes)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalInt(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int) {
		if n != testN {
			t.Fatalf("Got test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalInt32(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int32) {
		if n != int32(testN) {
			t.Fatalf("Got test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalInt64(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := 22

	ec.Subscribe("enc_int", func(n int64) {
		if n != int64(testN) {
			t.Fatalf("Got test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_int", testN)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalFloat32(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := float32(22)

	ec.Subscribe("enc_float", func(n float32) {
		if n != testN {
			t.Fatalf("Got test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_float", testN)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalFloat64(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	testN := float64(22.22)

	ec.Subscribe("enc_float", func(n float64) {
		if n != testN {
			t.Fatalf("Got test number of %d, wanted %d\n", n, testN)
		}
		ch <- true
	})
	ec.Publish("enc_float", testN)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestMarshalBool(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()
	ch := make(chan bool)

	ec.Subscribe("enc_bool", func(b bool) {
		if b != false {
			t.Fatal("Boolean values did not match")
		}
		ch <- true
	})
	ec.Publish("enc_bool", false)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}
