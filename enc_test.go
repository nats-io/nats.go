// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"bytes"
	"testing"
	"time"
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
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
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
			t.Fatalf("Received test bytes of '%s', wanted '%s'\n", b, testBytes)
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
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
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
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
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
			t.Fatalf("Received test number of %d, wanted %d\n", n, testN)
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
			t.Fatalf("Received test number of %f, wanted %f\n", n, testN)
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
			t.Fatalf("Received test number of %f, wanted %f\n", n, testN)
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

func TestExtendedSubscribeCB(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	subject := "cb_args"

	ec.Subscribe(subject, func(subj, s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		if subj != subject {
			t.Fatalf("Received subject of '%s', wanted '%s'\n", subj, subject)
		}
		ch <- true
	})
	ec.Publish(subject, testString)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestExtendedSubscribeCB2(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	oSubj := "cb_args"
	oReply := "foobar"

	ec.Subscribe(oSubj, func(subj, reply, s string) {
		if s != testString {
			t.Fatalf("Received test string of '%s', wanted '%s'\n", s, testString)
		}
		if subj != oSubj {
			t.Fatalf("Received subject of '%s', wanted '%s'\n", subj, oSubj)
		}
		if reply != oReply {
			t.Fatalf("Received reply of '%s', wanted '%s'\n", reply, oReply)
		}
		ch <- true
	})
	ec.PublishRequest(oSubj, oReply, testString)
	if e := wait(ch); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
}

func TestEncRequest(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ec.Subscribe("help", func(subj, reply, req string) {
		ec.Publish(reply, "I can help!")
	})

	var resp string

	err := ec.Request("help", "help me", &resp, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed at receiving proper response: %v\n", err)
	}
}

func TestEncRequestReceivesMsg(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ec.Subscribe("help", func(subj, reply, req string) {
		ec.Publish(reply, "I can help!")
	})

	var resp Msg

	err := ec.Request("help", "help me", &resp, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed at receiving proper response: %v\n", err)
	}
}

func TestAsyncMarshalErr(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ch := make(chan bool)

	testString := "Hello World!"
	subject := "err_marshall"

	ec.Subscribe(subject, func(subj, num int) {
		// This will never get called.
	})

	ec.Conn.Opts.AsyncErrorCB = func(c *Conn, s *Subscription, err error) {
		ch <- true
	}

	ec.Publish(subject, testString)
	if e := wait(ch); e != nil {
		t.Fatalf("Did not receive the message: %s", e)
	}
}
