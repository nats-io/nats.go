// Copyright 2020 The NATS Authors
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

package nats

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func startJetStream(t *testing.T) (*server.Server, *server.Stream, *server.Consumer, *Conn) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	sopts := natsserver.DefaultTestOptions
	sopts.JetStream = true
	sopts.StoreDir = td
	sopts.Port = -1
	sopts.NoLog = false
	sopts.TraceVerbose = true
	sopts.Trace = true
	sopts.LogFile = "/tmp/nats.log"

	srv, err := server.NewServer(&sopts)
	if err != nil {
		t.Fatal(err)
	}

	srv.ConfigureLogger()
	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatalf("server did not become ready")
	}

	str, err := srv.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"js.in.test"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	cons, err := str.AddConsumer(&server.ConsumerConfig{
		Durable:   "PULL",
		AckPolicy: server.AckExplicit,
	})
	if err != nil {
		t.Fatalf("consumer create failed: %s", err)
	}

	nc, err := Connect(srv.ClientURL(), UseOldRequestStyle())
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	for i := 1; i <= 20; i++ {
		err := nc.Publish("js.in.test", []byte(fmt.Sprintf("msg %d", i)), PublishExpectsStream("TEST"))
		if err != nil {
			t.Fatalf("publish failed: %s", err)
		}
	}

	return srv, str, cons, nc
}

func TestJetStreamPublish(t *testing.T) {
	srv, _, _, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	err := nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream("TEST"), PublishStreamTimeout(time.Second))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	err = nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream("OTHER"), PublishStreamTimeout(time.Second))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err.Error() != `received ack from stream "TEST"` {
		t.Fatalf("expected wrong stream error, got: %q", err)
	}

	err = nc.Publish("js.test", []byte("hello"), PublishExpectsStream("OTHER"), PublishStreamTimeout(time.Second))
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err != ErrNoResponders {
		t.Fatalf("expected no responders error, got %s", err)
	}

	err = nc.Publish("js.in.test", []byte("hello"), PublishExpectsStream())
	if err != nil {
		t.Fatalf("unexpected error publishing: %s", err)
	}
	err = nc.Publish("js.test", []byte("hello"), PublishExpectsStream())
	if err != ErrNoResponders {
		t.Fatalf("unexpected error publishing: %s", err)
	}
}

func TestMsg_ParseJSMsgMetadata(t *testing.T) {
	cases := []struct {
		meta    string
		pending int
	}{
		{"$JS.ACK.ORDERS.NEW.1.2.3.1587466354254920000", -1},
		{"$JS.ACK.ORDERS.NEW.1.2.3.1587466354254920000.10", 10},
	}

	for _, tc := range cases {
		msg := &Msg{Reply: tc.meta}
		meta, err := msg.JetStreamMetaData()
		if err != nil {
			t.Fatalf("could not get message metadata: %s", err)
		}

		if meta.Stream != "ORDERS" {
			t.Fatalf("Expected ORDERS got %q", meta.Stream)
		}

		if meta.Consumer != "NEW" {
			t.Fatalf("Expected NEW got %q", meta.Consumer)
		}

		if meta.Delivered != 1 {
			t.Fatalf("Expected 1 got %q", meta.Delivered)
		}

		if meta.StreamSeq != 2 {
			t.Fatalf("Expected 2 got %q", meta.StreamSeq)
		}

		if meta.ConsumerSeq != 3 {
			t.Fatalf("Expected 3 got %q", meta.ConsumerSeq)
		}

		if meta.TimeStamp != time.Unix(0, int64(1587466354254920000)) {
			t.Fatalf("Expected 2020-04-21T12:52:34.25492+02:00 got %q", meta.TimeStamp)
		}

		if meta.Pending != tc.pending {
			t.Fatalf("Expected %d got %d", tc.pending, meta.Pending)
		}
	}
}

func TestMsg_Ack(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.Ack(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.StreamSeq != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_Nak(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.Nak(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was acked")
	}
}

func TestMsg_AckTerm(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckTerm(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.StreamSeq != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_AckProgress(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", nil, time.Second)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckProgress(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was acked")
	}

	err = msg.Ack(AckWaitDuration(time.Second))
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	if cons.Info().AckFloor.StreamSeq != 1 {
		t.Fatalf("first message was not acked")
	}
}

func TestMsg_AckAndFetch(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	msg, err := nc.Request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", []byte("1"), time.Second)
	if err != nil {
		t.Fatalf("request failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received incorrect message %q", msg.Data)
	}

	for i := 1; i < 20; i++ {
		if cons.Info().AckFloor.StreamSeq == uint64(i) {
			t.Fatalf("message %d was already acked", i)
		}
		msg, err = msg.AckAndFetch()
		if err != nil {
			t.Fatalf("ack failed: %s", err)
		}
		if cons.Info().AckFloor.StreamSeq != uint64(i) {
			t.Fatalf("message %d was not acked", i)
		}
		if !bytes.Equal(msg.Data, []byte(fmt.Sprintf("msg %d", i+1))) {
			t.Fatalf("received incorrect message %q", msg.Data)
		}
	}
}

func TestMsg_AckNext(t *testing.T) {
	srv, _, cons, nc := startJetStream(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()
	defer nc.Close()

	sub, err := nc.SubscribeSync(NewInbox())
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}

	err = nc.PublishRequest("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", sub.Subject, nil)
	if err != nil {
		t.Fatalf("pull failed: %s", err)
	}

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("next failed: %s", err)
	}
	if !bytes.Equal(msg.Data, []byte("msg 1")) {
		t.Fatalf("received invalid 'msg 1': %q", msg.Data)
	}

	if cons.Info().AckFloor.StreamSeq != 0 {
		t.Fatalf("first message was already acked")
	}

	err = msg.AckNextRequest(&AckNextRequest{Batch: 5})
	if err != nil {
		t.Fatalf("ack failed: %s", err)
	}

	for i := 2; i < 7; i++ {
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("next failed: %s", err)
		}

		expect := fmt.Sprintf("msg %d", i)
		if !bytes.Equal(msg.Data, []byte(expect)) {
			t.Fatalf("expected %s got %#v", expect, msg)
		}
	}

	if cons.Info().AckFloor.StreamSeq != 1 {
		t.Fatalf("first message was not acked")
	}
}
