// Copyright 2020-2022 The NATS Authors
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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamManagementServerCompat(t *testing.T) {
	conf := createConfFile(t, []byte(`
                listen: 127.0.0.1:-1
                jetstream: enabled
                accounts: {
                  A {
                    users: [{ user: "foo" }]
                    jetstream: { max_mem: 64MB, max_file: 64MB }
                  }
                }
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s, nats.UserInfo("foo", ""))
	defer nc.Close()

	// Create the stream using our client API.
	var si *nats.StreamInfo
	t.Run("create stream", func(t *testing.T) {
		si, err := js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"foo", "bar"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	var err error
	t.Run("create consumer", func(t *testing.T) {
		t.Run("with durable set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})
		t.Run("with name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-1")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-1", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-1"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-1" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with same Durable and Name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-2")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-2", Name: "dlc-2", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-2"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-2" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with name and filter subject", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-3.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:       "dlc-3",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "foo",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-3"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-3" || ci.Stream != "foo" || ci.Config.FilterSubject != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("legacy ephemeral consumer without name", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"stream_name":"foo"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with invalid consumer name", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test.durable"}); err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidConsumerName, err)
			}
		})

		t.Run("consumer with given name already exists, configs do not match", func(t *testing.T) {
			// configs do not match
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckAllPolicy}); !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerNameAlreadyInUse, err)
			}
		})

		t.Run("consumer with given name already exists, configs are the same", func(t *testing.T) {
			// configs are the same
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); err != nil {
				t.Fatalf("Expected no error; got: %v", err)
			}
		})

		t.Run("stream does not exist", func(t *testing.T) {
			_, err = js.AddConsumer("missing", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNotFound {
				t.Fatalf("Expected stream not found error, got: %v", err)
			}
		})

		t.Run("params validation error", func(t *testing.T) {
			_, err = js.AddConsumer("", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNameRequired {
				t.Fatalf("Expected %v, got: %v", nats.ErrStreamNameRequired, err)
			}
			_, err = js.AddConsumer("bad.stream.name", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidStreamName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidStreamName, err)
			}
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "bad.consumer.name", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidConsumerName, err)
			}
		})
	})

	t.Run("consumer info", func(t *testing.T) {
		if _, err := js.ConsumerInfo("", "dlc"); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.ConsumerInfo("bad.stream.name", "dlc"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if _, err := js.ConsumerInfo("foo", ""); err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrConsumerNameRequired, err)
		}
		if _, err := js.ConsumerInfo("foo", "bad.consumer.name"); err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidConsumerName, err)
		}
		ci, err := js.ConsumerInfo("foo", "dlc")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci == nil || ci.Config.Durable != "dlc" {
			t.Fatalf("ConsumerInfo is not correct %+v", si)
		}
	})

	t.Run("consumer not found", func(t *testing.T) {
		ci, err := js.ConsumerInfo("foo", "cld")
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v, got: %v", nats.ErrConsumerNotFound, err)
		}
		if ci != nil {
			t.Fatalf("ConsumerInfo should be nil %+v", ci)
		}
	})

	t.Run("list streams", func(t *testing.T) {
		var infos []*nats.StreamInfo
		for info := range js.StreamsInfo() {
			infos = append(infos, info)
		}
		if len(infos) != 1 || infos[0].Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", infos)
		}
	})
}
