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

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jsv2/jetstream"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST_STREAM", Subjects: []string{"FOO.*"}})
	if err != nil {
		log.Fatal(err)
	}

	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "TestConsumerListener", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		log.Fatal(err)
	}

	l, err := cons.Listener(func(msg jetstream.Msg, err error) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(msg.Data()))
		msg.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
	defer l.Stop()

}
