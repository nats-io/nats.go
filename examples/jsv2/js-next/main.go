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

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST_STREAM", Subjects: []string{"FOO.*"}})
	if err != nil {
		log.Fatal(err)
	}

	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: "TestConsumer", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i <= 10; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			log.Fatal(err)
		}
	}

	// iterator with optional hints
	iter, err := cons.Messages()
	if err != nil {
		log.Fatal(err)
	}
	for {
		msg, err := iter.Next()
		if err != nil {
			log.Fatal(err)
		}
		if msg == nil {
			break
		}
		msg.Ack()
	}
}
