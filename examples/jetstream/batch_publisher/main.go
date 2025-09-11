// Copyright 2025 The NATS Authors
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
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a stream (if it doesn't exist)
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "BATCH_EXAMPLE",
		Subjects: []string{"batch.>"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stream created/updated: %s\n", stream.CachedInfo().Config.Name)

	// Create a batch publisher
	batch, err := js.NewBatchPublisher(
		jetstream.WithBatchID("example-batch-001"),
		jetstream.WithBatchExpectStream("BATCH_EXAMPLE"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Add messages to the batch
	// These are published immediately with batch headers
	for i := 1; i <= 5; i++ {
		subject := fmt.Sprintf("batch.item.%d", i)
		data := fmt.Sprintf("Message %d", i)
		
		if err := batch.Add(subject, []byte(data)); err != nil {
			log.Fatalf("Error adding message %d: %v", i, err)
		}
		fmt.Printf("Added message %d to batch\n", i)
	}

	// Add one more message using AddMsg
	msg := &nats.Msg{
		Subject: "batch.item.6",
		Data:    []byte("Message 6"),
	}
	if err := batch.AddMsg(msg); err != nil {
		log.Fatalf("Error adding message 6: %v", err)
	}
	fmt.Printf("Added message 6 to batch\n")

	fmt.Printf("Batch size before commit: %d\n", batch.Size())

	// Commit the batch with a final message
	// This adds the commit header and returns the acknowledgment
	ack, err := batch.Commit(ctx, "batch.item.7", []byte("Final message"))
	if err != nil {
		log.Fatalf("Error committing batch: %v", err)
	}

	fmt.Printf("\nBatch committed successfully!\n")
	fmt.Printf("  Stream: %s\n", ack.Stream)
	fmt.Printf("  Sequence: %d\n", ack.Sequence)
	fmt.Printf("  Batch ID: %s\n", ack.BatchID)
	fmt.Printf("  Batch Size: %d\n", ack.BatchSize)

	// Verify batch is closed
	if batch.IsClosed() {
		fmt.Println("\nBatch is now closed")
	}

	// Attempting to add more messages will fail
	if err := batch.Add("batch.item.8", []byte("Too late")); err != nil {
		fmt.Printf("Expected error adding to closed batch: %v\n", err)
	}
}