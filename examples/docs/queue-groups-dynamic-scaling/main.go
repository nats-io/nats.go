package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Worker that can be dynamically added/removed
	type Worker struct {
		ID   string
		sub  *nats.Subscription
		done chan bool
	}

	NewWorker := func(nc *nats.Conn, id string) *Worker {
		w := &Worker{ID: id, done: make(chan bool)}

		w.sub, _ = nc.QueueSubscribe("tasks", "workers", func(m *nats.Msg) {
			fmt.Printf("Worker %s processing: %s\n", id, string(m.Data))
			// Simulate work
			time.Sleep(100 * time.Millisecond)
		})

		return w
	}

	Stop := func(w *Worker) {
		w.sub.Unsubscribe()
		close(w.done)
	}

	// Dynamic scaling
	workers := make([]*Worker, 0)

	// Scale up
	for i := 1; i <= 5; i++ {
		worker := NewWorker(nc, fmt.Sprintf("%d", i))
		workers = append(workers, worker)
	}

	// Scale down
	if len(workers) > 0 {
		Stop(workers[0])
		workers = workers[1:]
	}
	// NATS-DOC-END

	time.Sleep(500 * time.Millisecond)
}
