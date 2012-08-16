// Copyright 2012 Apcera Inc. All rights reserved.

/*
	A Go client for the NATS messaging system (https://github.com/derekcollison/nats).

	Basic Usage

		nc := nats.Connect(nats.DefaultURL)

		// Simple Publisher
		nc.Publish("foo", []byte("Hello World"))

		// Simple Async Subscriber
		nc.Subscribe("foo", func(m *Msg) {
			fmt.Printf("Received a message: %s\n", string(m.Data))
		})

		// Simple Sync Subscriber
		sub, err := nc.Subscribe("foo")
		m, err := sub.NextMsg(timeout)

		// Unsubscribing
		sub, err := nc.Subscribe("foo", nil)
		sub.Unsubscribe()

		// Requests
		msg, err := nc.Request("help", []byte("help me"), 10*time.Millisecond)

		// Replies
		nc.Subscribe("help", func(m *Msg) {
			nc.Publish(m.Reply, []byte("I can help!"))
		})

		// Close connection
		nc := nats.Connect("nats://localhost:4222")
		nc.Close();

	Wildcard Subscriptions

		// "*" matches any token, at any level of the subject.
		nc.Subscribe("foo.*.baz", func(m *Msg) {
			fmt.Printf("Msg received on [%s] : %s\n", n.Subj, string(m.Data));
		})

		nc.Subscribe("foo.bar.*", func(m *Msg) {
			fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
		})

		// ">" matches any length of the tail of a subject, and can only be the last token
		// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
		nc.Subscribe("foo.>", func(m *Msg) {
			fmt.Printf("Msg received on [%s] : %s\n", m.Subject, string(m.Data));
		})

		// Matches all of the above
		nc.Publish("foo.bar.baz", []byte("Hello World"))

	Queues Groups

		// All subscriptions with the same queue name will form a queue group.
		// Each message will be delivered to only one subscriber per queue group, queuing semantics.
		// You can have as many queue groups as you wish.
		// Normal subscribers will continue to work as expected.

		nc.QueueSubscribe("foo", "job_workers", func(_ *Msg) {
			received += 1;
		})

	Advanced Usage


		// Flush connection to server, returns  when all messages have been processed.
		nc.Flush()
		fmt.Println("All clear!")

		// FlushTimeout specifies a timeout value as well.
		err := nc.FlushTimeout(1*time.Second)
		if err != nil {
		    fmt.Println("All clear!")
		} else {
		    fmt.Println("Flushed timed out!")
		}

		// Auto-unsubscribe after MAX_WANTED messages received
		const MAX_WANTED = 10
		sub, err := nc.Subscribe("foo")
		sub.AutoUnsubscribe(MAX_WANTED)

		// Multiple connections
		nc1 := nats.Connect("nats://host1:4222")
		nc1 := nats.Connect("nats://host2:4222")

		nc1.Subscribe("foo", func(m *Msg) {
		    fmt.Printf("Received a message: %s\n", string(m.Data))
		})

		nc2.Publish("foo", []byte("Hello World!"));

*/
package nats
