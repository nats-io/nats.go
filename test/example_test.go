package test

import (
	"fmt"
	"time"

	"github.com/nats-io/go-nats"
)

// Shows different ways to create a Conn
func Example_nats_Connect() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	nc.Close()

	nc, _ = nats.Connect("nats://derek:secretpassword@demo.nats.io:4222")
	nc.Close()

	nc, _ = nats.Connect("tls://derek:secretpassword@demo.nats.io:4443")
	nc.Close()

	opts := nats.Options{
		AllowReconnect: true,
		MaxReconnect:   10,
		ReconnectWait:  5 * time.Second,
		Timeout:        1 * time.Second,
	}

	nc, _ = opts.Connect()
	nc.Close()

	// Output:
	//
}

// This Example shows an asynchronous subscriber.
func Example_nats_Connect_Subscribe() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})

	// Output:
	//
}

// This Example shows a synchronous subscriber.
func Example_nats_Connect_SubscribeSync() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}

	// Output:
	// NextMsg timed out.
}

func Example_nats_Subscription_NextMsg() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	m, err := sub.NextMsg(1 * time.Second)
	if err == nil {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	} else {
		fmt.Println("NextMsg timed out.")
	}

	// Output:
	// NextMsg timed out.
}

func Example_nats_Subscription_Unsubscribe() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	// ...
	sub.Unsubscribe()

	// Output:
	//
}

func Example_nats_Connect_Publish() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Publish("foo", []byte("Hello World!"))

	// Output:
	//
}

func Example_nats_Connect_PublishMsg() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	nc.PublishMsg(msg)

	// Output:
	//
}

func Example_nats_Connect_Flush() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	err := nc.Flush()
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}

	// Output:
	//
}

func Example_nats_Connect_FlushTimeout() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}
	// Only wait for up to 1 second for Flush
	err := nc.FlushTimeout(1 * time.Second)
	if err == nil {
		// Everything has been processed by the server for nc *Conn.
	}

	// Output:
	//
}

func Example_nats_Connect_Request() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, []byte("I will help you"))
	})
	response, err := nc.Request("foo", []byte("help"), 50*time.Millisecond)
	if err == nil {
		fmt.Println(string(response.Data))
	}

	// Output:
	// I will help you
}

func Example_nats_Connect_QueueSubscribe() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received := 0

	nc.QueueSubscribe("foo", "worker_group", func(_ *nats.Msg) {
		received++
	})

	// Output:
	//
}

func Example_nats_Subscription_AutoUnsubscribe() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	received, wanted, total := 0, 10, 100

	sub, _ := nc.Subscribe("foo", func(_ *nats.Msg) {
		received++
	})
	sub.AutoUnsubscribe(wanted)

	for i := 0; i < total; i++ {
		nc.Publish("foo", []byte("Hello"))
	}
	nc.Flush()

	fmt.Printf("Received = %d", received)

	// Output:
	// Received = 10
}

func Example_nats_Connect_Close() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	nc.Close()

	// Output:
	//
}

// Shows how to wrap a Conn into an EncodedConn
func Example_nats_NewEncodedConn() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	c.Close()

	// Output:
	//
}

// EncodedConn can publish virtually anything just
// by passing it in. The encoder will be used to properly
// encode the raw Go type
func Example_nats_EncodedConn_Publish() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)

	// Output:
	//
}

// EncodedConn's subscribers will automatically decode the
// wire data into the requested Go type using the Decode()
// method of the registered Encoder. The callback signature
// can also vary to include additional data, such as subject
// and reply subjects.
func Example_nats_EncodedConn_Subscribe() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	c.Subscribe("hello", func(p *person) {
		fmt.Printf("Received a person! %+v\n", p)
	})

	c.Subscribe("hello", func(subj, reply string, p *person) {
		fmt.Printf("Received a person on subject %s! %+v\n", subj, p)
	})

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)

	// Output:
	//
}

// BindSendChan() allows binding of a Go channel to a nats
// subject for publish operations. The Encoder attached to the
// EncodedConn will be used for marshalling.
func Example_nats_EncodedConn_BindSendChan() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindSendChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	ch <- me

	// Output:
	//
}

// BindRecvChan() allows binding of a Go channel to a nats
// subject for subscribe operations. The Encoder attached to the
// EncodedConn will be used for un-marshalling.
func Example_nats_EncodedConn_BindRecvChan() {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, "json")
	defer c.Close()

	type person struct {
		Name    string
		Address string
		Age     int
	}

	ch := make(chan *person)
	c.BindRecvChan("hello", ch)

	me := &person{Name: "derek", Age: 22, Address: "85 Second St"}
	c.Publish("hello", me)

	// Receive the publish directly on a channel
	who := <-ch

	fmt.Printf("%v says hello!\n", who)

	// Output:
	// &{derek 85 Second St 22} says hello!
}
