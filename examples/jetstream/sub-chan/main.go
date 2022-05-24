// Copyright 2022 The NATS Authors
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
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
)

/*
This example showcases the implementation of a push-based subscription using a channel.
After running the `js-sub-chan` command, a new subscription to a given subject will be created
If `-consumer` option was not provided, an ephemeral consumer will be created. Otherwise, an existing durable consumer will be used.
To test it out, run `js-sub` using `-pub-sample` option - the message will be published to a given subject and will be consumed in the subscription.

Example usage:
./js-sub-chan -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' -pub-sample 'some message' 'ORDERS' 'ORDERS.*'
*/

func usage() {
	log.Printf("Usage: js-sub-chan [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] [-consumer name] [-pub-sample message] <stream> <subject>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {

	// CLI flags
	var (
		urls          = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
		userCreds     = flag.String("creds", "", "User Credentials File")
		nkeyFile      = flag.String("nkey", "", "NKey Seed File")
		tlsClientCert = flag.String("tlscert", "", "TLS client certificate file")
		tlsClientKey  = flag.String("tlskey", "", "Private key file for client certificate")
		tlsCACert     = flag.String("tlscacert", "", "CA certificate to verify peer against")
		consumerName  = flag.String("consumer", "", "Name of the durable consumer. If empty, ephemenral consumer will be used")
		showHelp      = flag.Bool("h", false, "Show help message")
		sampleMessage = flag.String("pub-sample", "", "Sample message to be published on given subject")
	)

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 2 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("JetStream Sample Channel Subscriber")}
	opts = setupConnOptions(opts)

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use TLS client authentication
	if *tlsClientCert != "" && *tlsClientKey != "" {
		opts = append(opts, nats.ClientCert(*tlsClientCert, *tlsClientKey))
	}

	// Use specific CA certificate
	if *tlsCACert != "" {
		opts = append(opts, nats.RootCAs(*tlsCACert))
	}

	// Use Nkey authentication.
	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve JetStream connection
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	streamName, subject := args[0], args[1]
	// Create default stream if it does not exist
	if err := setupStream(js, streamName, subject); err != nil {
		log.Fatal(err)
	}
	if *sampleMessage != "" {
		publishMessage(js, subject, *sampleMessage)
	}
	msgs := make(chan *nats.Msg, 64)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	// If consumer name was not provided, create subscription with ephemeral consumer
	if *consumerName == "" {
		log.Print("No consumer name provided. Creating Ephemeral consumer.")
		sub, err := js.ChanSubscribe(subject, msgs, nats.BindStream(streamName))
		go func() {
			for m := range msgs {
				log.Printf("Received message on [%s]: %s\n", m.Subject, m.Data)
				// When using ephememeral consumer, and  the default AckPolicy is set to 'AckPolicyExplicit',
				// so the message has to be acknowledged manually
				if err := m.Ack(); err != nil {
					log.Fatal(err)
				}
			}
		}()
		if err != nil {
			log.Fatal(err)
		}
		<-c
		if err := sub.Unsubscribe(); err != nil {
			log.Fatal(err)
		}
		log.Fatal("Exiting")
	}

	// Else, create subscription by connecting to an existing durable consumer
	// In case durable consumer is used, 'subject' argument must match the consumer's Delivery Subject
	log.Printf("Using existing durable consumer: %s", *consumerName)
	sub, err := js.ChanSubscribe(subject, msgs, nats.Bind(streamName, *consumerName))
	if err != nil {
		log.Fatal(err)
	}
	// Retrieve consumer info to verify whether acknowledgement is needed
	consumerInfo, err := sub.ConsumerInfo()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for m := range msgs {
			log.Printf("Received message on [%s]: %s\n", m.Subject, m.Data)
			if consumerInfo.Config.AckPolicy == nats.AckExplicitPolicy {
				if err := m.Ack(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()
	if err != nil {
		log.Fatal(err)
	}
	<-c
	if err := sub.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
	log.Fatal("Exiting")
}

func setupStream(js nats.JetStreamContext, stream, subject string) error {
	_, err := js.StreamInfo(stream)
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	if err == nil {
		log.Printf("Using existing stream: %s\n", stream)
		return nil
	}
	log.Printf("Stream '%s' not found. New stream will be created.\n", stream)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		MaxBytes: 256 << 20,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Created stream '%s'\nConsumed subjects: %s\n", stream, subject)
	return nil
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

// Publish sample message
func publishMessage(js nats.JetStreamContext, subject, message string) error {
	ack, err := js.Publish(subject, []byte(message))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Published sample message: '%s'", message)
	log.Printf("Subject: %s\n", subject)
	log.Printf("Stream: %s\n", ack.Stream)
	log.Printf("Message sequence number: %d\n\n", ack.Sequence)
	return nil
}
