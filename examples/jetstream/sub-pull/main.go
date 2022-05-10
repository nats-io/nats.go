package main

import (
	"errors"
	"flag"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

/*
This example showcases the implementation of a pull-based subscription.
After running the `js-pull` command, a pull subscription to a given subject will be created and the oldest unacknowledged message will be consumed.
Multiple messages can be consumed using `-count` option. If no messages are available in a given stream, command will time out.
If a given consumer does not exist, a default consumer will be created.
To test it out, run `js-pull` using `-pub-sample` option - the message will be published to a given subject and will be consumed in the subscription.

Example usage:
./js-pull -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' -pub-sample 'some message' 'ORDERS' 'ORDERS_CONSUMER' 'ORDERS.*'
*/

func usage() {
	log.Printf("Usage: js-pull [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] [-count number] [-pub-sample message] <stream> <consumer> <subject>\n")
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
		count         = flag.Int("count", 1, "Number of messages to pull")
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
	if len(args) != 3 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("JetStream Sample Pull Subscriber")}

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
	defer nc.Close()

	// Retrieve JetStream connection
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	streamName, consumer, subject := args[0], args[1], args[2]
	// Create default stream and publish message if stream does not exist
	if err := setupStream(js, streamName, subject); err != nil {
		log.Fatal(err)
	}
	// Create default consumer it does not exist
	if err := setupConsumer(js, streamName, consumer); err != nil {
		log.Fatal(err)
	}

	// Create a subscription to pull messages
	sub, err := js.PullSubscribe(subject, consumer, nats.MaxAckPending(1000), nats.Bind(streamName, consumer))
	if err != nil {
		log.Fatal(err)
	}
	if *sampleMessage != "" {
		if err := publishMessage(js, subject, *sampleMessage); err != nil {
			log.Fatal(err)
		}
	}

	// Pull selected number of messages from the consumer. If no messages are available for a consumer, server will time out
	msgs, err := sub.Fetch(*count)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range msgs {
		log.Printf("Received message: %s\n", msg.Data)
		if err := msg.AckSync(); err != nil {
			log.Fatal(err)
		}
		log.Printf("Message acknowledged\n")
	}

	if err := sub.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
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
	log.Printf("Created stream '%s'\nConsumed subjects: %s\n\n", stream, subject)
	return nil
}

func setupConsumer(js nats.JetStreamContext, stream, consumer string) error {
	_, err := js.ConsumerInfo(stream, consumer)
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		return err
	}
	if err == nil {
		log.Printf("Using existing consumer: %s\n", consumer)
		return nil
	}
	_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:   consumer,
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Created pull consumer '%s' on stream '%s'\n\n", consumer, stream)
	return nil
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
