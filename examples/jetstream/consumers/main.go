package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

/*
This example shows implementation of consumers management.
`js-consumer-management` command can be used to create, update or get information about a consumer.
`-push-target` option can be used to configure a delivery subject for a push-based consumer. If not provided, the consumer will be created in push mode.
`-filter` option can be used to filter subjects from the stream.
`-queue` option can be used to create a queue consumer.

Example usage:
./js-consumer-management -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' -push-target=ORDERS_RECV add ORDERS ORDERS_CONSUMER
*/

func usage() {
	log.Printf("Usage: js-consumer-management [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] [-push-target subject] [-filter subject] [-queue queue] <operation> [ add | delete | info ] <stream> <name>\n")
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
		pushTarget    = flag.String("push-target", "", "Deliverty subject for push-based consumer")
		filter        = flag.String("filter", "", "FilterSubject for a consumer")
		queue         = flag.String("queue", "", "Delivery target group for a consumer")
		showHelp      = flag.Bool("h", false, "Show help message")
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
	opts := []nats.Option{nats.Name("JetStream Sample Stream Management")}

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

	// Retrieve JetStream connections
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	operation, streamName, consumerName := args[0], args[1], args[2]

	switch operation {
	case "add":
		// create new stream if it does not exist
		if err := setupStream(js, streamName); err != nil {
			log.Fatal(err)
		}

		// Create a new durable consumer
		// If DeliverSubject is not empty, consumer will be created in 'push' mode
		// Otherwise, a 'pull' consumer will be created
		_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:        consumerName,
			DeliverSubject: *pushTarget,
			AckPolicy:      nats.AckExplicitPolicy,
			FilterSubject:  *filter,
			DeliverGroup:   *queue,
		})
		if err != nil {
			log.Fatal(err)
		}
		if *pushTarget != "" {
			log.Printf("Created push consumer '%s' on stream '%s'\nDelivery subject: %s", consumerName, streamName, *pushTarget)
			break
		}
		log.Printf("Created pull consumer '%s' on stream '%s'\n", consumerName, streamName)
	case "delete":
		// Remove an existing consumer. If consumer does not exist, a 'consumer not found' error will be returned
		err = js.DeleteConsumer(streamName, consumerName)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Removed consumer '%s'\n", consumerName)
	case "info":
		// Get an existing consumer
		cons, err := js.ConsumerInfo(streamName, consumerName)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Fetched consumer '%s' from stream '%s'", consumerName, streamName)
		log.Printf("Created on '%s'", cons.Created.Format(time.RFC3339))
		log.Printf("Deliver Subject: %s", cons.Config.DeliverSubject)
		log.Printf("Ack Policy: %s", cons.Config.AckPolicy)
		log.Printf("Filter subject: %s", cons.Config.FilterSubject)
		log.Printf("Queue group: %s", cons.Config.DeliverGroup)
	default:
		log.Fatalf("Operation not supported: %s\nAvailable options: %s", operation, "add, delete, info")
	}
}

func setupStream(js nats.JetStreamContext, stream string) error {
	_, err := js.StreamInfo(stream)
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	if err == nil {
		log.Printf("Using existing stream: %s\n", stream)
		return nil
	}
	log.Printf("Stream '%s' not found. New stream will be created.\n", stream)
	subs := []string{fmt.Sprintf("%s.*", stream)}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: subs,
		MaxBytes: 256 << 20,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Created stream '%s'\nConsumed subjects: %s\n", stream, strings.Join(subs, ", "))
	return nil
}
