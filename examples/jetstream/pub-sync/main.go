package main

import (
	"errors"
	"flag"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

/*
This example showcases implementation of synchronous publish on a stream.
`js-pub-sync` command can be used to publish a message on a given subject.
If a given stream does not exist, a new stream will be created.

Example usage:
./js-pub-sync -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' ORDERS ORDERS.created 'new order'
*/

func usage() {
	log.Printf("Usage: js-pub-sync [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] <stream> <subject> <message>\n")
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
	opts := []nats.Option{nats.Name("JetStream Sample Sync Publisher")}

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

	streamName, subject, message := args[0], args[1], []byte(args[2])

	// Create default stream if it does not exist
	if err := setupStream(js, streamName, subject); err != nil {
		log.Fatal(err)
	}

	// Publishes a message on subject. Subject has to be assigned to an existing stream
	ack, err := js.Publish(subject, []byte(message))
	if err != nil {
		if lerr := nc.LastError(); lerr != nil {
			log.Fatal(lerr)
		}
		log.Fatal(err)
	}

	log.Printf("Published message: '%s'\n", message)
	log.Printf("Subject: %s\n", subject)
	log.Printf("Stream: %s\n", ack.Stream)
	log.Printf("Message sequence number: %d\n", ack.Sequence)
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
