package main

import (
	"flag"
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

/*
This example shows implementation of JetStream KeyValue get operation.
`kv-get` command can be used to fetch a value from an existing bucket.

Example usage:
./kv-get -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' SOME_BUCKET my_key
*/

func usage() {
	log.Printf("Usage: kv-get [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] <bucket> <key>\n")
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
	if len(args) != 2 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("JetStream Sample KeyValue Get")}

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

	bucket, key := args[0], args[1]

	kv, err := js.KeyValue(bucket)
	if err != nil {
		log.Fatal(err)
	}
	entry, err := kv.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Value for '%s': %s\nRev: %d", key, entry.Value(), entry.Revision())
}
