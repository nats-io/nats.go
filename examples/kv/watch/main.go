package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
)

/*
This example shows implementation of JetStream KeyValue watcher.
`kv-watch` command can be used to monitor updates on a given key.

Example usage:
./kv-watch -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' SOME_BUCKET my_key
*/

func usage() {
	log.Printf("Usage: kv-watch [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] <bucket> <key>\n")
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
	opts := []nats.Option{nats.Name("JetStream Sample Key Watcher")}
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
	watcher, err := kv.Watch(key)
	if err != nil {
		log.Fatal(err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				log.Print("Waiting for updates")
				continue
			}
			log.Printf("Value for '%s': %s; Revision: %d", key, entry.Value(), entry.Revision())
		case <-c:
			log.Fatal("Exiting")
			return
		}
	}
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
