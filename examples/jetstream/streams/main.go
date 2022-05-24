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
	"flag"
	"log"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
)

/*
This example shows implementation of stream management.
`js-stream` command can be used to create, update and delete a stream.
The `subjects` option can be used to supply the stream with a list of comma-separated subjects to be consumed by the stream

Example usage:
./js-stream -creds '/path/to/nats/credentials' -s 'nats://127.0.0.1:4222' -subjects 'ORDERS.*' add 'ORDERS'
*/

func usage() {
	log.Printf("Usage: js-stream [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] [-subjects subjects] <operation> [ add | update | delete ] <name>\n")
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
		subjects      = flag.String("subjects", "foo", "A list of comma-separated subjects consumed by the stream")
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

	*subjects = strings.ReplaceAll(*subjects, " ", "")
	subs := strings.Split(*subjects, ",")
	operation, streamName := args[0], args[1]

	switch operation {
	case "add":
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: subs,
			MaxBytes: 256 << 20,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Created stream '%s'\nConsumed subjects: %s", streamName, strings.Join(subs, ", "))
	case "update":
		_, err = js.UpdateStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: subs,
			MaxBytes: 256 << 20,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Updated stream '%s'\nConsumed subjects: %s", streamName, strings.Join(subs, ", "))
	case "delete":
		err = js.DeleteStream(streamName)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Removed stream '%s'", streamName)
	default:
		log.Fatalf("Operation not supported: %s\nAvailable options: %s", operation, "add, update, delete")
	}
}
