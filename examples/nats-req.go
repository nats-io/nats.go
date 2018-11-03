// Copyright 2012-2018 The NATS Authors
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

// +build ignore

package main

import (
	"flag"
	"log"
	"time"

	"github.com/nats-io/go-nats"
)

// NOTE: Can test with demo servers.
// nats-req -s demo.nats.io <subject> <msg>
// nats-req -s demo.nats.io:4443 <subject> <msg> (TLS version)

func usage() {
	log.Fatalf("Usage: nats-req [-s server] <subject> <msg>")
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var nkeyFile = flag.String("nkey", "", "Use the nkey seed file for authentication")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Requestor")}

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
	subj, payload := args[0], []byte(args[1])

	msg, err := nc.Request(subj, []byte(payload), time.Second)
	if err != nil {
		if nc.LastError() != nil {
			log.Fatalf("%v for request", nc.LastError())
		}
		log.Fatalf("%v for request", err)
	}

	log.Printf("Published [%s] : '%s'", subj, payload)
	log.Printf("Received  [%v] : '%s'", msg.Subject, string(msg.Data))
}
