// Copyright 2012 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"log"

	"github.com/apcera/nats"
)

func usage() {
	log.Fatalf("Usage: nats-pub [-s server] [--ssl] [-t] <subject> <msg> \n")
}

func main() {
	var url = flag.String("s", nats.DefaultURL, "The nats server URL")
	var ssl = flag.Bool("ssl", false, "Use Secure Connection")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	opts := nats.DefaultOptions
	opts.Url = *url
	opts.Secure = *ssl

	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	subj, msg := args[0], []byte(args[1])

	nc.Publish(subj, msg)
	nc.Close()

	log.Printf("Published [%s] : '%s'\n", subj, msg)
}
