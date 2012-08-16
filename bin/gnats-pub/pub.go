// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"log"
	"flag"
	"github.com/apcera/nats"
)

func usage() {
    log.Fatalf("Usage: gnats-pub [-s server] [-t] <subject> <msg> \n")
}

func main() {
	var url  = flag.String("s", nats.DefaultURL, "The nats server URL")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

    args := flag.Args()
    if len(args) < 1 { usage() }

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	subj, msg := args[0], []byte(args[1])

	nc.Publish(subj, msg)
	nc.Close()

	log.Printf("Published [%s] : '%s'\n", subj, msg)
}