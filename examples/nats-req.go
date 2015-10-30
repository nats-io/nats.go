// Copyright 2012-2015 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats"
)

func usage() {
	log.Fatalf("Usage: nats-req [-s server (%s)] [--ssl] <subject> <msg> \n", nats.DefaultURL)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var ssl = flag.Bool("ssl", false, "Use Secure Connection")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	opts := nats.DefaultOptions
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}

	opts.Secure = *ssl

	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()
	subj, payload := args[0], []byte(args[1])

	msg, err := nc.Request(subj, []byte(payload), 1000*time.Millisecond)
	if err != nil {
		log.Fatalf("Error in Request: %v\n", err)
	}
	log.Printf("Published [%s] : '%s'\n", subj, payload)
	log.Printf("Received [%v] : '%s'\n", msg.Subject, string(msg.Data))
}
