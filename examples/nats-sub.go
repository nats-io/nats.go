// Copyright 2012 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"log"
	"runtime"

	"github.com/apcera/nats"
)

func usage() {
	log.Fatalf("Usage: nats-sub [-s server] [--ssl] [-t] <subject> \n")
}

var index = 0

func printMsg(m *nats.Msg, i int) {
	index += 1
	log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, string(m.Data))
}

func main() {
	var url = flag.String("s", nats.DefaultURL, "The nats server URL")
	var showTime = flag.Bool("t", false, "Display timestamps")
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

	subj, i := args[0], 0

	nc.Subscribe(subj, func(msg *nats.Msg) {
		i += 1
		printMsg(msg, i)
	})

	log.Printf("Listening on [%s]\n", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}
