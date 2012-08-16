// Copyright 2012 Apcera Inc. All rights reserved.

package main

import (
	"log"
	"flag"
	"runtime"
	"github.com/apcera/nats"
)

func usage() {
    log.Fatalf("Usage: gnats-sub [-s server] [-t] <subject> \n")
}

var index = 0

func printMsg(m *nats.Msg, i int) {
	index += 1
	log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, string(m.Data))
}

func main() {
	var url      = flag.String("s", nats.DefaultURL, "The nats server URL")
	var showTime = flag.Bool("t", false, "Display timestamps")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

    args := flag.Args()
    if len(args) < 1 { usage() }

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	subj, i := args[0], 0

	nc.Subscribe(subj, func(subj, _ string, data []byte, _ *nats.Subscription) {
		msg := &nats.Msg{Subject:subj, Data:data}
		i += 1
		printMsg(msg, i)
	})

	log.Printf("Listening on [%s]\n", subj)

	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}