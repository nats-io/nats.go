// Copyright 2015 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

// Some sane defaults
const (
	DefaultNumMsgs = 100000
	DefaultNumPubs = 1
	DefaultNumSubs = 0
	HashModulo     = 1000
)

func usage() {
	log.Fatalf("Usage: nats-bench [-s server (%s)] [--tls] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] <subject> <msg> \n", nats.DefaultURL)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var tls = flag.Bool("tls", false, "Use TLS Secure Connection")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Messages to Publish")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	// Setup the option block
	opts := nats.DefaultOptions
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}
	opts.Secure = *tls

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startwg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		go runSubscriber(&startwg, &donewg, opts, (*numMsgs)*(*numPubs))
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	for i := 0; i < *numPubs; i++ {
		go runPublisher(&startwg, &donewg, opts, *numMsgs)
	}

	log.Printf("Starting benchmark\n")
	log.Printf("msgs=%d, pubs=%d, subs=%d\n", *numMsgs, *numPubs, *numSubs)

	startwg.Wait()

	start := time.Now()
	donewg.Wait()
	delta := time.Since(start).Seconds()
	total := float64((*numMsgs) * (*numPubs))
	if *numSubs > 0 {
		total *= float64(*numSubs)
	}
	fmt.Printf("\nNATS throughput is %s msgs/sec\n", commaFormat(int64(total/delta)))
}

func runPublisher(startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()
	startwg.Done()

	args := flag.Args()
	subj, msg := args[0], []byte(args[1])

	for i := 0; i < numMsgs; i++ {
		nc.Publish(subj, msg)
		if i%HashModulo == 0 {
			fmt.Fprintf(os.Stderr, "#")
		}
	}
	nc.Flush()
	donewg.Done()
}

func runSubscriber(startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	args := flag.Args()
	subj := args[0]

	received := 0
	nc.Subscribe(subj, func(msg *nats.Msg) {
		received++
		if received%HashModulo == 0 {
			fmt.Fprintf(os.Stderr, "*")
		}
		if received >= numMsgs {
			donewg.Done()
			nc.Close()
		}
	})
	nc.Flush()
	startwg.Done()
}

func commaFormat(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}
	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}
