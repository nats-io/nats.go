// Copyright 2018 The NATS Authors
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
	"runtime"
	"time"

	"github.com/nats-io/go-nats"
)

// NOTE: Can test with demo servers.
// nats-echo -s demo.nats.io <subject>
// nats-echo -s demo.nats.io:4443 <subject> (TLS version)

func usage() {
	log.Fatalf("Usage: nats-echo [-s server] [-t] [-nkey seedfile] <subject>")
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Echoing to [%s]: %q", i, m.Reply, m.Data)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var nkeyFile = flag.String("nkey", "", "Use the nkey seed file for authentication")
	var showTime = flag.Bool("t", false, "Display timestamps")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Echo Service")}
	opts = setupConnOptions(opts)

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

	subj, i := args[0], 0

	nc.QueueSubscribe(subj, "echo", func(msg *nats.Msg) {
		i++
		if msg.Reply != "" {
			printMsg(msg, i)
			// Just echo back what they sent us.
			nc.Publish(msg.Reply, msg.Data)
		}
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Echo Service listening on [%s]\n", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Printf("Disconnected")
		log.Printf("Reconnecting for next %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatal("Exiting, no servers available")
	}))
	return opts
}
