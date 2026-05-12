// Copyright 2017-2025 The NATS Authors
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

//go:build tinygo && (pico_w || cyw43439)

// Pico W NATS example.
//
// Build:
//
//	tinygo build -target pico-w \
//	  -ldflags "-X main.ssid=MyWiFi -X main.pass=secret -X main.natsURL=nats://192.168.1.10:4222" \
//	  -o picow.uf2 ./examples/tinygo/picow/
//
// The Raspberry Pi Pico W uses the CYW43439 WiFi chip. WiFi initialisation
// requires a board-specific driver. Plug in any driver that implements the
// tinygo.org/x/drivers/netlink.Netlinker interface and calls
// tinygo.org/x/drivers/netdev.UseNetdev() to register itself with TinyGo's
// net package. See connectWiFi() below.
package main

import (
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"
	"tinygo.org/x/drivers/netlink"
)

// Injected via -ldflags.
var (
	ssid    string
	pass    string
	natsURL = nats.DefaultURL
)

func main() {
	if err := connectWiFi(); err != nil {
		panic("wifi: " + err.Error())
	}

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		panic("nats connect: " + err.Error())
	}
	defer nc.Close()

	received := make(chan string, 1)
	if _, err = nc.Subscribe("picow.hello", func(m *nats.Msg) {
		received <- string(m.Data)
	}); err != nil {
		panic("subscribe: " + err.Error())
	}

	if err = nc.Publish("picow.hello", []byte("hello from Pico W")); err != nil {
		panic("publish: " + err.Error())
	}

	select {
	case msg := <-received:
		fmt.Println("received:", msg)
	case <-time.After(5 * time.Second):
		panic("timeout waiting for message")
	}

	fmt.Println("OK")
}

// connectWiFi initialises the on-board CYW43439 and joins the access point.
//
// Replace the body with the driver that matches your SDK / firmware. The
// standard pattern for tinygo.org/x/drivers WiFi drivers is:
//
//  1. Construct and configure the driver (SPI pins, power pin, etc.)
//  2. Call netdev.UseNetdev(driver) to register it with TinyGo's net package.
//  3. Call driver.NetConnect(&netlink.ConnectParams{...}) to associate with AP.
func connectWiFi() error {
	// Placeholder — replace with the real CYW43439 driver once available in
	// tinygo.org/x/drivers, e.g.:
	//
	//   dev := cyw43439.NewDevice(...)
	//   netdev.UseNetdev(dev)
	//   return dev.NetConnect(&netlink.ConnectParams{
	//       Ssid:       ssid,
	//       Passphrase: pass,
	//   })
	_ = netlink.ConnectParams{Ssid: ssid, Passphrase: pass}
	return nil
}
