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

//go:build tinygo && (esp32c3 || esp32c3_generic)

// ESP32-C3 NATS example.
//
// Build:
//
//	tinygo build -target esp32c3-generic \
//	  -ldflags "-X main.ssid=MyWiFi -X main.pass=secret -X main.natsURL=nats://192.168.1.10:4222" \
//	  -o esp32c3.bin ./examples/tinygo/esp32c3/
//
// The ESP32-C3 has an integrated WiFi radio. TinyGo does not yet expose a
// native Go driver for the on-chip WiFi. The typical workaround is to use a
// secondary ESP32/ESP8266 module running Espressif AT firmware and connected
// over UART, driven by the tinygo.org/x/drivers/espat package.
//
// Alternatively, if a native driver becomes available (implementing the
// tinygo.org/x/drivers/netlink.Netlinker interface), replace connectWiFi()
// with that driver's initialisation code.
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
	if _, err = nc.Subscribe("esp32c3.hello", func(m *nats.Msg) {
		received <- string(m.Data)
	}); err != nil {
		panic("subscribe: " + err.Error())
	}

	if err = nc.Publish("esp32c3.hello", []byte("hello from ESP32-C3")); err != nil {
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

// connectWiFi joins the access point.
//
// For a production build using tinygo.org/x/drivers/espat (AT-command UART
// bridge to a separate ESP module):
//
//	esp := espat.NewDevice(&espat.Config{
//	    Uart: machine.UART1,
//	    Tx:   machine.GP4,
//	    Rx:   machine.GP5,
//	})
//	netdev.UseNetdev(esp)
//	return esp.NetConnect(&netlink.ConnectParams{
//	    Ssid:       ssid,
//	    Passphrase: pass,
//	})
//
// When a native on-chip WiFi driver exists, substitute the driver above.
func connectWiFi() error {
	_ = netlink.ConnectParams{Ssid: ssid, Passphrase: pass}
	return nil
}
