//go:build tinygo

package main

import (
	nats "github.com/nats-io/nats.go"
	"fmt"
)

func main() {
	fmt.Println("nats version:", nats.Version)
}
