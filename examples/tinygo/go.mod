module github.com/nats-io/nats.go/examples/tinygo

go 1.25.0

require (
	github.com/nats-io/nats.go v0.0.0
	tinygo.org/x/drivers v0.35.0
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/nats-io/nkeys v0.4.15 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
)

replace github.com/nats-io/nats.go => ../../
