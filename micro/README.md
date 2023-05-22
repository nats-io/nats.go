# NATS micro

## Overview

The `micro` package in the NATS.go library provides a simple way to create microservices that leverage NATS for messaging. It aims to simplify the complexities associated with developing distributed systems, and makes it easier to build resilient services that can handle failures and changes in the network.

## Usage

To start using the `micro` package, import it in your application:

```go
import "github.com/nats-io/nats.go/micro"
```

The core of the `micro` package is the Service. A Service is an object with a name and a version. You create a Service using the `micro.NewService()` function, passing in the NATS connection, the service name, and the service version:

```go
nc, _ := nats.Connect(nats.DefaultURL)
s := micro.NewService(nc, "serviceName", "v1.0.0")
```

Once you've created a Service, you can add endpoints to it. An endpoint is a function that can be called remotely. It's added to a Service using the `Handle` function:

```go
s.Handle("myFunc", func(ctx context.Context, req *MyRequest, resp *MyResponse) error {
    // Handler code
})
```

Once you've added your endpoints, call the `Run` function to start the service:

```go
if err := s.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

## Examples

For more detailed examples, refer to the `examples` directory in this repository.

## Documentation

The complete documentation is available on [GoDoc](https://godoc.org/github.com/nats-io/nats.go/micro).

## Contributing

We appreciate your help! Refer to the CONTRIBUTING.md file for guidelines.

## License

This project is licensed under the [MIT License](LICENSE).