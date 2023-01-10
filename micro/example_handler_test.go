package micro_test

import (
	"context"
	"os"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"golang.org/x/exp/slog"
)

func exampleCtx() context.Context {
	return slog.NewContext(context.Background(), slog.New(slog.NewJSONHandler(os.Stdout)))
}

type rectangle struct {
	height int
	width  int
}

// Handle is an implementation of micro.Handler used to
// calculate the area of a rectangle
func (r rectangle) Handle(ctx context.Context, req micro.Request) {
	area := r.height * r.width
	req.Respond([]byte(strconv.Itoa(area)))
}

func ExampleHandler() {
	ctx := exampleCtx()
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger := slog.FromContext(ctx)
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	rec := rectangle{10, 5}

	config := micro.Config{
		Name:    "RectangleAreaService",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Handler: rec,
			Subject: "rectangle.area",
		},
	}
	svc, err := micro.AddService(ctx, nc, config)
	if err != nil {
		logger := slog.FromContext(ctx)
		logger.Error("failed to add service", err)
		os.Exit(1)
	}
	defer svc.Stop(ctx)
}
