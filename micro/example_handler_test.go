package micro_test

import (
	"log"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

type rectangle struct {
	height int
	width  int
}

// Handle is an implementation of micro.Handler used to
// calculate the area of a rectangle
func (r rectangle) Handle(req micro.Request) {
	area := r.height * r.width
	req.Respond([]byte(strconv.Itoa(area)))
}

func ExampleHandler() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	rec := rectangle{10, 5}

	config := micro.Config{
		Name:        "RectangleAreaService",
		Version:     "0.1.0",
		RootSubject: "area",
		Endpoints: map[string]micro.Endpoint{
			"Rectangle": {
				Subject: "rec",
				Handler: rec,
			},
		},
	}
	svc, err := micro.AddService(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Stop()
}
