package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Service instance with queue group for load balancing
	createServiceInstance := func(nc *nats.Conn, instanceID string) {
		nc.QueueSubscribe("api.calculate", "api-workers", func(m *nats.Msg) {
			// Parse request
			var request map[string]int
			json.Unmarshal(m.Data, &request)

			// Process request
			result := request["a"] + request["b"]

			// Send response
			response := map[string]interface{}{
				"result":      result,
				"processedBy": instanceID,
			}
			responseData, _ := json.Marshal(response)
			m.Respond(responseData)

			fmt.Printf("Instance %s processed request\n", instanceID)
		})
	}

	// Start multiple service instances
	for i := 1; i <= 3; i++ {
		createServiceInstance(nc, fmt.Sprintf("instance-%d", i))
	}

	// Make requests - automatically load balanced
	for i := 0; i < 10; i++ {
		request := map[string]int{"a": i, "b": i * 2}
		requestData, _ := json.Marshal(request)

		msg, _ := nc.Request("api.calculate", requestData, time.Second)

		var response map[string]interface{}
		json.Unmarshal(msg.Data, &response)
		fmt.Printf("Result: %v, processed by: %s\n",
			response["result"], response["processedBy"])
	}
	// NATS-DOC-END
}
