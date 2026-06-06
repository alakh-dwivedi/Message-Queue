package main

import (
	"fmt"
	"log"
	"time"

	"messagequeue/pkg/client"
)

func main() {
	// Connect to our running server instance using our high-level native SDK
	c, err := client.NewClient("127.0.0.1:9099")
	if err != nil {
		log.Fatalf("Producer client startup failure: %v", err)
	}
	defer c.Close()

	for i := 1; i <= 3; i++ {
		payload := fmt.Sprintf("Sensor-Data-Telemetry-TelemetryPayload-%d", i*25)
		ackInfo, err := c.Publish("iot_metrics", payload)
		if err != nil {
			fmt.Printf("[Producer Error] Publish failed: %v\n", err)
		} else {
			fmt.Printf("[Producer SDK Success] Sent payload! Server Response -> %s\n", ackInfo)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
