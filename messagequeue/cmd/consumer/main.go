package main

import (
	"fmt"
	"log"

	"messagequeue/pkg/client"
)

func main() {
	c, err := client.NewClient("127.0.0.1:9099")
	if err != nil {
		log.Fatalf("Consumer client startup failure: %v", err)
	}
	defer c.Close()

	fmt.Println("[Consumer SDK] Registering stream subscription tracking...")
	stream, err := c.Subscribe("iot_metrics", "analytics_worker_node")
	if err != nil {
		log.Fatalf("Subscription failed: %v", err)
	}

	// Blocks and processes messages continuously from the Go channel
	for rawWireMsg := range stream {
		fmt.Printf("[Consumer SDK Received Line]: %s\n", rawWireMsg)

		// Example manual ACK checkpoint simulation pattern:
		// In a production system, you would extract the offset value from the string
		// and track it, then execute: c.Acknowledge("analytics_worker_node", offset)
	}
}
