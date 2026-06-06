/// Phase 1: Building the Core Engine with In-Memory Queues and Pub/Sub Mechanics

// package main

// import (
// 	"fmt"
// 	"sync"
// 	"time"
// )

// func main() {
// 	broker := NewBroker()
// 	var wg sync.WaitGroup

// 	// 1. Spin up an Order Processing Consumer
// 	orderChan, unsubscribe, _ := broker.Subscribe("orders", 2) // Small buffer to test pressure
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for msg := range orderChan {
// 			fmt.Printf("[Consumer] Processing Order ID: %s | Payload: %s\n", msg.ID, string(msg.Body))
// 			time.Sleep(50 * time.Millisecond) // Simulate slow worker processing overhead
// 		}
// 		fmt.Println("[Consumer] Channel closed. Exiting cleanly.")
// 	}()

// 	// 2. Publish messages via a Producer loop
// 	go func() {
// 		for i := 1; i <= 5; i++ {
// 			msg := Message{
// 				ID:        fmt.Sprintf("ORD-%03d", i),
// 				Topic:     "orders",
// 				Body:      []byte(fmt.Sprintf("Item-Data-%04d", i*100)),
// 				Timestamp: time.Now(),
// 			}

// 			err := broker.Publish("orders", msg)
// 			if err != nil {
// 				fmt.Printf("[Producer Error] Failed to send %s: %v\n", msg.ID, err)
// 			} else {
// 				fmt.Printf("[Producer] Sent: %s\n", msg.ID)
// 			}
// 			time.Sleep(10 * time.Millisecond) // Producer is faster than consumer!
// 		}

// 		// Cleanup phase
// 		time.Sleep(300 * time.Millisecond) // Give consumer time to catch up
// 		unsubscribe()
// 	}()

// 	wg.Wait()
// 	fmt.Println("Phase 1 complete. Core engine is rock solid!")
// }

/// ------- PHASE 2: Adding Crash-Resilient Persistence with Write-Ahead Logging -------
// package main

// import (
// 	"fmt"
// 	"os"
// )

// func main() {
// 	logFilename := "queue_data.wal"

// 	// Ensure clean testing environment by purging previous test runs
// 	os.Remove(logFilename)

// 	fmt.Println("--- STEP 1: Initializing Log Engine and Writing Data ---")
// 	logger, err := NewDiskLog(logFilename)
// 	if err != nil {
// 		fmt.Printf("Failed to open file: %v\n", err)
// 		return
// 	}

// 	m1 := Message{Topic: "billing", Body: []byte("Invoice #1001 Paid")}
// 	m2 := Message{Topic: "shipping", Body: []byte("Package #554 Shipped")}

// 	// Persist entries to disk log
// 	logger.Append(m1)
// 	logger.Append(m2)
// 	logger.Close()
// 	fmt.Println("Messages successfully serialized and saved safely to disk.")

// 	fmt.Println("\n--- STEP 2: Simulating Crash / ReBoot Recovery ---")
// 	// Now, read the file back into application state memory
// 	reader, err := NewRecoveryLog(logFilename)
// 	if err != nil {
// 		fmt.Printf("Failed to open recovery file: %v\n", err)
// 		return
// 	}
// 	defer reader.Close()

// 	recoveredData, err := reader.ReadAll()
// 	if err != nil {
// 		fmt.Printf("Error recovering data stream: %v\n", err)
// 		return
// 	}

// 	// Verify entries match our original structs completely
// 	for i, msg := range recoveredData {
// 		fmt.Printf("Recovered Log Record #%d -> Topic: [%s] | Body: %s\n", i+1, msg.Topic, string(msg.Body))
// 	}

// 	fmt.Println("\nPhase 2 complete. Your message data is completely crash-resilient!")
// }

/// Phase 3: Exposing the Message Queue via a Simple TCP Server Interface

// package main

// import (
// 	"log"
// )

// func main() {
// 	// Initialize our core thread-safe memory manager from Phase 1
// 	broker := NewBroker()

// 	// Wrap it inside our new TCP server engine
// 	server := NewServer("127.0.0.1:9099", broker)

// 	// Block and run the network server loop
// 	err := server.Start()
// 	if err != nil {
// 		log.Fatalf("Critical system infrastructure failure: %v", err)
// 	}
// }

package main

import (
	"fmt"
	"log"
	"messagequeue/internal/broker"
	"net"
	"time"
)

func main() {
	logFilename := "queue_data.wal"
	b := broker.NewBroker()

	// 1. Storage Recovery Setup
	diskLog, err := broker.NewDiskLog(logFilename)
	if err != nil {
		log.Fatalf("Failed to init storage: %v", err)
	}
	defer diskLog.Close()

	recovered, err := diskLog.ReadAll()
	if err == nil && len(recovered) > 0 {
		fmt.Printf("[Recovery] Reloading %d records from log...\n", len(recovered))
		for _, msg := range recovered {
			ts := b.GetOrCreateTopic(msg.Topic)
			ts.Messages = append(ts.Messages, msg)
			if msg.Offset > ts.NextOffset {
				ts.NextOffset = msg.Offset
			}
		}
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			minOffset := b.GetMinimumAckedOffset()
			if minOffset > 0 {
				// Execute storage file garbage collection optimization sweep
				if err := diskLog.Compact(minOffset); err != nil {
					fmt.Printf("[Compactor Error] Failed to optimize storage footprint: %v\n", err)
				}
			}
		}
	}()

	// 2. Launch Network Wrapper (Using the struct from server.go)
	srv := &Server{broker: b, diskLog: diskLog, listenAddr: "0.0.0.0:9099"}
	listener, err := net.Listen("tcp", srv.listenAddr)
	if err != nil {
		log.Fatalf("Server bind error: %v", err)
	}
	defer listener.Close()
	fmt.Printf("[Server] Message queue live on %s\n", srv.listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go srv.handleClient(conn) // Handled by server.go
	}
}
