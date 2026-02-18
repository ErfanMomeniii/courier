// Basic example: Sync data from a "database" to "Kafka" with automatic retries
//
// Run with: go run main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/erfanmomeniii/courier"
)

// Event represents a record in our outbox table
type Event struct {
	ID      int
	Type    string
	Payload string
}

// Simulated database (in real code, this would be your actual database)
var (
	mu     sync.Mutex
	outbox = []Event{
		{ID: 1, Type: "order.created", Payload: `{"order_id": 100}`},
		{ID: 2, Type: "user.signup", Payload: `{"user_id": 42}`},
		{ID: 3, Type: "payment.received", Payload: `{"amount": 99.99}`},
	}
	sent = make(map[int]bool)
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Println("Starting courier (Ctrl+C to stop)...")
	fmt.Println()

	// Create a coordinator that:
	// 1. Fetches unsent events every 2 seconds
	// 2. Processes each event (prints it)
	// 3. Marks events as sent
	// 4. Retries up to 5 times on failure
	coordinator := courier.SyncWithRetry(
		fetchUnsent,
		publishEvent,
		markAsSent,
		5, // retry up to 5 times
		courier.WithInterval(2*time.Second),
	)

	if err := coordinator.Start(ctx); err != nil && err != context.Canceled {
		fmt.Printf("Error: %v\n", err)
	}

	fmt.Println("Done!")
}

// fetchUnsent returns events that haven't been sent yet
func fetchUnsent(ctx context.Context) ([]Event, error) {
	mu.Lock()
	defer mu.Unlock()

	var unsent []Event
	for _, e := range outbox {
		if !sent[e.ID] {
			unsent = append(unsent, e)
		}
	}

	if len(unsent) > 0 {
		fmt.Printf("Fetched %d unsent events\n", len(unsent))
	}
	return unsent, nil
}

// publishEvent sends an event to Kafka (simulated)
func publishEvent(ctx context.Context, e Event) error {
	fmt.Printf("  -> Publishing: [%d] %s\n", e.ID, e.Type)
	// In real code: return kafka.Produce(e.Type, e.Payload)
	return nil
}

// markAsSent marks events as sent in the database
func markAsSent(ctx context.Context, events []Event) error {
	mu.Lock()
	defer mu.Unlock()

	for _, e := range events {
		sent[e.ID] = true
	}
	fmt.Printf("Marked %d events as sent\n\n", len(events))
	return nil
}
