// Example: Basic usage of courier with the simple API
//
// This example demonstrates how to use courier's convenience functions
// to sync records with minimal boilerplate.
//
// Run with: go run main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/erfanmomeniii/courier"
)

// Event represents an outbox event
type Event struct {
	ID        string
	EventType string
	Payload   string
	CreatedAt time.Time
}

// Simple in-memory storage (simulates a database)
var (
	mu      sync.Mutex
	events  []Event
	synced  = make(map[string]bool)
	counter int
)

func main() {
	fmt.Println("Courier Basic Example")
	fmt.Println("=====================")
	fmt.Println()

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Simulate events being added in the background
	go addEventsInBackground(ctx)

	// THE SIMPLE WAY: Just provide 3 functions!
	//
	// This creates a loop that:
	//   1. Calls fetchEvents() to get unprocessed records
	//   2. Calls processEvent() for each record
	//   3. Calls markAsSynced() with successful records
	//   4. Waits 2 seconds
	//   5. Repeats forever until Ctrl+C
	//
	coordinator := courier.Sync(
		fetchEvents,   // How to get records
		processEvent,  // What to do with each record
		markAsSynced,  // How to mark records as done
		courier.WithInterval(2*time.Second), // How often to check for new records
	)

	fmt.Println("Starting coordinator (Ctrl+C to stop)...")
	fmt.Println()

	if err := coordinator.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("Coordinator error: %v\n", err)
	}

	fmt.Println("\nDone!")
}

// fetchEvents retrieves unprocessed events
func fetchEvents(ctx context.Context) ([]Event, error) {
	mu.Lock()
	defer mu.Unlock()

	var unsynced []Event
	for _, e := range events {
		if !synced[e.ID] {
			unsynced = append(unsynced, e)
			if len(unsynced) >= 10 {
				break
			}
		}
	}
	return unsynced, nil
}

// processEvent handles a single event
func processEvent(ctx context.Context, event Event) error {
	fmt.Printf("  ✓ Published: [%s] %s -> %s\n", event.ID, event.EventType, event.Payload)
	return nil
}

// markAsSynced marks events as processed
func markAsSynced(ctx context.Context, records []Event) error {
	mu.Lock()
	defer mu.Unlock()

	for _, r := range records {
		synced[r.ID] = true
	}
	return nil
}

// addEventsInBackground simulates events being added to the outbox
func addEventsInBackground(ctx context.Context) {
	eventsToAdd := []struct{ typ, payload string }{
		{"user.created", `{"id": 1, "name": "Alice"}`},
		{"order.placed", `{"id": 100, "total": 99.99}`},
		{"user.updated", `{"id": 1, "name": "Alice Smith"}`},
		{"payment.received", `{"order_id": 100, "amount": 99.99}`},
		{"order.shipped", `{"id": 100, "tracking": "ABC123"}`},
	}

	for i, e := range eventsToAdd {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(500+i*300) * time.Millisecond):
			mu.Lock()
			counter++
			events = append(events, Event{
				ID:        fmt.Sprintf("evt-%d", counter),
				EventType: e.typ,
				Payload:   e.payload,
				CreatedAt: time.Now(),
			})
			mu.Unlock()
			fmt.Printf("+ Added event: %s\n", e.typ)
		}
	}
}
