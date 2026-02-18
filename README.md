# Courier

Reliable data synchronization for Go. Move data from A to B with retries, error handling, and resilience built-in.

```bash
go get github.com/erfanmomeniii/courier
```

## Quick Start

### The Simplest Way

```go
package main

import (
    "context"
    "time"
    "github.com/erfanmomeniii/courier"
)

func main() {
    ctx := context.Background()

    coordinator := courier.Sync(
        fetchRecords,  // 1. How to get records from source
        processRecord, // 2. What to do with each record
        markDone,      // 3. How to mark records as processed
        courier.WithInterval(5*time.Second), // Check every 5 seconds
    )

    // Runs forever: fetch -> process -> mark -> wait 5s -> repeat
    coordinator.Start(ctx)
}

func fetchRecords(ctx context.Context) ([]Event, error) {
    return db.Query("SELECT * FROM outbox WHERE processed = false LIMIT 100")
}

func processRecord(ctx context.Context, e Event) error {
    return kafka.Publish(e)
}

func markDone(ctx context.Context, events []Event) error {
    return db.Exec("UPDATE outbox SET processed = true WHERE id IN (?)", ids(events))
}
```

### With Automatic Retries

```go
// Retry failed records up to 5 times with exponential backoff
coordinator := courier.SyncWithRetry(
    fetchRecords,
    processRecord,
    markDone,
    5, // max attempts
    courier.WithInterval(5*time.Second),
)
```

### Run Once (for cron jobs)

```go
// No loop - runs exactly once and exits
err := courier.RunOnce(ctx, fetchRecords, processRecord, markDone)
```

### Full Resilience

```go
// Retry + Dead Letter Queue + Circuit Breaker
applier := courier.ProcessFunc(processRecord)
resilient, dlq := courier.ResilientApplier[Event](applier, nil)

source := courier.BatchSourceFunc[Event]{
    FetchFunc: fetchRecords,
    MarkFunc:  markDone,
}

coordinator := courier.NewPollingCoordinator(source, resilient)
coordinator.Start(ctx)

// Check permanently failed records
failed, _ := dlq.Receive(ctx, 100)
```

## How It Works

```
coordinator.Start(ctx) runs this loop forever:

    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │fetchRecords │────▶│processRecord│────▶│  markDone   │
    │  (get 100)  │     │ (each one)  │     │  (batch)    │
    └─────────────┘     └─────────────┘     └─────────────┘
           ▲                                       │
           │              wait 5 seconds           │
           └───────────────────────────────────────┘
```

## Configuration

```go
courier.WithInterval(5*time.Second)  // How often to poll (default: 5s)
courier.WithBatchSize(100)           // Max records per batch (default: 100)
courier.WithFastPolling()            // 100ms interval
courier.WithSlowPolling()            // 30s interval
```

## Resilience Patterns

### Retry

```go
// Simple (default settings)
applier = courier.WithRetry(applier)

// Custom
applier = courier.NewRetryableApplier(applier, courier.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
})
```

### Dead Letter Queue

```go
// Records that fail after all retries go here
applier, dlq = courier.WithDLQ(applier)

// Later, inspect failures
failed, _ := dlq.Receive(ctx, 100)
for _, f := range failed {
    log.Printf("Record %v failed: %v", f.Record, f.Error)
}
```

### Circuit Breaker

```go
// Stops calling downstream when too many failures
applier = courier.WithCircuitBreaker(applier)
```

### All At Once

```go
// Combines retry + DLQ + circuit breaker
applier, dlq = courier.ResilientApplier(baseApplier, nil)
```

## Real-Time Streaming

For continuous streams instead of polling:

```go
events := make(chan Event)
source := courier.NewChannelSource(events, markDone)

coordinator := courier.NewStreamingCoordinator(source, applier,
    courier.WithStreamWorkers(4), // 4 parallel workers
)
coordinator.Start(ctx)
```

## Common Use Cases

### Database to Kafka (Outbox Pattern)

```go
courier.SyncWithRetry(
    func(ctx context.Context) ([]Event, error) {
        return db.Query("SELECT * FROM outbox WHERE sent = false LIMIT 100")
    },
    func(ctx context.Context, e Event) error {
        return kafka.Produce(e.Topic, e.Key, e.Value)
    },
    func(ctx context.Context, events []Event) error {
        return db.Exec("UPDATE outbox SET sent = true WHERE id IN (?)", ids(events))
    },
    5,
)
```

### External API to Database

```go
courier.Sync(
    func(ctx context.Context) ([]Order, error) {
        return externalAPI.GetPendingOrders()
    },
    func(ctx context.Context, o Order) error {
        return db.Insert("orders", o)
    },
    func(ctx context.Context, orders []Order) error {
        return externalAPI.MarkAsProcessed(ids(orders))
    },
)
```

### Fan-out to Multiple Destinations

```go
applier := courier.NewFanOutApplier(
    kafkaApplier,
    elasticsearchApplier,
    metricsApplier,
)
```

## Testing

```go
func TestSync(t *testing.T) {
    var processed []string

    err := courier.RunOnce(ctx,
        func(ctx context.Context) ([]string, error) {
            return []string{"a", "b", "c"}, nil
        },
        func(ctx context.Context, s string) error {
            processed = append(processed, s)
            return nil
        },
        func(ctx context.Context, records []string) error {
            return nil
        },
    )

    assert.NoError(t, err)
    assert.Equal(t, []string{"a", "b", "c"}, processed)
}
```

## License

MIT
