# Courier

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A lightweight Go library for **reliable data synchronization** between systems. Courier helps you move data from point A to point B with built-in resilience patterns.

## The Problem

When building distributed systems, you often need to:
- Publish database changes to a message broker (transactional outbox)
- Keep a cache in sync with your database
- Update a search index when data changes
- Deliver webhooks reliably
- Replicate data between services

These tasks share common challenges:
- **Reliability**: What if the target system is down?
- **Ordering**: How do you process records in order?
- **Failures**: How do you handle and retry failed records?
- **Duplicates**: How do you prevent processing the same record twice?
- **Rate Limits**: How do you avoid overwhelming downstream services?
- **Observability**: How do you monitor the sync process?

Courier provides a simple, composable solution for these problems.

### What Courier Is NOT

Courier is **not** an event bus, message broker, or CDC tool. It's the **glue code** you write to move data reliably between systems - extracted into a well-tested library. You still need your own database, message broker, or other infrastructure.

## How It Works

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Source    │────▶│ Coordinator  │────▶│   Applier   │
│ (where data │     │ (orchestrates│     │ (where data │
│  comes from)│     │  the flow)   │     │   goes to)  │
└─────────────┘     └──────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │  Middleware │
                    │ (retry, DLQ,│
                    │  circuit    │
                    │  breaker)   │
                    └─────────────┘
```

| Component | What It Does | You Implement |
|-----------|--------------|---------------|
| **Source** | Where records come from | `BatchSource` or `StreamSource` interface |
| **Applier** | What happens to records | `Applier` interface |
| **Coordinator** | Orchestrates the flow | Use `PollingCoordinator` or `StreamingCoordinator` |
| **Middleware** | Adds resilience | Optional: wrap your applier |

## Installation

```bash
go get github.com/erfanmomeniii/courier
```

Requires Go 1.21+

---

## Quick Start (The Easy Way)

The simplest way to use Courier - just provide three functions:

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/erfanmomeniii/courier"
)

type Event struct {
    ID   string
    Data string
}

func main() {
    // Just 3 functions + configure how often to run:
    coordinator := courier.Sync(
        fetchEvents,    // 1. How to get records
        processEvent,   // 2. What to do with each record
        markAsSynced,   // 3. How to mark records as done
        courier.WithInterval(5*time.Second), // Run every 5 seconds
    )

    // coordinator.Start() runs FOREVER in a loop:
    //   1. Call fetchEvents() to get records
    //   2. For each record, call processEvent()
    //   3. Call markAsSynced() with successful records
    //   4. Wait 5 seconds
    //   5. Repeat from step 1...
    //
    // Stops when context is cancelled (Ctrl+C)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    log.Fatal(coordinator.Start(ctx))
}

func fetchEvents(ctx context.Context) ([]Event, error) {
    // Fetch unprocessed events from your database
    return db.Query("SELECT * FROM outbox WHERE synced_at IS NULL LIMIT 100")
}

func processEvent(ctx context.Context, event Event) error {
    // Do something with each event (called once per record)
    return sendToKafka(event)
}

func markAsSynced(ctx context.Context, events []Event) error {
    // Mark events as processed (called once per batch)
    return db.Exec("UPDATE outbox SET synced_at = NOW() WHERE id IN (?)", ids(events))
}
```

**How it works:**

```
┌──────────────────────────────────────────────────────────────────┐
│                     coordinator.Start(ctx)                       │
│                                                                  │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│   │ fetchEvents │───▶│processEvent │───▶│markAsSynced │──┐       │
│   │  (get 100)  │    │ (each one)  │    │  (batch)    │  │       │
│   └─────────────┘    └─────────────┘    └─────────────┘  │       │
│         ▲                                                │       │
│         │                 Wait 5 seconds                 │       │
│         └────────────────────────────────────────────────┘       │
│                                                                  │
│   Runs forever until ctx is cancelled                            │
└──────────────────────────────────────────────────────────────────┘
```

### Common Configurations

```go
// Run every 5 seconds (default)
coordinator := courier.Sync(fetch, process, mark, courier.WithInterval(5*time.Second))

// Run every 100ms for low latency
coordinator := courier.Sync(fetch, process, mark, courier.WithFastPolling())

// Run every 30 seconds for less frequent sync
coordinator := courier.Sync(fetch, process, mark, courier.WithSlowPolling())
```

### Add Retry with One Line

```go
// Automatically retry failed records up to 5 times with exponential backoff
// Still runs every 5 seconds, but retries failures within each cycle
coordinator := courier.SyncWithRetry(fetch, process, mark, 5,
    courier.WithInterval(5*time.Second),
)
```

### One-Shot Sync (No Loop)

```go
// Run exactly ONCE, then stop. Perfect for cron jobs or manual triggers.
err := courier.RunOnce(ctx, fetchEvents, processEvent, markAsSynced)
if err != nil {
    log.Fatal(err)
}
// Done! No loop, no waiting.
```

### Add Full Resilience

```go
// Create your base applier
applier := courier.ProcessFunc(processEvent)

// Wrap with retry + DLQ + circuit breaker in one call
resilientApplier, dlq := courier.ResilientApplier[Event](applier, nil)

// Use it with a source
source := courier.BatchSourceFunc[Event]{
    FetchFunc: fetchEvents,
    MarkFunc:  markAsSynced,
}
coordinator := courier.NewPollingCoordinator(source, resilientApplier,
    courier.WithInterval(5*time.Second),
)

// Later, check what failed permanently
failedRecords, _ := dlq.Receive(ctx, 100)
for _, f := range failedRecords {
    log.Printf("Failed record: %v, error: %v", f.Record, f.Error)
}
```

---

## Step-by-Step Guide (The Detailed Way)

### Step 1: Define Your Record Type

First, define the data structure you want to sync:

```go
// Your record can be any Go type
type OutboxEvent struct {
    ID        int64
    EventType string
    Payload   []byte
    CreatedAt time.Time
}
```

### Step 2: Choose Your Coordination Strategy

| Question | If Yes → Use |
|----------|--------------|
| Does your source push events? (Kafka, RabbitMQ, Redis Streams) | `StreamingCoordinator` + `StreamSource` |
| Do you need to poll for new records? (Database, API, Files) | `PollingCoordinator` + `BatchSource` |

### Step 3: Implement the Source Interface

#### Option A: BatchSource (for polling)

Implement this interface when you need to periodically fetch records:

```go
type BatchSource[T any] interface {
    // FetchRecords retrieves unprocessed records
    // Return empty slice when no records available
    FetchRecords(ctx context.Context) ([]T, error)

    // MarkAsSynced marks records as successfully processed
    // Called after Applier returns them as "synced"
    MarkAsSynced(ctx context.Context, records []T) error
}
```

**Complete Example - PostgreSQL Outbox:**

```go
type PostgresOutboxSource struct {
    db        *sql.DB
    batchSize int
}

func NewPostgresOutboxSource(db *sql.DB, batchSize int) *PostgresOutboxSource {
    return &PostgresOutboxSource{db: db, batchSize: batchSize}
}

// FetchRecords retrieves unsynced events from the outbox table
func (s *PostgresOutboxSource) FetchRecords(ctx context.Context) ([]OutboxEvent, error) {
    query := `
        SELECT id, event_type, payload, created_at
        FROM outbox
        WHERE synced_at IS NULL
        ORDER BY id ASC
        LIMIT $1
    `
    rows, err := s.db.QueryContext(ctx, query, s.batchSize)
    if err != nil {
        return nil, fmt.Errorf("query outbox: %w", err)
    }
    defer rows.Close()

    var events []OutboxEvent
    for rows.Next() {
        var e OutboxEvent
        if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
            return nil, fmt.Errorf("scan row: %w", err)
        }
        events = append(events, e)
    }
    return events, rows.Err()
}

// MarkAsSynced updates the synced_at timestamp for processed records
func (s *PostgresOutboxSource) MarkAsSynced(ctx context.Context, records []OutboxEvent) error {
    if len(records) == 0 {
        return nil
    }

    // Collect IDs
    ids := make([]int64, len(records))
    for i, r := range records {
        ids[i] = r.ID
    }

    query := `UPDATE outbox SET synced_at = NOW() WHERE id = ANY($1)`
    _, err := s.db.ExecContext(ctx, query, pq.Array(ids))
    return err
}
```

#### Option B: StreamSource (for message queues)

Implement this interface when your source pushes events:

```go
type StreamSource[T any] interface {
    // Records returns a channel that emits records as they arrive
    // Close the channel when the source stops
    Records() <-chan T

    // Ack acknowledges successful processing
    // Called for each record in the "synced" list
    Ack(ctx context.Context, record T) error

    // Nack reports processing failure
    // Called for each record in the "failed" list
    Nack(ctx context.Context, record T, err error) error

    // Close stops the source and releases resources
    Close() error
}
```

**Complete Example - Kafka Consumer:**

```go
type KafkaSource struct {
    reader  *kafka.Reader
    records chan KafkaMessage
    closed  bool
    mu      sync.Mutex
}

type KafkaMessage struct {
    Topic     string
    Partition int
    Offset    int64
    Key       []byte
    Value     []byte
}

func NewKafkaSource(brokers []string, topic, groupID string) *KafkaSource {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: brokers,
        Topic:   topic,
        GroupID: groupID,
    })

    s := &KafkaSource{
        reader:  reader,
        records: make(chan KafkaMessage, 100),
    }
    go s.readLoop()
    return s
}

func (s *KafkaSource) readLoop() {
    defer close(s.records)
    for {
        s.mu.Lock()
        if s.closed {
            s.mu.Unlock()
            return
        }
        s.mu.Unlock()

        msg, err := s.reader.FetchMessage(context.Background())
        if err != nil {
            continue
        }

        s.records <- KafkaMessage{
            Topic:     msg.Topic,
            Partition: msg.Partition,
            Offset:    msg.Offset,
            Key:       msg.Key,
            Value:     msg.Value,
        }
    }
}

func (s *KafkaSource) Records() <-chan KafkaMessage {
    return s.records
}

func (s *KafkaSource) Ack(ctx context.Context, record KafkaMessage) error {
    return s.reader.CommitMessages(ctx, kafka.Message{
        Topic:     record.Topic,
        Partition: record.Partition,
        Offset:    record.Offset,
    })
}

func (s *KafkaSource) Nack(ctx context.Context, record KafkaMessage, err error) error {
    // Log the failure - message will be redelivered on restart
    log.Printf("Failed to process message: %v", err)
    return nil
}

func (s *KafkaSource) Close() error {
    s.mu.Lock()
    s.closed = true
    s.mu.Unlock()
    return s.reader.Close()
}
```

### Step 4: Implement the Applier Interface

The Applier defines what happens to each batch of records:

```go
type Applier[T any] interface {
    // Apply processes a batch of records
    // Returns:
    //   - synced: records that succeeded (will be marked as synced)
    //   - failed: records that failed (will be retried or sent to DLQ)
    //   - err: fatal error that stops all processing
    Apply(ctx context.Context, records []T) (synced []T, failed []T, err error)
}
```

**Understanding the Return Values:**

| Return | Meaning | What Happens Next |
|--------|---------|-------------------|
| `synced` | Successfully processed | `MarkAsSynced()` or `Ack()` is called |
| `failed` | Individual failures | Retried (if retry enabled) or sent to DLQ |
| `err` | Fatal/infrastructure error | Processing stops, all records are failed |

**Complete Example - HTTP Webhook Applier:**

```go
type WebhookApplier struct {
    client  *http.Client
    baseURL string
}

func NewWebhookApplier(baseURL string, timeout time.Duration) *WebhookApplier {
    return &WebhookApplier{
        client:  &http.Client{Timeout: timeout},
        baseURL: baseURL,
    }
}

func (a *WebhookApplier) Apply(ctx context.Context, records []OutboxEvent) ([]OutboxEvent, []OutboxEvent, error) {
    var synced, failed []OutboxEvent

    for _, event := range records {
        // Create request
        req, err := http.NewRequestWithContext(ctx, "POST",
            a.baseURL+"/webhook",
            bytes.NewReader(event.Payload))
        if err != nil {
            failed = append(failed, event)
            continue
        }
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("X-Event-Type", event.EventType)

        // Send request
        resp, err := a.client.Do(req)
        if err != nil {
            // Network error - mark as failed for retry
            failed = append(failed, event)
            continue
        }
        resp.Body.Close()

        // Check response
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            synced = append(synced, event)
        } else if resp.StatusCode >= 500 {
            // Server error - retry
            failed = append(failed, event)
        } else {
            // Client error (4xx) - don't retry, mark as synced to skip
            // (or you could send to DLQ)
            synced = append(synced, event)
        }
    }

    return synced, failed, nil
}
```

**Complete Example - Kafka Producer Applier:**

```go
type KafkaProducerApplier struct {
    writer *kafka.Writer
    topic  string
}

func NewKafkaProducerApplier(brokers []string, topic string) *KafkaProducerApplier {
    return &KafkaProducerApplier{
        writer: &kafka.Writer{
            Addr:     kafka.TCP(brokers...),
            Topic:    topic,
            Balancer: &kafka.LeastBytes{},
        },
        topic: topic,
    }
}

func (a *KafkaProducerApplier) Apply(ctx context.Context, records []OutboxEvent) ([]OutboxEvent, []OutboxEvent, error) {
    // Convert to Kafka messages
    messages := make([]kafka.Message, len(records))
    for i, r := range records {
        messages[i] = kafka.Message{
            Key:   []byte(fmt.Sprintf("%d", r.ID)),
            Value: r.Payload,
            Headers: []kafka.Header{
                {Key: "event-type", Value: []byte(r.EventType)},
            },
        }
    }

    // Write batch
    err := a.writer.WriteMessages(ctx, messages...)
    if err != nil {
        // If batch write fails, return fatal error
        // All records will be retried
        return nil, records, fmt.Errorf("kafka write: %w", err)
    }

    return records, nil, nil
}

func (a *KafkaProducerApplier) Close() error {
    return a.writer.Close()
}
```

### Step 5: Add Resilience (Optional but Recommended)

Wrap your applier with middleware for production use:

```go
// Create your base applier
baseApplier := NewWebhookApplier("https://api.example.com", 10*time.Second)

// Create a dead letter queue for failed records
dlq := courier.NewInMemoryDLQ[OutboxEvent](10000)

// Create retry policy
retryPolicy := courier.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
    Jitter:       0.1,
}

// Create circuit breaker config
cbConfig := courier.CircuitBreakerConfig{
    FailureThreshold: 5,
    SuccessThreshold: 2,
    Timeout:          30 * time.Second,
}

// Wrap applier: Circuit Breaker → Retry → DLQ → Base Applier
resilientApplier := courier.NewCircuitBreakerApplier(
    courier.NewRetryableApplier(
        courier.NewDLQApplier(baseApplier, dlq),
        retryPolicy,
    ),
    cbConfig,
)
```

**Middleware Order (outermost to innermost):**

```
Request flow:
    CircuitBreaker → Retry → DLQ → Your Applier

1. Circuit Breaker: Blocks if downstream is unhealthy
2. Retry: Retries failed records with backoff
3. DLQ: Captures permanently failed records
4. Your Applier: Does the actual work
```

### Step 6: Create and Start the Coordinator

**For Polling (database, API, files):**

```go
func main() {
    // Create source
    db, _ := sql.Open("postgres", connectionString)
    source := NewPostgresOutboxSource(db, 100)

    // Create applier with resilience
    applier := createResilientApplier()

    // Create coordinator
    coord := courier.NewPollingCoordinator(source, applier,
        courier.WithInterval(5*time.Second),    // Poll every 5 seconds
        courier.WithBatchSize(100),             // Fetch up to 100 records
        courier.WithLogger(slog.Default()),     // Use structured logging
        courier.WithErrorHandler(func(err error) {
            log.Printf("Error: %v", err)
        }),
    )

    // Start with graceful shutdown
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    log.Println("Starting coordinator...")
    if err := coord.Start(ctx); err != nil && err != context.Canceled {
        log.Fatalf("Coordinator error: %v", err)
    }
    log.Println("Shutdown complete")
}
```

**For Streaming (Kafka, RabbitMQ, Redis Streams):**

```go
func main() {
    // Create source
    source := NewKafkaSource(
        []string{"localhost:9092"},
        "events-topic",
        "my-consumer-group",
    )
    defer source.Close()

    // Create applier with resilience
    applier := createResilientApplier()

    // Create coordinator
    coord := courier.NewStreamingCoordinator(source, applier,
        courier.WithWorkers(4),                 // 4 concurrent workers
        courier.WithBufferSize(100),            // Buffer up to 100 records
        courier.WithStreamLogger(slog.Default()),
    )

    // Start with graceful shutdown
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
    defer cancel()

    log.Println("Starting streaming coordinator...")
    if err := coord.Start(ctx); err != nil && err != context.Canceled {
        log.Fatalf("Coordinator error: %v", err)
    }
}
```

---

## Quick Reference

### Convenience Functions (Easiest)

```go
// Simplest: just provide 3 functions
coord := courier.Sync(fetch, process, mark)

// Process batches atomically
coord := courier.SyncBatch(fetch, processBatch, mark)

// With automatic retry
coord := courier.SyncWithRetry(fetch, process, mark, 5) // 5 attempts

// One-shot (no loop)
err := courier.RunOnce(ctx, fetch, process, mark)

// Create resilient applier (retry + DLQ + circuit breaker)
applier, dlq := courier.ResilientApplier(baseApplier, nil)
```

### Applier Helpers

```go
// From a simple function (easiest)
applier := courier.ProcessFunc(func(ctx context.Context, event Event) error {
    return sendWebhook(event)
})

// Process entire batch atomically
applier := courier.ProcessBatchFunc(func(ctx context.Context, events []Event) error {
    return bulkInsert(events)
})

// Add retry with defaults
applier := courier.WithRetry(baseApplier)

// Add DLQ
applier, dlq := courier.WithDLQ(baseApplier)

// Add circuit breaker
applier := courier.WithCircuitBreaker(baseApplier)

// Full control with ApplierFunc
applier := courier.ApplierFunc[Event](func(ctx context.Context, events []Event) ([]Event, []Event, error) {
    var synced, failed []Event
    // ... custom logic
    return synced, failed, nil
})
```

### Source Helpers

```go
// Quick BatchSource from functions
source := courier.BatchSourceFunc[Event]{
    FetchFunc: func(ctx context.Context) ([]Event, error) {
        return fetchFromDatabase(ctx)
    },
    MarkFunc: func(ctx context.Context, records []Event) error {
        return markAsSynced(ctx, records)
    },
}
```

### Resilience Patterns

| Pattern | When to Use | Code |
|---------|-------------|------|
| **Retry** | Transient failures (network, timeout) | `courier.NewRetryableApplier(applier, policy)` |
| **DLQ** | Capture permanently failed records | `courier.NewDLQApplier(applier, dlq)` |
| **Circuit Breaker** | Prevent cascading failures | `courier.NewCircuitBreakerApplier(applier, config)` |
| **Rate Limiting** | Protect downstream services | `courier.NewRateLimitedApplier(applier, limiter, perBatch)` |
| **Deduplication** | Prevent duplicate processing | `courier.NewDeduplicatingApplier(applier, dedup, getKey)` |
| **Timeout** | Limit processing time | `courier.NewTimeoutApplier(applier, timeout, perBatch)` |
| **Validation** | Reject invalid records early | `courier.NewValidatingApplier(applier, skip, validators...)` |
| **Health Check** | Monitor applier health | `courier.NewHealthCheckApplier(applier, health)` |

### Built-in Middleware

```go
// Logging - log every apply operation
courier.LoggingMiddleware[Event](log.Printf)(applier)

// Timing - measure apply duration
courier.TimingMiddleware[Event](func(d time.Duration) {
    metrics.ObserveDuration(d)
})(applier)

// Recovery - recover from panics
courier.RecoveryMiddleware[Event](func(r any) {
    log.Printf("Panic: %v", r)
})(applier)

// Filter - skip certain records
courier.FilterMiddleware[Event](func(e Event) bool {
    return e.Type != "ignored"
})(applier)

// Transform - modify records before processing
courier.TransformMiddleware[Event](func(e Event) Event {
    e.Payload = encrypt(e.Payload)
    return e
})(applier)

// Chain multiple middleware
courier.Chain(
    courier.LoggingMiddleware[Event](log.Printf),
    courier.TimingMiddleware[Event](recordDuration),
)(applier)
```

### Rate Limiting

Protect downstream services from being overwhelmed:

```go
// Allow 100 operations per second
limiter := courier.NewRateLimiter(100, time.Second)

// Rate limit per batch (each Apply call counts as 1)
applier := courier.NewRateLimitedApplier(baseApplier, limiter, true)

// Rate limit per record (each record counts as 1)
applier := courier.NewRateLimitedApplier(baseApplier, limiter, false)
```

### Deduplication

Prevent processing the same record twice:

```go
// Create deduplicator with 1 hour TTL and max 100k entries
dedup := courier.NewDeduplicator[string](time.Hour, 100000)

// Wrap applier - duplicate records are skipped (counted as synced)
applier := courier.NewDeduplicatingApplier(
    baseApplier,
    dedup,
    func(e Event) string { return e.ID }, // Extract unique key
)
```

### Timeout

Limit how long processing can take:

```go
// Timeout for entire batch
applier := courier.NewTimeoutApplier(baseApplier, 30*time.Second, true)

// Timeout per record (continues with other records if one times out)
applier := courier.NewTimeoutApplier(baseApplier, 5*time.Second, false)
```

### Validation

Reject invalid records before processing:

```go
// Built-in validators
applier := courier.NewValidatingApplier(baseApplier, false, // false = failed validation counts as failed
    courier.NotEmpty(func(e Event) string { return e.ID }, "ID"),
    courier.Positive(func(e Event) int { return e.Priority }, "Priority"),
    courier.InRange(func(e Event) int { return e.Size }, 1, 1000, "Size"),
    courier.MatchesPattern(func(e Event) bool {
        return strings.HasPrefix(e.Type, "event.")
    }, "Type must start with 'event.'"),
)

// Set first param to true to skip invalid records (count as synced)
applier := courier.NewValidatingApplier(baseApplier, true, validators...)
```

### Health Checks

Monitor the health of your applier:

```go
// Create health check (degraded after 3 errors, unhealthy after 10)
health := courier.NewHealthCheck(3, 10)

// Wrap applier
applier := courier.NewHealthCheckApplier(baseApplier, health)

// Check status anytime
if !health.IsHealthy() {
    log.Printf("Status: %s", health.Status()) // "healthy", "degraded", or "unhealthy"
}

// Get detailed metrics
details := health.Details()
log.Printf("Processed: %d, Failed: %d, Errors: %d",
    details.TotalProcessed, details.TotalFailed, details.ConsecutiveErrs)
```

### Batch Splitting

Split large batches into smaller chunks:

```go
// Process at most 100 records at a time
applier := courier.NewBatchSplitter(baseApplier, 100)
```

### Parallel Processing

Process records concurrently within a batch:

```go
// Process records with 10 concurrent workers
applier := courier.NewParallelApplier(func(ctx context.Context, e Event) error {
    return processEvent(e)
}, 10)
```

### Routing

Route records to different appliers based on content:

```go
router := courier.NewRouterApplier(
    func(e Event) string { return e.Type }, // Selector function
    map[string]courier.Applier[Event]{
        "order":   orderApplier,
        "payment": paymentApplier,
        "user":    userApplier,
    },
    defaultApplier, // Fallback for unknown types (can be nil to skip)
)
```

### Fan-Out

Send each record to multiple destinations:

```go
// Record is synced only if ALL appliers succeed
applier := courier.NewFanOutApplier(
    kafkaApplier,
    elasticsearchApplier,
    cacheApplier,
)
```

### Lifecycle Hooks

```go
hooks := &courier.Hooks[Event]{
    BeforeFetch: func(ctx context.Context) { },
    AfterFetch:  func(ctx context.Context, records []Event, err error) { },
    BeforeApply: func(ctx context.Context, records []Event) { },
    AfterApply:  func(ctx context.Context, synced, failed []Event, err error) { },
    BeforeMark:  func(ctx context.Context, records []Event) { },
    AfterMark:   func(ctx context.Context, records []Event, err error) { },
}

hookedSource := courier.NewHookedSource(source, hooks)
hookedApplier := courier.NewHookedApplier(applier, hooks)
```

---

## Configuration Reference

### Polling Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithInterval(d)` | 5s | Time between polls |
| `WithBatchSize(n)` | 100 | Max records per fetch |
| `WithMaxRetries(n)` | 3 | Retry attempts for coordinator-level retries |
| `WithRetryDelay(d)` | 1s | Delay between retries |
| `WithLogger(l)` | slog.Default() | Structured logger |
| `WithErrorHandler(fn)` | no-op | Error callback |
| `WithMetrics(h)` | no-op | Metrics handler |

### Streaming Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithWorkers(n)` | 1 | Concurrent workers |
| `WithBufferSize(n)` | 100 | Batch buffer size |
| `WithStreamLogger(l)` | slog.Default() | Structured logger |
| `WithStreamErrorHandler(fn)` | no-op | Error callback |
| `WithStreamMetrics(h)` | no-op | Metrics handler |

### Retry Policy Options

| Field | Default | Description |
|-------|---------|-------------|
| `MaxAttempts` | 5 | Maximum retry attempts (0 = unlimited) |
| `InitialDelay` | 100ms | First retry delay |
| `MaxDelay` | 30s | Maximum delay cap |
| `Multiplier` | 2.0 | Exponential backoff multiplier |
| `Jitter` | 0.1 | Random jitter (0-1, e.g., 0.1 = ±10%) |

### Circuit Breaker Options

| Field | Default | Description |
|-------|---------|-------------|
| `FailureThreshold` | 5 | Failures before opening circuit |
| `SuccessThreshold` | 2 | Successes in half-open to close |
| `Timeout` | 30s | Time before trying half-open |
| `OnStateChange` | nil | Callback for state transitions |

---

## Metrics Integration

```go
metrics := &courier.MetricsFunc{
    OnRecordsFetched: func(count int) {
        prometheus.RecordsFetched.Add(float64(count))
    },
    OnRecordsApplied: func(synced, failed int) {
        prometheus.RecordsSynced.Add(float64(synced))
        prometheus.RecordsFailed.Add(float64(failed))
    },
    OnRecordsMarked: func(count int) {
        prometheus.RecordsMarked.Add(float64(count))
    },
    OnPollDuration: func(d time.Duration) {
        prometheus.PollDuration.Observe(d.Seconds())
    },
    OnApplyDuration: func(d time.Duration) {
        prometheus.ApplyDuration.Observe(d.Seconds())
    },
    OnErrorOccurred: func(errType string) {
        prometheus.Errors.WithLabelValues(errType).Inc()
    },
}

coord := courier.NewPollingCoordinator(source, applier,
    courier.WithMetrics(metrics),
)
```

---

## Examples

Complete working examples in the `_examples` directory:

| Example | Description | Source | Applier |
|---------|-------------|--------|---------|
| [basic](_examples/basic) | In-memory demo | In-memory list | Console output |
| [postgres](_examples/postgres) | Transactional outbox | PostgreSQL table | HTTP webhook |
| [redis](_examples/redis) | Stream processing | Redis Streams | Redis Pub/Sub |
| [kafka](_examples/kafka) | Event pipeline | Kafka consumer | Kafka producer |

---

## Common Use Cases

| Use Case | Source | Applier | Coordinator |
|----------|--------|---------|-------------|
| Transactional Outbox | Database table | Message broker | Polling |
| Cache Invalidation | Message queue | Redis/Memcached | Streaming |
| Search Indexing | Database CDC | Elasticsearch | Polling |
| Webhook Delivery | Database table | HTTP client | Polling |
| Event Replay | Event store | Multiple targets | Polling |
| Data Replication | Source database | Target database | Polling |
| Real-time ETL | Kafka topic | Data warehouse | Streaming |

---


## License

MIT License - see [LICENSE](LICENSE) for details.
