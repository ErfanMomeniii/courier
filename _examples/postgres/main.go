// Example: PostgreSQL outbox pattern with courier
//
// This example demonstrates using courier with PostgreSQL to implement
// the transactional outbox pattern. It shows how to:
//   - Read events from an outbox table
//   - Apply events to a destination (simulated HTTP API)
//   - Use retry, DLQ, and circuit breaker for resilience
//
// Prerequisites:
//
//	docker run -d --name postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
//
// Then create the outbox table:
//
//	CREATE TABLE outbox (
//	    id SERIAL PRIMARY KEY,
//	    event_type VARCHAR(255) NOT NULL,
//	    payload JSONB NOT NULL,
//	    created_at TIMESTAMP DEFAULT NOW(),
//	    synced_at TIMESTAMP NULL
//	);
//
// Run with: go run main.go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/erfanmomeniii/courier"
	_ "github.com/lib/pq"
)

// OutboxEvent represents a row in the outbox table.
type OutboxEvent struct {
	ID        int64
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// PostgresOutboxSource reads events from a PostgreSQL outbox table.
type PostgresOutboxSource struct {
	db        *sql.DB
	batchSize int
	logger    *slog.Logger
}

// NewPostgresOutboxSource creates a new PostgreSQL outbox source.
func NewPostgresOutboxSource(db *sql.DB, batchSize int, logger *slog.Logger) *PostgresOutboxSource {
	return &PostgresOutboxSource{
		db:        db,
		batchSize: batchSize,
		logger:    logger,
	}
}

// FetchRecords retrieves unsynced events from the outbox table.
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	s.logger.Debug("fetched events from outbox", "count", len(events))
	return events, nil
}

// MarkAsSynced marks events as successfully synced.
func (s *PostgresOutboxSource) MarkAsSynced(ctx context.Context, records []OutboxEvent) error {
	if len(records) == 0 {
		return nil
	}

	// Build list of IDs to mark
	ids := make([]int64, len(records))
	for i, r := range records {
		ids[i] = r.ID
	}

	// Use ANY for efficient batch update
	query := `
		UPDATE outbox
		SET synced_at = NOW()
		WHERE id = ANY($1)
	`

	result, err := s.db.ExecContext(ctx, query, pqArray(ids))
	if err != nil {
		return fmt.Errorf("mark as synced: %w", err)
	}

	affected, _ := result.RowsAffected()
	s.logger.Debug("marked events as synced", "count", affected)
	return nil
}

// pqArray converts a slice to a PostgreSQL array literal.
func pqArray(ids []int64) string {
	if len(ids) == 0 {
		return "{}"
	}
	result := "{"
	for i, id := range ids {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("%d", id)
	}
	result += "}"
	return result
}

// HTTPApplier sends events to an HTTP API.
type HTTPApplier struct {
	client  *http.Client
	baseURL string
	logger  *slog.Logger
}

// NewHTTPApplier creates an applier that sends events to an HTTP API.
func NewHTTPApplier(baseURL string, timeout time.Duration, logger *slog.Logger) *HTTPApplier {
	return &HTTPApplier{
		client: &http.Client{
			Timeout: timeout,
		},
		baseURL: baseURL,
		logger:  logger,
	}
}

// Apply sends events to the HTTP API.
func (a *HTTPApplier) Apply(ctx context.Context, records []OutboxEvent) ([]OutboxEvent, []OutboxEvent, error) {
	var synced, failed []OutboxEvent

	for _, event := range records {
		// In a real implementation, you would POST to your API
		// For demo purposes, we'll simulate success/failure
		a.logger.Info("applying event",
			"id", event.ID,
			"type", event.EventType,
		)

		// Simulate API call - in production, replace with actual HTTP request:
		// resp, err := a.client.Post(a.baseURL+"/events", "application/json", bytes.NewReader(event.Payload))

		// Simulate: fail every 5th event for demo
		if event.ID%5 == 0 {
			a.logger.Warn("event failed (simulated)", "id", event.ID)
			failed = append(failed, event)
		} else {
			synced = append(synced, event)
		}
	}

	return synced, failed, nil
}

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("Courier PostgreSQL Outbox Example")
	fmt.Println("==================================")
	fmt.Println()

	// Database connection string (configure for your environment)
	connStr := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to PostgreSQL")

	// Create outbox table if not exists
	if err := createOutboxTable(db); err != nil {
		logger.Error("failed to create outbox table", "error", err)
		os.Exit(1)
	}

	// Insert some test events
	if err := insertTestEvents(db); err != nil {
		logger.Error("failed to insert test events", "error", err)
		os.Exit(1)
	}

	// Create source and applier
	source := NewPostgresOutboxSource(db, 10, logger)
	httpApplier := NewHTTPApplier("http://localhost:8080", 5*time.Second, logger)

	// Create DLQ for failed records
	dlq := courier.NewInMemoryDLQ[OutboxEvent](100)

	// Build applier pipeline with resilience patterns:
	// 1. Circuit breaker - prevent cascading failures
	// 2. Retry - exponential backoff for transient failures
	// 3. DLQ - capture permanently failed records

	retryPolicy := courier.RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}

	cbConfig := courier.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
		OnStateChange: func(from, to courier.CircuitState) {
			logger.Warn("circuit breaker state changed",
				"from", from.String(),
				"to", to.String(),
			)
		},
	}

	// Chain appliers: Circuit Breaker -> Retry -> DLQ -> HTTP
	applier := courier.NewCircuitBreakerApplier(
		courier.NewRetryableApplier(
			courier.NewDLQApplier(httpApplier, dlq),
			retryPolicy,
		),
		cbConfig,
	)

	// Create coordinator with hooks for observability
	hooks := &courier.Hooks[OutboxEvent]{
		BeforeFetch: func(ctx context.Context) {
			logger.Debug("fetching records...")
		},
		AfterApply: func(ctx context.Context, synced, failed []OutboxEvent, err error) {
			logger.Info("apply complete",
				"synced", len(synced),
				"failed", len(failed),
				"error", err,
			)
		},
	}

	hookedSource := courier.NewHookedSource(source, hooks)

	coord := courier.NewPollingCoordinator(hookedSource, applier,
		courier.WithInterval(2*time.Second),
		courier.WithBatchSize(10),
		courier.WithLogger(logger),
		courier.WithErrorHandler(func(err error) {
			logger.Error("coordinator error", "error", err)
		}),
	)

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Start coordinator
	logger.Info("starting coordinator (Ctrl+C to stop)")

	if err := coord.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("coordinator error", "error", err)
	}

	// Report DLQ status
	dlqCount, _ := dlq.Count(context.Background())
	logger.Info("shutdown complete",
		"dlq_count", dlqCount,
	)

	if dlqCount > 0 {
		fmt.Println("\nFailed records in DLQ:")
		for _, fr := range dlq.All() {
			fmt.Printf("  - ID: %d, Type: %s, Attempts: %d\n",
				fr.Record.ID, fr.Record.EventType, fr.Attempts)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func createOutboxTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS outbox (
			id SERIAL PRIMARY KEY,
			event_type VARCHAR(255) NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMP DEFAULT NOW(),
			synced_at TIMESTAMP NULL
		)
	`
	_, err := db.Exec(query)
	return err
}

func insertTestEvents(db *sql.DB) error {
	// Check if we already have unsynced events
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM outbox WHERE synced_at IS NULL").Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil // Already have events
	}

	// Insert test events
	events := []struct {
		eventType string
		payload   string
	}{
		{"user.created", `{"user_id": 1, "name": "Alice"}`},
		{"user.created", `{"user_id": 2, "name": "Bob"}`},
		{"order.placed", `{"order_id": 100, "total": 99.99}`},
		{"order.placed", `{"order_id": 101, "total": 149.99}`},
		{"payment.received", `{"payment_id": 1000, "amount": 99.99}`},
		{"user.updated", `{"user_id": 1, "name": "Alice Smith"}`},
		{"order.shipped", `{"order_id": 100, "tracking": "ABC123"}`},
		{"inventory.updated", `{"sku": "WIDGET-01", "quantity": 50}`},
		{"user.deleted", `{"user_id": 2}`},
		{"notification.sent", `{"type": "email", "to": "alice@example.com"}`},
	}

	for _, e := range events {
		_, err := db.Exec(
			"INSERT INTO outbox (event_type, payload) VALUES ($1, $2)",
			e.eventType, e.payload,
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}

	return nil
}
