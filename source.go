package courier

import "context"

// BatchSource defines the interface for fetching records in batches.
// Implement this for polling-based sources like databases or file systems.
//
// Example implementations:
//   - PostgreSQL outbox table reader
//   - MySQL change data capture
//   - File-based event store
type BatchSource[T any] interface {
	// FetchRecords retrieves a batch of records ready to be synced.
	// Returns an empty slice when no records are available.
	// The implementation should respect the batch size configured
	// in the coordinator options.
	FetchRecords(ctx context.Context) ([]T, error)

	// MarkAsSynced marks the given records as successfully synced.
	// This is called after the Applier has successfully processed
	// the records. Implementations typically update a status column
	// or delete the records from an outbox table.
	MarkAsSynced(ctx context.Context, records []T) error
}

// StreamSource defines the interface for streaming records.
// Implement this for event-driven sources like message queues.
//
// Example implementations:
//   - Kafka consumer
//   - RabbitMQ subscriber
//   - Redis Streams reader
//   - NATS subscriber
type StreamSource[T any] interface {
	// Records returns a channel that emits records as they become available.
	// The channel should be closed when the source is stopped or encounters
	// an unrecoverable error.
	Records() <-chan T

	// Ack acknowledges successful processing of a record.
	// Called after the Applier has successfully processed the record.
	Ack(ctx context.Context, record T) error

	// Nack indicates processing failure for a record.
	// The implementation decides whether to requeue, dead-letter,
	// or discard the record based on its configuration.
	Nack(ctx context.Context, record T, err error) error

	// Close stops the source and releases resources.
	// After Close is called, the Records channel should be closed.
	Close() error
}

// BatchSourceFunc is a function adapter for simple BatchSource implementations.
type BatchSourceFunc[T any] struct {
	FetchFunc func(ctx context.Context) ([]T, error)
	MarkFunc  func(ctx context.Context, records []T) error
}

// FetchRecords implements BatchSource.
func (f BatchSourceFunc[T]) FetchRecords(ctx context.Context) ([]T, error) {
	return f.FetchFunc(ctx)
}

// MarkAsSynced implements BatchSource.
func (f BatchSourceFunc[T]) MarkAsSynced(ctx context.Context, records []T) error {
	return f.MarkFunc(ctx, records)
}
