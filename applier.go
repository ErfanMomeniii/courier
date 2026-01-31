package courier

import "context"

// Applier defines the interface for processing records.
// Implement this to define what happens when records are synced.
//
// Example implementations:
//   - Kafka producer (publish events)
//   - HTTP client (send webhooks)
//   - Cache updater (sync to Redis)
//   - Search indexer (update Elasticsearch)
type Applier[T any] interface {
	// Apply processes a batch of records and returns the results.
	//
	// Returns:
	//   - synced: records that were successfully processed
	//   - failed: records that failed to process
	//   - err: fatal error that should stop processing
	//
	// The distinction between failed records and fatal errors:
	//   - Failed records: individual record failures (e.g., validation error)
	//     These records can be retried or dead-lettered.
	//   - Fatal error: infrastructure failure (e.g., broker unavailable)
	//     This stops the coordinator and triggers error handling.
	Apply(ctx context.Context, records []T) (synced []T, failed []T, err error)
}

// ApplierFunc is a function adapter for simple Applier implementations.
type ApplierFunc[T any] func(ctx context.Context, records []T) (synced []T, failed []T, err error)

// Apply implements Applier.
func (f ApplierFunc[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	return f(ctx, records)
}

// SimpleApplier is a convenience wrapper for appliers that process
// records individually without partial failure handling.
type SimpleApplier[T any] struct {
	fn func(ctx context.Context, record T) error
}

// NewSimpleApplier creates an Applier from a function that processes
// single records. Records that return an error are marked as failed.
func NewSimpleApplier[T any](fn func(ctx context.Context, record T) error) *SimpleApplier[T] {
	return &SimpleApplier[T]{fn: fn}
}

// Apply implements Applier by processing records one at a time.
func (s *SimpleApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	var synced, failed []T
	for _, record := range records {
		if err := s.fn(ctx, record); err != nil {
			failed = append(failed, record)
		} else {
			synced = append(synced, record)
		}
	}
	return synced, failed, nil
}

// BatchApplier is a convenience wrapper for appliers that process
// entire batches atomically (all succeed or all fail).
type BatchApplier[T any] struct {
	fn func(ctx context.Context, records []T) error
}

// NewBatchApplier creates an Applier from a function that processes
// batches atomically. If the function returns an error, all records
// are marked as failed.
func NewBatchApplier[T any](fn func(ctx context.Context, records []T) error) *BatchApplier[T] {
	return &BatchApplier[T]{fn: fn}
}

// Apply implements Applier by processing the entire batch atomically.
func (b *BatchApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if err := b.fn(ctx, records); err != nil {
		return nil, records, nil
	}
	return records, nil, nil
}
