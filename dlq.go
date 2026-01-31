package courier

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrRecordNotFound is returned when a record cannot be found in the DLQ.
var ErrRecordNotFound = errors.New("courier: record not found in DLQ")

// FailedRecord wraps a record with failure metadata.
type FailedRecord[T any] struct {
	// ID is a unique identifier for the failed record (optional).
	ID string

	// Record is the original record that failed.
	Record T

	// Error is the last error that occurred.
	Error error

	// Attempts is the number of processing attempts.
	Attempts int

	// FirstFailure is when the record first failed.
	FirstFailure time.Time

	// LastFailure is when the record last failed.
	LastFailure time.Time

	// Metadata allows storing additional context.
	Metadata map[string]any
}

// DeadLetterQueue defines the interface for handling permanently failed records.
type DeadLetterQueue[T any] interface {
	// Send adds a failed record to the dead letter queue.
	Send(ctx context.Context, record FailedRecord[T]) error

	// Receive retrieves records from the dead letter queue for reprocessing.
	// Returns up to limit records. Use limit=0 for all available.
	Receive(ctx context.Context, limit int) ([]FailedRecord[T], error)

	// Remove deletes a record from the dead letter queue (after successful reprocessing).
	// For InMemoryDLQ, removal is done by index (FIFO order).
	Remove(ctx context.Context, record FailedRecord[T]) error

	// Count returns the number of records in the queue.
	Count(ctx context.Context) (int, error)
}

// InMemoryDLQ is a simple in-memory dead letter queue for testing and development.
// Note: This implementation is not suitable for production use as it doesn't
// persist data and has limited matching capabilities for Remove().
type InMemoryDLQ[T any] struct {
	mu      sync.RWMutex
	records []FailedRecord[T]
	maxSize int
}

// NewInMemoryDLQ creates an in-memory dead letter queue.
// Set maxSize to 0 for unlimited size.
// Warning: Records are lost on restart. Use a persistent implementation for production.
func NewInMemoryDLQ[T any](maxSize int) *InMemoryDLQ[T] {
	return &InMemoryDLQ[T]{
		maxSize: maxSize,
		records: make([]FailedRecord[T], 0),
	}
}

// Send adds a record to the queue.
// If the queue is at max capacity, the oldest record is removed.
func (q *InMemoryDLQ[T]) Send(ctx context.Context, record FailedRecord[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// If at max size, remove oldest
	if q.maxSize > 0 && len(q.records) >= q.maxSize {
		q.records = q.records[1:]
	}

	q.records = append(q.records, record)
	return nil
}

// Receive retrieves records from the queue without removing them.
// Returns up to limit records in FIFO order. Use limit=0 for all records.
func (q *InMemoryDLQ[T]) Receive(ctx context.Context, limit int) ([]FailedRecord[T], error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if limit <= 0 || limit > len(q.records) {
		limit = len(q.records)
	}

	result := make([]FailedRecord[T], limit)
	copy(result, q.records[:limit])
	return result, nil
}

// Remove deletes a record from the queue by matching the ID field.
// If ID is empty, removes the first record (FIFO).
// Returns ErrRecordNotFound if no matching record exists.
func (q *InMemoryDLQ[T]) Remove(ctx context.Context, record FailedRecord[T]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.records) == 0 {
		return ErrRecordNotFound
	}

	// If ID is provided, match by ID
	if record.ID != "" {
		for i, r := range q.records {
			if r.ID == record.ID {
				q.records = append(q.records[:i], q.records[i+1:]...)
				return nil
			}
		}
		return ErrRecordNotFound
	}

	// No ID provided - remove first record (FIFO behavior)
	q.records = q.records[1:]
	return nil
}

// RemoveFirst removes and returns the first record from the queue.
// Returns ErrRecordNotFound if the queue is empty.
func (q *InMemoryDLQ[T]) RemoveFirst(ctx context.Context) (FailedRecord[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero FailedRecord[T]
	if len(q.records) == 0 {
		return zero, ErrRecordNotFound
	}

	record := q.records[0]
	q.records = q.records[1:]
	return record, nil
}

// Count returns the queue size.
func (q *InMemoryDLQ[T]) Count(ctx context.Context) (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.records), nil
}

// All returns all records (for inspection/debugging).
// Returns a copy of the internal slice.
func (q *InMemoryDLQ[T]) All() []FailedRecord[T] {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]FailedRecord[T], len(q.records))
	copy(result, q.records)
	return result
}

// Clear removes all records from the queue.
func (q *InMemoryDLQ[T]) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.records = q.records[:0]
}

// DLQApplier wraps an applier to send failed records to a dead letter queue.
// Records that fail to apply are sent to the DLQ and not returned in the failed list.
type DLQApplier[T any] struct {
	applier Applier[T]
	dlq     DeadLetterQueue[T]
	getId   func(T) string // optional identifier function for tracking
}

// NewDLQApplier creates an applier that sends failed records to a DLQ.
// Failed records are moved to the DLQ and not returned in the failed list,
// allowing the coordinator to mark them as "handled" (via DLQ).
func NewDLQApplier[T any](applier Applier[T], dlq DeadLetterQueue[T]) *DLQApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if dlq == nil {
		panic("courier: dlq cannot be nil")
	}
	return &DLQApplier[T]{
		applier: applier,
		dlq:     dlq,
	}
}

// WithIdentifier sets a function to extract record IDs for better tracking.
// The ID is stored in FailedRecord.ID and can be used for deduplication
// and targeted removal from the DLQ.
func (d *DLQApplier[T]) WithIdentifier(fn func(T) string) *DLQApplier[T] {
	d.getId = fn
	return d
}

// Apply processes records and sends failures to the DLQ.
// Returns:
//   - synced: records successfully processed by the underlying applier
//   - failed: records that failed AND could not be sent to the DLQ (should be rare)
//   - err: fatal error from the underlying applier
//
// Records successfully sent to the DLQ are NOT included in the failed list,
// as they have been "handled" by moving them to the DLQ for later inspection.
func (d *DLQApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	synced, failed, err := d.applier.Apply(ctx, records)

	if len(failed) == 0 {
		return synced, failed, err
	}

	// Send failed records to DLQ
	now := time.Now()
	var dlqFailed []T // Records that failed to be sent to DLQ

	for _, rec := range failed {
		failedRec := FailedRecord[T]{
			Record:       rec,
			Attempts:     1,
			FirstFailure: now,
			LastFailure:  now,
		}

		// Set ID if identifier function is provided
		if d.getId != nil {
			failedRec.ID = d.getId(rec)
		}

		if sendErr := d.dlq.Send(ctx, failedRec); sendErr != nil {
			// Failed to send to DLQ - include in failed list so caller knows
			dlqFailed = append(dlqFailed, rec)
		}
	}

	// Return only records that couldn't be sent to DLQ
	// Records in DLQ are considered "handled"
	return synced, dlqFailed, err
}

// DLQ returns the underlying dead letter queue for inspection or manual processing.
func (d *DLQApplier[T]) DLQ() DeadLetterQueue[T] {
	return d.dlq
}
