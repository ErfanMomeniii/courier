package courier

import (
	"context"
	"time"
)

// Sync is the simplest way to use Courier for polling-based synchronization.
// It creates a coordinator with sensible defaults and production-ready resilience.
//
// Example:
//
//	coordinator := courier.Sync(
//	    fetchRecords,    // func(ctx) ([]T, error)
//	    processRecord,   // func(ctx, T) error
//	    markAsSynced,    // func(ctx, []T) error
//	)
//	coordinator.Start(ctx)
func Sync[T any](
	fetch func(ctx context.Context) ([]T, error),
	process func(ctx context.Context, record T) error,
	mark func(ctx context.Context, records []T) error,
	opts ...PollingOption,
) *PollingCoordinator[T] {
	source := BatchSourceFunc[T]{
		FetchFunc: fetch,
		MarkFunc:  mark,
	}
	applier := NewSimpleApplier(process)
	return NewPollingCoordinator(source, applier, opts...)
}

// SyncBatch is like Sync but processes records in batches instead of one at a time.
//
// Example:
//
//	coordinator := courier.SyncBatch(
//	    fetchRecords,     // func(ctx) ([]T, error)
//	    processBatch,     // func(ctx, []T) error
//	    markAsSynced,     // func(ctx, []T) error
//	)
func SyncBatch[T any](
	fetch func(ctx context.Context) ([]T, error),
	process func(ctx context.Context, records []T) error,
	mark func(ctx context.Context, records []T) error,
	opts ...PollingOption,
) *PollingCoordinator[T] {
	source := BatchSourceFunc[T]{
		FetchFunc: fetch,
		MarkFunc:  mark,
	}
	applier := NewBatchApplier(process)
	return NewPollingCoordinator(source, applier, opts...)
}

// SyncWithRetry is like Sync but adds automatic retry with exponential backoff.
//
// Example:
//
//	coordinator := courier.SyncWithRetry(
//	    fetchRecords,
//	    processRecord,
//	    markAsSynced,
//	    5, // max 5 attempts
//	)
func SyncWithRetry[T any](
	fetch func(ctx context.Context) ([]T, error),
	process func(ctx context.Context, record T) error,
	mark func(ctx context.Context, records []T) error,
	maxAttempts int,
	opts ...PollingOption,
) *PollingCoordinator[T] {
	source := BatchSourceFunc[T]{
		FetchFunc: fetch,
		MarkFunc:  mark,
	}
	applier := NewRetryableApplier(
		NewSimpleApplier(process),
		RetryPolicy{
			MaxAttempts:  maxAttempts,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.1,
		},
	)
	return NewPollingCoordinator(source, applier, opts...)
}

// ResilientApplier creates an applier with production-ready resilience:
// retry with exponential backoff, dead letter queue, and circuit breaker.
//
// Order: CircuitBreaker -> DLQ -> Retry -> YourApplier
// - Circuit breaker protects against cascading failures
// - DLQ captures records that fail after all retries
// - Retry handles transient failures with exponential backoff
//
// Example:
//
//	applier, dlq := courier.ResilientApplier(baseApplier, nil)
//	// dlq contains failed records for inspection
func ResilientApplier[T any](applier Applier[T], dlq DeadLetterQueue[T]) (Applier[T], DeadLetterQueue[T]) {
	if dlq == nil {
		dlq = NewInMemoryDLQ[T](10000)
	}

	// Order: CircuitBreaker -> DLQ -> Retry -> Applier
	// This way, retry happens first, and only permanently failed records go to DLQ
	resilient := NewCircuitBreakerApplier(
		NewDLQApplier(
			NewRetryableApplier(applier, DefaultRetryPolicy()),
			dlq,
		),
		DefaultCircuitBreakerConfig(),
	)

	return resilient, dlq
}

// ProcessFunc creates a simple applier from a function that processes one record.
// This is the easiest way to create an applier.
//
// Example:
//
//	applier := courier.ProcessFunc(func(ctx context.Context, event Event) error {
//	    return sendWebhook(event)
//	})
func ProcessFunc[T any](fn func(ctx context.Context, record T) error) Applier[T] {
	return NewSimpleApplier(fn)
}

// ProcessBatchFunc creates an applier from a function that processes a batch.
//
// Example:
//
//	applier := courier.ProcessBatchFunc(func(ctx context.Context, events []Event) error {
//	    return bulkInsert(events)
//	})
func ProcessBatchFunc[T any](fn func(ctx context.Context, records []T) error) Applier[T] {
	return NewBatchApplier(fn)
}

// WithRetry wraps an applier with retry logic using default settings.
// For custom retry settings, use NewRetryableApplier directly.
func WithRetry[T any](applier Applier[T]) *RetryableApplier[T] {
	return NewRetryableApplier(applier, DefaultRetryPolicy())
}

// WithDLQ wraps an applier with a dead letter queue.
// Returns both the wrapped applier and the DLQ for inspection.
func WithDLQ[T any](applier Applier[T]) (*DLQApplier[T], *InMemoryDLQ[T]) {
	dlq := NewInMemoryDLQ[T](10000)
	return NewDLQApplier(applier, dlq), dlq
}

// WithCircuitBreaker wraps an applier with circuit breaker using default settings.
func WithCircuitBreaker[T any](applier Applier[T]) *CircuitBreakerApplier[T] {
	return NewCircuitBreakerApplier(applier, DefaultCircuitBreakerConfig())
}

// Quick polling configuration presets

// WithFastPolling configures the coordinator for fast polling (100ms interval).
// Use for low-latency requirements.
func WithFastPolling() PollingOption {
	return WithInterval(100 * time.Millisecond)
}

// WithSlowPolling configures the coordinator for slow polling (30s interval).
// Use for infrequent sync or resource-constrained environments.
func WithSlowPolling() PollingOption {
	return WithInterval(30 * time.Second)
}

// RunOnce executes a single sync cycle without starting a coordinator.
// Useful for testing, cron jobs, or manual triggers.
//
// Example:
//
//	err := courier.RunOnce(ctx, fetchRecords, processRecord, markAsSynced)
func RunOnce[T any](
	ctx context.Context,
	fetch func(ctx context.Context) ([]T, error),
	process func(ctx context.Context, record T) error,
	mark func(ctx context.Context, records []T) error,
) error {
	records, err := fetch(ctx)
	if err != nil {
		return &FetchError{Err: err}
	}

	if len(records) == 0 {
		return nil
	}

	var synced, failed []T
	for _, r := range records {
		if err := process(ctx, r); err != nil {
			failed = append(failed, r)
		} else {
			synced = append(synced, r)
		}
	}

	if len(synced) > 0 {
		if err := mark(ctx, synced); err != nil {
			return &MarkError{Count: len(synced), Err: err}
		}
	}

	if len(failed) > 0 {
		return &ApplyError{Record: failed, Err: nil}
	}

	return nil
}

// MustSync is like Sync but panics if the coordinator fails to start.
// Use only in main() or init() where panicking is acceptable.
func MustSync[T any](
	ctx context.Context,
	fetch func(ctx context.Context) ([]T, error),
	process func(ctx context.Context, record T) error,
	mark func(ctx context.Context, records []T) error,
	opts ...PollingOption,
) {
	coord := Sync(fetch, process, mark, opts...)
	if err := coord.Start(ctx); err != nil && err != context.Canceled {
		panic("courier: " + err.Error())
	}
}
