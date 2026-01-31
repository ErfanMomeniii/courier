package courier

import (
	"context"
	"errors"
	"time"
)

// ErrTimeout is returned when an operation times out.
var ErrTimeout = errors.New("courier: operation timed out")

// TimeoutApplier wraps an applier with a timeout.
type TimeoutApplier[T any] struct {
	applier  Applier[T]
	timeout  time.Duration
	perBatch bool // if true, timeout applies to entire batch; if false, per record
}

// NewTimeoutApplier creates an applier with a timeout.
// If perBatch is true, the timeout applies to the entire batch.
// If perBatch is false, each record gets its own timeout (processed sequentially).
func NewTimeoutApplier[T any](applier Applier[T], timeout time.Duration, perBatch bool) *TimeoutApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if timeout <= 0 {
		panic("courier: timeout must be positive")
	}
	return &TimeoutApplier[T]{
		applier:  applier,
		timeout:  timeout,
		perBatch: perBatch,
	}
}

// Apply implements Applier with timeout.
func (t *TimeoutApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if t.perBatch {
		return t.applyBatch(ctx, records)
	}
	return t.applyPerRecord(ctx, records)
}

func (t *TimeoutApplier[T]) applyBatch(ctx context.Context, records []T) ([]T, []T, error) {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	synced, failed, err := t.applier.Apply(ctx, records)
	if errors.Is(err, context.DeadlineExceeded) {
		return synced, append(failed, records[len(synced):]...), ErrTimeout
	}
	return synced, failed, err
}

func (t *TimeoutApplier[T]) applyPerRecord(ctx context.Context, records []T) ([]T, []T, error) {
	var synced, failed []T

	for _, record := range records {
		recordCtx, cancel := context.WithTimeout(ctx, t.timeout)
		s, f, err := t.applier.Apply(recordCtx, []T{record})
		cancel()

		synced = append(synced, s...)
		failed = append(failed, f...)

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// Continue processing other records, mark this one as failed
				continue
			}
			// Fatal error - fail remaining records
			for _, remaining := range records[len(synced)+len(failed):] {
				failed = append(failed, remaining)
			}
			return synced, failed, err
		}
	}

	return synced, failed, nil
}

// TimeoutMiddleware creates a middleware that adds timeout to an applier.
func TimeoutMiddleware[T any](timeout time.Duration, perBatch bool) Middleware[T] {
	if timeout <= 0 {
		panic("courier: timeout must be positive")
	}
	return func(applier Applier[T]) Applier[T] {
		return NewTimeoutApplier(applier, timeout, perBatch)
	}
}

// ContextApplier allows adding values to the context before applying.
type ContextApplier[T any] struct {
	applier Applier[T]
	prepare func(ctx context.Context, records []T) context.Context
}

// NewContextApplier creates an applier that enriches the context.
// The prepare function can add values to the context before applying.
func NewContextApplier[T any](applier Applier[T], prepare func(ctx context.Context, records []T) context.Context) *ContextApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if prepare == nil {
		panic("courier: prepare cannot be nil")
	}
	return &ContextApplier[T]{
		applier: applier,
		prepare: prepare,
	}
}

// Apply implements Applier with context enrichment.
func (c *ContextApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	enrichedCtx := c.prepare(ctx, records)
	return c.applier.Apply(enrichedCtx, records)
}
