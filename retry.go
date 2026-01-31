package courier

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines how retries should be handled.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of attempts (including the first).
	// Set to 0 for unlimited retries.
	MaxAttempts int

	// InitialDelay is the delay before the first retry.
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration

	// Multiplier is the factor by which the delay increases after each retry.
	Multiplier float64

	// Jitter adds randomness to delays to prevent thundering herd.
	// Value between 0 and 1 (e.g., 0.1 = 10% jitter).
	Jitter float64
}

// DefaultRetryPolicy returns a sensible default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
}

// Delay calculates the delay for the given attempt number (0-indexed).
func (p RetryPolicy) Delay(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}

	delay := float64(p.InitialDelay) * math.Pow(p.Multiplier, float64(attempt-1))

	if p.MaxDelay > 0 && time.Duration(delay) > p.MaxDelay {
		delay = float64(p.MaxDelay)
	}

	if p.Jitter > 0 {
		jitter := delay * p.Jitter * (rand.Float64()*2 - 1) // -jitter to +jitter
		delay += jitter
	}

	return time.Duration(delay)
}

// ShouldRetry returns true if another attempt should be made.
func (p RetryPolicy) ShouldRetry(attempt int) bool {
	if p.MaxAttempts == 0 {
		return true // unlimited
	}
	return attempt < p.MaxAttempts
}

// RetryableApplier wraps an Applier with retry logic.
type RetryableApplier[T any] struct {
	applier Applier[T]
	policy  RetryPolicy
	onRetry func(record T, attempt int, err error)
}

// NewRetryableApplier creates an applier that retries failed records.
// Panics if applier is nil.
func NewRetryableApplier[T any](applier Applier[T], policy RetryPolicy) *RetryableApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	return &RetryableApplier[T]{
		applier: applier,
		policy:  policy,
	}
}

// OnRetry sets a callback that's invoked before each retry.
func (r *RetryableApplier[T]) OnRetry(fn func(record T, attempt int, err error)) *RetryableApplier[T] {
	r.onRetry = fn
	return r
}

// Apply implements Applier with retry logic for failed records.
func (r *RetryableApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	var allSynced, allFailed []T
	pending := records

	for attempt := 0; len(pending) > 0 && r.policy.ShouldRetry(attempt); attempt++ {
		// Wait before retry (skip first attempt)
		if attempt > 0 {
			delay := r.policy.Delay(attempt)
			select {
			case <-ctx.Done():
				return allSynced, append(allFailed, pending...), ctx.Err()
			case <-time.After(delay):
			}

			if r.onRetry != nil {
				for _, rec := range pending {
					r.onRetry(rec, attempt, nil)
				}
			}
		}

		synced, failed, err := r.applier.Apply(ctx, pending)
		if err != nil {
			// Fatal error - don't retry
			return allSynced, append(allFailed, pending...), err
		}

		allSynced = append(allSynced, synced...)
		pending = failed
	}

	// Any remaining pending records are permanently failed
	allFailed = append(allFailed, pending...)

	return allSynced, allFailed, nil
}

// RetryableFunc is a helper to retry a single function with exponential backoff.
func RetryableFunc(ctx context.Context, policy RetryPolicy, fn func() error) error {
	var lastErr error

	for attempt := 0; policy.ShouldRetry(attempt); attempt++ {
		if attempt > 0 {
			delay := policy.Delay(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := fn(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return lastErr
}
