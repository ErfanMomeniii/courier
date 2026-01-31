package courier

import (
	"context"
	"sync"
	"time"
)

// RateLimiter provides rate limiting functionality.
type RateLimiter struct {
	rate     int           // operations per interval
	interval time.Duration // time interval
	tokens   int           // current available tokens
	mu       sync.Mutex
	lastTime time.Time
}

// NewRateLimiter creates a rate limiter that allows 'rate' operations per 'interval'.
// For example, NewRateLimiter(100, time.Second) allows 100 ops/sec.
func NewRateLimiter(rate int, interval time.Duration) *RateLimiter {
	if rate <= 0 {
		panic("courier: rate must be positive")
	}
	if interval <= 0 {
		panic("courier: interval must be positive")
	}
	return &RateLimiter{
		rate:     rate,
		interval: interval,
		tokens:   rate,
		lastTime: time.Now(),
	}
}

// Wait blocks until a token is available or context is cancelled.
func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		if r.TryAcquire() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval / time.Duration(r.rate)):
			// Check again after a fraction of the interval
		}
	}
}

// TryAcquire attempts to acquire a token without blocking.
// Returns true if a token was acquired.
func (r *RateLimiter) TryAcquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.refill()

	if r.tokens > 0 {
		r.tokens--
		return true
	}
	return false
}

// Acquire blocks until n tokens are available.
func (r *RateLimiter) Acquire(ctx context.Context, n int) error {
	for i := 0; i < n; i++ {
		if err := r.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *RateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(r.lastTime)

	// Calculate tokens to add based on elapsed time
	tokensToAdd := int(float64(r.rate) * (float64(elapsed) / float64(r.interval)))
	if tokensToAdd > 0 {
		r.tokens += tokensToAdd
		if r.tokens > r.rate {
			r.tokens = r.rate
		}
		r.lastTime = now
	}
}

// RateLimitedApplier wraps an applier with rate limiting.
type RateLimitedApplier[T any] struct {
	applier Applier[T]
	limiter *RateLimiter
	perBatch bool // if true, limit per batch; if false, limit per record
}

// NewRateLimitedApplier creates an applier that rate limits operations.
// If perBatch is true, each batch counts as one operation.
// If perBatch is false, each record counts as one operation.
func NewRateLimitedApplier[T any](applier Applier[T], limiter *RateLimiter, perBatch bool) *RateLimitedApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if limiter == nil {
		panic("courier: limiter cannot be nil")
	}
	return &RateLimitedApplier[T]{
		applier:  applier,
		limiter:  limiter,
		perBatch: perBatch,
	}
}

// Apply implements Applier with rate limiting.
func (r *RateLimitedApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if r.perBatch {
		if err := r.limiter.Wait(ctx); err != nil {
			return nil, records, err
		}
	} else {
		if err := r.limiter.Acquire(ctx, len(records)); err != nil {
			return nil, records, err
		}
	}
	return r.applier.Apply(ctx, records)
}

// RateLimitMiddleware creates a middleware that rate limits operations.
func RateLimitMiddleware[T any](limiter *RateLimiter, perBatch bool) Middleware[T] {
	if limiter == nil {
		panic("courier: limiter cannot be nil")
	}
	return func(applier Applier[T]) Applier[T] {
		return NewRateLimitedApplier(applier, limiter, perBatch)
	}
}
