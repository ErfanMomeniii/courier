package courier

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	// CircuitClosed means the circuit is operating normally.
	CircuitClosed CircuitState = iota
	// CircuitOpen means the circuit has tripped and requests are blocked.
	CircuitOpen
	// CircuitHalfOpen means the circuit is testing if the service has recovered.
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// ErrCircuitOpen is returned when the circuit breaker is open.
	ErrCircuitOpen = errors.New("courier: circuit breaker is open")
)

// CircuitBreakerConfig configures the circuit breaker behavior.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening the circuit.
	FailureThreshold int

	// SuccessThreshold is the number of successes in half-open state
	// before closing the circuit.
	SuccessThreshold int

	// Timeout is how long to wait before transitioning from open to half-open.
	Timeout time.Duration

	// OnStateChange is called when the circuit state changes.
	OnStateChange func(from, to CircuitState)
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          30 * time.Second,
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	config CircuitBreakerConfig

	mu              sync.RWMutex
	state           CircuitState
	failures        int
	successes       int
	lastFailureTime time.Time
}

// NewCircuitBreaker creates a new circuit breaker.
// Panics if thresholds are <= 0 or timeout is < 0.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		panic("courier: failure threshold must be positive")
	}
	if config.SuccessThreshold <= 0 {
		panic("courier: success threshold must be positive")
	}
	if config.Timeout < 0 {
		panic("courier: timeout cannot be negative")
	}
	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Allow checks if a request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		// Check if timeout has elapsed
		if time.Since(cb.lastFailureTime) >= cb.config.Timeout {
			cb.transitionTo(CircuitHalfOpen)
			return true
		}
		return false

	case CircuitHalfOpen:
		// Allow limited requests in half-open state
		return true

	default:
		return false
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.transitionTo(CircuitClosed)
		}
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case CircuitClosed:
		cb.failures++
		if cb.failures >= cb.config.FailureThreshold {
			cb.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		// Any failure in half-open goes back to open
		cb.transitionTo(CircuitOpen)
	}
}

// Reset manually resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionTo(CircuitClosed)
}

func (cb *CircuitBreaker) transitionTo(state CircuitState) {
	if cb.state == state {
		return
	}

	oldState := cb.state
	cb.state = state
	cb.failures = 0
	cb.successes = 0

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(oldState, state)
	}
}

// CircuitBreakerApplier wraps an applier with circuit breaker protection.
type CircuitBreakerApplier[T any] struct {
	applier Applier[T]
	cb      *CircuitBreaker
}

// NewCircuitBreakerApplier creates an applier with circuit breaker protection.
// Panics if applier is nil.
func NewCircuitBreakerApplier[T any](applier Applier[T], config CircuitBreakerConfig) *CircuitBreakerApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	return &CircuitBreakerApplier[T]{
		applier: applier,
		cb:      NewCircuitBreaker(config),
	}
}

// Apply implements Applier with circuit breaker protection.
// A failure is recorded when:
//   - The applier returns a fatal error, OR
//   - More records failed than succeeded (len(failed) > len(synced))
//
// This heuristic treats partial failures as acceptable as long as
// the majority of records succeed.
func (c *CircuitBreakerApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if !c.cb.Allow() {
		return nil, records, ErrCircuitOpen
	}

	synced, failed, err := c.applier.Apply(ctx, records)

	// Record result: failure if fatal error OR majority of records failed
	if err != nil || len(failed) > len(synced) {
		c.cb.RecordFailure()
	} else {
		c.cb.RecordSuccess()
	}

	return synced, failed, err
}

// CircuitBreaker returns the underlying circuit breaker for inspection.
func (c *CircuitBreakerApplier[T]) CircuitBreaker() *CircuitBreaker {
	return c.cb
}
