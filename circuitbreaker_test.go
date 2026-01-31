package courier

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	if cb.State() != CircuitClosed {
		t.Errorf("Initial state = %v, want CircuitClosed", cb.State())
	}
	if !cb.Allow() {
		t.Error("CircuitClosed should allow requests")
	}
}

func TestCircuitBreaker_TransitionToOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Record failures to trigger open
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != CircuitOpen {
		t.Errorf("State = %v, want CircuitOpen after %d failures", cb.State(), 3)
	}
	if cb.Allow() {
		t.Error("CircuitOpen should not allow requests")
	}
}

func TestCircuitBreaker_TransitionToHalfOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("Expected CircuitOpen, got %v", cb.State())
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Allow() should transition to half-open
	if !cb.Allow() {
		t.Error("Should allow request after timeout (transition to half-open)")
	}
	if cb.State() != CircuitHalfOpen {
		t.Errorf("State = %v, want CircuitHalfOpen", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToClosed(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and trigger half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()

	if cb.State() != CircuitHalfOpen {
		t.Fatalf("Expected CircuitHalfOpen, got %v", cb.State())
	}

	// Record successes to close
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != CircuitClosed {
		t.Errorf("State = %v, want CircuitClosed after successes", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          50 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and trigger half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow()

	if cb.State() != CircuitHalfOpen {
		t.Fatalf("Expected CircuitHalfOpen, got %v", cb.State())
	}

	// Any failure in half-open goes back to open
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("State = %v, want CircuitOpen after failure in half-open", cb.State())
	}
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Record some failures (but not enough to open)
	cb.RecordFailure()
	cb.RecordFailure()

	// Success should reset failure count
	cb.RecordSuccess()

	// Now need 3 more failures to open
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitClosed {
		t.Error("Circuit should still be closed (failure count was reset)")
	}

	// One more failure opens it
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Error("Circuit should be open after 3 consecutive failures")
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitOpen {
		t.Fatalf("Expected CircuitOpen, got %v", cb.State())
	}

	// Manual reset
	cb.Reset()

	if cb.State() != CircuitClosed {
		t.Errorf("State after Reset = %v, want CircuitClosed", cb.State())
	}
	if !cb.Allow() {
		t.Error("Should allow requests after Reset")
	}
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	var transitions []struct{ from, to CircuitState }
	var mu sync.Mutex

	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		Timeout:          50 * time.Millisecond,
		OnStateChange: func(from, to CircuitState) {
			mu.Lock()
			transitions = append(transitions, struct{ from, to CircuitState }{from, to})
			mu.Unlock()
		},
	}
	cb := NewCircuitBreaker(config)

	// Closed -> Open
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout, then trigger half-open
	time.Sleep(60 * time.Millisecond)
	cb.Allow() // Open -> HalfOpen

	// Success closes it
	cb.RecordSuccess() // HalfOpen -> Closed

	mu.Lock()
	defer mu.Unlock()

	if len(transitions) != 3 {
		t.Fatalf("Expected 3 transitions, got %d", len(transitions))
	}

	expected := []struct{ from, to CircuitState }{
		{CircuitClosed, CircuitOpen},
		{CircuitOpen, CircuitHalfOpen},
		{CircuitHalfOpen, CircuitClosed},
	}

	for i, exp := range expected {
		if transitions[i].from != exp.from || transitions[i].to != exp.to {
			t.Errorf("Transition %d: got %v->%v, want %v->%v",
				i, transitions[i].from, transitions[i].to, exp.from, exp.to)
		}
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("CircuitState(%d).String() = %s, want %s", tt.state, got, tt.want)
		}
	}
}

func TestCircuitBreaker_Concurrent(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 50,
		Timeout:          100 * time.Millisecond,
	}
	cb := NewCircuitBreaker(config)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cb.Allow()
				if j%2 == 0 {
					cb.RecordSuccess()
				} else {
					cb.RecordFailure()
				}
			}
		}()
	}
	wg.Wait()

	// Should not panic and state should be valid
	state := cb.State()
	if state != CircuitClosed && state != CircuitOpen && state != CircuitHalfOpen {
		t.Errorf("Invalid state after concurrent access: %v", state)
	}
}

func TestDefaultCircuitBreakerConfig(t *testing.T) {
	config := DefaultCircuitBreakerConfig()

	if config.FailureThreshold != 5 {
		t.Errorf("FailureThreshold = %d, want 5", config.FailureThreshold)
	}
	if config.SuccessThreshold != 2 {
		t.Errorf("SuccessThreshold = %d, want 2", config.SuccessThreshold)
	}
	if config.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want 30s", config.Timeout)
	}
}

func TestCircuitBreakerApplier_AllowsWhenClosed(t *testing.T) {
	records := []string{"a", "b", "c"}

	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return recs, nil, nil
	})

	cbApplier := NewCircuitBreakerApplier(baseApplier, DefaultCircuitBreakerConfig())

	synced, failed, err := cbApplier.Apply(context.Background(), records)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("synced = %d, want 3", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
}

func TestCircuitBreakerApplier_RejectsWhenOpen(t *testing.T) {
	records := []string{"a", "b", "c"}

	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return nil, recs, nil // All fail
	})

	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          1 * time.Hour, // Long timeout so it stays open
	}
	cbApplier := NewCircuitBreakerApplier(baseApplier, config)

	// First two calls open the circuit
	cbApplier.Apply(context.Background(), records)
	cbApplier.Apply(context.Background(), records)

	// Third call should be rejected
	synced, failed, err := cbApplier.Apply(context.Background(), records)

	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got: %v", err)
	}
	if len(synced) != 0 {
		t.Errorf("synced = %d, want 0", len(synced))
	}
	if len(failed) != 3 {
		t.Errorf("failed = %d, want 3 (all rejected)", len(failed))
	}
}

func TestCircuitBreakerApplier_RecordsSuccess(t *testing.T) {
	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return recs, nil, nil // All succeed
	})

	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		Timeout:          100 * time.Millisecond,
	}
	cbApplier := NewCircuitBreakerApplier(baseApplier, config)

	// Record some failures first
	cb := cbApplier.CircuitBreaker()
	cb.RecordFailure()
	cb.RecordFailure()

	// Successful apply should reset failures
	cbApplier.Apply(context.Background(), []string{"a"})

	// Need 3 more failures now
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != CircuitClosed {
		t.Error("Circuit should still be closed (failures were reset)")
	}
}

func TestCircuitBreakerApplier_RecordsFailure(t *testing.T) {
	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		// More failures than successes
		return []string{"a"}, []string{"b", "c", "d"}, nil
	})

	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          1 * time.Hour,
	}
	cbApplier := NewCircuitBreakerApplier(baseApplier, config)

	// Two applies with more failures than successes should open circuit
	cbApplier.Apply(context.Background(), []string{"a", "b", "c", "d"})
	cbApplier.Apply(context.Background(), []string{"a", "b", "c", "d"})

	if cbApplier.CircuitBreaker().State() != CircuitOpen {
		t.Error("Circuit should be open after repeated failures")
	}
}

func TestCircuitBreakerApplier_RecordsFatalError(t *testing.T) {
	fatalErr := errors.New("fatal error")
	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return nil, nil, fatalErr
	})

	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          1 * time.Hour,
	}
	cbApplier := NewCircuitBreakerApplier(baseApplier, config)

	// Fatal errors should count as failures
	cbApplier.Apply(context.Background(), []string{"a"})
	cbApplier.Apply(context.Background(), []string{"a"})

	if cbApplier.CircuitBreaker().State() != CircuitOpen {
		t.Error("Circuit should be open after fatal errors")
	}
}

func TestCircuitBreakerApplier_CircuitBreaker(t *testing.T) {
	baseApplier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return recs, nil, nil
	})

	cbApplier := NewCircuitBreakerApplier(baseApplier, DefaultCircuitBreakerConfig())
	cb := cbApplier.CircuitBreaker()

	if cb == nil {
		t.Fatal("CircuitBreaker() should not return nil")
	}
	if cb.State() != CircuitClosed {
		t.Error("Expected initial state to be CircuitClosed")
	}
}

func TestErrCircuitOpen(t *testing.T) {
	if ErrCircuitOpen.Error() != "courier: circuit breaker is open" {
		t.Errorf("ErrCircuitOpen message = %s, unexpected", ErrCircuitOpen.Error())
	}
}
