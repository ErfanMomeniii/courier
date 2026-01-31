package courier

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryPolicy_Delay(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0, // No jitter for predictable tests
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 0},                      // First attempt, no delay
		{1, 100 * time.Millisecond}, // 100ms
		{2, 200 * time.Millisecond}, // 100 * 2
		{3, 400 * time.Millisecond}, // 100 * 2^2
		{4, 800 * time.Millisecond}, // 100 * 2^3
		{5, 1 * time.Second},        // Capped at MaxDelay
		{6, 1 * time.Second},        // Still capped
	}

	for _, tt := range tests {
		got := policy.Delay(tt.attempt)
		if got != tt.want {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestRetryPolicy_DelayWithJitter(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.5, // 50% jitter
	}

	// Run multiple times to verify jitter adds randomness
	delays := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		delay := policy.Delay(1)
		delays[delay] = true
	}

	// With 50% jitter, we should see variation
	if len(delays) < 5 {
		t.Errorf("Expected multiple different delays with jitter, got %d unique values", len(delays))
	}

	// All delays should be within expected range (50ms to 150ms for attempt 1)
	for delay := range delays {
		if delay < 50*time.Millisecond || delay > 150*time.Millisecond {
			t.Errorf("Delay %v outside expected jitter range [50ms, 150ms]", delay)
		}
	}
}

func TestRetryPolicy_ShouldRetry(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
		attempt     int
		want        bool
	}{
		{"within limit", 5, 3, true},
		{"at limit", 5, 5, false},
		{"over limit", 5, 6, false},
		{"unlimited zero attempt", 0, 0, true},
		{"unlimited many attempts", 0, 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := RetryPolicy{MaxAttempts: tt.maxAttempts}
			got := policy.ShouldRetry(tt.attempt)
			if got != tt.want {
				t.Errorf("ShouldRetry(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()

	if policy.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", policy.MaxAttempts)
	}
	if policy.InitialDelay != 100*time.Millisecond {
		t.Errorf("InitialDelay = %v, want 100ms", policy.InitialDelay)
	}
	if policy.MaxDelay != 30*time.Second {
		t.Errorf("MaxDelay = %v, want 30s", policy.MaxDelay)
	}
	if policy.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", policy.Multiplier)
	}
	if policy.Jitter != 0.1 {
		t.Errorf("Jitter = %v, want 0.1", policy.Jitter)
	}
}

func TestRetryableApplier_SuccessOnFirstAttempt(t *testing.T) {
	records := []string{"a", "b", "c"}
	var applyCalls int

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		applyCalls++
		return recs, nil, nil
	})

	policy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   2.0,
	}

	retryable := NewRetryableApplier(applier, policy)
	synced, failed, err := retryable.Apply(context.Background(), records)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("synced = %d, want 3", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
	if applyCalls != 1 {
		t.Errorf("applyCalls = %d, want 1", applyCalls)
	}
}

func TestRetryableApplier_RetryOnFailure(t *testing.T) {
	records := []string{"a", "b", "c"}
	var applyCalls int

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		applyCalls++
		if applyCalls < 3 {
			// Fail first two attempts
			return nil, recs, nil
		}
		return recs, nil, nil
	})

	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0, // No increase for fast tests
	}

	retryable := NewRetryableApplier(applier, policy)
	synced, failed, err := retryable.Apply(context.Background(), records)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("synced = %d, want 3", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
	if applyCalls != 3 {
		t.Errorf("applyCalls = %d, want 3", applyCalls)
	}
}

func TestRetryableApplier_ExhaustsRetries(t *testing.T) {
	records := []string{"a", "b"}
	var applyCalls int

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		applyCalls++
		return nil, recs, nil // Always fail
	})

	policy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	retryable := NewRetryableApplier(applier, policy)
	synced, failed, err := retryable.Apply(context.Background(), records)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 0 {
		t.Errorf("synced = %d, want 0", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("failed = %d, want 2", len(failed))
	}
	if applyCalls != 3 {
		t.Errorf("applyCalls = %d, want 3", applyCalls)
	}
}

func TestRetryableApplier_FatalErrorStopsRetry(t *testing.T) {
	records := []string{"a", "b"}
	var applyCalls int
	fatalErr := errors.New("fatal error")

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		applyCalls++
		return nil, nil, fatalErr
	})

	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	retryable := NewRetryableApplier(applier, policy)
	synced, failed, err := retryable.Apply(context.Background(), records)

	if !errors.Is(err, fatalErr) {
		t.Errorf("expected fatal error, got: %v", err)
	}
	if len(synced) != 0 {
		t.Errorf("synced = %d, want 0", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("failed = %d, want 2 (records marked as failed)", len(failed))
	}
	if applyCalls != 1 {
		t.Errorf("applyCalls = %d, want 1 (no retry on fatal error)", applyCalls)
	}
}

func TestRetryableApplier_PartialRetry(t *testing.T) {
	records := []string{"a", "b", "c"}
	var applyCalls int

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		applyCalls++
		// First call: "a" succeeds, "b" and "c" fail
		// Second call: "b" succeeds, "c" fails
		// Third call: "c" succeeds
		var synced, failed []string
		for _, r := range recs {
			switch r {
			case "a":
				synced = append(synced, r)
			case "b":
				if applyCalls >= 2 {
					synced = append(synced, r)
				} else {
					failed = append(failed, r)
				}
			case "c":
				if applyCalls >= 3 {
					synced = append(synced, r)
				} else {
					failed = append(failed, r)
				}
			}
		}
		return synced, failed, nil
	})

	policy := RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	retryable := NewRetryableApplier(applier, policy)
	synced, failed, err := retryable.Apply(context.Background(), records)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("synced = %d, want 3", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
	if applyCalls != 3 {
		t.Errorf("applyCalls = %d, want 3", applyCalls)
	}
}

func TestRetryableApplier_OnRetryCallback(t *testing.T) {
	records := []string{"a"}
	var retryCallbacks int

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		return nil, recs, nil // Always fail
	})

	policy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	retryable := NewRetryableApplier(applier, policy).
		OnRetry(func(record string, attempt int, err error) {
			retryCallbacks++
		})

	retryable.Apply(context.Background(), records)

	// OnRetry is called before attempts 2 and 3 (not before attempt 1)
	if retryCallbacks != 2 {
		t.Errorf("retryCallbacks = %d, want 2", retryCallbacks)
	}
}

func TestRetryableApplier_ContextCancellation(t *testing.T) {
	records := []string{"a"}
	var applyCalls int32

	applier := ApplierFunc[string](func(ctx context.Context, recs []string) ([]string, []string, error) {
		atomic.AddInt32(&applyCalls, 1)
		return nil, recs, nil // Always fail
	})

	policy := RetryPolicy{
		MaxAttempts:  10,
		InitialDelay: 50 * time.Millisecond,
		Multiplier:   1.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	retryable := NewRetryableApplier(applier, policy)

	// Cancel after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, _, err := retryable.Apply(ctx, records)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	calls := atomic.LoadInt32(&applyCalls)
	if calls >= 10 {
		t.Errorf("applyCalls = %d, expected less than 10 due to cancellation", calls)
	}
}

func TestRetryableFunc_Success(t *testing.T) {
	var calls int
	policy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	err := RetryableFunc(context.Background(), policy, func() error {
		calls++
		if calls < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}

func TestRetryableFunc_ExhaustsRetries(t *testing.T) {
	var calls int
	policy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}
	expectedErr := errors.New("persistent error")

	err := RetryableFunc(context.Background(), policy, func() error {
		calls++
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got: %v", expectedErr, err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryableFunc_ContextCancellation(t *testing.T) {
	var calls int32
	policy := RetryPolicy{
		MaxAttempts:  10,
		InitialDelay: 50 * time.Millisecond,
		Multiplier:   1.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := RetryableFunc(ctx, policy, func() error {
		atomic.AddInt32(&calls, 1)
		return errors.New("error")
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	finalCalls := atomic.LoadInt32(&calls)
	if finalCalls >= 10 {
		t.Errorf("calls = %d, expected less than 10 due to cancellation", finalCalls)
	}
}
