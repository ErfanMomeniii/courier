package courier

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Integration tests that combine multiple components

func TestIntegration_PollingWithRetryAndDLQ(t *testing.T) {
	// Setup: source with records, some that always fail
	source := &testSource{
		records: []string{"ok1", "fail", "ok2", "fail", "ok3"},
	}

	// Applier that fails specific records
	failCount := make(map[string]int)
	var mu sync.Mutex
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		var synced, failed []string
		mu.Lock()
		defer mu.Unlock()
		for _, r := range records {
			if r == "fail" {
				failCount[r]++
				failed = append(failed, r)
			} else {
				synced = append(synced, r)
			}
		}
		return synced, failed, nil
	})

	// Build pipeline: Retry -> DLQ -> Applier
	dlq := NewInMemoryDLQ[string](100)
	retryPolicy := RetryPolicy{
		MaxAttempts:  2,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	applier := NewRetryableApplier(
		NewDLQApplier(baseApplier, dlq),
		retryPolicy,
	)

	// Create coordinator
	coord := NewPollingCoordinator(source, applier,
		WithInterval(50*time.Millisecond),
		WithBatchSize(10),
	)

	// Run once
	ctx := context.Background()
	coord.RunOnce(ctx)

	// Verify: 3 OK records synced, 2 fail records in DLQ
	if len(source.marked) != 3 {
		t.Errorf("marked = %d, want 3", len(source.marked))
	}

	dlqCount, _ := dlq.Count(ctx)
	if dlqCount != 2 {
		t.Errorf("DLQ count = %d, want 2", dlqCount)
	}
}

func TestIntegration_CircuitBreakerWithRetry(t *testing.T) {
	var applyCalls int32

	// Applier that always fails
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		atomic.AddInt32(&applyCalls, 1)
		return nil, records, nil
	})

	// Build pipeline: CircuitBreaker -> Retry -> Applier
	cbConfig := CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		Timeout:          1 * time.Hour, // Long timeout
	}

	retryPolicy := RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	applier := NewCircuitBreakerApplier(
		NewRetryableApplier(baseApplier, retryPolicy),
		cbConfig,
	)

	ctx := context.Background()
	records := []string{"a"}

	// First call: retries 3 times, then fails -> circuit counts 1 failure
	applier.Apply(ctx, records)
	calls1 := atomic.LoadInt32(&applyCalls)
	if calls1 != 3 {
		t.Errorf("First batch: applyCalls = %d, want 3", calls1)
	}

	// Second call: retries 3 times, then fails -> circuit opens
	applier.Apply(ctx, records)
	calls2 := atomic.LoadInt32(&applyCalls)
	if calls2 != 6 {
		t.Errorf("Second batch: applyCalls = %d, want 6", calls2)
	}

	// Third call: circuit is open, should reject immediately
	_, _, err := applier.Apply(ctx, records)
	calls3 := atomic.LoadInt32(&applyCalls)

	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Expected ErrCircuitOpen, got: %v", err)
	}
	if calls3 != 6 {
		t.Errorf("Third batch: applyCalls = %d, want 6 (no new calls)", calls3)
	}
}

func TestIntegration_MiddlewareChainWithHooks(t *testing.T) {
	var events []string
	var mu sync.Mutex

	addEvent := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}

	// Base applier
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		addEvent("apply")
		return records, nil, nil
	})

	// Middleware chain
	chained := Chain(
		LoggingMiddleware[string](func(format string, args ...any) {
			addEvent("log")
		}),
		TimingMiddleware[string](func(d time.Duration) {
			addEvent("timing")
		}),
		RecoveryMiddleware[string](func(r any) {
			addEvent("panic")
		}),
	)(baseApplier)

	// Hooks
	hooks := &Hooks[string]{
		BeforeApply: func(ctx context.Context, records []string) {
			addEvent("hook-before")
		},
		AfterApply: func(ctx context.Context, synced, failed []string, err error) {
			addEvent("hook-after")
		},
	}

	// Combine with hooks
	hookedApplier := NewHookedApplier(chained, hooks)

	hookedApplier.Apply(context.Background(), []string{"test"})

	mu.Lock()
	defer mu.Unlock()

	// Expected order: hook-before, log, timing, apply, timing-after (implicit), log-after (implicit), hook-after
	// The "timing" and "log" are recorded during the middleware execution
	if len(events) < 5 {
		t.Errorf("events = %v, expected at least 5 events", events)
	}

	// Verify key events occurred
	hasApply := false
	hasHookBefore := false
	hasHookAfter := false
	for _, e := range events {
		switch e {
		case "apply":
			hasApply = true
		case "hook-before":
			hasHookBefore = true
		case "hook-after":
			hasHookAfter = true
		}
	}

	if !hasApply || !hasHookBefore || !hasHookAfter {
		t.Errorf("Missing events: apply=%v, hook-before=%v, hook-after=%v",
			hasApply, hasHookBefore, hasHookAfter)
	}
}

func TestIntegration_FullPipelineWithPolling(t *testing.T) {
	// Full integration: Source -> Hooked -> CircuitBreaker -> Retry -> DLQ -> Filter -> Transform -> Applier
	var processed []string
	var mu sync.Mutex

	source := &testSource{
		records: []string{"a", "skip-b", "c", "fail-d", "e"},
	}

	// Base applier checks for "fail" substring (after transform adds "processed:" prefix)
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		mu.Lock()
		defer mu.Unlock()

		var synced, failed []string
		for _, r := range records {
			// Check if original record (before transform) was a fail record
			// After transform, "fail-d" becomes "processed:fail-d"
			// Use strings.Contains for safer check
			if len(r) > 10 && r[10] == 'f' { // "processed:" is 10 chars
				failed = append(failed, r)
			} else {
				processed = append(processed, r)
				synced = append(synced, r)
			}
		}
		return synced, failed, nil
	})

	dlq := NewInMemoryDLQ[string](100)

	// Build full pipeline
	applier := NewCircuitBreakerApplier(
		NewRetryableApplier(
			NewDLQApplier(
				Chain(
					FilterMiddleware[string](func(s string) bool {
						return len(s) < 5 || s[:5] != "skip-"
					}),
					TransformMiddleware[string](func(s string) string {
						return "processed:" + s
					}),
				)(baseApplier),
				dlq,
			),
			RetryPolicy{
				MaxAttempts:  2,
				InitialDelay: time.Millisecond,
				Multiplier:   1.0,
			},
		),
		CircuitBreakerConfig{
			FailureThreshold: 10,
			SuccessThreshold: 2,
			Timeout:          time.Hour,
		},
	)

	hooks := &Hooks[string]{
		BeforeFetch: func(ctx context.Context) {},
		AfterFetch:  func(ctx context.Context, records []string, err error) {},
	}

	hookedSource := NewHookedSource(source, hooks)

	coord := NewPollingCoordinator(hookedSource, applier,
		WithInterval(50*time.Millisecond),
		WithBatchSize(10),
	)

	// Run once
	coord.RunOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()

	// Expected: a, c, e processed (transformed)
	// skip-b filtered out
	// fail-d in DLQ
	if len(processed) != 3 {
		t.Errorf("processed = %d, want 3", len(processed))
	}

	for _, p := range processed {
		if len(p) < 10 || p[:10] != "processed:" {
			t.Errorf("record not transformed: %s", p)
		}
	}

	dlqCount, _ := dlq.Count(context.Background())
	if dlqCount != 1 {
		t.Errorf("DLQ count = %d, want 1", dlqCount)
	}

	// skip-b should count as synced (filtered)
	// So total synced = a + skip-b + c + e = 4
	if len(source.marked) != 4 {
		t.Errorf("marked = %d, want 4", len(source.marked))
	}
}

func TestIntegration_StreamingWithRetry(t *testing.T) {
	// Create a stream source
	streamSource := &testStreamSource{
		ch: make(chan string, 10),
	}

	// Add records
	go func() {
		streamSource.ch <- "a"
		streamSource.ch <- "b"
		streamSource.ch <- "fail"
		close(streamSource.ch)
	}()

	var applyCalls int32
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		atomic.AddInt32(&applyCalls, 1)
		var synced, failed []string
		for _, r := range records {
			if r == "fail" {
				failed = append(failed, r)
			} else {
				synced = append(synced, r)
			}
		}
		return synced, failed, nil
	})

	retryPolicy := RetryPolicy{
		MaxAttempts:  2,
		InitialDelay: time.Millisecond,
		Multiplier:   1.0,
	}

	applier := NewRetryableApplier(baseApplier, retryPolicy)

	coord := NewStreamingCoordinator(streamSource, applier,
		WithWorkers(1),
		WithBufferSize(10),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	coord.Start(ctx)

	// Verify acks and nacks
	if len(streamSource.acked) != 2 {
		t.Errorf("acked = %d, want 2", len(streamSource.acked))
	}
	if len(streamSource.nacked) != 1 {
		t.Errorf("nacked = %d, want 1", len(streamSource.nacked))
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	source := &testSource{
		records:    []string{"a", "b", "c"},
		fetchDelay: 100 * time.Millisecond,
	}

	var processed []string
	var mu sync.Mutex

	applier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		mu.Lock()
		processed = append(processed, records...)
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		return records, nil, nil
	})

	coord := NewPollingCoordinator(source, applier,
		WithInterval(50*time.Millisecond),
		WithBatchSize(10),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Start in background
	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Let it run for a bit
	time.Sleep(200 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for completion
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Coordinator did not stop within timeout")
	}

	// Should have processed some records
	mu.Lock()
	defer mu.Unlock()
	if len(processed) == 0 {
		t.Error("Expected some records to be processed")
	}
}

func TestIntegration_MetricsCollection(t *testing.T) {
	var fetchedCount, syncedCount, failedCount int32
	var pollDurations []time.Duration
	var mu sync.Mutex

	metrics := &MetricsFunc{
		OnRecordsFetched: func(count int) {
			atomic.AddInt32(&fetchedCount, int32(count))
		},
		OnRecordsApplied: func(synced, failed int) {
			atomic.AddInt32(&syncedCount, int32(synced))
			atomic.AddInt32(&failedCount, int32(failed))
		},
		OnPollDuration: func(d time.Duration) {
			mu.Lock()
			pollDurations = append(pollDurations, d)
			mu.Unlock()
		},
	}

	source := &testSource{
		records: []string{"a", "b", "c"},
	}

	applier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records[:2], records[2:], nil // 2 synced, 1 failed
	})

	coord := NewPollingCoordinator(source, applier,
		WithInterval(50*time.Millisecond),
		WithBatchSize(10),
		WithMetrics(metrics),
	)

	coord.RunOnce(context.Background())

	if atomic.LoadInt32(&fetchedCount) != 3 {
		t.Errorf("fetchedCount = %d, want 3", fetchedCount)
	}
	if atomic.LoadInt32(&syncedCount) != 2 {
		t.Errorf("syncedCount = %d, want 2", syncedCount)
	}
	if atomic.LoadInt32(&failedCount) != 1 {
		t.Errorf("failedCount = %d, want 1", failedCount)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(pollDurations) != 1 {
		t.Errorf("pollDurations = %d, want 1", len(pollDurations))
	}
}

// Test helpers

type testSource struct {
	records    []string
	marked     []string
	fetchDelay time.Duration
	mu         sync.Mutex
}

func (s *testSource) FetchRecords(ctx context.Context) ([]string, error) {
	if s.fetchDelay > 0 {
		time.Sleep(s.fetchDelay)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, len(s.records))
	copy(result, s.records)
	s.records = nil // Consume records
	return result, nil
}

func (s *testSource) MarkAsSynced(ctx context.Context, records []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, records...)
	return nil
}

type testStreamSource struct {
	ch     chan string
	acked  []string
	nacked []string
	mu     sync.Mutex
}

func (s *testStreamSource) Records() <-chan string {
	return s.ch
}

func (s *testStreamSource) Ack(ctx context.Context, record string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = append(s.acked, record)
	return nil
}

func (s *testStreamSource) Nack(ctx context.Context, record string, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nacked = append(s.nacked, record)
	return nil
}

func (s *testStreamSource) Close() error {
	return nil
}

// Test sentinel errors are properly defined and usable
func TestSentinelErrors(t *testing.T) {
	// Verify sentinel errors are defined
	if ErrCoordinatorStopped == nil {
		t.Error("ErrCoordinatorStopped should not be nil")
	}
	if ErrSourceClosed == nil {
		t.Error("ErrSourceClosed should not be nil")
	}
	if ErrNoRecords == nil {
		t.Error("ErrNoRecords should not be nil")
	}

	// Verify error messages
	if ErrCoordinatorStopped.Error() != "courier: coordinator stopped" {
		t.Errorf("ErrCoordinatorStopped message = %q", ErrCoordinatorStopped.Error())
	}
	if ErrSourceClosed.Error() != "courier: source closed" {
		t.Errorf("ErrSourceClosed message = %q", ErrSourceClosed.Error())
	}
	if ErrNoRecords.Error() != "courier: no records available" {
		t.Errorf("ErrNoRecords message = %q", ErrNoRecords.Error())
	}

	// Verify errors can be wrapped and checked with errors.Is
	wrappedErr := errors.Join(errors.New("outer error"), ErrCoordinatorStopped)
	if !errors.Is(wrappedErr, ErrCoordinatorStopped) {
		t.Error("errors.Is should find ErrCoordinatorStopped in wrapped error")
	}
}
