package courier_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erfanmomeniii/courier"
)

func TestSync(t *testing.T) {
	records := []string{"a", "b", "c"}
	var processedCount int32
	var markedCount int32

	coord := courier.Sync(
		func(ctx context.Context) ([]string, error) {
			if atomic.LoadInt32(&markedCount) > 0 {
				return nil, nil // Already processed
			}
			return records, nil
		},
		func(ctx context.Context, record string) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
		func(ctx context.Context, records []string) error {
			atomic.AddInt32(&markedCount, int32(len(records)))
			return nil
		},
		courier.WithInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	coord.Start(ctx)

	if atomic.LoadInt32(&processedCount) != 3 {
		t.Errorf("expected 3 processed, got %d", processedCount)
	}
	if atomic.LoadInt32(&markedCount) != 3 {
		t.Errorf("expected 3 marked, got %d", markedCount)
	}
}

func TestSyncBatch(t *testing.T) {
	records := []string{"a", "b", "c"}
	var batchCalls int32
	var markedCount int32

	coord := courier.SyncBatch(
		func(ctx context.Context) ([]string, error) {
			if atomic.LoadInt32(&markedCount) > 0 {
				return nil, nil
			}
			return records, nil
		},
		func(ctx context.Context, records []string) error {
			atomic.AddInt32(&batchCalls, 1)
			return nil
		},
		func(ctx context.Context, records []string) error {
			atomic.AddInt32(&markedCount, int32(len(records)))
			return nil
		},
		courier.WithInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	coord.Start(ctx)

	if atomic.LoadInt32(&batchCalls) != 1 {
		t.Errorf("expected 1 batch call, got %d", batchCalls)
	}
}

func TestSyncWithRetry(t *testing.T) {
	records := []string{"a"}
	var attempts int32

	coord := courier.SyncWithRetry(
		func(ctx context.Context) ([]string, error) {
			if atomic.LoadInt32(&attempts) >= 2 {
				return nil, nil
			}
			return records, nil
		},
		func(ctx context.Context, record string) error {
			count := atomic.AddInt32(&attempts, 1)
			if count < 2 {
				return errors.New("transient error")
			}
			return nil
		},
		func(ctx context.Context, records []string) error {
			return nil
		},
		3,
		courier.WithInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	coord.Start(ctx)

	if atomic.LoadInt32(&attempts) < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attempts)
	}
}

func TestProcessFunc(t *testing.T) {
	var called bool
	applier := courier.ProcessFunc(func(ctx context.Context, s string) error {
		called = true
		return nil
	})

	synced, failed, err := applier.Apply(context.Background(), []string{"test"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected function to be called")
	}
	if len(synced) != 1 {
		t.Errorf("expected 1 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestProcessBatchFunc(t *testing.T) {
	var receivedCount int
	applier := courier.ProcessBatchFunc(func(ctx context.Context, records []string) error {
		receivedCount = len(records)
		return nil
	})

	synced, failed, err := applier.Apply(context.Background(), []string{"a", "b", "c"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedCount != 3 {
		t.Errorf("expected 3 records, got %d", receivedCount)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestWithRetry(t *testing.T) {
	var attempts int
	applier := courier.ProcessFunc(func(ctx context.Context, s string) error {
		attempts++
		if attempts < 3 {
			return errors.New("fail")
		}
		return nil
	})

	retryable := courier.WithRetry(applier)
	synced, failed, err := retryable.Apply(context.Background(), []string{"test"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts)
	}
	if len(synced) != 1 {
		t.Errorf("expected 1 synced after retries, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestWithDLQ(t *testing.T) {
	applier := courier.ProcessFunc(func(ctx context.Context, s string) error {
		return errors.New("always fails")
	})

	dlqApplier, dlq := courier.WithDLQ(applier)
	synced, failed, err := dlqApplier.Apply(context.Background(), []string{"test"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(synced) != 0 {
		t.Errorf("expected 0 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed (sent to DLQ), got %d", len(failed))
	}

	count, _ := dlq.Count(context.Background())
	if count != 1 {
		t.Errorf("expected 1 in DLQ, got %d", count)
	}
}

func TestWithCircuitBreaker(t *testing.T) {
	applier := courier.ProcessFunc(func(ctx context.Context, s string) error {
		return nil
	})

	cbApplier := courier.WithCircuitBreaker(applier)
	synced, _, err := cbApplier.Apply(context.Background(), []string{"test"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("expected 1 synced, got %d", len(synced))
	}

	if cbApplier.CircuitBreaker().State() != courier.CircuitClosed {
		t.Error("expected circuit to be closed")
	}
}

func TestResilientApplier(t *testing.T) {
	var attempts int
	applier := courier.ProcessFunc(func(ctx context.Context, s string) error {
		attempts++
		if attempts < 2 {
			return errors.New("transient")
		}
		return nil
	})

	resilient, dlq := courier.ResilientApplier[string](applier, nil)
	synced, failed, err := resilient.Apply(context.Background(), []string{"test"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("expected 1 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}

	count, _ := dlq.Count(context.Background())
	if count != 0 {
		t.Errorf("expected 0 in DLQ, got %d", count)
	}
}

func TestRunOnce(t *testing.T) {
	records := []string{"a", "b", "c"}
	var processedCount int
	var markedRecords []string

	err := courier.RunOnce(
		context.Background(),
		func(ctx context.Context) ([]string, error) {
			return records, nil
		},
		func(ctx context.Context, record string) error {
			processedCount++
			return nil
		},
		func(ctx context.Context, records []string) error {
			markedRecords = records
			return nil
		},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processedCount != 3 {
		t.Errorf("expected 3 processed, got %d", processedCount)
	}
	if len(markedRecords) != 3 {
		t.Errorf("expected 3 marked, got %d", len(markedRecords))
	}
}

func TestRunOnce_FetchError(t *testing.T) {
	err := courier.RunOnce(
		context.Background(),
		func(ctx context.Context) ([]string, error) {
			return nil, errors.New("fetch failed")
		},
		func(ctx context.Context, record string) error {
			return nil
		},
		func(ctx context.Context, records []string) error {
			return nil
		},
	)

	var fetchErr *courier.FetchError
	if !errors.As(err, &fetchErr) {
		t.Errorf("expected FetchError, got %T", err)
	}
}

func TestRunOnce_NoRecords(t *testing.T) {
	var processCalled bool

	err := courier.RunOnce(
		context.Background(),
		func(ctx context.Context) ([]string, error) {
			return nil, nil
		},
		func(ctx context.Context, record string) error {
			processCalled = true
			return nil
		},
		func(ctx context.Context, records []string) error {
			return nil
		},
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processCalled {
		t.Error("process should not be called for empty records")
	}
}

func TestWithFastPolling(t *testing.T) {
	// Just verify it doesn't panic
	opt := courier.WithFastPolling()
	if opt == nil {
		t.Error("expected non-nil option")
	}
}

func TestWithSlowPolling(t *testing.T) {
	// Just verify it doesn't panic
	opt := courier.WithSlowPolling()
	if opt == nil {
		t.Error("expected non-nil option")
	}
}
