package courier_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/erfanmomeniii/courier"
)

func TestBatchSplitter_NormalBatch(t *testing.T) {
	var callCount int32

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		atomic.AddInt32(&callCount, 1)
		return records, nil, nil
	})

	splitter := courier.NewBatchSplitter(applier, 10)

	synced, failed, err := splitter.Apply(context.Background(), []int{1, 2, 3, 4, 5})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 5 {
		t.Errorf("expected 5 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("expected 1 call for small batch, got %d", callCount)
	}
}

func TestBatchSplitter_LargeBatch(t *testing.T) {
	var callCount int32
	var totalRecords int32

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		atomic.AddInt32(&callCount, 1)
		atomic.AddInt32(&totalRecords, int32(len(records)))
		return records, nil, nil
	})

	splitter := courier.NewBatchSplitter(applier, 3)

	// 10 records with max batch of 3 = 4 calls (3+3+3+1)
	records := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	synced, failed, err := splitter.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 10 {
		t.Errorf("expected 10 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
	if atomic.LoadInt32(&callCount) != 4 {
		t.Errorf("expected 4 calls, got %d", callCount)
	}
	if atomic.LoadInt32(&totalRecords) != 10 {
		t.Errorf("expected 10 total records, got %d", totalRecords)
	}
}

func TestBatchSplitter_FatalError(t *testing.T) {
	callCount := 0

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		callCount++
		if callCount == 2 {
			return nil, records, errors.New("fatal error")
		}
		return records, nil, nil
	})

	splitter := courier.NewBatchSplitter(applier, 3)

	records := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	synced, failed, err := splitter.Apply(context.Background(), records)

	if err == nil {
		t.Error("expected error")
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced (first batch), got %d", len(synced))
	}
	if len(failed) != 6 {
		t.Errorf("expected 6 failed (remaining), got %d", len(failed))
	}
}

func TestParallelApplier(t *testing.T) {
	var processed int32

	applier := courier.NewParallelApplier(func(ctx context.Context, record int) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}, 4)

	records := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	synced, failed, err := applier.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 10 {
		t.Errorf("expected 10 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
	if atomic.LoadInt32(&processed) != 10 {
		t.Errorf("expected 10 processed, got %d", processed)
	}
}

func TestParallelApplier_PartialFailure(t *testing.T) {
	applier := courier.NewParallelApplier(func(ctx context.Context, record int) error {
		if record%2 == 0 {
			return errors.New("even numbers fail")
		}
		return nil
	}, 4)

	records := []int{1, 2, 3, 4, 5}
	synced, failed, err := applier.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced (odd numbers), got %d", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("expected 2 failed (even numbers), got %d", len(failed))
	}
}

func TestRouterApplier(t *testing.T) {
	type Event struct {
		Type string
		Data string
	}

	routeACalls := 0
	routeBCalls := 0

	routeA := courier.ApplierFunc[Event](func(ctx context.Context, records []Event) ([]Event, []Event, error) {
		routeACalls += len(records)
		return records, nil, nil
	})

	routeB := courier.ApplierFunc[Event](func(ctx context.Context, records []Event) ([]Event, []Event, error) {
		routeBCalls += len(records)
		return records, nil, nil
	})

	router := courier.NewRouterApplier(
		func(e Event) string { return e.Type },
		map[string]courier.Applier[Event]{
			"typeA": routeA,
			"typeB": routeB,
		},
		nil,
	)

	records := []Event{
		{Type: "typeA", Data: "1"},
		{Type: "typeB", Data: "2"},
		{Type: "typeA", Data: "3"},
		{Type: "unknown", Data: "4"}, // Will be skipped
	}

	synced, failed, err := router.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 4 { // All synced (unknown is skipped = synced)
		t.Errorf("expected 4 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
	if routeACalls != 2 {
		t.Errorf("expected 2 routeA calls, got %d", routeACalls)
	}
	if routeBCalls != 1 {
		t.Errorf("expected 1 routeB call, got %d", routeBCalls)
	}
}

func TestRouterApplier_WithFallback(t *testing.T) {
	type Event struct {
		Type string
	}

	fallbackCalls := 0

	fallback := courier.ApplierFunc[Event](func(ctx context.Context, records []Event) ([]Event, []Event, error) {
		fallbackCalls += len(records)
		return records, nil, nil
	})

	router := courier.NewRouterApplier(
		func(e Event) string { return e.Type },
		map[string]courier.Applier[Event]{},
		fallback,
	)

	records := []Event{{Type: "unknown"}, {Type: "other"}}
	synced, _, err := router.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if fallbackCalls != 2 {
		t.Errorf("expected 2 fallback calls, got %d", fallbackCalls)
	}
}

func TestBatchSplitMiddleware(t *testing.T) {
	var callCount int32

	middleware := courier.BatchSplitMiddleware[int](5)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		atomic.AddInt32(&callCount, 1)
		return records, nil, nil
	})

	wrapped := middleware(applier)
	synced, _, err := wrapped.Apply(context.Background(), []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 10 {
		t.Errorf("expected 10 synced, got %d", len(synced))
	}
	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}
