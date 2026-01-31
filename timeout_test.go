package courier_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/erfanmomeniii/courier"
)

func TestTimeoutApplier_PerBatch_Success(t *testing.T) {
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		time.Sleep(10 * time.Millisecond)
		return records, nil, nil
	})

	timeoutApplier := courier.NewTimeoutApplier(applier, 100*time.Millisecond, true)

	synced, failed, err := timeoutApplier.Apply(context.Background(), []int{1, 2, 3})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestTimeoutApplier_PerBatch_Timeout(t *testing.T) {
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		select {
		case <-time.After(500 * time.Millisecond):
			return records, nil, nil
		case <-ctx.Done():
			return nil, records, ctx.Err()
		}
	})

	timeoutApplier := courier.NewTimeoutApplier(applier, 50*time.Millisecond, true)

	_, _, err := timeoutApplier.Apply(context.Background(), []int{1, 2, 3})

	if !errors.Is(err, courier.ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestTimeoutApplier_PerRecord_Success(t *testing.T) {
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		time.Sleep(5 * time.Millisecond)
		return records, nil, nil
	})

	timeoutApplier := courier.NewTimeoutApplier(applier, 100*time.Millisecond, false)

	synced, failed, err := timeoutApplier.Apply(context.Background(), []int{1, 2, 3})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestTimeoutApplier_PerRecord_SomeTimeout(t *testing.T) {
	callCount := 0
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		callCount++
		if callCount == 2 {
			// Second record takes too long
			select {
			case <-time.After(500 * time.Millisecond):
				return records, nil, nil
			case <-ctx.Done():
				return nil, records, ctx.Err()
			}
		}
		return records, nil, nil
	})

	timeoutApplier := courier.NewTimeoutApplier(applier, 50*time.Millisecond, false)

	synced, failed, err := timeoutApplier.Apply(context.Background(), []int{1, 2, 3})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed (timeout), got %d", len(failed))
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	middleware := courier.TimeoutMiddleware[int](100*time.Millisecond, true)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		return records, nil, nil
	})

	wrapped := middleware(applier)
	synced, _, err := wrapped.Apply(context.Background(), []int{1, 2, 3})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced, got %d", len(synced))
	}
}

func TestContextApplier(t *testing.T) {
	type contextKey string
	const batchSizeKey contextKey = "batchSize"

	var capturedSize int

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		if v, ok := ctx.Value(batchSizeKey).(int); ok {
			capturedSize = v
		}
		return records, nil, nil
	})

	contextApplier := courier.NewContextApplier(applier, func(ctx context.Context, records []int) context.Context {
		return context.WithValue(ctx, batchSizeKey, len(records))
	})

	contextApplier.Apply(context.Background(), []int{1, 2, 3, 4, 5})

	if capturedSize != 5 {
		t.Errorf("expected batch size 5 in context, got %d", capturedSize)
	}
}

func TestErrTimeout(t *testing.T) {
	if courier.ErrTimeout.Error() != "courier: operation timed out" {
		t.Errorf("unexpected error message: %s", courier.ErrTimeout.Error())
	}
}
