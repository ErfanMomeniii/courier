package courier_test

import (
	"context"
	"errors"
	"testing"

	"github.com/erfanmomeniii/courier"
)

func TestSimpleApplier(t *testing.T) {
	processedIDs := make([]string, 0)
	failID := "fail"

	applier := courier.NewSimpleApplier(func(ctx context.Context, record testRecord) error {
		processedIDs = append(processedIDs, record.ID)
		if record.ID == failID {
			return errors.New("intentional failure")
		}
		return nil
	})

	records := []testRecord{
		{ID: "1", Data: "test1"},
		{ID: "fail", Data: "test2"},
		{ID: "3", Data: "test3"},
	}

	synced, failed, err := applier.Apply(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(failed))
	}
	if len(processedIDs) != 3 {
		t.Errorf("expected 3 processed, got %d", len(processedIDs))
	}
}

func TestBatchApplier_Success(t *testing.T) {
	called := false
	applier := courier.NewBatchApplier(func(ctx context.Context, records []testRecord) error {
		called = true
		return nil
	})

	records := []testRecord{
		{ID: "1", Data: "test1"},
		{ID: "2", Data: "test2"},
	}

	synced, failed, err := applier.Apply(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !called {
		t.Error("batch function was not called")
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}
}

func TestBatchApplier_Failure(t *testing.T) {
	applier := courier.NewBatchApplier(func(ctx context.Context, records []testRecord) error {
		return errors.New("batch failed")
	})

	records := []testRecord{
		{ID: "1", Data: "test1"},
		{ID: "2", Data: "test2"},
	}

	synced, failed, err := applier.Apply(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}

	if len(synced) != 0 {
		t.Errorf("expected 0 synced, got %d", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("expected 2 failed, got %d", len(failed))
	}
}

func TestApplierFunc(t *testing.T) {
	applier := courier.ApplierFunc[testRecord](func(ctx context.Context, records []testRecord) ([]testRecord, []testRecord, error) {
		return records[:1], records[1:], nil
	})

	records := []testRecord{
		{ID: "1", Data: "test1"},
		{ID: "2", Data: "test2"},
	}

	synced, failed, err := applier.Apply(context.Background(), records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(synced) != 1 {
		t.Errorf("expected 1 synced, got %d", len(synced))
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(failed))
	}
}
