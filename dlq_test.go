package courier

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInMemoryDLQ_SendAndReceive(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	// Send records
	records := []FailedRecord[string]{
		{Record: "a", Attempts: 1, FirstFailure: time.Now()},
		{Record: "b", Attempts: 2, FirstFailure: time.Now()},
		{Record: "c", Attempts: 3, FirstFailure: time.Now()},
	}

	for _, r := range records {
		if err := dlq.Send(ctx, r); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
	}

	// Check count
	count, err := dlq.Count(ctx)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}

	// Receive all
	received, err := dlq.Receive(ctx, 0)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if len(received) != 3 {
		t.Errorf("Receive returned %d records, want 3", len(received))
	}

	// Verify order (FIFO)
	if received[0].Record != "a" || received[1].Record != "b" || received[2].Record != "c" {
		t.Error("Records not in expected order")
	}
}

func TestInMemoryDLQ_ReceiveWithLimit(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	// Send 5 records
	for i := 0; i < 5; i++ {
		dlq.Send(ctx, FailedRecord[string]{Record: string(rune('a' + i))})
	}

	// Receive with limit
	received, err := dlq.Receive(ctx, 2)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}
	if len(received) != 2 {
		t.Errorf("Receive returned %d records, want 2", len(received))
	}

	// Verify first two records
	if received[0].Record != "a" || received[1].Record != "b" {
		t.Error("Unexpected records received")
	}
}

func TestInMemoryDLQ_Remove(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	// Send records
	dlq.Send(ctx, FailedRecord[string]{Record: "a"})
	dlq.Send(ctx, FailedRecord[string]{Record: "b"})
	dlq.Send(ctx, FailedRecord[string]{Record: "c"})

	// Remove one
	err := dlq.Remove(ctx, FailedRecord[string]{Record: "b"})
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify count decreased
	count, _ := dlq.Count(ctx)
	if count != 2 {
		t.Errorf("Count = %d, want 2", count)
	}
}

func TestInMemoryDLQ_MaxSize(t *testing.T) {
	dlq := NewInMemoryDLQ[string](3) // Max 3 records
	ctx := context.Background()

	// Send 5 records
	for i := 0; i < 5; i++ {
		dlq.Send(ctx, FailedRecord[string]{Record: string(rune('a' + i))})
	}

	// Should only have 3 (oldest removed)
	count, _ := dlq.Count(ctx)
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}

	// Check that oldest were removed
	received, _ := dlq.Receive(ctx, 0)
	if received[0].Record != "c" || received[1].Record != "d" || received[2].Record != "e" {
		t.Error("Expected oldest records to be evicted")
	}
}

func TestInMemoryDLQ_UnlimitedSize(t *testing.T) {
	dlq := NewInMemoryDLQ[string](0) // Unlimited
	ctx := context.Background()

	// Send many records
	for i := 0; i < 100; i++ {
		dlq.Send(ctx, FailedRecord[string]{Record: string(rune(i))})
	}

	count, _ := dlq.Count(ctx)
	if count != 100 {
		t.Errorf("Count = %d, want 100", count)
	}
}

func TestInMemoryDLQ_All(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	dlq.Send(ctx, FailedRecord[string]{Record: "a"})
	dlq.Send(ctx, FailedRecord[string]{Record: "b"})

	all := dlq.All()
	if len(all) != 2 {
		t.Errorf("All returned %d records, want 2", len(all))
	}

	// Verify it's a copy (modifying shouldn't affect original)
	all[0].Record = "modified"
	original := dlq.All()
	if original[0].Record != "a" {
		t.Error("All() should return a copy, not the original slice")
	}
}

func TestInMemoryDLQ_Clear(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	dlq.Send(ctx, FailedRecord[string]{Record: "a"})
	dlq.Send(ctx, FailedRecord[string]{Record: "b"})

	dlq.Clear()

	count, _ := dlq.Count(ctx)
	if count != 0 {
		t.Errorf("Count after Clear = %d, want 0", count)
	}
}

func TestInMemoryDLQ_Concurrent(t *testing.T) {
	dlq := NewInMemoryDLQ[int](1000)
	ctx := context.Background()

	// Concurrent sends
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				dlq.Send(ctx, FailedRecord[int]{Record: id*100 + j})
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	count, _ := dlq.Count(ctx)
	if count != 1000 {
		t.Errorf("Count = %d, want 1000", count)
	}
}

func TestFailedRecord_Fields(t *testing.T) {
	now := time.Now()
	err := errors.New("test error")

	fr := FailedRecord[string]{
		Record:       "test",
		Error:        err,
		Attempts:     3,
		FirstFailure: now.Add(-time.Hour),
		LastFailure:  now,
		Metadata:     map[string]any{"key": "value"},
	}

	if fr.Record != "test" {
		t.Errorf("Record = %s, want 'test'", fr.Record)
	}
	if !errors.Is(fr.Error, err) {
		t.Errorf("Error = %v, want %v", fr.Error, err)
	}
	if fr.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", fr.Attempts)
	}
	if fr.Metadata["key"] != "value" {
		t.Error("Metadata not set correctly")
	}
}

func TestDLQApplier_SendsFailedToDLQ(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	// Applier that fails some records
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
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

	dlqApplier := NewDLQApplier(baseApplier, dlq)

	records := []string{"ok1", "fail", "ok2", "fail"}
	synced, failed, err := dlqApplier.Apply(ctx, records)

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Synced should have successful records
	if len(synced) != 2 {
		t.Errorf("synced = %d, want 2", len(synced))
	}

	// Failed should be empty (moved to DLQ)
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0 (should be in DLQ)", len(failed))
	}

	// DLQ should have failed records
	dlqCount, _ := dlq.Count(ctx)
	if dlqCount != 2 {
		t.Errorf("DLQ count = %d, want 2", dlqCount)
	}
}

func TestDLQApplier_PropagatesFatalError(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()
	fatalErr := errors.New("fatal error")

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return nil, nil, fatalErr
	})

	dlqApplier := NewDLQApplier(baseApplier, dlq)

	_, _, err := dlqApplier.Apply(ctx, []string{"a", "b"})

	if !errors.Is(err, fatalErr) {
		t.Errorf("expected fatal error, got: %v", err)
	}
}

func TestDLQApplier_DLQ(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records, nil, nil
	})

	dlqApplier := NewDLQApplier(baseApplier, dlq)

	if dlqApplier.DLQ() != dlq {
		t.Error("DLQ() should return the underlying DLQ")
	}
}

func TestDLQApplier_WithIdentifier(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return nil, records, nil // All fail
	})

	dlqApplier := NewDLQApplier(baseApplier, dlq).
		WithIdentifier(func(s string) string {
			return "id-" + s
		})

	// Verify the applier is properly configured (identifier function is stored)
	if dlqApplier.getId == nil {
		t.Error("WithIdentifier should set getId function")
	}
}

func TestDLQApplier_FailedRecordMetadata(t *testing.T) {
	dlq := NewInMemoryDLQ[string](10)
	ctx := context.Background()

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return nil, records, nil // All fail
	})

	dlqApplier := NewDLQApplier(baseApplier, dlq)

	beforeApply := time.Now()
	dlqApplier.Apply(ctx, []string{"test"})
	afterApply := time.Now()

	received, _ := dlq.Receive(ctx, 1)
	if len(received) != 1 {
		t.Fatal("Expected 1 record in DLQ")
	}

	fr := received[0]
	if fr.Record != "test" {
		t.Errorf("Record = %s, want 'test'", fr.Record)
	}
	if fr.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", fr.Attempts)
	}
	if fr.FirstFailure.Before(beforeApply) || fr.FirstFailure.After(afterApply) {
		t.Error("FirstFailure timestamp out of expected range")
	}
	if fr.LastFailure.Before(beforeApply) || fr.LastFailure.After(afterApply) {
		t.Error("LastFailure timestamp out of expected range")
	}
}
