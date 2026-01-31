package courier_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erfanmomeniii/courier"
)

func TestDeduplicator_Basic(t *testing.T) {
	dedup := courier.NewDeduplicator[string](0, 0)

	if dedup.IsDuplicate("key1") {
		t.Error("key1 should not be duplicate initially")
	}

	dedup.MarkSeen("key1")

	if !dedup.IsDuplicate("key1") {
		t.Error("key1 should be duplicate after marking")
	}

	if dedup.IsDuplicate("key2") {
		t.Error("key2 should not be duplicate")
	}
}

func TestDeduplicator_Remove(t *testing.T) {
	dedup := courier.NewDeduplicator[string](0, 0)

	dedup.MarkSeen("key1")
	if !dedup.IsDuplicate("key1") {
		t.Error("key1 should be duplicate")
	}

	dedup.Remove("key1")
	if dedup.IsDuplicate("key1") {
		t.Error("key1 should not be duplicate after removal")
	}
}

func TestDeduplicator_Clear(t *testing.T) {
	dedup := courier.NewDeduplicator[string](0, 0)

	dedup.MarkSeen("key1")
	dedup.MarkSeen("key2")

	if dedup.Size() != 2 {
		t.Errorf("expected size 2, got %d", dedup.Size())
	}

	dedup.Clear()

	if dedup.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", dedup.Size())
	}
}

func TestDeduplicator_MaxSize(t *testing.T) {
	dedup := courier.NewDeduplicator[string](0, 3)

	dedup.MarkSeen("key1")
	time.Sleep(10 * time.Millisecond)
	dedup.MarkSeen("key2")
	time.Sleep(10 * time.Millisecond)
	dedup.MarkSeen("key3")

	if dedup.Size() != 3 {
		t.Errorf("expected size 3, got %d", dedup.Size())
	}

	// Adding key4 should evict key1 (oldest)
	dedup.MarkSeen("key4")

	if dedup.Size() != 3 {
		t.Errorf("expected size 3 after eviction, got %d", dedup.Size())
	}

	if dedup.IsDuplicate("key1") {
		t.Error("key1 should have been evicted")
	}
	if !dedup.IsDuplicate("key4") {
		t.Error("key4 should be present")
	}
}

func TestDeduplicator_TTL(t *testing.T) {
	dedup := courier.NewDeduplicator[string](50*time.Millisecond, 0)

	dedup.MarkSeen("key1")
	if !dedup.IsDuplicate("key1") {
		t.Error("key1 should be duplicate")
	}

	// Wait for TTL to expire (TTL + cleanup interval)
	time.Sleep(100 * time.Millisecond)

	if dedup.IsDuplicate("key1") {
		t.Error("key1 should have expired")
	}
}

func TestDeduplicatingApplier(t *testing.T) {
	type Record struct {
		ID   string
		Data string
	}

	var processedCount int32

	dedup := courier.NewDeduplicator[string](0, 0)
	applier := courier.ApplierFunc[Record](func(ctx context.Context, records []Record) ([]Record, []Record, error) {
		atomic.AddInt32(&processedCount, int32(len(records)))
		return records, nil, nil
	})

	dedupApplier := courier.NewDeduplicatingApplier(applier, dedup, func(r Record) string { return r.ID })

	// First batch
	records1 := []Record{{ID: "1", Data: "a"}, {ID: "2", Data: "b"}}
	synced, failed, err := dedupApplier.Apply(context.Background(), records1)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}

	// Second batch with duplicates
	records2 := []Record{{ID: "1", Data: "a"}, {ID: "3", Data: "c"}}
	synced, failed, err = dedupApplier.Apply(context.Background(), records2)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 { // 1 duplicate + 1 new (both synced)
		t.Errorf("expected 2 synced, got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}

	// Should have processed 3 unique records (1, 2, 3)
	if atomic.LoadInt32(&processedCount) != 3 {
		t.Errorf("expected 3 processed, got %d", processedCount)
	}
}

func TestDeduplicatingApplier_AllDuplicates(t *testing.T) {
	type Record struct {
		ID string
	}

	var processedCount int32

	dedup := courier.NewDeduplicator[string](0, 0)
	applier := courier.ApplierFunc[Record](func(ctx context.Context, records []Record) ([]Record, []Record, error) {
		atomic.AddInt32(&processedCount, int32(len(records)))
		return records, nil, nil
	})

	dedupApplier := courier.NewDeduplicatingApplier(applier, dedup, func(r Record) string { return r.ID })

	// First batch
	records := []Record{{ID: "1"}, {ID: "2"}}
	dedupApplier.Apply(context.Background(), records)

	// Second batch with all duplicates
	synced, failed, err := dedupApplier.Apply(context.Background(), records)

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("expected 2 synced (duplicates), got %d", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed, got %d", len(failed))
	}

	// Should have only processed the first batch
	if atomic.LoadInt32(&processedCount) != 2 {
		t.Errorf("expected 2 processed (first batch only), got %d", processedCount)
	}
}

func TestDeduplicatingApplier_Deduplicator(t *testing.T) {
	dedup := courier.NewDeduplicator[string](0, 0)
	applier := courier.ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records, nil, nil
	})

	dedupApplier := courier.NewDeduplicatingApplier(applier, dedup, func(s string) string { return s })

	if dedupApplier.Deduplicator() != dedup {
		t.Error("Deduplicator() should return the underlying deduplicator")
	}
}
