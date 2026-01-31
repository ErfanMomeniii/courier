package courier

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestChain_Order(t *testing.T) {
	var order []string
	var mu sync.Mutex

	middleware1 := func(applier Applier[string]) Applier[string] {
		return ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
			mu.Lock()
			order = append(order, "m1-before")
			mu.Unlock()
			synced, failed, err := applier.Apply(ctx, records)
			mu.Lock()
			order = append(order, "m1-after")
			mu.Unlock()
			return synced, failed, err
		})
	}

	middleware2 := func(applier Applier[string]) Applier[string] {
		return ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
			mu.Lock()
			order = append(order, "m2-before")
			mu.Unlock()
			synced, failed, err := applier.Apply(ctx, records)
			mu.Lock()
			order = append(order, "m2-after")
			mu.Unlock()
			return synced, failed, err
		})
	}

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		mu.Lock()
		order = append(order, "apply")
		mu.Unlock()
		return records, nil, nil
	})

	// Chain: first middleware is outermost
	chained := Chain(middleware1, middleware2)(baseApplier)
	chained.Apply(context.Background(), []string{"test"})

	expected := []string{"m1-before", "m2-before", "apply", "m2-after", "m1-after"}
	mu.Lock()
	defer mu.Unlock()

	if len(order) != len(expected) {
		t.Fatalf("order length = %d, want %d", len(order), len(expected))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %s, want %s", i, order[i], v)
		}
	}
}

func TestChain_Empty(t *testing.T) {
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records, nil, nil
	})

	chained := Chain[string]()(baseApplier)
	synced, _, err := chained.Apply(context.Background(), []string{"a", "b"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 2 {
		t.Errorf("synced = %d, want 2", len(synced))
	}
}

func TestLoggingMiddleware(t *testing.T) {
	var logs []string
	var mu sync.Mutex

	logFn := func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, format)
		mu.Unlock()
	}

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records[:1], records[1:], nil // 1 synced, rest failed
	})

	logged := LoggingMiddleware[string](logFn)(baseApplier)
	logged.Apply(context.Background(), []string{"a", "b", "c"})

	mu.Lock()
	defer mu.Unlock()

	if len(logs) != 2 {
		t.Fatalf("expected 2 log messages, got %d", len(logs))
	}
	if logs[0] != "applying %d records" {
		t.Errorf("first log = %s, want 'applying %%d records'", logs[0])
	}
	if logs[1] != "applied records: synced=%d failed=%d err=%v duration=%v" {
		t.Errorf("second log unexpected: %s", logs[1])
	}
}

func TestTimingMiddleware(t *testing.T) {
	var recordedDuration time.Duration
	var mu sync.Mutex

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		time.Sleep(10 * time.Millisecond)
		return records, nil, nil
	})

	timed := TimingMiddleware[string](func(d time.Duration) {
		mu.Lock()
		recordedDuration = d
		mu.Unlock()
	})(baseApplier)

	timed.Apply(context.Background(), []string{"a"})

	mu.Lock()
	defer mu.Unlock()

	if recordedDuration < 10*time.Millisecond {
		t.Errorf("duration = %v, expected >= 10ms", recordedDuration)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	var recoveredValue any
	var mu sync.Mutex

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		panic("test panic")
	})

	recovered := RecoveryMiddleware[string](func(r any) {
		mu.Lock()
		recoveredValue = r
		mu.Unlock()
	})(baseApplier)

	synced, failed, err := recovered.Apply(context.Background(), []string{"a", "b"})

	mu.Lock()
	defer mu.Unlock()

	if recoveredValue != "test panic" {
		t.Errorf("recovered = %v, want 'test panic'", recoveredValue)
	}
	if len(synced) != 0 {
		t.Errorf("synced = %d, want 0", len(synced))
	}
	if len(failed) != 2 {
		t.Errorf("failed = %d, want 2", len(failed))
	}
	if err == nil {
		t.Error("expected error from panic recovery")
	}
}

func TestRecoveryMiddleware_NoPanic(t *testing.T) {
	var panicCalled bool

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records, nil, nil
	})

	recovered := RecoveryMiddleware[string](func(r any) {
		panicCalled = true
	})(baseApplier)

	synced, failed, err := recovered.Apply(context.Background(), []string{"a"})

	if panicCalled {
		t.Error("onPanic should not be called when there's no panic")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("synced = %d, want 1", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
}

func TestFilterMiddleware(t *testing.T) {
	var appliedRecords []string
	var mu sync.Mutex

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		mu.Lock()
		appliedRecords = append(appliedRecords, records...)
		mu.Unlock()
		return records, nil, nil
	})

	// Filter: only process records starting with "ok"
	filtered := FilterMiddleware[string](func(s string) bool {
		return len(s) > 0 && s[0] == 'o'
	})(baseApplier)

	synced, failed, err := filtered.Apply(context.Background(), []string{"ok1", "skip", "ok2", "nope"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Only "ok1" and "ok2" should be applied
	if len(appliedRecords) != 2 {
		t.Errorf("applied = %d records, want 2", len(appliedRecords))
	}

	// All records should be in synced (skipped count as synced)
	if len(synced) != 4 {
		t.Errorf("synced = %d, want 4 (including skipped)", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
}

func TestFilterMiddleware_AllFiltered(t *testing.T) {
	var applyCalled bool

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		applyCalled = true
		return records, nil, nil
	})

	filtered := FilterMiddleware[string](func(s string) bool {
		return false // Filter everything
	})(baseApplier)

	synced, failed, err := filtered.Apply(context.Background(), []string{"a", "b", "c"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if applyCalled {
		t.Error("base applier should not be called when all records are filtered")
	}
	if len(synced) != 3 {
		t.Errorf("synced = %d, want 3 (all skipped = synced)", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
}

func TestTransformMiddleware(t *testing.T) {
	var transformedRecords []string
	var mu sync.Mutex

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		mu.Lock()
		transformedRecords = records
		mu.Unlock()
		return records, nil, nil
	})

	transformed := TransformMiddleware[string](func(s string) string {
		return "transformed-" + s
	})(baseApplier)

	transformed.Apply(context.Background(), []string{"a", "b"})

	mu.Lock()
	defer mu.Unlock()

	if len(transformedRecords) != 2 {
		t.Fatalf("transformed = %d records, want 2", len(transformedRecords))
	}
	if transformedRecords[0] != "transformed-a" {
		t.Errorf("transformedRecords[0] = %s, want 'transformed-a'", transformedRecords[0])
	}
	if transformedRecords[1] != "transformed-b" {
		t.Errorf("transformedRecords[1] = %s, want 'transformed-b'", transformedRecords[1])
	}
}

func TestHookedSource_FetchHooks(t *testing.T) {
	var beforeCalled, afterCalled bool
	var afterRecords []string
	var afterErr error

	source := BatchSourceFunc[string]{
		FetchFunc: func(ctx context.Context) ([]string, error) {
			return []string{"a", "b"}, nil
		},
		MarkFunc: func(ctx context.Context, records []string) error {
			return nil
		},
	}

	hooks := &Hooks[string]{
		BeforeFetch: func(ctx context.Context) {
			beforeCalled = true
		},
		AfterFetch: func(ctx context.Context, records []string, err error) {
			afterCalled = true
			afterRecords = records
			afterErr = err
		},
	}

	hooked := NewHookedSource(source, hooks)
	records, err := hooked.FetchRecords(context.Background())

	if !beforeCalled {
		t.Error("BeforeFetch was not called")
	}
	if !afterCalled {
		t.Error("AfterFetch was not called")
	}
	if len(records) != 2 {
		t.Errorf("records = %d, want 2", len(records))
	}
	if len(afterRecords) != 2 {
		t.Errorf("afterRecords = %d, want 2", len(afterRecords))
	}
	if err != nil || afterErr != nil {
		t.Error("unexpected error")
	}
}

func TestHookedSource_MarkHooks(t *testing.T) {
	var beforeRecords, afterRecords []string

	source := BatchSourceFunc[string]{
		FetchFunc: func(ctx context.Context) ([]string, error) {
			return nil, nil
		},
		MarkFunc: func(ctx context.Context, records []string) error {
			return nil
		},
	}

	hooks := &Hooks[string]{
		BeforeMark: func(ctx context.Context, records []string) {
			beforeRecords = records
		},
		AfterMark: func(ctx context.Context, records []string, err error) {
			afterRecords = records
		},
	}

	hooked := NewHookedSource(source, hooks)
	hooked.MarkAsSynced(context.Background(), []string{"a", "b"})

	if len(beforeRecords) != 2 {
		t.Errorf("beforeRecords = %d, want 2", len(beforeRecords))
	}
	if len(afterRecords) != 2 {
		t.Errorf("afterRecords = %d, want 2", len(afterRecords))
	}
}

func TestHookedSource_NilHooks(t *testing.T) {
	source := BatchSourceFunc[string]{
		FetchFunc: func(ctx context.Context) ([]string, error) {
			return []string{"a"}, nil
		},
		MarkFunc: func(ctx context.Context, records []string) error {
			return nil
		},
	}

	// Should not panic with nil hooks
	hooked := NewHookedSource(source, nil)
	records, err := hooked.FetchRecords(context.Background())

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("records = %d, want 1", len(records))
	}

	// Mark should also work
	err = hooked.MarkAsSynced(context.Background(), records)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHookedApplier_ApplyHooks(t *testing.T) {
	var beforeRecords []string
	var afterSynced, afterFailed []string

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records[:1], records[1:], nil
	})

	hooks := &Hooks[string]{
		BeforeApply: func(ctx context.Context, records []string) {
			beforeRecords = records
		},
		AfterApply: func(ctx context.Context, synced, failed []string, err error) {
			afterSynced = synced
			afterFailed = failed
		},
	}

	hooked := NewHookedApplier(baseApplier, hooks)
	hooked.Apply(context.Background(), []string{"a", "b", "c"})

	if len(beforeRecords) != 3 {
		t.Errorf("beforeRecords = %d, want 3", len(beforeRecords))
	}
	if len(afterSynced) != 1 {
		t.Errorf("afterSynced = %d, want 1", len(afterSynced))
	}
	if len(afterFailed) != 2 {
		t.Errorf("afterFailed = %d, want 2", len(afterFailed))
	}
}

func TestHookedApplier_NilHooks(t *testing.T) {
	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		return records, nil, nil
	})

	// Should not panic with nil hooks
	hooked := NewHookedApplier(baseApplier, nil)
	synced, failed, err := hooked.Apply(context.Background(), []string{"a"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 1 {
		t.Errorf("synced = %d, want 1", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}
}

func TestHooks_OnRecord(t *testing.T) {
	var recordCallbacks []string
	var mu sync.Mutex

	hooks := &Hooks[string]{
		OnRecord: func(ctx context.Context, record string, err error) {
			mu.Lock()
			recordCallbacks = append(recordCallbacks, record)
			mu.Unlock()
		},
	}

	// OnRecord is typically called by the coordinator, not the HookedApplier
	// Test that the hook is stored correctly
	if hooks.OnRecord == nil {
		t.Error("OnRecord should be set")
	}

	// Call it manually to verify it works
	hooks.OnRecord(context.Background(), "test", nil)

	mu.Lock()
	defer mu.Unlock()
	if len(recordCallbacks) != 1 || recordCallbacks[0] != "test" {
		t.Error("OnRecord callback not working correctly")
	}
}

func TestMiddleware_Integration(t *testing.T) {
	var logs []string
	var durations []time.Duration
	var mu sync.Mutex

	baseApplier := ApplierFunc[string](func(ctx context.Context, records []string) ([]string, []string, error) {
		time.Sleep(5 * time.Millisecond)
		return records, nil, nil
	})

	// Build middleware chain
	chained := Chain(
		LoggingMiddleware[string](func(format string, args ...any) {
			mu.Lock()
			logs = append(logs, format)
			mu.Unlock()
		}),
		TimingMiddleware[string](func(d time.Duration) {
			mu.Lock()
			durations = append(durations, d)
			mu.Unlock()
		}),
		FilterMiddleware[string](func(s string) bool {
			return s != "skip"
		}),
	)(baseApplier)

	synced, failed, err := chained.Apply(context.Background(), []string{"a", "skip", "b"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(synced) != 3 { // 2 applied + 1 skipped
		t.Errorf("synced = %d, want 3", len(synced))
	}
	if len(failed) != 0 {
		t.Errorf("failed = %d, want 0", len(failed))
	}

	mu.Lock()
	defer mu.Unlock()

	if len(logs) != 2 {
		t.Errorf("logs = %d, want 2", len(logs))
	}
	if len(durations) != 1 {
		t.Errorf("durations = %d, want 1", len(durations))
	}
	if durations[0] < 5*time.Millisecond {
		t.Errorf("duration = %v, expected >= 5ms", durations[0])
	}
}
