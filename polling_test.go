package courier_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erfanmomeniii/courier"
)

type testRecord struct {
	ID   string
	Data string
}

type mockBatchSource struct {
	mu         sync.Mutex
	records    []testRecord
	fetchCalls int
	markCalls  int
	fetchErr   error
	markErr    error
	markedIDs  []string
	fetchDelay time.Duration
}

func (m *mockBatchSource) FetchRecords(ctx context.Context) ([]testRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fetchCalls++
	if m.fetchDelay > 0 {
		time.Sleep(m.fetchDelay)
	}
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	result := make([]testRecord, len(m.records))
	copy(result, m.records)
	return result, nil
}

func (m *mockBatchSource) MarkAsSynced(ctx context.Context, records []testRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.markCalls++
	if m.markErr != nil {
		return m.markErr
	}
	for _, r := range records {
		m.markedIDs = append(m.markedIDs, r.ID)
	}
	return nil
}

type mockApplier struct {
	mu         sync.Mutex
	applyCalls int
	applyErr   error
	failIDs    map[string]bool
	applied    []testRecord
}

func (m *mockApplier) Apply(ctx context.Context, records []testRecord) ([]testRecord, []testRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.applyCalls++
	if m.applyErr != nil {
		return nil, nil, m.applyErr
	}

	var synced, failed []testRecord
	for _, r := range records {
		m.applied = append(m.applied, r)
		if m.failIDs != nil && m.failIDs[r.ID] {
			failed = append(failed, r)
		} else {
			synced = append(synced, r)
		}
	}
	return synced, failed, nil
}

func TestPollingCoordinator_RunOnce(t *testing.T) {
	source := &mockBatchSource{
		records: []testRecord{
			{ID: "1", Data: "test1"},
			{ID: "2", Data: "test2"},
		},
	}
	applier := &mockApplier{}

	coord := courier.NewPollingCoordinator(source, applier)

	ctx := context.Background()
	err := coord.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	if source.fetchCalls != 1 {
		t.Errorf("expected 1 fetch call, got %d", source.fetchCalls)
	}
	if applier.applyCalls != 1 {
		t.Errorf("expected 1 apply call, got %d", applier.applyCalls)
	}
	if source.markCalls != 1 {
		t.Errorf("expected 1 mark call, got %d", source.markCalls)
	}
	if len(source.markedIDs) != 2 {
		t.Errorf("expected 2 marked IDs, got %d", len(source.markedIDs))
	}
}

func TestPollingCoordinator_NoRecords(t *testing.T) {
	source := &mockBatchSource{records: nil}
	applier := &mockApplier{}

	coord := courier.NewPollingCoordinator(source, applier)

	err := coord.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	if applier.applyCalls != 0 {
		t.Errorf("expected 0 apply calls for empty records, got %d", applier.applyCalls)
	}
	if source.markCalls != 0 {
		t.Errorf("expected 0 mark calls for empty records, got %d", source.markCalls)
	}
}

func TestPollingCoordinator_FetchError(t *testing.T) {
	fetchErr := errors.New("fetch failed")
	source := &mockBatchSource{fetchErr: fetchErr}
	applier := &mockApplier{}

	var capturedErr error
	coord := courier.NewPollingCoordinator(source, applier,
		courier.WithErrorHandler(func(err error) {
			capturedErr = err
		}),
	)

	err := coord.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from RunOnce")
	}
	if capturedErr == nil {
		t.Error("error handler was not called")
	}
}

func TestPollingCoordinator_ApplyPartialFailure(t *testing.T) {
	source := &mockBatchSource{
		records: []testRecord{
			{ID: "1", Data: "test1"},
			{ID: "2", Data: "test2"},
			{ID: "3", Data: "test3"},
		},
	}
	applier := &mockApplier{
		failIDs: map[string]bool{"2": true},
	}

	coord := courier.NewPollingCoordinator(source, applier)

	err := coord.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	// Only synced records should be marked
	if len(source.markedIDs) != 2 {
		t.Errorf("expected 2 marked IDs (synced only), got %d", len(source.markedIDs))
	}
}

func TestPollingCoordinator_StartStop(t *testing.T) {
	source := &mockBatchSource{
		records:    []testRecord{{ID: "1", Data: "test"}},
		fetchDelay: 10 * time.Millisecond,
	}
	applier := &mockApplier{}

	coord := courier.NewPollingCoordinator(source, applier,
		courier.WithInterval(50*time.Millisecond),
	)

	// Start in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Wait for some polls
	time.Sleep(150 * time.Millisecond)

	if !coord.Running() {
		t.Error("coordinator should be running")
	}

	// Stop coordinator
	coord.Stop()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop in time")
	}

	if coord.Running() {
		t.Error("coordinator should not be running after stop")
	}

	if source.fetchCalls < 2 {
		t.Errorf("expected at least 2 fetch calls, got %d", source.fetchCalls)
	}
}

func TestPollingCoordinator_ContextCancellation(t *testing.T) {
	source := &mockBatchSource{records: nil}
	applier := &mockApplier{}

	coord := courier.NewPollingCoordinator(source, applier,
		courier.WithInterval(time.Hour), // Long interval
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Wait for coordinator to start
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop on context cancellation")
	}
}

func TestPollingCoordinator_RunOnceWhileRunning(t *testing.T) {
	source := &mockBatchSource{
		records:    []testRecord{{ID: "1", Data: "test"}},
		fetchDelay: 50 * time.Millisecond,
	}
	applier := &mockApplier{}

	coord := courier.NewPollingCoordinator(source, applier,
		courier.WithInterval(100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start coordinator in background
	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Wait for coordinator to start
	time.Sleep(20 * time.Millisecond)

	// Try to call RunOnce while running - should return ErrCoordinatorRunning
	err := coord.RunOnce(context.Background())
	if !errors.Is(err, courier.ErrCoordinatorRunning) {
		t.Errorf("expected ErrCoordinatorRunning, got %v", err)
	}

	// Cleanup
	cancel()
	<-done
}

func TestPollingCoordinator_Metrics(t *testing.T) {
	source := &mockBatchSource{
		records: []testRecord{
			{ID: "1", Data: "test1"},
			{ID: "2", Data: "test2"},
		},
	}
	applier := &mockApplier{
		failIDs: map[string]bool{"2": true},
	}

	var fetchedCount, syncedCount, failedCount int32
	metrics := courier.MetricsFunc{
		OnRecordsFetched: func(count int) {
			atomic.StoreInt32(&fetchedCount, int32(count))
		},
		OnRecordsApplied: func(synced, failed int) {
			atomic.StoreInt32(&syncedCount, int32(synced))
			atomic.StoreInt32(&failedCount, int32(failed))
		},
	}

	coord := courier.NewPollingCoordinator(source, applier,
		courier.WithMetrics(metrics),
	)

	coord.RunOnce(context.Background())

	if atomic.LoadInt32(&fetchedCount) != 2 {
		t.Errorf("expected 2 fetched, got %d", fetchedCount)
	}
	if atomic.LoadInt32(&syncedCount) != 1 {
		t.Errorf("expected 1 synced, got %d", syncedCount)
	}
	if atomic.LoadInt32(&failedCount) != 1 {
		t.Errorf("expected 1 failed, got %d", failedCount)
	}
}
