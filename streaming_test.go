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

type mockStreamSource struct {
	mu      sync.Mutex
	records chan testRecord
	acked   []string
	nacked  []string
	ackErr  error
	nackErr error
	closed  bool
}

func newMockStreamSource(bufSize int) *mockStreamSource {
	return &mockStreamSource{
		records: make(chan testRecord, bufSize),
	}
}

func (m *mockStreamSource) Records() <-chan testRecord {
	return m.records
}

func (m *mockStreamSource) Ack(ctx context.Context, record testRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ackErr != nil {
		return m.ackErr
	}
	m.acked = append(m.acked, record.ID)
	return nil
}

func (m *mockStreamSource) Nack(ctx context.Context, record testRecord, err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nackErr != nil {
		return m.nackErr
	}
	m.nacked = append(m.nacked, record.ID)
	return nil
}

func (m *mockStreamSource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		close(m.records)
		m.closed = true
	}
	return nil
}

func (m *mockStreamSource) Send(records ...testRecord) {
	for _, r := range records {
		m.records <- r
	}
}

func TestStreamingCoordinator_ProcessRecords(t *testing.T) {
	source := newMockStreamSource(10)
	applier := &mockApplier{}

	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(1),
		courier.WithBufferSize(2),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start coordinator
	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Send records
	source.Send(
		testRecord{ID: "1", Data: "test1"},
		testRecord{ID: "2", Data: "test2"},
	)

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Close source and stop
	source.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}

	source.mu.Lock()
	ackedCount := len(source.acked)
	source.mu.Unlock()

	if ackedCount != 2 {
		t.Errorf("expected 2 acked records, got %d", ackedCount)
	}
}

func TestStreamingCoordinator_PartialFailure(t *testing.T) {
	source := newMockStreamSource(10)
	applier := &mockApplier{
		failIDs: map[string]bool{"2": true},
	}

	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(1),
		courier.WithBufferSize(3),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	source.Send(
		testRecord{ID: "1", Data: "test1"},
		testRecord{ID: "2", Data: "test2"},
		testRecord{ID: "3", Data: "test3"},
	)

	time.Sleep(100 * time.Millisecond)
	source.Close()

	<-done

	source.mu.Lock()
	ackedCount := len(source.acked)
	nackedCount := len(source.nacked)
	source.mu.Unlock()

	if ackedCount != 2 {
		t.Errorf("expected 2 acked records, got %d", ackedCount)
	}
	if nackedCount != 1 {
		t.Errorf("expected 1 nacked record, got %d", nackedCount)
	}
}

func TestStreamingCoordinator_FatalError(t *testing.T) {
	source := newMockStreamSource(10)
	applyErr := errors.New("fatal error")
	applier := &mockApplier{applyErr: applyErr}

	var capturedErr error
	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(1),
		courier.WithBufferSize(1),
		courier.WithStreamErrorHandler(func(err error) {
			capturedErr = err
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	source.Send(testRecord{ID: "1", Data: "test1"})

	time.Sleep(100 * time.Millisecond)
	source.Close()

	<-done

	if capturedErr == nil {
		t.Error("expected error to be captured")
	}

	source.mu.Lock()
	nackedCount := len(source.nacked)
	source.mu.Unlock()

	if nackedCount != 1 {
		t.Errorf("expected 1 nacked record on fatal error, got %d", nackedCount)
	}
}

func TestStreamingCoordinator_MultipleWorkers(t *testing.T) {
	source := newMockStreamSource(100)

	var processedCount int32
	applier := courier.ApplierFunc[testRecord](func(ctx context.Context, records []testRecord) ([]testRecord, []testRecord, error) {
		atomic.AddInt32(&processedCount, int32(len(records)))
		time.Sleep(10 * time.Millisecond) // Simulate work
		return records, nil, nil
	})

	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(4),
		courier.WithBufferSize(10),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	// Send many records
	for i := 0; i < 50; i++ {
		source.Send(testRecord{ID: string(rune('A' + i)), Data: "test"})
	}

	time.Sleep(200 * time.Millisecond)
	source.Close()

	<-done

	if atomic.LoadInt32(&processedCount) != 50 {
		t.Errorf("expected 50 processed records, got %d", processedCount)
	}
}

func TestStreamingCoordinator_Stop(t *testing.T) {
	source := newMockStreamSource(10)
	applier := &mockApplier{}

	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(1),
	)

	ctx := context.Background()

	done := make(chan error)
	go func() {
		done <- coord.Start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	if !coord.Running() {
		t.Error("coordinator should be running")
	}

	coord.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop")
	}

	if coord.Running() {
		t.Error("coordinator should not be running after stop")
	}
}

func TestStreamingCoordinator_ProcessSingle(t *testing.T) {
	source := newMockStreamSource(10)
	applier := &mockApplier{}

	coord := courier.NewStreamingCoordinator(source, applier)

	record := testRecord{ID: "single", Data: "test"}
	err := coord.ProcessSingle(context.Background(), record)
	if err != nil {
		t.Fatalf("ProcessSingle failed: %v", err)
	}

	source.mu.Lock()
	defer source.mu.Unlock()

	if len(source.acked) != 1 || source.acked[0] != "single" {
		t.Errorf("expected single record to be acked, got %v", source.acked)
	}
}

func TestStreamingCoordinator_SourceClosed(t *testing.T) {
	source := newMockStreamSource(10)
	applier := &mockApplier{}

	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(1),
		courier.WithBufferSize(10),
	)

	// Send some records then close the source
	go func() {
		source.Send(testRecord{ID: "1", Data: "test1"})
		source.Send(testRecord{ID: "2", Data: "test2"})
		time.Sleep(50 * time.Millisecond)
		source.Close() // Close the source
	}()

	// Start should return ErrSourceClosed when source closes
	err := coord.Start(context.Background())
	if !errors.Is(err, courier.ErrSourceClosed) {
		t.Errorf("expected ErrSourceClosed, got %v", err)
	}

	// Verify records were processed
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.acked) != 2 {
		t.Errorf("expected 2 acked records, got %d", len(source.acked))
	}
}
