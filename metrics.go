package courier

import "time"

// MetricsHandler defines the interface for collecting metrics.
// Implement this to integrate with your observability stack
// (Prometheus, StatsD, OpenTelemetry, etc.).
type MetricsHandler interface {
	// RecordsFetched is called after fetching records from the source.
	RecordsFetched(count int)

	// RecordsApplied is called after applying records.
	RecordsApplied(synced, failed int)

	// RecordsMarked is called after marking records as synced.
	RecordsMarked(count int)

	// PollDuration records the time taken for a complete poll cycle.
	PollDuration(d time.Duration)

	// ApplyDuration records the time taken to apply a batch.
	ApplyDuration(d time.Duration)

	// ErrorOccurred is called when an error occurs.
	ErrorOccurred(errType string)
}

// noopMetrics is the default no-op metrics handler.
type noopMetrics struct{}

func (noopMetrics) RecordsFetched(int)          {}
func (noopMetrics) RecordsApplied(int, int)     {}
func (noopMetrics) RecordsMarked(int)           {}
func (noopMetrics) PollDuration(time.Duration)  {}
func (noopMetrics) ApplyDuration(time.Duration) {}
func (noopMetrics) ErrorOccurred(string)        {}

// MetricsFunc provides a simple way to implement MetricsHandler
// using callback functions. Nil callbacks are safely ignored.
type MetricsFunc struct {
	OnRecordsFetched func(count int)
	OnRecordsApplied func(synced, failed int)
	OnRecordsMarked  func(count int)
	OnPollDuration   func(d time.Duration)
	OnApplyDuration  func(d time.Duration)
	OnErrorOccurred  func(errType string)
}

func (m MetricsFunc) RecordsFetched(count int) {
	if m.OnRecordsFetched != nil {
		m.OnRecordsFetched(count)
	}
}

func (m MetricsFunc) RecordsApplied(synced, failed int) {
	if m.OnRecordsApplied != nil {
		m.OnRecordsApplied(synced, failed)
	}
}

func (m MetricsFunc) RecordsMarked(count int) {
	if m.OnRecordsMarked != nil {
		m.OnRecordsMarked(count)
	}
}

func (m MetricsFunc) PollDuration(d time.Duration) {
	if m.OnPollDuration != nil {
		m.OnPollDuration(d)
	}
}

func (m MetricsFunc) ApplyDuration(d time.Duration) {
	if m.OnApplyDuration != nil {
		m.OnApplyDuration(d)
	}
}

func (m MetricsFunc) ErrorOccurred(errType string) {
	if m.OnErrorOccurred != nil {
		m.OnErrorOccurred(errType)
	}
}
