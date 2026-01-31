package courier

import (
	"errors"
	"fmt"
)

var (
	// ErrCoordinatorStopped is returned when operations are attempted
	// on a stopped coordinator.
	ErrCoordinatorStopped = errors.New("courier: coordinator stopped")

	// ErrCoordinatorRunning is returned when an operation cannot be performed
	// because the coordinator is already running (e.g., calling RunOnce while Start is active).
	ErrCoordinatorRunning = errors.New("courier: coordinator is running")

	// ErrSourceClosed is returned when the source has been closed.
	ErrSourceClosed = errors.New("courier: source closed")

	// ErrNoRecords is returned when no records are available.
	// This is informational, not an error condition.
	ErrNoRecords = errors.New("courier: no records available")
)

// ApplyError wraps errors that occur during record application.
type ApplyError struct {
	// Record is the record that failed to be applied.
	// Type is interface{} to avoid generic type in error.
	Record interface{}

	// Err is the underlying error.
	Err error
}

func (e *ApplyError) Error() string {
	return fmt.Sprintf("courier: failed to apply record: %v", e.Err)
}

func (e *ApplyError) Unwrap() error {
	return e.Err
}

// FetchError wraps errors that occur during record fetching.
type FetchError struct {
	Err error
}

func (e *FetchError) Error() string {
	return fmt.Sprintf("courier: failed to fetch records: %v", e.Err)
}

func (e *FetchError) Unwrap() error {
	return e.Err
}

// MarkError wraps errors that occur when marking records as synced.
type MarkError struct {
	Count int
	Err   error
}

func (e *MarkError) Error() string {
	return fmt.Sprintf("courier: failed to mark %d records as synced: %v", e.Count, e.Err)
}

func (e *MarkError) Unwrap() error {
	return e.Err
}
