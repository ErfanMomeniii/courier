package courier

import (
	"context"
	"sync"
	"time"
)

// HealthStatus represents the health state of a component.
type HealthStatus string

const (
	// HealthStatusHealthy indicates the component is operating normally.
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusDegraded indicates the component is operating with issues.
	HealthStatusDegraded HealthStatus = "degraded"
	// HealthStatusUnhealthy indicates the component is not operating.
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// HealthCheck provides health status information for a coordinator.
type HealthCheck struct {
	mu               sync.RWMutex
	status           HealthStatus
	lastSuccessTime  time.Time
	lastErrorTime    time.Time
	lastError        error
	consecutiveErrs  int
	consecutiveOK    int
	totalProcessed   int64
	totalFailed      int64
	degradedThreshold int
	unhealthyThreshold int
}

// NewHealthCheck creates a health check with configurable thresholds.
// degradedThreshold: consecutive errors before marking as degraded.
// unhealthyThreshold: consecutive errors before marking as unhealthy.
func NewHealthCheck(degradedThreshold, unhealthyThreshold int) *HealthCheck {
	if degradedThreshold <= 0 {
		degradedThreshold = 3
	}
	if unhealthyThreshold <= 0 {
		unhealthyThreshold = 10
	}
	return &HealthCheck{
		status:             HealthStatusHealthy,
		degradedThreshold:  degradedThreshold,
		unhealthyThreshold: unhealthyThreshold,
	}
}

// RecordSuccess records a successful operation.
func (h *HealthCheck) RecordSuccess(processed int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastSuccessTime = time.Now()
	h.consecutiveOK++
	h.consecutiveErrs = 0
	h.totalProcessed += int64(processed)
	h.updateStatus()
}

// RecordFailure records a failed operation.
func (h *HealthCheck) RecordFailure(failed int, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastErrorTime = time.Now()
	h.lastError = err
	h.consecutiveErrs++
	h.consecutiveOK = 0
	h.totalFailed += int64(failed)
	h.updateStatus()
}

func (h *HealthCheck) updateStatus() {
	switch {
	case h.consecutiveErrs >= h.unhealthyThreshold:
		h.status = HealthStatusUnhealthy
	case h.consecutiveErrs >= h.degradedThreshold:
		h.status = HealthStatusDegraded
	default:
		h.status = HealthStatusHealthy
	}
}

// Status returns the current health status.
func (h *HealthCheck) Status() HealthStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.status
}

// IsHealthy returns true if the status is healthy.
func (h *HealthCheck) IsHealthy() bool {
	return h.Status() == HealthStatusHealthy
}

// Details returns detailed health information.
func (h *HealthCheck) Details() HealthDetails {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return HealthDetails{
		Status:           h.status,
		LastSuccessTime:  h.lastSuccessTime,
		LastErrorTime:    h.lastErrorTime,
		LastError:        h.lastError,
		ConsecutiveErrs:  h.consecutiveErrs,
		ConsecutiveOK:    h.consecutiveOK,
		TotalProcessed:   h.totalProcessed,
		TotalFailed:      h.totalFailed,
	}
}

// HealthDetails contains detailed health information.
type HealthDetails struct {
	Status           HealthStatus
	LastSuccessTime  time.Time
	LastErrorTime    time.Time
	LastError        error
	ConsecutiveErrs  int
	ConsecutiveOK    int
	TotalProcessed   int64
	TotalFailed      int64
}

// HealthCheckApplier wraps an applier to track health.
type HealthCheckApplier[T any] struct {
	applier Applier[T]
	health  *HealthCheck
}

// NewHealthCheckApplier creates an applier that tracks health metrics.
func NewHealthCheckApplier[T any](applier Applier[T], health *HealthCheck) *HealthCheckApplier[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if health == nil {
		health = NewHealthCheck(3, 10)
	}
	return &HealthCheckApplier[T]{
		applier: applier,
		health:  health,
	}
}

// Apply implements Applier with health tracking.
func (h *HealthCheckApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	synced, failed, err := h.applier.Apply(ctx, records)

	if err != nil {
		h.health.RecordFailure(len(records), err)
	} else if len(failed) > len(synced) {
		h.health.RecordFailure(len(failed), nil)
	} else {
		h.health.RecordSuccess(len(synced))
	}

	return synced, failed, err
}

// Health returns the health check for inspection.
func (h *HealthCheckApplier[T]) Health() *HealthCheck {
	return h.health
}

// HealthCheckMiddleware creates a middleware that tracks health.
func HealthCheckMiddleware[T any](health *HealthCheck) Middleware[T] {
	return func(applier Applier[T]) Applier[T] {
		return NewHealthCheckApplier(applier, health)
	}
}
