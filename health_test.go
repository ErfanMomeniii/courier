package courier_test

import (
	"context"
	"errors"
	"testing"

	"github.com/erfanmomeniii/courier"
)

func TestHealthCheck_InitialState(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	if health.Status() != courier.HealthStatusHealthy {
		t.Errorf("expected healthy, got %s", health.Status())
	}
	if !health.IsHealthy() {
		t.Error("expected IsHealthy to return true")
	}
}

func TestHealthCheck_RecordSuccess(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	health.RecordSuccess(5)

	details := health.Details()
	if details.TotalProcessed != 5 {
		t.Errorf("expected 5 processed, got %d", details.TotalProcessed)
	}
	if details.ConsecutiveOK != 1 {
		t.Errorf("expected 1 consecutive OK, got %d", details.ConsecutiveOK)
	}
}

func TestHealthCheck_DegradedAfterFailures(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	// Record 3 failures (degraded threshold)
	for i := 0; i < 3; i++ {
		health.RecordFailure(1, errors.New("test error"))
	}

	if health.Status() != courier.HealthStatusDegraded {
		t.Errorf("expected degraded, got %s", health.Status())
	}
}

func TestHealthCheck_UnhealthyAfterManyFailures(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	// Record 10 failures (unhealthy threshold)
	for i := 0; i < 10; i++ {
		health.RecordFailure(1, errors.New("test error"))
	}

	if health.Status() != courier.HealthStatusUnhealthy {
		t.Errorf("expected unhealthy, got %s", health.Status())
	}
}

func TestHealthCheck_RecoveryAfterSuccess(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	// Make it degraded
	for i := 0; i < 5; i++ {
		health.RecordFailure(1, nil)
	}

	if health.Status() != courier.HealthStatusDegraded {
		t.Errorf("expected degraded, got %s", health.Status())
	}

	// Record success - should reset
	health.RecordSuccess(1)

	if health.Status() != courier.HealthStatusHealthy {
		t.Errorf("expected healthy after success, got %s", health.Status())
	}

	details := health.Details()
	if details.ConsecutiveErrs != 0 {
		t.Errorf("expected 0 consecutive errors after success, got %d", details.ConsecutiveErrs)
	}
}

func TestHealthCheck_Details(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)

	health.RecordSuccess(10)
	health.RecordSuccess(5)
	health.RecordFailure(2, errors.New("test"))

	details := health.Details()

	if details.TotalProcessed != 15 {
		t.Errorf("expected 15 processed, got %d", details.TotalProcessed)
	}
	if details.TotalFailed != 2 {
		t.Errorf("expected 2 failed, got %d", details.TotalFailed)
	}
	if details.LastError == nil {
		t.Error("expected last error to be set")
	}
	if details.ConsecutiveErrs != 1 {
		t.Errorf("expected 1 consecutive error, got %d", details.ConsecutiveErrs)
	}
}

func TestHealthCheckApplier(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		return records, nil, nil
	})

	healthApplier := courier.NewHealthCheckApplier(applier, health)

	synced, _, err := healthApplier.Apply(context.Background(), []int{1, 2, 3})

	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if len(synced) != 3 {
		t.Errorf("expected 3 synced, got %d", len(synced))
	}

	details := health.Details()
	if details.TotalProcessed != 3 {
		t.Errorf("expected 3 processed, got %d", details.TotalProcessed)
	}
}

func TestHealthCheckApplier_RecordsFailure(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		return nil, records, nil // All failed
	})

	healthApplier := courier.NewHealthCheckApplier(applier, health)

	healthApplier.Apply(context.Background(), []int{1, 2, 3})

	details := health.Details()
	if details.TotalFailed != 3 {
		t.Errorf("expected 3 failed, got %d", details.TotalFailed)
	}
	if details.ConsecutiveErrs != 1 {
		t.Errorf("expected 1 consecutive error, got %d", details.ConsecutiveErrs)
	}
}

func TestHealthCheckApplier_RecordsFatalError(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		return nil, nil, errors.New("fatal error")
	})

	healthApplier := courier.NewHealthCheckApplier(applier, health)

	healthApplier.Apply(context.Background(), []int{1, 2, 3})

	details := health.Details()
	if details.LastError == nil {
		t.Error("expected last error to be set")
	}
	if details.ConsecutiveErrs != 1 {
		t.Errorf("expected 1 consecutive error, got %d", details.ConsecutiveErrs)
	}
}

func TestHealthCheckApplier_Health(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)
	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		return records, nil, nil
	})

	healthApplier := courier.NewHealthCheckApplier(applier, health)

	if healthApplier.Health() != health {
		t.Error("Health() should return the underlying health check")
	}
}

func TestHealthCheckMiddleware(t *testing.T) {
	health := courier.NewHealthCheck(3, 10)
	middleware := courier.HealthCheckMiddleware[int](health)

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
	if health.Details().TotalProcessed != 3 {
		t.Errorf("expected health to track 3 processed")
	}
}

func TestHealthStatus_String(t *testing.T) {
	tests := []struct {
		status   courier.HealthStatus
		expected string
	}{
		{courier.HealthStatusHealthy, "healthy"},
		{courier.HealthStatusDegraded, "degraded"},
		{courier.HealthStatusUnhealthy, "unhealthy"},
	}

	for _, tt := range tests {
		if string(tt.status) != tt.expected {
			t.Errorf("expected %s, got %s", tt.expected, tt.status)
		}
	}
}
