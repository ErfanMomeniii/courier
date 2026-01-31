package courier_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erfanmomeniii/courier"
)

func TestRateLimiter_Basic(t *testing.T) {
	limiter := courier.NewRateLimiter(10, time.Second)

	// Should be able to acquire 10 tokens quickly
	for i := 0; i < 10; i++ {
		if !limiter.TryAcquire() {
			t.Errorf("expected to acquire token %d", i)
		}
	}

	// 11th should fail
	if limiter.TryAcquire() {
		t.Error("expected 11th acquire to fail")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	limiter := courier.NewRateLimiter(10, 100*time.Millisecond)

	// Exhaust tokens
	for i := 0; i < 10; i++ {
		limiter.TryAcquire()
	}

	// Wait for refill
	time.Sleep(150 * time.Millisecond)

	// Should have tokens again
	if !limiter.TryAcquire() {
		t.Error("expected tokens to refill")
	}
}

func TestRateLimiter_Wait(t *testing.T) {
	limiter := courier.NewRateLimiter(5, 100*time.Millisecond)

	// Exhaust tokens
	for i := 0; i < 5; i++ {
		limiter.TryAcquire()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := limiter.Wait(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected wait to succeed, got %v", err)
	}

	// Should have waited for at least some refill time
	if elapsed < 10*time.Millisecond {
		t.Error("expected wait to block")
	}
}

func TestRateLimiter_WaitCancellation(t *testing.T) {
	limiter := courier.NewRateLimiter(1, time.Minute) // Very slow refill

	// Exhaust token
	limiter.TryAcquire()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := limiter.Wait(ctx)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRateLimitedApplier_PerBatch(t *testing.T) {
	limiter := courier.NewRateLimiter(2, 100*time.Millisecond)
	var callCount int32

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		atomic.AddInt32(&callCount, 1)
		return records, nil, nil
	})

	rateLimited := courier.NewRateLimitedApplier(applier, limiter, true)

	ctx := context.Background()

	// First two calls should be immediate
	for i := 0; i < 2; i++ {
		_, _, err := rateLimited.Apply(ctx, []int{1, 2, 3})
		if err != nil {
			t.Fatalf("apply failed: %v", err)
		}
	}

	if atomic.LoadInt32(&callCount) != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestRateLimitedApplier_PerRecord(t *testing.T) {
	limiter := courier.NewRateLimiter(5, 100*time.Millisecond)
	var callCount int32

	applier := courier.ApplierFunc[int](func(ctx context.Context, records []int) ([]int, []int, error) {
		atomic.AddInt32(&callCount, 1)
		return records, nil, nil
	})

	rateLimited := courier.NewRateLimitedApplier(applier, limiter, false)

	ctx := context.Background()

	// Exhaust all 5 tokens with 5 records
	_, _, err := rateLimited.Apply(ctx, []int{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	limiter := courier.NewRateLimiter(10, time.Second)
	middleware := courier.RateLimitMiddleware[int](limiter, true)

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
}
