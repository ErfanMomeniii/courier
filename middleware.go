package courier

import (
	"context"
	"time"
)

// Middleware is a function that wraps an Applier to add behavior.
type Middleware[T any] func(Applier[T]) Applier[T]

// Chain combines multiple middlewares into one.
// Middlewares are applied in order: first middleware is outermost.
func Chain[T any](middlewares ...Middleware[T]) Middleware[T] {
	return func(applier Applier[T]) Applier[T] {
		for i := len(middlewares) - 1; i >= 0; i-- {
			applier = middlewares[i](applier)
		}
		return applier
	}
}

// Hook points for the processing pipeline.
type Hooks[T any] struct {
	// BeforeFetch is called before fetching records from the source.
	BeforeFetch func(ctx context.Context)

	// AfterFetch is called after fetching records.
	AfterFetch func(ctx context.Context, records []T, err error)

	// BeforeApply is called before applying records.
	BeforeApply func(ctx context.Context, records []T)

	// AfterApply is called after applying records.
	AfterApply func(ctx context.Context, synced, failed []T, err error)

	// BeforeMark is called before marking records as synced.
	BeforeMark func(ctx context.Context, records []T)

	// AfterMark is called after marking records as synced.
	AfterMark func(ctx context.Context, records []T, err error)

	// OnRecord is called for each individual record during processing.
	OnRecord func(ctx context.Context, record T, err error)
}

// HookedSource wraps a BatchSource with hooks.
type HookedSource[T any] struct {
	source BatchSource[T]
	hooks  *Hooks[T]
}

// NewHookedSource creates a source with hook support.
func NewHookedSource[T any](source BatchSource[T], hooks *Hooks[T]) *HookedSource[T] {
	if hooks == nil {
		hooks = &Hooks[T]{}
	}
	return &HookedSource[T]{
		source: source,
		hooks:  hooks,
	}
}

// FetchRecords implements BatchSource with hooks.
func (h *HookedSource[T]) FetchRecords(ctx context.Context) ([]T, error) {
	if h.hooks.BeforeFetch != nil {
		h.hooks.BeforeFetch(ctx)
	}

	records, err := h.source.FetchRecords(ctx)

	if h.hooks.AfterFetch != nil {
		h.hooks.AfterFetch(ctx, records, err)
	}

	return records, err
}

// MarkAsSynced implements BatchSource with hooks.
func (h *HookedSource[T]) MarkAsSynced(ctx context.Context, records []T) error {
	if h.hooks.BeforeMark != nil {
		h.hooks.BeforeMark(ctx, records)
	}

	err := h.source.MarkAsSynced(ctx, records)

	if h.hooks.AfterMark != nil {
		h.hooks.AfterMark(ctx, records, err)
	}

	return err
}

// HookedApplier wraps an Applier with hooks.
type HookedApplier[T any] struct {
	applier Applier[T]
	hooks   *Hooks[T]
}

// NewHookedApplier creates an applier with hook support.
func NewHookedApplier[T any](applier Applier[T], hooks *Hooks[T]) *HookedApplier[T] {
	if hooks == nil {
		hooks = &Hooks[T]{}
	}
	return &HookedApplier[T]{
		applier: applier,
		hooks:   hooks,
	}
}

// Apply implements Applier with hooks.
func (h *HookedApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if h.hooks.BeforeApply != nil {
		h.hooks.BeforeApply(ctx, records)
	}

	synced, failed, err := h.applier.Apply(ctx, records)

	if h.hooks.AfterApply != nil {
		h.hooks.AfterApply(ctx, synced, failed, err)
	}

	return synced, failed, err
}

// LoggingMiddleware creates a middleware that logs apply operations.
// Panics if logFn is nil.
func LoggingMiddleware[T any](logFn func(format string, args ...any)) Middleware[T] {
	if logFn == nil {
		panic("courier: logFn cannot be nil")
	}
	return func(applier Applier[T]) Applier[T] {
		return ApplierFunc[T](func(ctx context.Context, records []T) ([]T, []T, error) {
			start := time.Now()
			logFn("applying %d records", len(records))

			synced, failed, err := applier.Apply(ctx, records)

			logFn("applied records: synced=%d failed=%d err=%v duration=%v",
				len(synced), len(failed), err, time.Since(start))

			return synced, failed, err
		})
	}
}

// TimingMiddleware creates a middleware that records timing metrics.
// Panics if onDuration is nil.
func TimingMiddleware[T any](onDuration func(d time.Duration)) Middleware[T] {
	if onDuration == nil {
		panic("courier: onDuration cannot be nil")
	}
	return func(applier Applier[T]) Applier[T] {
		return ApplierFunc[T](func(ctx context.Context, records []T) ([]T, []T, error) {
			start := time.Now()
			synced, failed, err := applier.Apply(ctx, records)
			onDuration(time.Since(start))
			return synced, failed, err
		})
	}
}

// RecoveryMiddleware creates a middleware that recovers from panics.
func RecoveryMiddleware[T any](onPanic func(recovered any)) Middleware[T] {
	return func(applier Applier[T]) Applier[T] {
		return ApplierFunc[T](func(ctx context.Context, records []T) (synced []T, failed []T, err error) {
			defer func() {
				if r := recover(); r != nil {
					if onPanic != nil {
						onPanic(r)
					}
					failed = records
					err = &ApplyError{Err: newPanicError("panic recovered")}
				}
			}()
			return applier.Apply(ctx, records)
		})
	}
}

// FilterMiddleware creates a middleware that filters records before applying.
// Records that don't match the predicate are skipped and counted as synced,
// allowing them to be marked as processed without being sent to the applier.
// Panics if predicate is nil.
func FilterMiddleware[T any](predicate func(T) bool) Middleware[T] {
	if predicate == nil {
		panic("courier: predicate cannot be nil")
	}
	return func(applier Applier[T]) Applier[T] {
		return ApplierFunc[T](func(ctx context.Context, records []T) ([]T, []T, error) {
			var filtered []T
			var skipped []T
			for _, r := range records {
				if predicate(r) {
					filtered = append(filtered, r)
				} else {
					skipped = append(skipped, r)
				}
			}

			if len(filtered) == 0 {
				return skipped, nil, nil // Skipped records count as synced
			}

			synced, failed, err := applier.Apply(ctx, filtered)
			synced = append(synced, skipped...) // Include skipped as synced
			return synced, failed, err
		})
	}
}

// TransformMiddleware creates a middleware that transforms records before applying.
// Panics if transform is nil.
func TransformMiddleware[T any](transform func(T) T) Middleware[T] {
	if transform == nil {
		panic("courier: transform cannot be nil")
	}
	return func(applier Applier[T]) Applier[T] {
		return ApplierFunc[T](func(ctx context.Context, records []T) ([]T, []T, error) {
			transformed := make([]T, len(records))
			for i, r := range records {
				transformed[i] = transform(r)
			}
			return applier.Apply(ctx, transformed)
		})
	}
}

type panicError struct{ msg string }

func (e *panicError) Error() string { return e.msg }

func newPanicError(msg string) error { return &panicError{msg} }
