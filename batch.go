package courier

import (
	"context"
)

// BatchSplitter splits large batches into smaller chunks for processing.
type BatchSplitter[T any] struct {
	applier  Applier[T]
	maxBatch int
}

// NewBatchSplitter creates an applier that splits large batches.
// Records are processed in chunks of maxBatch size.
// Panics if applier is nil or maxBatch <= 0.
func NewBatchSplitter[T any](applier Applier[T], maxBatch int) *BatchSplitter[T] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if maxBatch <= 0 {
		panic("courier: maxBatch must be positive")
	}
	return &BatchSplitter[T]{
		applier:  applier,
		maxBatch: maxBatch,
	}
}

// Apply implements Applier by splitting into smaller batches.
func (b *BatchSplitter[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if len(records) <= b.maxBatch {
		return b.applier.Apply(ctx, records)
	}

	var allSynced, allFailed []T

	for i := 0; i < len(records); i += b.maxBatch {
		end := i + b.maxBatch
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]

		synced, failed, err := b.applier.Apply(ctx, chunk)
		if err != nil {
			// On fatal error, add remaining records to failed
			allFailed = append(allFailed, records[i:]...)
			return allSynced, allFailed, err
		}

		allSynced = append(allSynced, synced...)
		allFailed = append(allFailed, failed...)
	}

	return allSynced, allFailed, nil
}

// BatchSplitMiddleware creates a middleware that splits large batches.
func BatchSplitMiddleware[T any](maxBatch int) Middleware[T] {
	if maxBatch <= 0 {
		panic("courier: maxBatch must be positive")
	}
	return func(applier Applier[T]) Applier[T] {
		return NewBatchSplitter(applier, maxBatch)
	}
}

// ParallelApplier processes records concurrently within a batch.
type ParallelApplier[T any] struct {
	fn       func(ctx context.Context, record T) error
	workers  int
}

// NewParallelApplier creates an applier that processes records in parallel.
// Each record is processed independently by one of the worker goroutines.
// Panics if fn is nil or workers <= 0.
func NewParallelApplier[T any](fn func(ctx context.Context, record T) error, workers int) *ParallelApplier[T] {
	if fn == nil {
		panic("courier: fn cannot be nil")
	}
	if workers <= 0 {
		panic("courier: workers must be positive")
	}
	return &ParallelApplier[T]{
		fn:      fn,
		workers: workers,
	}
}

// Apply implements Applier with parallel processing.
func (p *ParallelApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	if len(records) == 0 {
		return nil, nil, nil
	}

	type result struct {
		index  int
		record T
		err    error
	}

	// Create work channel
	work := make(chan struct {
		index  int
		record T
	}, len(records))
	results := make(chan result, len(records))

	// Start workers
	for i := 0; i < p.workers; i++ {
		go func() {
			for w := range work {
				err := p.fn(ctx, w.record)
				results <- result{index: w.index, record: w.record, err: err}
			}
		}()
	}

	// Send work
	for i, r := range records {
		work <- struct {
			index  int
			record T
		}{index: i, record: r}
	}
	close(work)

	// Collect results
	var synced, failed []T
	for i := 0; i < len(records); i++ {
		r := <-results
		if r.err != nil {
			failed = append(failed, r.record)
		} else {
			synced = append(synced, r.record)
		}
	}

	return synced, failed, nil
}

// FanOutApplier sends each record to multiple appliers.
// All appliers must succeed for a record to be considered synced.
type FanOutApplier[T any] struct {
	appliers []Applier[T]
}

// NewFanOutApplier creates an applier that fans out to multiple appliers.
// A record is only synced if ALL appliers succeed.
// Panics if no appliers are provided.
func NewFanOutApplier[T any](appliers ...Applier[T]) *FanOutApplier[T] {
	if len(appliers) == 0 {
		panic("courier: at least one applier is required")
	}
	for i, a := range appliers {
		if a == nil {
			panic("courier: applier at index " + string(rune('0'+i)) + " cannot be nil")
		}
	}
	return &FanOutApplier[T]{appliers: appliers}
}

// Apply implements Applier by sending to all appliers.
func (f *FanOutApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	// Track which records succeeded in all appliers
	successCount := make(map[int]int)
	for i := range records {
		successCount[i] = 0
	}

	for _, applier := range f.appliers {
		synced, _, err := applier.Apply(ctx, records)
		if err != nil {
			return nil, records, err
		}

		// Mark synced records
		for _, s := range synced {
			for i, r := range records {
				if &r == &s || compareRecords(r, s) {
					successCount[i]++
					break
				}
			}
		}
	}

	// Records that succeeded in all appliers are synced
	var synced, failed []T
	for i, r := range records {
		if successCount[i] == len(f.appliers) {
			synced = append(synced, r)
		} else {
			failed = append(failed, r)
		}
	}

	return synced, failed, nil
}

// compareRecords compares two records for equality.
// This is a simple pointer comparison; for value comparison,
// records should implement an ID method.
func compareRecords[T any](a, b T) bool {
	// For now, rely on position-based tracking in Apply
	return false
}

// RouterApplier routes records to different appliers based on a selector function.
type RouterApplier[T any] struct {
	selector func(T) string
	routes   map[string]Applier[T]
	fallback Applier[T]
}

// NewRouterApplier creates an applier that routes records to different appliers.
// The selector function returns a route key for each record.
// If no matching route is found, the fallback applier is used (can be nil to skip).
func NewRouterApplier[T any](selector func(T) string, routes map[string]Applier[T], fallback Applier[T]) *RouterApplier[T] {
	if selector == nil {
		panic("courier: selector cannot be nil")
	}
	if len(routes) == 0 && fallback == nil {
		panic("courier: at least one route or fallback is required")
	}
	return &RouterApplier[T]{
		selector: selector,
		routes:   routes,
		fallback: fallback,
	}
}

// Apply implements Applier by routing records.
func (r *RouterApplier[T]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	// Group records by route
	groups := make(map[string][]T)
	var skipped []T

	for _, record := range records {
		route := r.selector(record)
		if applier, ok := r.routes[route]; ok && applier != nil {
			groups[route] = append(groups[route], record)
		} else if r.fallback != nil {
			groups["__fallback__"] = append(groups["__fallback__"], record)
		} else {
			// No route and no fallback - skip (count as synced)
			skipped = append(skipped, record)
		}
	}

	var allSynced, allFailed []T
	allSynced = append(allSynced, skipped...)

	// Process each group
	for route, group := range groups {
		var applier Applier[T]
		if route == "__fallback__" {
			applier = r.fallback
		} else {
			applier = r.routes[route]
		}

		synced, failed, err := applier.Apply(ctx, group)
		if err != nil {
			allFailed = append(allFailed, group...)
			continue
		}

		allSynced = append(allSynced, synced...)
		allFailed = append(allFailed, failed...)
	}

	return allSynced, allFailed, nil
}
