package courier

import (
	"context"
	"sync"
	"time"
)

// Deduplicator tracks processed record IDs to prevent duplicate processing.
type Deduplicator[K comparable] struct {
	mu      sync.RWMutex
	seen    map[K]time.Time
	ttl     time.Duration
	maxSize int
}

// NewDeduplicator creates a deduplicator with the given TTL for entries.
// Set ttl to 0 for no expiration. Set maxSize to 0 for unlimited size.
func NewDeduplicator[K comparable](ttl time.Duration, maxSize int) *Deduplicator[K] {
	d := &Deduplicator[K]{
		seen:    make(map[K]time.Time),
		ttl:     ttl,
		maxSize: maxSize,
	}
	if ttl > 0 {
		go d.cleanupLoop()
	}
	return d
}

// IsDuplicate checks if the key has been seen before.
// Returns true if the key is a duplicate.
func (d *Deduplicator[K]) IsDuplicate(key K) bool {
	d.mu.RLock()
	_, exists := d.seen[key]
	d.mu.RUnlock()
	return exists
}

// MarkSeen marks a key as seen.
func (d *Deduplicator[K]) MarkSeen(key K) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Evict oldest if at max size
	if d.maxSize > 0 && len(d.seen) >= d.maxSize {
		d.evictOldest()
	}

	d.seen[key] = time.Now()
}

// Remove removes a key from the seen set.
func (d *Deduplicator[K]) Remove(key K) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, key)
}

// Clear removes all entries.
func (d *Deduplicator[K]) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = make(map[K]time.Time)
}

// Size returns the number of tracked keys.
func (d *Deduplicator[K]) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.seen)
}

func (d *Deduplicator[K]) evictOldest() {
	var oldestKey K
	var oldestTime time.Time
	first := true

	for k, t := range d.seen {
		if first || t.Before(oldestTime) {
			oldestKey = k
			oldestTime = t
			first = false
		}
	}

	if !first {
		delete(d.seen, oldestKey)
	}
}

func (d *Deduplicator[K]) cleanupLoop() {
	ticker := time.NewTicker(d.ttl / 2)
	defer ticker.Stop()

	for range ticker.C {
		d.cleanup()
	}
}

func (d *Deduplicator[K]) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for k, t := range d.seen {
		if now.Sub(t) > d.ttl {
			delete(d.seen, k)
		}
	}
}

// DeduplicatingApplier wraps an applier to skip duplicate records.
type DeduplicatingApplier[T any, K comparable] struct {
	applier Applier[T]
	dedup   *Deduplicator[K]
	getKey  func(T) K
}

// NewDeduplicatingApplier creates an applier that skips duplicates.
// The getKey function extracts a unique key from each record.
// Duplicate records are counted as synced (skipped).
func NewDeduplicatingApplier[T any, K comparable](
	applier Applier[T],
	dedup *Deduplicator[K],
	getKey func(T) K,
) *DeduplicatingApplier[T, K] {
	if applier == nil {
		panic("courier: applier cannot be nil")
	}
	if dedup == nil {
		panic("courier: dedup cannot be nil")
	}
	if getKey == nil {
		panic("courier: getKey cannot be nil")
	}
	return &DeduplicatingApplier[T, K]{
		applier: applier,
		dedup:   dedup,
		getKey:  getKey,
	}
}

// Apply implements Applier with deduplication.
func (d *DeduplicatingApplier[T, K]) Apply(ctx context.Context, records []T) ([]T, []T, error) {
	var unique []T
	var duplicates []T

	for _, r := range records {
		key := d.getKey(r)
		if d.dedup.IsDuplicate(key) {
			duplicates = append(duplicates, r)
		} else {
			unique = append(unique, r)
		}
	}

	if len(unique) == 0 {
		// All duplicates - count as synced
		return duplicates, nil, nil
	}

	synced, failed, err := d.applier.Apply(ctx, unique)
	if err != nil {
		return duplicates, append(failed, unique...), err
	}

	// Mark synced records as seen
	for _, r := range synced {
		d.dedup.MarkSeen(d.getKey(r))
	}

	// Duplicates count as synced
	synced = append(synced, duplicates...)
	return synced, failed, nil
}

// Deduplicator returns the underlying deduplicator for inspection.
func (d *DeduplicatingApplier[T, K]) Deduplicator() *Deduplicator[K] {
	return d.dedup
}
