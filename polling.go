package courier

import (
	"context"
	"sync"
	"time"
)

// PollingCoordinator orchestrates batch-based sync operations.
// It periodically fetches records from a BatchSource, applies them
// using an Applier, and marks successful records as synced.
//
// Use this for database outbox tables, file-based queues, or any
// source that requires polling.
type PollingCoordinator[T any] struct {
	source  BatchSource[T]
	applier Applier[T]
	config  *pollingConfig

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewPollingCoordinator creates a new polling-based coordinator.
func NewPollingCoordinator[T any](
	source BatchSource[T],
	applier Applier[T],
	opts ...PollingOption,
) *PollingCoordinator[T] {
	config := defaultPollingConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &PollingCoordinator[T]{
		source:  source,
		applier: applier,
		config:  config,
	}
}

// Start begins the polling loop. It blocks until the context is
// cancelled or Stop is called. Returns nil on graceful shutdown.
func (c *PollingCoordinator[T]) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		close(c.doneCh)
		c.mu.Unlock()
	}()

	ticker := time.NewTicker(c.config.interval)
	defer ticker.Stop()

	// Run immediately on start
	c.poll(ctx)

	for {
		select {
		case <-ctx.Done():
			c.config.logger.Info("polling coordinator stopped by context")
			return ctx.Err()
		case <-c.stopCh:
			c.config.logger.Info("polling coordinator stopped")
			return nil
		case <-ticker.C:
			c.poll(ctx)
		}
	}
}

// Stop gracefully stops the coordinator and waits for it to finish.
func (c *PollingCoordinator[T]) Stop() {
	c.mu.RLock()
	if !c.running {
		c.mu.RUnlock()
		return
	}
	stopCh := c.stopCh
	doneCh := c.doneCh
	c.mu.RUnlock()

	close(stopCh)
	<-doneCh
}

// Running returns whether the coordinator is currently running.
func (c *PollingCoordinator[T]) Running() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// RunOnce executes a single poll cycle. Useful for testing or
// manual triggering. Returns ErrCoordinatorRunning if called while
// coordinator is running via Start().
func (c *PollingCoordinator[T]) RunOnce(ctx context.Context) error {
	c.mu.RLock()
	running := c.running
	c.mu.RUnlock()

	// Prevent RunOnce while Start() is running to avoid race conditions
	if running {
		return ErrCoordinatorRunning
	}

	return c.poll(ctx)
}

func (c *PollingCoordinator[T]) poll(ctx context.Context) error {
	start := time.Now()
	defer func() {
		c.config.metricsHandler.PollDuration(time.Since(start))
	}()

	// Fetch records
	records, err := c.source.FetchRecords(ctx)
	if err != nil {
		c.config.logger.Error("failed to fetch records", "error", err)
		c.config.errorHandler(&FetchError{Err: err})
		c.config.metricsHandler.ErrorOccurred("fetch")
		return err
	}

	c.config.metricsHandler.RecordsFetched(len(records))

	if len(records) == 0 {
		c.config.logger.Debug("no records to process")
		return nil
	}

	c.config.logger.Debug("fetched records", "count", len(records))

	// Apply records
	applyStart := time.Now()
	synced, failed, err := c.applier.Apply(ctx, records)
	c.config.metricsHandler.ApplyDuration(time.Since(applyStart))

	if err != nil {
		c.config.logger.Error("fatal error applying records", "error", err)
		c.config.errorHandler(err)
		c.config.metricsHandler.ErrorOccurred("apply_fatal")
		return err
	}

	c.config.metricsHandler.RecordsApplied(len(synced), len(failed))

	if len(failed) > 0 {
		c.config.logger.Warn("some records failed to apply",
			"synced", len(synced),
			"failed", len(failed),
		)
	}

	// Mark synced records
	if len(synced) > 0 {
		if err := c.source.MarkAsSynced(ctx, synced); err != nil {
			c.config.logger.Error("failed to mark records as synced",
				"count", len(synced),
				"error", err,
			)
			c.config.errorHandler(&MarkError{Count: len(synced), Err: err})
			c.config.metricsHandler.ErrorOccurred("mark")
			return err
		}
		c.config.metricsHandler.RecordsMarked(len(synced))
		c.config.logger.Debug("marked records as synced", "count", len(synced))
	}

	return nil
}
