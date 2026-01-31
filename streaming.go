package courier

import (
	"context"
	"sync"
)

// StreamingCoordinator orchestrates event-driven sync operations.
// It continuously processes records from a StreamSource, applies them
// using an Applier, and acknowledges processed records.
//
// Use this for message queues like Kafka, RabbitMQ, or Redis Streams.
type StreamingCoordinator[T any] struct {
	source  StreamSource[T]
	applier Applier[T]
	config  *streamingConfig

	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewStreamingCoordinator creates a new streaming-based coordinator.
func NewStreamingCoordinator[T any](
	source StreamSource[T],
	applier Applier[T],
	opts ...StreamingOption,
) *StreamingCoordinator[T] {
	config := defaultStreamingConfig()
	for _, opt := range opts {
		opt(config)
	}

	return &StreamingCoordinator[T]{
		source:  source,
		applier: applier,
		config:  config,
	}
}

// Start begins processing records from the stream. It blocks until
// the context is cancelled, Stop is called, or the source closes.
func (c *StreamingCoordinator[T]) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true

	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	// Create buffered channel for batch processing
	batchCh := make(chan []T, c.config.workers)

	// Start workers
	for i := 0; i < c.config.workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i, batchCh)
	}

	// Collect records into batches
	result := c.collector(ctx, batchCh)

	// Wait for workers to finish
	close(batchCh)
	c.wg.Wait()

	c.config.logger.Info("streaming coordinator stopped")

	// Return appropriate error based on how collector terminated
	if result == collectorSourceClosed {
		return ErrSourceClosed
	}
	return ctx.Err()
}

// Stop gracefully stops the coordinator.
func (c *StreamingCoordinator[T]) Stop() {
	c.mu.RLock()
	if !c.running {
		c.mu.RUnlock()
		return
	}
	cancel := c.cancel
	c.mu.RUnlock()

	if cancel != nil {
		cancel()
	}
}

// Running returns whether the coordinator is currently running.
func (c *StreamingCoordinator[T]) Running() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

// collectorResult indicates how the collector terminated
type collectorResult int

const (
	collectorContextDone collectorResult = iota
	collectorSourceClosed
)

func (c *StreamingCoordinator[T]) collector(ctx context.Context, batchCh chan<- []T) collectorResult {
	records := c.source.Records()
	batch := make([]T, 0, c.config.bufferSize)

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch
			if len(batch) > 0 {
				batchCh <- batch
			}
			return collectorContextDone

		case record, ok := <-records:
			if !ok {
				// Source closed, flush remaining batch
				if len(batch) > 0 {
					batchCh <- batch
				}
				return collectorSourceClosed
			}

			batch = append(batch, record)

			// Flush when batch is full
			if len(batch) >= c.config.bufferSize {
				batchCh <- batch
				batch = make([]T, 0, c.config.bufferSize)
			}
		}
	}
}

func (c *StreamingCoordinator[T]) worker(ctx context.Context, id int, batchCh <-chan []T) {
	defer c.wg.Done()

	for batch := range batchCh {
		c.processBatch(ctx, batch)
	}
}

func (c *StreamingCoordinator[T]) processBatch(ctx context.Context, records []T) {
	if len(records) == 0 {
		return
	}

	c.config.logger.Debug("processing batch", "count", len(records))

	// Apply records
	synced, failed, err := c.applier.Apply(ctx, records)

	if err != nil {
		c.config.logger.Error("fatal error applying records", "error", err)
		c.config.errorHandler(err)
		c.config.metricsHandler.ErrorOccurred("apply_fatal")
		// Nack all records on fatal error
		for _, record := range records {
			if nackErr := c.source.Nack(ctx, record, err); nackErr != nil {
				c.config.logger.Error("failed to nack record", "error", nackErr)
			}
		}
		return
	}

	c.config.metricsHandler.RecordsApplied(len(synced), len(failed))

	// Ack synced records
	for _, record := range synced {
		if err := c.source.Ack(ctx, record); err != nil {
			c.config.logger.Error("failed to ack record", "error", err)
			c.config.errorHandler(err)
			c.config.metricsHandler.ErrorOccurred("ack")
		}
	}

	// Nack failed records
	for _, record := range failed {
		if nackErr := c.source.Nack(ctx, record, &ApplyError{Record: record}); nackErr != nil {
			c.config.logger.Error("failed to nack record", "error", nackErr)
			c.config.errorHandler(nackErr)
			c.config.metricsHandler.ErrorOccurred("nack")
		}
	}

	if len(failed) > 0 {
		c.config.logger.Warn("some records failed to apply",
			"synced", len(synced),
			"failed", len(failed),
		)
	}
}

// ProcessSingle provides a simple interface for processing individual
// records without batching. Useful for real-time processing requirements.
func (c *StreamingCoordinator[T]) ProcessSingle(ctx context.Context, record T) error {
	synced, failed, err := c.applier.Apply(ctx, []T{record})
	if err != nil {
		if nackErr := c.source.Nack(ctx, record, err); nackErr != nil {
			c.config.logger.Error("failed to nack record", "error", nackErr)
		}
		return err
	}

	if len(failed) > 0 {
		applyErr := &ApplyError{Record: record}
		if nackErr := c.source.Nack(ctx, record, applyErr); nackErr != nil {
			c.config.logger.Error("failed to nack record", "error", nackErr)
		}
		return applyErr
	}

	if len(synced) > 0 {
		if err := c.source.Ack(ctx, record); err != nil {
			c.config.logger.Error("failed to ack record", "error", err)
			return err
		}
	}

	return nil
}
