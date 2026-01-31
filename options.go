package courier

import (
	"log/slog"
	"time"
)

// PollingOption configures a PollingCoordinator.
type PollingOption func(*pollingConfig)

type pollingConfig struct {
	interval       time.Duration
	batchSize      int
	maxRetries     int
	retryDelay     time.Duration
	logger         *slog.Logger
	errorHandler   func(error)
	metricsHandler MetricsHandler
}

func defaultPollingConfig() *pollingConfig {
	return &pollingConfig{
		interval:       5 * time.Second,
		batchSize:      100,
		maxRetries:     3,
		retryDelay:     time.Second,
		logger:         slog.Default(),
		errorHandler:   func(error) {},
		metricsHandler: noopMetrics{},
	}
}

// WithInterval sets the polling interval.
// Default: 5 seconds. Panics if duration is <= 0.
func WithInterval(d time.Duration) PollingOption {
	if d <= 0 {
		panic("courier: interval must be positive")
	}
	return func(c *pollingConfig) {
		c.interval = d
	}
}

// WithBatchSize sets the maximum number of records to fetch per poll.
// Default: 100. Panics if size is <= 0.
func WithBatchSize(size int) PollingOption {
	if size <= 0 {
		panic("courier: batch size must be positive")
	}
	return func(c *pollingConfig) {
		c.batchSize = size
	}
}

// WithMaxRetries sets the maximum number of retry attempts for failed operations.
// Default: 3. Use 0 for no retries. Panics if n < 0.
func WithMaxRetries(n int) PollingOption {
	if n < 0 {
		panic("courier: max retries cannot be negative")
	}
	return func(c *pollingConfig) {
		c.maxRetries = n
	}
}

// WithRetryDelay sets the delay between retry attempts.
// Default: 1 second. Panics if duration is < 0.
func WithRetryDelay(d time.Duration) PollingOption {
	if d < 0 {
		panic("courier: retry delay cannot be negative")
	}
	return func(c *pollingConfig) {
		c.retryDelay = d
	}
}

// WithLogger sets a custom logger.
// Default: slog.Default(). If nil is passed, uses slog.Default().
func WithLogger(logger *slog.Logger) PollingOption {
	return func(c *pollingConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithErrorHandler sets a callback for handling errors.
// This is called for all errors, including those that are retried.
// If nil is passed, uses a no-op handler.
func WithErrorHandler(handler func(error)) PollingOption {
	return func(c *pollingConfig) {
		if handler != nil {
			c.errorHandler = handler
		}
	}
}

// WithMetrics sets a metrics handler for observability.
// If nil is passed, uses a no-op handler.
func WithMetrics(handler MetricsHandler) PollingOption {
	return func(c *pollingConfig) {
		if handler != nil {
			c.metricsHandler = handler
		}
	}
}

// StreamingOption configures a StreamingCoordinator.
type StreamingOption func(*streamingConfig)

type streamingConfig struct {
	workers        int
	bufferSize     int
	logger         *slog.Logger
	errorHandler   func(error)
	metricsHandler MetricsHandler
}

func defaultStreamingConfig() *streamingConfig {
	return &streamingConfig{
		workers:        1,
		bufferSize:     100,
		logger:         slog.Default(),
		errorHandler:   func(error) {},
		metricsHandler: noopMetrics{},
	}
}

// WithWorkers sets the number of concurrent workers processing records.
// Default: 1. Panics if n <= 0.
func WithWorkers(n int) StreamingOption {
	if n <= 0 {
		panic("courier: workers must be positive")
	}
	return func(c *streamingConfig) {
		c.workers = n
	}
}

// WithBufferSize sets the internal buffer size for record processing.
// Default: 100. Panics if size <= 0.
func WithBufferSize(size int) StreamingOption {
	if size <= 0 {
		panic("courier: buffer size must be positive")
	}
	return func(c *streamingConfig) {
		c.bufferSize = size
	}
}

// WithStreamLogger sets a custom logger for streaming coordinator.
// If nil is passed, uses slog.Default().
func WithStreamLogger(logger *slog.Logger) StreamingOption {
	return func(c *streamingConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithStreamErrorHandler sets a callback for handling errors.
// If nil is passed, uses a no-op handler.
func WithStreamErrorHandler(handler func(error)) StreamingOption {
	return func(c *streamingConfig) {
		if handler != nil {
			c.errorHandler = handler
		}
	}
}

// WithStreamMetrics sets a metrics handler for streaming coordinator.
// If nil is passed, uses a no-op handler.
func WithStreamMetrics(handler MetricsHandler) StreamingOption {
	return func(c *streamingConfig) {
		if handler != nil {
			c.metricsHandler = handler
		}
	}
}
