// Example: Redis Streams with courier
//
// This example demonstrates using courier with Redis Streams for
// event-driven processing. It shows how to:
//   - Consume events from a Redis Stream using consumer groups
//   - Apply events to a destination
//   - Use the streaming coordinator for continuous processing
//
// Prerequisites:
//
//	docker run -d --name redis -p 6379:6379 redis
//
// Run with: go run main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/erfanmomeniii/courier"
	"github.com/redis/go-redis/v9"
)

// StreamEvent represents an event from Redis Stream.
type StreamEvent struct {
	ID      string          // Redis stream message ID
	Type    string          // Event type
	Payload json.RawMessage // Event data
}

// RedisStreamSource consumes events from a Redis Stream.
type RedisStreamSource struct {
	client    *redis.Client
	stream    string
	group     string
	consumer  string
	blockTime time.Duration
	logger    *slog.Logger

	mu       sync.Mutex
	records  chan StreamEvent
	closed   bool
	stopOnce sync.Once
}

// NewRedisStreamSource creates a new Redis Stream source.
func NewRedisStreamSource(
	client *redis.Client,
	stream, group, consumer string,
	logger *slog.Logger,
) *RedisStreamSource {
	return &RedisStreamSource{
		client:    client,
		stream:    stream,
		group:     group,
		consumer:  consumer,
		blockTime: 5 * time.Second,
		logger:    logger,
		records:   make(chan StreamEvent, 100),
	}
}

// Start begins consuming from the stream (call before using Records()).
func (s *RedisStreamSource) Start(ctx context.Context) error {
	// Create consumer group if it doesn't exist
	err := s.client.XGroupCreateMkStream(ctx, s.stream, s.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("create consumer group: %w", err)
	}

	// Start background reader
	go s.readLoop(ctx)
	return nil
}

func (s *RedisStreamSource) readLoop(ctx context.Context) {
	defer close(s.records)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		// Read from stream using consumer group
		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, ">"},
			Count:    10,
			Block:    s.blockTime,
		}).Result()

		if err != nil {
			if err == redis.Nil || ctx.Err() != nil {
				continue
			}
			s.logger.Error("failed to read from stream", "error", err)
			time.Sleep(time.Second)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				event := StreamEvent{
					ID: msg.ID,
				}

				// Extract event fields
				if v, ok := msg.Values["type"].(string); ok {
					event.Type = v
				}
				if v, ok := msg.Values["payload"].(string); ok {
					event.Payload = json.RawMessage(v)
				}

				select {
				case s.records <- event:
					s.logger.Debug("received event",
						"id", event.ID,
						"type", event.Type,
					)
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// Records returns the channel of events.
func (s *RedisStreamSource) Records() <-chan StreamEvent {
	return s.records
}

// Ack acknowledges successful processing.
func (s *RedisStreamSource) Ack(ctx context.Context, record StreamEvent) error {
	return s.client.XAck(ctx, s.stream, s.group, record.ID).Err()
}

// Nack indicates processing failure - the message will be redelivered.
func (s *RedisStreamSource) Nack(ctx context.Context, record StreamEvent, err error) error {
	// For Redis Streams, we don't need to do anything for NACK
	// The message will be redelivered on the next XREADGROUP with "0" ID
	// In production, you might move this to a separate dead-letter stream
	s.logger.Warn("event processing failed",
		"id", record.ID,
		"error", err,
	)
	return nil
}

// Close stops the source.
func (s *RedisStreamSource) Close() error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	})
	return nil
}

// RedisPublishApplier publishes events to Redis Pub/Sub.
type RedisPublishApplier struct {
	client  *redis.Client
	channel string
	logger  *slog.Logger
}

// NewRedisPublishApplier creates an applier that publishes to Redis Pub/Sub.
func NewRedisPublishApplier(client *redis.Client, channel string, logger *slog.Logger) *RedisPublishApplier {
	return &RedisPublishApplier{
		client:  client,
		channel: channel,
		logger:  logger,
	}
}

// Apply publishes events to Redis Pub/Sub.
func (a *RedisPublishApplier) Apply(ctx context.Context, records []StreamEvent) ([]StreamEvent, []StreamEvent, error) {
	var synced, failed []StreamEvent

	for _, event := range records {
		// Create message to publish
		msg := map[string]interface{}{
			"original_id": event.ID,
			"type":        event.Type,
			"payload":     event.Payload,
			"processed":   time.Now().UTC().Format(time.RFC3339),
		}

		data, err := json.Marshal(msg)
		if err != nil {
			a.logger.Error("failed to marshal event", "id", event.ID, "error", err)
			failed = append(failed, event)
			continue
		}

		// Publish to channel
		if err := a.client.Publish(ctx, a.channel, data).Err(); err != nil {
			a.logger.Error("failed to publish event", "id", event.ID, "error", err)
			failed = append(failed, event)
			continue
		}

		a.logger.Info("published event",
			"id", event.ID,
			"type", event.Type,
			"channel", a.channel,
		)
		synced = append(synced, event)
	}

	return synced, failed, nil
}

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("Courier Redis Streams Example")
	fmt.Println("==============================")
	fmt.Println()

	// Redis connection
	redisAddr := getEnv("REDIS_URL", "localhost:6379")
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer client.Close()

	// Verify connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	logger.Info("connected to Redis", "addr", redisAddr)

	// Configuration
	streamName := "events:outbox"
	groupName := "courier-processors"
	consumerName := "worker-1"
	publishChannel := "events:processed"

	// Add some test events to the stream
	if err := addTestEvents(ctx, client, streamName, logger); err != nil {
		logger.Error("failed to add test events", "error", err)
		os.Exit(1)
	}

	// Create source
	source := NewRedisStreamSource(client, streamName, groupName, consumerName, logger)

	// Create applier pipeline with retry
	baseApplier := NewRedisPublishApplier(client, publishChannel, logger)

	retryPolicy := courier.RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}

	applier := courier.NewRetryableApplier(baseApplier, retryPolicy).
		OnRetry(func(record StreamEvent, attempt int, err error) {
			logger.Warn("retrying event",
				"id", record.ID,
				"attempt", attempt,
			)
		})

	// Create streaming coordinator
	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(2),
		courier.WithBufferSize(10),
		courier.WithStreamLogger(logger),
		courier.WithStreamErrorHandler(func(err error) {
			logger.Error("streaming error", "error", err)
		}),
	)

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Start source
	if err := source.Start(ctx); err != nil {
		logger.Error("failed to start source", "error", err)
		os.Exit(1)
	}

	// Start a goroutine to listen for published messages (demo only)
	go subscribeToProcessed(ctx, client, publishChannel, logger)

	// Start coordinator
	logger.Info("starting streaming coordinator (Ctrl+C to stop)")

	if err := coord.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("coordinator error", "error", err)
	}

	logger.Info("shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func addTestEvents(ctx context.Context, client *redis.Client, stream string, logger *slog.Logger) error {
	// Check if stream already has pending messages
	info, err := client.XInfoStream(ctx, stream).Result()
	if err == nil && info.Length > 0 {
		logger.Info("stream already has events", "count", info.Length)
		return nil
	}

	// Add test events
	events := []struct {
		eventType string
		payload   string
	}{
		{"user.created", `{"user_id": 1, "name": "Alice"}`},
		{"order.placed", `{"order_id": 100, "total": 99.99}`},
		{"payment.received", `{"payment_id": 1000, "amount": 99.99}`},
		{"user.updated", `{"user_id": 1, "email": "alice@example.com"}`},
		{"order.shipped", `{"order_id": 100, "carrier": "FedEx"}`},
	}

	for _, e := range events {
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{
				"type":    e.eventType,
				"payload": e.payload,
			},
		}).Result()
		if err != nil {
			return fmt.Errorf("add event to stream: %w", err)
		}
		logger.Debug("added event to stream", "type", e.eventType)
	}

	logger.Info("added test events to stream", "count", len(events))
	return nil
}

func subscribeToProcessed(ctx context.Context, client *redis.Client, channel string, logger *slog.Logger) {
	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			logger.Info("subscriber received processed event",
				"channel", msg.Channel,
				"payload_size", len(msg.Payload),
			)
		}
	}
}
