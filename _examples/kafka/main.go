// Example: Kafka consumer/producer with courier
//
// This example demonstrates using courier with Kafka for
// event-driven processing. It shows how to:
//   - Consume events from a Kafka topic using consumer groups
//   - Apply events and produce to another topic
//   - Use the streaming coordinator with circuit breaker
//
// Prerequisites:
//
//	docker-compose up -d  # With Kafka configuration
//
// Or use a managed Kafka service.
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
	"github.com/segmentio/kafka-go"
)

// KafkaEvent represents a Kafka message.
type KafkaEvent struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}

// KafkaSource consumes events from a Kafka topic.
type KafkaSource struct {
	reader *kafka.Reader
	logger *slog.Logger

	mu       sync.Mutex
	records  chan KafkaEvent
	closed   bool
	stopOnce sync.Once
}

// NewKafkaSource creates a new Kafka source.
func NewKafkaSource(brokers []string, topic, groupID string, logger *slog.Logger) *KafkaSource {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,        // 10KB
		MaxBytes:       10e6,        // 10MB
		MaxWait:        time.Second, // Reduced for demo
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // Disable auto-commit; we'll commit manually
	})

	return &KafkaSource{
		reader:  reader,
		logger:  logger,
		records: make(chan KafkaEvent, 100),
	}
}

// Start begins consuming from Kafka (call before using Records()).
func (s *KafkaSource) Start(ctx context.Context) {
	go s.readLoop(ctx)
}

func (s *KafkaSource) readLoop(ctx context.Context) {
	defer close(s.records)

	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		// Use FetchMessage (not ReadMessage) to manually control commits
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error("failed to fetch message", "error", err)
			time.Sleep(time.Second)
			continue
		}

		event := KafkaEvent{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Key:       string(msg.Key),
			Value:     msg.Value,
			Headers:   make(map[string]string),
			Timestamp: msg.Time,
		}

		for _, h := range msg.Headers {
			event.Headers[h.Key] = string(h.Value)
		}

		select {
		case s.records <- event:
			s.logger.Debug("received message",
				"topic", event.Topic,
				"partition", event.Partition,
				"offset", event.Offset,
			)
		case <-ctx.Done():
			return
		}
	}
}

// Records returns the channel of events.
func (s *KafkaSource) Records() <-chan KafkaEvent {
	return s.records
}

// Ack commits the message offset.
func (s *KafkaSource) Ack(ctx context.Context, record KafkaEvent) error {
	return s.reader.CommitMessages(ctx, kafka.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
	})
}

// Nack doesn't commit the offset, allowing reprocessing on restart.
func (s *KafkaSource) Nack(ctx context.Context, record KafkaEvent, err error) error {
	s.logger.Warn("message processing failed",
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"error", err,
	)
	// In production, you might:
	// 1. Send to a dead-letter topic
	// 2. Store in a database for later retry
	// 3. Alert on-call engineers
	return nil
}

// Close stops the source.
func (s *KafkaSource) Close() error {
	var closeErr error
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		closeErr = s.reader.Close()
	})
	return closeErr
}

// KafkaProducerApplier produces events to a Kafka topic.
type KafkaProducerApplier struct {
	writer *kafka.Writer
	topic  string
	logger *slog.Logger
}

// NewKafkaProducerApplier creates an applier that produces to Kafka.
func NewKafkaProducerApplier(brokers []string, topic string, logger *slog.Logger) *KafkaProducerApplier {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false, // Sync for reliability
	}

	return &KafkaProducerApplier{
		writer: writer,
		topic:  topic,
		logger: logger,
	}
}

// Apply produces events to the output topic.
func (a *KafkaProducerApplier) Apply(ctx context.Context, records []KafkaEvent) ([]KafkaEvent, []KafkaEvent, error) {
	var synced, failed []KafkaEvent

	messages := make([]kafka.Message, 0, len(records))
	for _, event := range records {
		// Transform the event for output
		output := map[string]interface{}{
			"source_topic":     event.Topic,
			"source_partition": event.Partition,
			"source_offset":    event.Offset,
			"processed_at":     time.Now().UTC().Format(time.RFC3339),
			"data":             json.RawMessage(event.Value),
		}

		data, err := json.Marshal(output)
		if err != nil {
			a.logger.Error("failed to marshal output", "error", err)
			failed = append(failed, event)
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(event.Key),
			Value: data,
			Headers: []kafka.Header{
				{Key: "source-topic", Value: []byte(event.Topic)},
				{Key: "source-offset", Value: []byte(fmt.Sprintf("%d", event.Offset))},
			},
		})
	}

	// Batch write to Kafka
	if len(messages) > 0 {
		if err := a.writer.WriteMessages(ctx, messages...); err != nil {
			a.logger.Error("failed to write messages", "error", err)
			// On batch failure, mark all as failed
			failed = append(failed, records[:len(messages)]...)
		} else {
			synced = append(synced, records[:len(messages)]...)
			a.logger.Info("produced messages",
				"topic", a.topic,
				"count", len(messages),
			)
		}
	}

	// Add already-failed events
	synced = append(synced, records[len(messages)+len(failed):]...)

	return synced, failed, nil
}

// Close closes the Kafka writer.
func (a *KafkaProducerApplier) Close() error {
	return a.writer.Close()
}

func main() {
	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	fmt.Println("Courier Kafka Example")
	fmt.Println("=====================")
	fmt.Println()

	// Kafka configuration
	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
	inputTopic := getEnv("KAFKA_INPUT_TOPIC", "events.input")
	outputTopic := getEnv("KAFKA_OUTPUT_TOPIC", "events.processed")
	groupID := getEnv("KAFKA_GROUP_ID", "courier-processor")

	logger.Info("connecting to Kafka",
		"brokers", brokers,
		"input_topic", inputTopic,
		"output_topic", outputTopic,
		"group_id", groupID,
	)

	// Create source
	source := NewKafkaSource(brokers, inputTopic, groupID, logger)
	defer source.Close()

	// Create producer applier
	producerApplier := NewKafkaProducerApplier(brokers, outputTopic, logger)
	defer producerApplier.Close()

	// Create DLQ for failed records
	dlq := courier.NewInMemoryDLQ[KafkaEvent](1000)

	// Build applier pipeline:
	// 1. Circuit breaker - prevent cascading failures when Kafka is unhealthy
	// 2. Retry - handle transient network issues
	// 3. DLQ - capture permanently failed records

	retryPolicy := courier.RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 200 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.15,
	}

	cbConfig := courier.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
		OnStateChange: func(from, to courier.CircuitState) {
			logger.Warn("circuit breaker state changed",
				"from", from.String(),
				"to", to.String(),
			)
		},
	}

	// Chain appliers
	applier := courier.NewCircuitBreakerApplier(
		courier.NewRetryableApplier(
			courier.NewDLQApplier(producerApplier, dlq),
			retryPolicy,
		),
		cbConfig,
	)

	// Create streaming coordinator
	coord := courier.NewStreamingCoordinator(source, applier,
		courier.WithWorkers(3),
		courier.WithBufferSize(50),
		courier.WithStreamLogger(logger),
		courier.WithStreamErrorHandler(func(err error) {
			logger.Error("streaming error", "error", err)
		}),
	)

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Start source
	source.Start(ctx)

	// Optionally, produce some test messages
	go produceTestMessages(ctx, brokers, inputTopic, logger)

	// Start coordinator
	logger.Info("starting streaming coordinator (Ctrl+C to stop)")

	if err := coord.Start(ctx); err != nil && err != context.Canceled {
		logger.Error("coordinator error", "error", err)
	}

	// Report DLQ status
	dlqCount, _ := dlq.Count(context.Background())
	logger.Info("shutdown complete",
		"dlq_count", dlqCount,
	)

	if dlqCount > 0 {
		fmt.Println("\nFailed records in DLQ:")
		for _, fr := range dlq.All() {
			fmt.Printf("  - Topic: %s, Partition: %d, Offset: %d, Attempts: %d\n",
				fr.Record.Topic, fr.Record.Partition, fr.Record.Offset, fr.Attempts)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func produceTestMessages(ctx context.Context, brokers []string, topic string, logger *slog.Logger) {
	// Wait a bit for the coordinator to start
	time.Sleep(2 * time.Second)

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	events := []struct {
		key   string
		value string
	}{
		{"user-1", `{"event": "user.created", "data": {"id": 1, "name": "Alice"}}`},
		{"order-100", `{"event": "order.placed", "data": {"id": 100, "total": 99.99}}`},
		{"payment-1000", `{"event": "payment.received", "data": {"id": 1000, "amount": 99.99}}`},
		{"user-1", `{"event": "user.updated", "data": {"id": 1, "email": "alice@example.com"}}`},
		{"order-100", `{"event": "order.shipped", "data": {"id": 100, "carrier": "FedEx"}}`},
	}

	for _, e := range events {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(e.key),
			Value: []byte(e.value),
		})
		if err != nil {
			logger.Error("failed to produce test message", "error", err)
			continue
		}
		logger.Info("produced test message", "key", e.key, "topic", topic)
		time.Sleep(500 * time.Millisecond)
	}
}
