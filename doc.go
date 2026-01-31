// Package courier provides a lightweight library for eventual consistency
// coordination in Go. It offers generic interfaces and coordinators for
// reliably syncing data between systems using polling (batch) or
// event-driven (streaming) patterns.
//
// The library follows the transactional outbox pattern and similar
// eventual consistency approaches without forcing any specific
// infrastructure dependencies.
//
// # Core Concepts
//
// Sources define where records come from:
//   - BatchSource: for polling-based fetching (databases, files)
//   - StreamSource: for event-driven streaming (message queues)
//
// Appliers define what happens to records:
//   - Applier: processes records and reports success/failure
//
// Coordinators orchestrate the sync process:
//   - PollingCoordinator: periodically fetches and processes batches
//   - StreamingCoordinator: continuously processes streaming records
//
// # Basic Usage
//
//	// Define your record type
//	type OutboxEvent struct {
//	    ID      string
//	    Payload []byte
//	}
//
//	// Implement BatchSource for your database
//	type PostgresSource struct { ... }
//
//	// Implement Applier for your message broker
//	type KafkaApplier struct { ... }
//
//	// Create and run coordinator
//	coord := courier.NewPollingCoordinator(source, applier,
//	    courier.WithInterval(5 * time.Second),
//	    courier.WithBatchSize(100),
//	)
//	coord.Start(ctx)
package courier
