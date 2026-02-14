//go:build unit

package task_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
)

// failingDeserializer always returns an error on deserialize
type failingDeserializer struct{}

func (f failingDeserializer) Deserialise(_ string, _ []byte) (any, error) {
	return nil, errors.New("intentional deserialization failure")
}

// panicProcessor panics when processing records
type panicProcessor[K, V any] struct {
	ctx processor.Context[K, V]
}

func newPanicProcessor[K, V any]() *panicProcessor[K, V] {
	return &panicProcessor[K, V]{}
}

func (p *panicProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *panicProcessor[K, V]) Process(_ context.Context, _ *record.Record[K, V]) error {
	panic("intentional panic for testing")
}

func (p *panicProcessor[K, V]) Close() error {
	return nil
}

func TestTopologyTask_BasicProcessing(t *testing.T) {
	// Topology: Source("input") -> Passthrough -> Sink("output")
	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var passthroughSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("passthrough", passthroughSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"passthrough",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 100, "test-key", "test-value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	producer.AssertProducedCount(t, 1)
	producer.AssertProducedCountForTopic(t, "output", 1)
	producer.AssertProducedString(t, "output", "test-key", "test-value")
}

func TestTopologyTask_MultipleRecordsProcessing(t *testing.T) {
	// Test processing multiple records sequentially
	// Note: Offset tracking is now handled by the client via MarkRecords/Commit,
	// not by the task itself. This test verifies task processing still works correctly.
	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)

	var supplier processor.Supplier[[]byte, []byte, []byte, []byte] = func() processor.Processor[[]byte, []byte, []byte, []byte] {
		return builtins.NewPassthroughProcessor[[]byte, []byte]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	client := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	// Process multiple records - simulating what the runner does
	rec1 := mockkafka.ConsumerRecord("input", 0, 0, "k1", "v1")
	err = tsk.Process(context.Background(), rec1)
	require.NoError(t, err)
	// Runner would call: client.MarkRecords(rec1)

	rec2 := mockkafka.ConsumerRecord("input", 0, 1, "k2", "v2")
	err = tsk.Process(context.Background(), rec2)
	require.NoError(t, err)
	// Runner would call: client.MarkRecords(rec2)

	rec3 := mockkafka.ConsumerRecord("input", 0, 2, "k3", "v3")
	err = tsk.Process(context.Background(), rec3)
	require.NoError(t, err)
	// Runner would call: client.MarkRecords(rec3)

	// Verify all records were produced
	client.AssertProducedCount(t, 3)
	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertProducedString(t, "output", "k2", "v2")
	client.AssertProducedString(t, "output", "k3", "v3")

	// Demonstrate the mark/commit pattern the runner would use
	client.MarkRecords(rec1, rec2, rec3)
	client.AssertMarkedCount(t, 3)
	client.AssertMarkedOffset(t, tp, 3) // next offset = 2 + 1

	err = client.Commit(context.Background())
	require.NoError(t, err)
	client.AssertCommittedOffset(t, tp, 3)
}

func TestTopologyTask_FilterProcessing(t *testing.T) {
	// Topology: Source -> Filter (pass if value != "drop") -> Sink

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var filterSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewFilterProcessor(
			func(_ context.Context, k, v string) (bool, error) {
				return v != "drop", nil
			},
		)
	}
	topo.AddProcessor("filter", filterSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"filter",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	// Process record that should pass
	passRec := mockkafka.ConsumerRecord("input", 0, 0, "key1", "keep-me")
	err = tsk.Process(context.Background(), passRec)
	require.NoError(t, err)
	producer.AssertProducedCount(t, 1)

	// Process record that should be dropped
	dropRec := mockkafka.ConsumerRecord("input", 0, 1, "key2", "drop")
	err = tsk.Process(context.Background(), dropRec)
	require.NoError(t, err)
	producer.AssertProducedCount(t, 1) // Still 1, not 2

	// Process another record that passes
	passRec2 := mockkafka.ConsumerRecord("input", 0, 2, "key3", "also-keep")
	err = tsk.Process(context.Background(), passRec2)
	require.NoError(t, err)
	producer.AssertProducedCount(t, 2)
}

func TestTopologyTask_MapTransformation(t *testing.T) {
	// Topology: Source -> Map (uppercase value, prefix key) -> Sink

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var mapSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return "prefix-" + k, "TRANSFORMED:" + v, nil
			},
		)
	}
	topo.AddProcessor("map", mapSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "map",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "original-key", "original-value")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	producer.AssertProducedCount(t, 1)
	producer.AssertProducedString(t, "output", "prefix-original-key", "TRANSFORMED:original-value")
}

func TestTopologyTask_ChainedProcessors(t *testing.T) {
	// Topology: Source -> Filter -> Map -> Sink

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	// Filter: only pass values starting with "valid"
	var filterSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewFilterProcessor(
			func(_ context.Context, k, v string) (bool, error) {
				return len(v) >= 5 && v[:5] == "valid", nil
			},
		)
	}
	topo.AddProcessor("filter", filterSupplier.ToUntyped(), "source")

	// Map: transform the value
	var mapSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return k, "processed:" + v, nil
			},
		)
	}
	topo.AddProcessor("map", mapSupplier.ToUntyped(), "filter")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "map",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	// Record that passes filter
	validRec := mockkafka.ConsumerRecord("input", 0, 0, "k1", "valid-data")
	err = tsk.Process(context.Background(), validRec)
	require.NoError(t, err)
	producer.AssertProducedCount(t, 1)

	records := producer.ProducedRecords()
	require.Equal(t, "processed:valid-data", string(records[0].Value))

	// Record that fails filter
	invalidRec := mockkafka.ConsumerRecord("input", 0, 1, "k2", "invalid")
	err = tsk.Process(context.Background(), invalidRec)
	require.NoError(t, err)
	producer.AssertProducedCount(t, 1) // Still 1, filtered record doesn't reach sink
}

func TestTopologyTask_PanicRecovery(t *testing.T) {
	// Test: Processor panic is recovered and error is returned
	// The task should continue to be usable after a panic

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var panicSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return newPanicProcessor[string, string]()
	}
	topo.AddProcessor("panic-proc", panicSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"panic-proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	client := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	// Error should be returned (panic recovered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic recovered")

	// No record should be produced since panic occurred
	client.AssertNoProducedRecords(t)

	// Runner decides what to do with failed records - it might:
	// 1. Skip to DLQ and still mark the record
	// 2. Retry
	// 3. Stop processing
	// The task doesn't track offsets - that's the runner's job via MarkRecords
}

func TestTopologyTask_DeserializationError(t *testing.T) {
	topo := topology.New()
	// Use failing deserializer for value
	topo.AddSource("source", "input", serde.ToUntypedDeserialser(serde.Bytes()), failingDeserializer{})

	var supplier processor.Supplier[[]byte, any, []byte, any] = func() processor.Processor[[]byte, any, []byte, any] {
		return builtins.NewPassthroughProcessor[[]byte, any]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	client := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	// Error should be returned for deserialization failure
	require.Error(t, err)
	require.Contains(t, err.Error(), "deserialize")

	// No record should be produced
	client.AssertNoProducedRecords(t)

	// Demonstrate that runner can still mark the record (e.g., for DLQ handling)
	// This is the runner's decision, not the task's
	client.MarkRecords(rec)
	client.AssertMarkedCount(t, 1)
	client.AssertMarkedOffset(t, tp, 1) // next offset = 0 + 1

	err = client.Commit(context.Background())
	require.NoError(t, err)
	client.AssertCommittedOffset(t, tp, 1)
}

func TestTopologyTask_ProducerError(t *testing.T) {
	// Test: Producer errors are propagated back

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient(mockkafka.WithSendError(errors.New("kafka unavailable")))

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	// Error should be returned
	require.Error(t, err)
	require.Contains(t, err.Error(), "kafka unavailable")
}

func TestTopologyTask_Partition(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "my-topic", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)

	var supplier processor.Supplier[[]byte, []byte, []byte, []byte] = func() processor.Processor[[]byte, []byte, []byte, []byte] {
		return builtins.NewPassthroughProcessor[[]byte, []byte]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "my-topic", Partition: 7}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	require.Equal(t, tp, tsk.Partition())
}

func TestTopologyTask_IsClosed(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "source",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)

	require.False(t, tsk.IsClosed(), "task should not be closed initially")

	err = tsk.Close()
	require.NoError(t, err)

	require.True(t, tsk.IsClosed(), "task should be closed after Close()")
}

func TestTopologyTask_HeaderPreservation(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := kafka.ConsumerRecord{
		Topic:     "input",
		Partition: 0,
		Offset:    0,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Headers: []kafka.Header{
			{Key: "correlation-id", Value: []byte("abc123")},
			{Key: "trace-id", Value: []byte("xyz789")},
		},
		Timestamp:   time.Now(),
		LeaderEpoch: 1,
	}

	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)

	producer.AssertProducedCount(t, 1)
	producer.AssertHeader(t, "output", []byte("key"), "correlation-id", []byte("abc123"))
	producer.AssertHeader(t, "output", []byte("key"), "trace-id", []byte("xyz789"))
}

func TestTopologyTaskFactory_NoSourceNodes(t *testing.T) {
	topo := topology.New()
	topo.AddSink("sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()))

	_, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no source nodes")
}

func TestTopologyTaskFactory_UnknownSourceTopic(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "known-topic", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "source",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "unknown-topic", Partition: 0}

	_, err = factory.CreateTask(tp, producer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no source node for topic")
}

func TestTopologyTaskFactory_DuplicateSourceTopic(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source1", "same-topic", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSource(
		"source2", "same-topic", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)

	_, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate source topic")
}

func TestTopologyTaskFactory_MultipleSourceTopics(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source1", "topic-a", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSource(
		"source2", "topic-b", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSink(
		"sink1", "output-a", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()),
		"source1",
	)
	topo.AddSink(
		"sink2", "output-b", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()),
		"source2",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()

	taskA, err := factory.CreateTask(kafka.TopicPartition{Topic: "topic-a", Partition: 0}, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := taskA.Close()
		require.NoError(t, closeErr)
	}()

	taskB, err := factory.CreateTask(kafka.TopicPartition{Topic: "topic-b", Partition: 0}, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := taskB.Close()
		require.NoError(t, closeErr)
	}()

	recA := mockkafka.ConsumerRecord("topic-a", 0, 0, "keyA", "valueA")
	err = taskA.Process(context.Background(), recA)
	require.NoError(t, err)

	recB := mockkafka.ConsumerRecord("topic-b", 0, 0, "keyB", "valueB")
	err = taskB.Process(context.Background(), recB)
	require.NoError(t, err)

	producer.AssertProducedCount(t, 2)
	producer.AssertProducedCountForTopic(t, "output-a", 1)
	producer.AssertProducedCountForTopic(t, "output-b", 1)
}

func TestTopologyTask_ProcessorError(t *testing.T) {
	// Test: Processor returns error (not panic)
	processorErr := errors.New("processor failed")

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var errorSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return "", "", processorErr
			},
		)
	}
	topo.AddProcessor("error-proc", errorSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"error-proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.ErrorIs(t, err, processorErr)

	// No record should be produced
	producer.AssertNoProducedRecords(t)
}

func TestTopologyTask_FilterError(t *testing.T) {
	// Test: Filter predicate returns error
	filterErr := errors.New("filter predicate failed")

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var filterSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewFilterProcessor(
			func(_ context.Context, k, v string) (bool, error) {
				return false, filterErr
			},
		)
	}
	topo.AddProcessor("filter", filterSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"filter",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.ErrorIs(t, err, filterErr)

	producer.AssertNoProducedRecords(t)
}

func TestTopologyTask_MarkAndCommitIntegration(t *testing.T) {
	// Integration test demonstrating the full mark/commit flow
	// that the runner would use after processing records

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	client := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)
	defer func() { _ = tsk.Close() }()

	// Simulate runner processing a batch of records
	records := []kafka.ConsumerRecord{
		mockkafka.ConsumerRecord("input", 0, 0, "k1", "v1"),
		mockkafka.ConsumerRecord("input", 0, 1, "k2", "v2"),
		mockkafka.ConsumerRecord("input", 0, 2, "k3", "v3"),
	}

	for _, rec := range records {
		err = tsk.Process(context.Background(), rec)
		require.NoError(t, err)

		// Runner marks each record after successful processing
		client.MarkRecords(rec)
	}

	// Verify processing worked
	client.AssertProducedCount(t, 3)

	// Verify marking state before commit
	client.AssertMarkedCount(t, 3)
	client.AssertMarkedOffset(t, tp, 3) // next offset to fetch = 2 + 1

	// Runner commits periodically
	err = client.Commit(context.Background())
	require.NoError(t, err)

	// Verify commit state
	client.AssertCommittedOffset(t, tp, 3)
	client.AssertNoMarkedRecords(t) // cleared after successful commit
}

func TestTopologyTask_MultiPartitionMarkAndCommit(t *testing.T) {
	// Test that mark/commit works correctly across multiple partitions
	// with a shared client (as would happen with a single consumer)

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	// Single client shared by multiple tasks (like a real consumer)
	client := mockkafka.NewClient()

	tp0 := kafka.TopicPartition{Topic: "input", Partition: 0}
	tp1 := kafka.TopicPartition{Topic: "input", Partition: 1}

	task0, err := factory.CreateTask(tp0, client)
	require.NoError(t, err)
	defer func() { _ = task0.Close() }()

	task1, err := factory.CreateTask(tp1, client)
	require.NoError(t, err)
	defer func() { _ = task1.Close() }()

	// Process and mark records on partition 0
	rec0 := mockkafka.ConsumerRecord("input", 0, 5, "k0", "v0")
	err = task0.Process(context.Background(), rec0)
	require.NoError(t, err)
	client.MarkRecords(rec0)

	// Process and mark records on partition 1
	rec1 := mockkafka.ConsumerRecord("input", 1, 10, "k1", "v1")
	err = task1.Process(context.Background(), rec1)
	require.NoError(t, err)
	client.MarkRecords(rec1)

	// Verify both are marked with correct offsets
	client.AssertMarkedCount(t, 2)
	client.AssertMarkedOffset(t, tp0, 6)  // 5 + 1
	client.AssertMarkedOffset(t, tp1, 11) // 10 + 1

	// Single commit covers all partitions
	err = client.Commit(context.Background())
	require.NoError(t, err)

	// Verify both partitions are committed
	client.AssertCommittedOffset(t, tp0, 6)
	client.AssertCommittedOffset(t, tp1, 11)
}

// failingSerializer always returns an error on serialize
type failingSerializer struct{}

func (f failingSerializer) Serialise(_ string, _ any) ([]byte, error) {
	return nil, errors.New("intentional serialization failure")
}

func TestTopologyTask_SinkSerdeError_NotWrappedAsProcessError(t *testing.T) {
	// Verify that a SerdeError from sink serialization is NOT wrapped in ProcessError
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("proc", supplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		failingSerializer{}, // key serializer that always fails
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mockkafka.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() { _ = tsk.Close() }()

	rec := mockkafka.ConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)

	// Should be a SerdeError, NOT a ProcessError
	_, isSerde := task.AsSerdeError(err)
	require.True(t, isSerde, "expected SerdeError but got %T", err)

	_, isProcess := task.AsProcessError(err)
	require.False(t, isProcess, "SerdeError should not be wrapped as ProcessError")
}

func TestTopologyTask_CommitError(t *testing.T) {
	// Test that commit errors are properly propagated and marked records preserved

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()),
		"source",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	commitErr := errors.New("commit failed")
	client := mockkafka.NewClient(mockkafka.WithCommitError(commitErr))
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)
	defer func() { _ = tsk.Close() }()

	// Process and mark a record
	rec := mockkafka.ConsumerRecord("input", 0, 0, "k", "v")
	err = tsk.Process(context.Background(), rec)
	require.NoError(t, err)
	client.MarkRecords(rec)

	// Commit should fail
	err = client.Commit(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, commitErr)

	// Marked records should still be there (not cleared on error)
	// This allows retry
	client.AssertMarkedCount(t, 1)
}
