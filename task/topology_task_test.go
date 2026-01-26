package task_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
)

// mockProducer captures records sent to it for verification
type mockProducer struct {
	records []producedRecord
	sendErr error
}

type producedRecord struct {
	topic   string
	key     []byte
	value   []byte
	headers map[string][]byte
}

func newMockProducer() *mockProducer {
	return &mockProducer{
		records: make([]producedRecord, 0),
	}
}

func (m *mockProducer) Send(_ context.Context, topic string, key, value []byte, headers map[string][]byte) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.records = append(
		m.records, producedRecord{
			topic:   topic,
			key:     key,
			value:   value,
			headers: headers,
		},
	)
	return nil
}

func (m *mockProducer) Flush(_ time.Duration) error {
	return nil
}

func (m *mockProducer) Close() {}

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

func (p *panicProcessor[K, V]) Process(_ *record.Record[K, V]) error {
	panic("intentional panic for testing")
}

func (p *panicProcessor[K, V]) Close() error {
	return nil
}

// closeTrackingProcessor tracks whether Close was called
type closeTrackingProcessor[K, V any] struct {
	closed bool
	ctx    processor.Context[K, V]
}

func newCloseTrackingProcessor[K, V any]() *closeTrackingProcessor[K, V] {
	return &closeTrackingProcessor[K, V]{}
}

func (p *closeTrackingProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *closeTrackingProcessor[K, V]) Process(r *record.Record[K, V]) error {
	return p.ctx.Forward(r)
}

func (p *closeTrackingProcessor[K, V]) Close() error {
	p.closed = true
	return nil
}

func newTestRecord(topic string, partition int32, offset int64, key, value string) kafka.ConsumerRecord {
	return kafka.ConsumerRecord{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		Key:         []byte(key),
		Value:       []byte(value),
		Headers:     make(map[string][]byte),
		Timestamp:   time.Now(),
		LeaderEpoch: 1,
	}
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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := newTestRecord("input", 0, 100, "test-key", "test-value")
	err = tsk.Process(rec)
	require.NoError(t, err)

	require.Len(t, producer.records, 1)
	require.Equal(t, "output", producer.records[0].topic)
	require.Equal(t, "test-key", string(producer.records[0].key))
	require.Equal(t, "test-value", string(producer.records[0].value))
}

func TestTopologyTask_OffsetTracking(t *testing.T) {
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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	_, ok := tsk.CurrentOffset()
	require.False(t, ok, "expected no offset before processing")

	// Process first record at offset 0
	rec1 := newTestRecord("input", 0, 0, "k1", "v1")
	err = tsk.Process(rec1)
	require.NoError(t, err)

	offset, ok := tsk.CurrentOffset()
	require.True(t, ok)
	require.Equal(t, int64(1), offset.Offset, "offset should be next offset to fetch (0+1)")

	// Process second record at offset 5
	rec2 := newTestRecord("input", 0, 5, "k2", "v2")
	err = tsk.Process(rec2)
	require.NoError(t, err)

	offset, ok = tsk.CurrentOffset()
	require.True(t, ok)
	require.Equal(t, int64(6), offset.Offset, "offset should be next offset to fetch (5+1)")
}

func TestTopologyTask_FilterProcessing(t *testing.T) {
	// Topology: Source -> Filter (pass if value != "drop") -> Sink

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var filterSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewFilterProcessor(
			func(k, v string) bool {
				return v != "drop"
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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	// Process record that should pass
	passRec := newTestRecord("input", 0, 0, "key1", "keep-me")
	err = tsk.Process(passRec)
	require.NoError(t, err)
	require.Len(t, producer.records, 1, "record should pass filter")

	// Process record that should be dropped
	dropRec := newTestRecord("input", 0, 1, "key2", "drop")
	err = tsk.Process(dropRec)
	require.NoError(t, err)
	require.Len(t, producer.records, 1, "record should be filtered out")

	// Process another record that passes
	passRec2 := newTestRecord("input", 0, 2, "key3", "also-keep")
	err = tsk.Process(passRec2)
	require.NoError(t, err)
	require.Len(t, producer.records, 2, "record should pass filter")
}

func TestTopologyTask_MapTransformation(t *testing.T) {
	// Topology: Source -> Map (uppercase value, prefix key) -> Sink

	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.String()), serde.ToUntypedDeserialser(serde.String()),
	)

	var mapSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(k, v string) (string, string) {
				return "prefix-" + k, "TRANSFORMED:" + v
			},
		)
	}
	topo.AddProcessor("map", mapSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "map",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := newTestRecord("input", 0, 0, "original-key", "original-value")
	err = tsk.Process(rec)
	require.NoError(t, err)

	require.Len(t, producer.records, 1)
	require.Equal(t, "prefix-original-key", string(producer.records[0].key))
	require.Equal(t, "TRANSFORMED:original-value", string(producer.records[0].value))
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
			func(k, v string) bool {
				return len(v) >= 5 && v[:5] == "valid"
			},
		)
	}
	topo.AddProcessor("filter", filterSupplier.ToUntyped(), "source")

	// Map: transform the value
	var mapSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(k, v string) (string, string) {
				return k, "processed:" + v
			},
		)
	}
	topo.AddProcessor("map", mapSupplier.ToUntyped(), "filter")
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.String()), serde.ToUntypedSerialiser(serde.String()), "map",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	// Record that passes filter
	validRec := newTestRecord("input", 0, 0, "k1", "valid-data")
	err = tsk.Process(validRec)
	require.NoError(t, err)
	require.Len(t, producer.records, 1)
	require.Equal(t, "processed:valid-data", string(producer.records[0].value))

	// Record that fails filter
	invalidRec := newTestRecord("input", 0, 1, "k2", "invalid")
	err = tsk.Process(invalidRec)
	require.NoError(t, err)
	require.Len(t, producer.records, 1, "filtered record should not reach sink")
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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := newTestRecord("input", 0, 0, "key", "value")
	err = tsk.Process(rec)

	// Error should be returned (panic recovered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic recovered")

	// Offset should still be updated (record was "processed" even if it errored)
	offset, ok := tsk.CurrentOffset()
	require.True(t, ok)
	require.Equal(t, int64(1), offset.Offset)
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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := newTestRecord("input", 0, 0, "key", "value")
	err = tsk.Process(rec)

	// Error should be returned for deserialization failure
	require.Error(t, err)
	require.Contains(t, err.Error(), "deserialize")

	// No record should be produced
	require.Len(t, producer.records, 0)

	// Offset should still be tracked (bad records are skipped, not retried forever)
	offset, ok := tsk.CurrentOffset()
	require.True(t, ok)
	require.Equal(t, int64(1), offset.Offset)
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

	producer := newMockProducer()
	producer.sendErr = errors.New("kafka unavailable")

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	rec := newTestRecord("input", 0, 0, "key", "value")
	err = tsk.Process(rec)

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

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "my-topic", Partition: 7}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	require.Equal(t, tp, tsk.Partition())
}

func TestTopologyTask_InitialOffsetState(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input", serde.ToUntypedDeserialser(serde.Bytes()), serde.ToUntypedDeserialser(serde.Bytes()),
	)
	topo.AddSink(
		"sink", "output", serde.ToUntypedSerialiser(serde.Bytes()), serde.ToUntypedSerialiser(serde.Bytes()), "source",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := newMockProducer()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	defer func() {
		closeErr := tsk.Close()
		require.NoError(t, closeErr)
	}()

	_, ok := tsk.CurrentOffset()
	require.False(t, ok, "no offset should be available before processing")
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

	producer := newMockProducer()
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
		Headers: map[string][]byte{
			"correlation-id": []byte("abc123"),
			"trace-id":       []byte("xyz789"),
		},
		Timestamp:   time.Now(),
		LeaderEpoch: 1,
	}

	err = tsk.Process(rec)
	require.NoError(t, err)

	require.Len(t, producer.records, 1)
	require.Equal(t, []byte("abc123"), producer.records[0].headers["correlation-id"])
	require.Equal(t, []byte("xyz789"), producer.records[0].headers["trace-id"])
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

	producer := newMockProducer()
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

	producer := newMockProducer()

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

	recA := newTestRecord("topic-a", 0, 0, "keyA", "valueA")
	err = taskA.Process(recA)
	require.NoError(t, err)

	recB := newTestRecord("topic-b", 0, 0, "keyB", "valueB")
	err = taskB.Process(recB)
	require.NoError(t, err)

	require.Len(t, producer.records, 2)
	require.Equal(t, "output-a", producer.records[0].topic)
	require.Equal(t, "output-b", producer.records[1].topic)
}
