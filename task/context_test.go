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

// forwardingProcessor forwards records and tracks errors
type forwardingProcessor[K, V any] struct {
	ctx       processor.Context[K, V]
	transform func(K, V) (K, V, error)
}

func newForwardingProcessor[K, V any](transform func(K, V) (K, V, error)) *forwardingProcessor[K, V] {
	return &forwardingProcessor[K, V]{transform: transform}
}

func (p *forwardingProcessor[K, V]) Init(ctx processor.Context[K, V]) {
	p.ctx = ctx
}

func (p *forwardingProcessor[K, V]) Process(ctx context.Context, r *record.Record[K, V]) error {
	if p.transform != nil {
		k, v, err := p.transform(r.Key, r.Value)
		if err != nil {
			return err
		}
		r.Key = k
		r.Value = v
	}
	return p.ctx.Forward(ctx, r)
}

func (p *forwardingProcessor[K, V]) Close() error {
	return nil
}

func TestContext_Forward_PropagatesDownstreamError(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var proc1Supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return newForwardingProcessor[string, string](nil)
	}
	topo.AddProcessor("proc1", proc1Supplier.ToUntyped(), "source")

	errDownstream := errors.New("downstream error")
	var proc2Supplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return "", "", errDownstream
			},
		)
	}
	topo.AddProcessor("proc2", proc2Supplier.ToUntyped(), "proc1")

	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc2",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mock.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	//nolint:errcheck
	defer tsk.Close()

	rec := newTestConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.ErrorIs(t, err, errDownstream)
}

func TestContext_Forward_MultipleChildren_StopsOnFirstError(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var forwarder processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("forwarder", forwarder.ToUntyped(), "source")

	// First child sink - will error because producer errors
	topo.AddSink(
		"sink1", "output1",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"forwarder",
	)

	// Second child sink - should not be reached
	topo.AddSink(
		"sink2", "output2",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"forwarder",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	// Use the mock client with error injection
	producer := mock.NewClient(mock.WithSendError(errors.New("producer error")))

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	//nolint:errcheck
	defer tsk.Close()

	rec := newTestConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	// Should have attempted to produce but failed - no records actually produced
	producer.AssertNoProducedRecords(t)
}

func TestContext_Forward_WrapsErrorWithNodeInfo(t *testing.T) {
	// Test that Forward wraps errors with context about which node failed

	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var forwarder processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewPassthroughProcessor[string, string]()
	}
	topo.AddProcessor("forwarder", forwarder.ToUntyped(), "source")

	// Child that errors
	childErr := errors.New("child processing error")
	var failingChild processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return "", "", childErr
			},
		)
	}
	topo.AddProcessor("failing-child", failingChild.ToUntyped(), "forwarder")

	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"failing-child",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mock.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	//nolint:errcheck
	defer tsk.Close()

	rec := newTestConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.ErrorIs(t, err, childErr)
	// Error should contain context about the forward path
	require.Contains(t, err.Error(), "forward")
}

func TestContext_ForwardTo_PropagatesError(t *testing.T) {
	// Test that ForwardTo properly propagates errors from named children
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	// Branch processor that uses ForwardTo
	predicates := []builtins.PredicateFunc[string, string]{
		func(_ context.Context, k, v string) (bool, error) {
			return true, nil
		},
	}
	var branchSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewBranchProcessor(predicates, []string{"target"})
	}
	topo.AddProcessor("branch", branchSupplier.ToUntyped(), "source")

	// Named child that errors
	childErr := errors.New("named child error")
	var failingChild processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(_ context.Context, k, v string) (string, string, error) {
				return "", "", childErr
			},
		)
	}
	topo.AddProcessorWithChildName("target-processor", failingChild.ToUntyped(), "branch", "target")

	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"target-processor",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mock.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	//nolint:errcheck
	defer tsk.Close()

	rec := newTestConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.ErrorIs(t, err, childErr)
}

func TestContext_ForwardTo_UnknownChildReturnsError(t *testing.T) {
	// Test that ForwardTo returns an error when the child name is not found
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	// Branch processor tries to forward to a child that doesn't exist
	predicates := []builtins.PredicateFunc[string, string]{
		func(_ context.Context, k, v string) (bool, error) {
			return true, nil
		},
	}
	var branchSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewBranchProcessor(predicates, []string{"missing-target"})
	}
	topo.AddProcessor("branch", branchSupplier.ToUntyped(), "source")

	// Note: We don't add a processor with child name "missing-target"
	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	producer := mock.NewClient()
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}

	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)
	//nolint:errcheck
	defer tsk.Close()

	rec := newTestConsumerRecord("input", 0, 0, "key", "value")
	err = tsk.Process(context.Background(), rec)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown child name")
}

func newTestConsumerRecord(topic string, partition int32, offset int64, key, value string) kafka.ConsumerRecord {
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
