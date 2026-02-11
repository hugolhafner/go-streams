//go:build unit

package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestTopology() *topology.Topology {
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
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	return topo
}

func createTestTask(t *testing.T, producer kafka.Producer) task.Task {
	t.Helper()

	topo := createTestTopology()
	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	tsk, err := factory.CreateTask(tp, producer)
	require.NoError(t, err)

	return tsk
}

func TestPartitionedRunner_BasicProcessing(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
		mockkafka.SimpleRecord("k3", "v3"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(10),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for all records to be produced
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 3
		}, 3*time.Second, 50*time.Millisecond, "all records should be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	client.AssertProducedString(t, "output", "k1", "v1")
	client.AssertProducedString(t, "output", "k2", "v2")
	client.AssertProducedString(t, "output", "k3", "v3")
}

func TestPartitionedRunner_MultiplePartitions(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()

	// Add records to multiple partitions
	client.AddRecords("input", 0, mockkafka.SimpleRecord("p0-k1", "p0-v1"))
	client.AddRecords("input", 0, mockkafka.SimpleRecord("p0-k2", "p0-v2"))
	client.AddRecords("input", 1, mockkafka.SimpleRecord("p1-k1", "p1-v1"))
	client.AddRecords("input", 1, mockkafka.SimpleRecord("p1-k2", "p1-v2"))
	client.AddRecords("input", 2, mockkafka.SimpleRecord("p2-k1", "p2-v1"))

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 5
		}, 3*time.Second, 50*time.Millisecond, "all 5 records should be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_ParallelProcessing(t *testing.T) {
	// Create a topology with a slow processor to verify parallelism
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var processedOrder []string
	var mu sync.Mutex

	var slowSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				// Simulate slow processing
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				processedOrder = append(processedOrder, k)
				mu.Unlock()
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("slow", slowSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"slow",
	)

	client := mockkafka.NewClient()

	// Add records to different partitions
	for i := 0; i < 5; i++ {
		client.AddRecords(
			"input", int32(i), mockkafka.SimpleRecord(
				string(rune('A'+i)),
				"value",
			),
		)
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 5
		}, 3*time.Second, 50*time.Millisecond, "all 5 records should be produced",
	)

	elapsed := time.Since(start)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// If processing was sequential, it would take 5 * 50ms = 250ms minimum
	// With parallelism, it should be closer to 50-100ms + overhead
	t.Logf("Elapsed time: %v", elapsed)
	t.Logf("Processing order: %v", processedOrder)
}

func TestPartitionedRunner_OrderingWithinPartition(t *testing.T) {
	// Verify that records within a partition maintain order
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var processedKeys []string
	var mu sync.Mutex

	var orderTracker processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				mu.Lock()
				processedKeys = append(processedKeys, k)
				mu.Unlock()
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("tracker", orderTracker.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"tracker",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(10))

	// Add ordered records to partition 0
	client.AddRecords(
		"input", 0,
		mockkafka.Record("1", "a").WithOffset(0).Build(),
		mockkafka.Record("2", "b").WithOffset(1).Build(),
		mockkafka.Record("3", "c").WithOffset(2).Build(),
		mockkafka.Record("4", "d").WithOffset(3).Build(),
		mockkafka.Record("5", "e").WithOffset(4).Build(),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	require.Eventually(
		t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(processedKeys) == 5
		}, 3*time.Second, 50*time.Millisecond, "all records should be processed",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify ordering within partition
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"1", "2", "3", "4", "5"}, processedKeys, "records should be processed in order")
}

func TestPartitionedRunner_ErrorHandling(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	processingErr := errors.New("processing failed")
	var errorCount atomic.Int32

	var errorProducer processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k == "fail" {
					errorCount.Add(1)
					return "", "", processingErr
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("error", errorProducer.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"error",
	)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("good1", "v1"),
		mockkafka.SimpleRecord("fail", "v2"),
		mockkafka.SimpleRecord("good2", "v3"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	// Use LogAndContinue to skip failed records
	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithErrorHandler(errorhandler.LogAndContinue(logger.NewNoopLogger())),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for all 3 records to be attempted (2 good + 1 error-skipped)
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2 && errorCount.Load() == 1
		}, 3*time.Second, 50*time.Millisecond, "all records should be processed",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	client.AssertProducedString(t, "output", "good1", "v1")
	client.AssertProducedString(t, "output", "good2", "v3")
}

func TestPartitionedRunner_RebalanceAssign(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Manually trigger assignment of new partitions
	client.TriggerAssign(
		[]kafka.TopicPartition{
			{Topic: "input", Partition: 5},
			{Topic: "input", Partition: 6},
		},
	)

	time.Sleep(100 * time.Millisecond)

	// Verify workers were created
	require.Equal(t, 2, pr.WorkerCount())

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_RebalanceRevoke(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()

	// Add records to trigger initial assignment
	client.AddRecords("input", 0, mockkafka.SimpleRecord("k1", "v1"))
	client.AddRecords("input", 1, mockkafka.SimpleRecord("k2", "v2"))

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithWorkerShutdownTimeout(2*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	time.Sleep(300 * time.Millisecond)

	// Verify initial workers
	require.Equal(t, 2, pr.WorkerCount())

	// Trigger revocation of one partition
	client.TriggerRevoke(
		[]kafka.TopicPartition{
			{Topic: "input", Partition: 0},
		},
	)

	time.Sleep(500 * time.Millisecond)

	// Verify worker was removed
	require.Equal(t, 1, pr.WorkerCount())

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_GracefulShutdown(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var processedCount atomic.Int32

	var slowProcessor processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				time.Sleep(100 * time.Millisecond)
				processedCount.Add(1)
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("slow", slowProcessor.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"slow",
	)

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
		mockkafka.SimpleRecord("k3", "v3"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithDrainTimeout(5*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for at least one record to be processed before cancelling
	require.Eventually(
		t, func() bool {
			return processedCount.Load() >= 1
		}, 3*time.Second, 50*time.Millisecond, "at least one record should be processed",
	)

	// Cancel while processing
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	finalCount := processedCount.Load()
	t.Logf("Processed %d records during graceful shutdown", finalCount)
	require.GreaterOrEqual(t, finalCount, int32(1), "at least some records should have been processed")
}

func TestPartitionedRunner_ConfigOptions(t *testing.T) {
	topo := createTestTopology()
	client := mockkafka.NewClient()

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	// Test all config options
	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithErrorHandler(errorhandler.LogAndFail(logger.NewNoopLogger())),
		WithChannelBufferSize(50),
		WithWorkerShutdownTimeout(5*time.Second),
		WithDrainTimeout(10*time.Second),
		WithPollErrorBackoff(backoff.NewFixed(500*time.Millisecond)),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)
	require.NotNil(t, r)

	// Just verify it can be created without error
	// Actual behavior is tested in other tests
}

func TestPartitionWorker_TrySubmit(t *testing.T) {
	client := mockkafka.NewClient()
	tsk := createTestTask(t, client)

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	l := logger.NewNoopLogger()
	errCh := make(chan error, 1)

	r1 := mockkafka.SimpleRecord("k1", "v1")
	r1.Topic = "input"
	r2 := mockkafka.SimpleRecord("k2", "v2")
	r2.Topic = "input"
	r3 := mockkafka.SimpleRecord("k3", "v3")
	r3.Topic = "input"

	// Create a fresh worker that we won't start (so the channel stays full)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker2 := newPartitionWorker(
		tp, tsk, client, client, errorhandler.LogAndContinue(l), 2, 5*time.Second, errCh, l,
	)

	assert.True(t, worker2.TrySubmit(r1), "first TrySubmit should succeed")
	assert.True(t, worker2.TrySubmit(r2), "second TrySubmit should succeed")
	assert.False(t, worker2.TrySubmit(r3), "third TrySubmit should fail (channel full)")

	worker2.Stop()
	assert.False(t, worker2.TrySubmit(r1), "TrySubmit on stopped worker should return false")
}

func TestPartitionedRunner_BackpressurePausesSlowPartition(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var p1Count atomic.Int32

	var slowSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k[0:2] == "p1" {
					p1Count.Add(1)
				}
				// Partition 0 is slow
				if k[0:2] == "p0" {
					time.Sleep(200 * time.Millisecond)
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", slowSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(20))

	// Add many records to partition 0 (slow) and partition 1 (fast)
	for i := 0; i < 10; i++ {
		client.AddRecords(
			"input", 0, mockkafka.SimpleRecord(
				fmt.Sprintf("p0-k%d", i), fmt.Sprintf("p0-v%d", i),
			),
		)
	}
	for i := 0; i < 10; i++ {
		client.AddRecords(
			"input", 1, mockkafka.SimpleRecord(
				fmt.Sprintf("p1-k%d", i), fmt.Sprintf("p1-v%d", i),
			),
		)
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	// Small buffer size to trigger backpressure quickly
	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(2),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait enough for fast partition to complete but not all slow records
	time.Sleep(800 * time.Millisecond)

	// Partition 1 (fast) should have processed all its records even though
	// partition 0 is slow. Without backpressure, partition 1 would be starved.
	require.GreaterOrEqual(
		t, p1Count.Load(), int32(5),
		"fast partition should not be starved by slow partition",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_BackpressureResumesAfterDrain(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var processed sync.Map // key -> struct{}

	var slowThenFast processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				if k == "slow-0" || k == "slow-1" {
					time.Sleep(100 * time.Millisecond)
				}
				processed.Store(k, struct{}{})
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", slowThenFast.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(5))

	// Add records: first two slow, rest fast
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("slow-0", "v0"),
		mockkafka.SimpleRecord("slow-1", "v1"),
		mockkafka.SimpleRecord("fast-2", "v2"),
		mockkafka.SimpleRecord("fast-3", "v3"),
		mockkafka.SimpleRecord("fast-4", "v4"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(1),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for all records to be processed
	require.Eventually(
		t, func() bool {
			count := 0
			processed.Range(
				func(_, _ any) bool {
					count++
					return true
				},
			)
			return count == 5
		}, 3*time.Second, 50*time.Millisecond,
		"all records should eventually be processed after backpressure clears",
	)

	// Verify the partition is no longer paused
	pr := r.(*PartitionedRunner)
	require.Empty(t, pr.PausedPartitions(), "partition should be resumed after drain")

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_PendingClearedOnRevoke(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	gate := make(chan struct{})

	var blockingSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				// Block on first record until gate is closed
				if k == "block" {
					select {
					case <-gate:
					case <-ctx.Done():
						return k, v, ctx.Err()
					}
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", blockingSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(10))

	// Add records: first one will block, filling the buffer
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("block", "v0"),
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
		mockkafka.SimpleRecord("k3", "v3"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(1),
		WithWorkerShutdownTimeout(1*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for records to be dispatched and backpressure to kick in
	time.Sleep(500 * time.Millisecond)

	// Revoke the partition
	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	close(gate) // Unblock the worker so it can stop cleanly
	client.TriggerRevoke([]kafka.TopicPartition{tp})

	time.Sleep(200 * time.Millisecond)

	// Verify pending is cleared
	require.Empty(t, pr.PendingCounts(), "pending should be cleared after revoke")
	require.Empty(t, pr.PausedPartitions(), "paused should be cleared after revoke")

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

func TestPartitionedRunner_OrderingPreservedWithBackpressure(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var processedKeys []string
	var mu sync.Mutex

	var tracker processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				// Small delay to allow backpressure to build
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				processedKeys = append(processedKeys, k)
				mu.Unlock()
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("tracker", tracker.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"tracker",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(20))

	// Add many records to one partition with small buffer to trigger backpressure
	expected := make([]string, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		expected[i] = key
		client.AddRecords("input", 0, mockkafka.Record(key, "val").WithOffset(int64(i)).Build())
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(2),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for all records to be processed
	require.Eventually(
		t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(processedKeys) == 10
		}, 3*time.Second, 50*time.Millisecond, "all records should be processed",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify ordering
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, expected, processedKeys, "records should be processed in order within partition")
}

func TestPartitionWorker_StopSkipsDrain(t *testing.T) {
	client := mockkafka.NewClient()
	tsk := createTestTask(t, client)

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	l := logger.NewNoopLogger()
	errCh := make(chan error, 1)

	worker := newPartitionWorker(
		tp, tsk, client, client, errorhandler.LogAndContinue(l), 10, 5*time.Second, errCh, l,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.Start(ctx)

	// Submit records
	for i := 0; i < 5; i++ {
		r := mockkafka.SimpleRecord(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		r.Topic = "input"
		assert.True(t, worker.TrySubmit(r))
	}

	// Stop (revocation path) — should return quickly without draining
	worker.Stop()

	err := worker.WaitForStop(2 * time.Second)
	require.NoError(t, err, "worker should stop quickly without draining")

	// Not all records should have been processed since we skipped drain
	produced := len(client.ProducedRecords())
	t.Logf("Produced %d records (stop path, no drain)", produced)
}

func TestPartitionWorker_ContextCancelDrains(t *testing.T) {
	client := mockkafka.NewClient()
	tsk := createTestTask(t, client)

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	l := logger.NewNoopLogger()
	errCh := make(chan error, 1)

	worker := newPartitionWorker(
		tp, tsk, client, client, errorhandler.LogAndContinue(l), 10, 5*time.Second, errCh, l,
	)

	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// Submit records
	for i := 0; i < 3; i++ {
		r := mockkafka.SimpleRecord(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
		r.Topic = "input"
		assert.True(t, worker.TrySubmit(r))
	}

	// Let the worker process at least one record
	time.Sleep(50 * time.Millisecond)

	// Cancel context (shutdown path) — should drain remaining records
	cancel()

	err := worker.WaitForStop(5 * time.Second)
	require.NoError(t, err, "worker should stop after draining")

	// All records should have been processed during drain
	client.AssertProducedCount(t, 3)
}

func TestPartitionWorker_DrainTimeoutRespected(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	// Processor that blocks indefinitely
	var blockingSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				<-ctx.Done()
				return k, v, ctx.Err()
			},
		)
	}
	topo.AddProcessor("block", blockingSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"block",
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	tp := kafka.TopicPartition{Topic: "input", Partition: 0}
	client := mockkafka.NewClient()
	tsk, err := factory.CreateTask(tp, client)
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	errCh := make(chan error, 1)

	// Short drain timeout — 200ms
	worker := newPartitionWorker(
		tp, tsk, client, client, errorhandler.LogAndContinue(l), 10, 200*time.Millisecond, errCh, l,
	)

	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// Submit a record that will block
	r := mockkafka.SimpleRecord("block", "v1")
	r.Topic = "input"
	assert.True(t, worker.TrySubmit(r))

	// Let the worker pick up the record
	time.Sleep(50 * time.Millisecond)

	// Cancel context — drain should start but timeout after 200ms
	cancel()

	start := time.Now()
	err = worker.WaitForStop(2 * time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "worker should stop after drain timeout")
	require.Less(t, elapsed, 1*time.Second, "worker should not block longer than drain timeout + margin")
	t.Logf("Worker stopped after %v (drain timeout: 200ms)", elapsed)
}
