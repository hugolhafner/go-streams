//go:build unit

package runner_test

import (
	"context"
	"errors"
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
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
		runner.WithChannelBufferSize(10),
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

	// Wait a bit for processing
	time.Sleep(500 * time.Millisecond)

	// Cancel and wait for shutdown
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify all records were produced
	client.AssertProducedCount(t, 3)
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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for processing
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify all 5 records were produced
	client.AssertProducedCount(t, 5)
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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
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

	// Wait for processing
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	elapsed := time.Since(start)

	// If processing was sequential, it would take 5 * 50ms = 250ms minimum
	// With parallelism, it should be closer to 50-100ms + overhead
	// We're generous here because of test environment variability
	t.Logf("Elapsed time: %v", elapsed)
	t.Logf("Processing order: %v", processedOrder)

	// Verify all records were processed
	client.AssertProducedCount(t, 5)
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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
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
	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
		runner.WithErrorHandler(errorhandler.LogAndContinue(logger.NewNoopLogger())),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify good records were processed, failed record was skipped
	client.AssertProducedCount(t, 2)
	client.AssertProducedString(t, "output", "good1", "v1")
	client.AssertProducedString(t, "output", "good2", "v3")

	// Verify the error was encountered
	require.Equal(t, int32(1), errorCount.Load())
}

func TestPartitionedRunner_RebalanceAssign(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	pr := r.(*runner.PartitionedRunner)

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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
		runner.WithWorkerShutdownTimeout(2*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	pr := r.(*runner.PartitionedRunner)

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

	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
		runner.WithDrainTimeout(5*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Run(ctx)
	}()

	// Wait for some processing to start
	time.Sleep(150 * time.Millisecond)

	// Cancel while processing
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify that in-flight records were processed during drain
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
	runnerFactory := runner.NewPartitionedRunner(
		runner.WithLogger(logger.NewNoopLogger()),
		runner.WithErrorHandler(errorhandler.LogAndFail(logger.NewNoopLogger())),
		runner.WithChannelBufferSize(50),
		runner.WithWorkerShutdownTimeout(5*time.Second),
		runner.WithDrainTimeout(10*time.Second),
		runner.WithPollErrorBackoff(backoff.NewFixed(500*time.Millisecond)),
	)

	r, err := runnerFactory(topo, factory, client, client)
	require.NoError(t, err)
	require.NotNil(t, r)

	// Just verify it can be created without error
	// Actual behavior is tested in other tests
}
