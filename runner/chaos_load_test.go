//go:build unit

package runner

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"github.com/stretchr/testify/require"
)

// Test 18: Hot key with Zipfian distribution triggers backpressure on hot partition.
func TestChaos_HotKey_BackpressureOnHotPartition(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var slowSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				time.Sleep(1 * time.Millisecond)
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

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(50))

	// Zipfian distribution: ~80% to partition 0
	rng := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(rng, 1.5, 1.0, 3) // 4 partitions (0-3)

	totalRecords := 200
	for i := 0; i < totalRecords; i++ {
		partition := int32(zipf.Uint64())
		client.AddRecords(
			"input", partition,
			mockkafka.SimpleRecord(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)),
		)
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(5),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Check that backpressure was triggered at some point
	sawPaused := false
	require.Eventually(
		t, func() bool {
			if len(pr.PausedPartitions()) > 0 {
				sawPaused = true
				return true
			}
			return false
		}, 2*time.Second, 20*time.Millisecond,
	)

	// All records should eventually be produced
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == totalRecords
		}, 10*time.Second, 50*time.Millisecond, "all %d records should be produced", totalRecords,
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	require.True(t, sawPaused, "partition 0 should have been paused due to hot key backpressure")
}

// Test 19: Burst of 500 records to single partition, FIFO ordering preserved.
func TestChaos_Burst_500Records_SinglePartition(t *testing.T) {
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
				mu.Lock()
				processedKeys = append(processedKeys, k)
				mu.Unlock()
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", tracker.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(100))

	totalRecords := 500
	expected := make([]string, totalRecords)
	for i := 0; i < totalRecords; i++ {
		key := fmt.Sprintf("k%04d", i)
		expected[i] = key
		client.AddRecords("input", 0, mockkafka.Record(key, "val").WithOffset(int64(i)).Build())
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(5),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Check pending was non-zero during processing (informational, not asserted)
	sawPending := false
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(pr.PendingCounts()) > 0 {
			sawPending = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == totalRecords
		}, 10*time.Second, 50*time.Millisecond, "all 500 records should be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify FIFO ordering
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, expected, processedKeys, "processing order should be FIFO")
	t.Logf("sawPending=%v (informational - fast processing may drain before observation)", sawPending)
}

// Test 20: Multi-partition burst with per-partition ordering preserved.
func TestChaos_MultiPartitionBurst_OrderPreservedPerPartition(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var mu sync.Mutex
	perPartition := make(map[int32][]string)

	var tracker processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				// Extract partition from value (format: "p<N>")
				var partition int32
				fmt.Sscanf(v, "p%d", &partition)
				mu.Lock()
				perPartition[partition] = append(perPartition[partition], k)
				mu.Unlock()
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", tracker.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(50))

	numPartitions := 8
	recordsPerPartition := 100 / numPartitions // ~12-13 per partition
	totalRecords := 0
	expectedPerPartition := make(map[int32][]string)

	for p := 0; p < numPartitions; p++ {
		for i := 0; i < recordsPerPartition; i++ {
			key := fmt.Sprintf("p%d-k%03d", p, i)
			value := fmt.Sprintf("p%d", p)
			client.AddRecords(
				"input", int32(p),
				mockkafka.Record(key, value).WithOffset(int64(i)).Build(),
			)
			expectedPerPartition[int32(p)] = append(expectedPerPartition[int32(p)], key)
			totalRecords++
		}
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(3),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == totalRecords
		}, 10*time.Second, 50*time.Millisecond, "all records should be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Verify per-partition ordering
	mu.Lock()
	defer mu.Unlock()
	for p := int32(0); p < int32(numPartitions); p++ {
		require.Equal(
			t, expectedPerPartition[p], perPartition[p],
			"ordering should be preserved for partition %d", p,
		)
	}
}

// Test 21: Slow consumer with variable speed - backpressure stabilizes and all records processed.
func TestChaos_SlowConsumer_BackpressureStabilizes(t *testing.T) {
	topo := topology.New()
	topo.AddSource(
		"source", "input",
		serde.ToUntypedDeserialser(serde.String()),
		serde.ToUntypedDeserialser(serde.String()),
	)

	var seed int64 = 42

	var variableSpeedSupplier processor.Supplier[string, string, string, string] = func() processor.Processor[string, string, string, string] {
		// Each partition worker gets its own rng to avoid data races
		seed++
		localRng := rand.New(rand.NewSource(seed))
		return builtins.NewMapProcessor(
			func(ctx context.Context, k, v string) (string, string, error) {
				// 50% fast (1ms), 50% slow (20ms)
				if localRng.Float64() < 0.5 {
					time.Sleep(1 * time.Millisecond)
				} else {
					time.Sleep(20 * time.Millisecond)
				}
				return k, v, nil
			},
		)
	}
	topo.AddProcessor("proc", variableSpeedSupplier.ToUntyped(), "source")
	topo.AddSink(
		"sink", "output",
		serde.ToUntypedSerialiser(serde.String()),
		serde.ToUntypedSerialiser(serde.String()),
		"proc",
	)

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(20))

	totalRecords := 0
	for p := 0; p < 4; p++ {
		for i := 0; i < 10; i++ {
			client.AddRecords(
				"input", int32(p),
				mockkafka.SimpleRecord(fmt.Sprintf("p%d-k%d", p, i), "val"),
			)
			totalRecords++
		}
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(3),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	pr := r.(*PartitionedRunner)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// All records should eventually be processed
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == totalRecords
		}, 10*time.Second, 50*time.Millisecond, "all records should be produced",
	)

	// After all records processed, partitions should be resumed
	require.Eventually(
		t, func() bool {
			return len(pr.PausedPartitions()) == 0
		}, 3*time.Second, 50*time.Millisecond, "all partitions should be resumed after processing",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}
