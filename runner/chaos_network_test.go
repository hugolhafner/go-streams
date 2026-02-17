//go:build unit

package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	mockkafka "github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"github.com/stretchr/testify/require"
)

// Test 1: SingleThreaded poll error backs off and retries, eventually processes records.
func TestChaos_SingleThreaded_PollError_BacksOffAndRetries(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)

	// Fail first 3 polls then succeed
	var pollCount atomic.Int32
	client.SetPollErrorFunc(
		func() error {
			count := pollCount.Add(1)
			if count <= 3 {
				return errors.New("transient poll error")
			}
			return nil
		},
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithPollErrorBackoff(backoff.NewFixed(10*time.Millisecond)),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Records should eventually be produced after errors clear
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 50*time.Millisecond, "records should be produced after poll errors clear",
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
}

// Test 2: Partitioned poll error preserves backpressure state.
func TestChaos_Partitioned_PollError_PreservesBackpressureState(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(20))

	// Add records to 2 partitions
	for i := 0; i < 5; i++ {
		client.AddRecords("input", 0, mockkafka.SimpleRecord(fmt.Sprintf("p0-k%d", i), fmt.Sprintf("v%d", i)))
		client.AddRecords("input", 1, mockkafka.SimpleRecord(fmt.Sprintf("p1-k%d", i), fmt.Sprintf("v%d", i)))
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithChannelBufferSize(1),
		WithPollErrorBackoff(backoff.NewFixed(10*time.Millisecond)),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Wait for at least some records to be produced before injecting errors
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) >= 1
		}, 3*time.Second, 50*time.Millisecond, "at least one record should be produced",
	)

	// Inject poll errors for 500ms
	client.SetPollError(errors.New("transient poll error"))
	time.Sleep(500 * time.Millisecond)
	client.SetPollError(nil)

	// All records should eventually be produced
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 10
		}, 5*time.Second, 50*time.Millisecond, "all records should be produced after errors clear",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

// Test 3: SingleThreaded commit error on shutdown - runner returns nil, records processed.
func TestChaos_SingleThreaded_CommitError_ShutdownStillClean(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Wait for record to be processed
	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 1
		}, 3*time.Second, 50*time.Millisecond, "record should be produced",
	)

	// Set commit error before shutdown
	client.SetCommitError(errors.New("commit error on shutdown"))

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "runner should return nil even with commit error on shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// Records were processed
	client.AssertProducedString(t, "output", "k1", "v1")
	// Offsets not committed due to error
	require.Empty(t, client.CommittedOffsets())
}

// Test 4: Partitioned send error - transient failures then recovery via retry.
func TestChaos_Partitioned_SendError_TransientThenRecover(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)

	// Fail first 2 sends then succeed
	var sendCount atomic.Int32
	client.SetSendErrorFunc(
		func(topic string, key, value []byte) error {
			count := sendCount.Add(1)
			if count <= 2 {
				return errors.New("transient send error")
			}
			return nil
		},
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	l := logger.NewNoopLogger()
	runnerFactory := NewPartitionedRunner(
		WithLogger(l),
		WithErrorHandler(
			errorhandler.WithMaxAttempts(3, backoff.NewFixed(0), errorhandler.LogAndFail(l)),
		),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 50*time.Millisecond, "all records should eventually be produced",
	)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}

	// No DLQ records
	require.Empty(t, client.ProducedRecordsForTopic("dlq"))
}

// Test 5: Rapid rebalance storm - no race or panic.
func TestChaos_Partitioned_RebalanceStorm_NoRaceOrPanic(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient()
	for i := 0; i < 3; i++ {
		client.AddRecords(
			"input", int32(i),
			mockkafka.SimpleRecord(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)),
		)
	}

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewPartitionedRunner(
		WithLogger(logger.NewNoopLogger()),
		WithWorkerShutdownTimeout(1*time.Second),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	// Wait for initial assignment (runner must be subscribed)
	require.Eventually(
		t, func() bool {
			return r.(*PartitionedRunner).WorkerCount() >= 1
		}, 3*time.Second, 50*time.Millisecond, "initial workers should be created",
	)

	// Rapid interleaved assign/revoke cycles
	for i := 0; i < 10; i++ {
		tp := kafka.TopicPartition{Topic: "input", Partition: int32(10 + i)}

		client.TriggerAssign([]kafka.TopicPartition{tp})
		time.Sleep(20 * time.Millisecond)

		client.TriggerRevoke([]kafka.TopicPartition{tp})
		time.Sleep(20 * time.Millisecond)
	}

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "runner should exit cleanly after rebalance storm")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}

// Test 6: SingleThreaded with slow poll - records still processed.
func TestChaos_SingleThreaded_SlowPoll_ProcessesEventually(t *testing.T) {
	topo := createTestTopology()

	client := mockkafka.NewClient(mockkafka.WithPollDelay(50 * time.Millisecond))
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
	)

	factory, err := task.NewTopologyTaskFactory(topo, logger.NewNoopLogger())
	require.NoError(t, err)

	runnerFactory := NewSingleThreadedRunner(
		WithLogger(logger.NewNoopLogger()),
	)

	r, err := runnerFactory(topo, factory, client, client, streamsotel.Noop())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

	require.Eventually(
		t, func() bool {
			return len(client.ProducedRecords()) == 2
		}, 3*time.Second, 50*time.Millisecond, "records should be produced despite slow poll",
	)

	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "elapsed time should reflect poll delay")

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for runner to stop")
	}
}
