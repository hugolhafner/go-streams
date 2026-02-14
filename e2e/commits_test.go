//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

// TestE2E_OffsetCommit_ProgressesThroughTopic verifies that:
// 1. Offsets are committed eventually during processing.
// 2. Offsets are committed on shutdown.
// 3. A restarted consumer resumes from the correct offset (no duplicates).
func TestE2E_OffsetCommit_ProgressesThroughTopic(t *testing.T) {
	broker := ensureContainer(t)
	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(
			source, func(_ context.Context, value string) (string, error) {
				return strings.ToUpper(value), nil
			},
		)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Phase 1: Process first batch and verify eventual commit / shutdown commit
	{
		client, err := kafka.NewKgoClient(
			kafka.WithBootstrapServers([]string{broker}),
			kafka.WithGroupID(groupID),
		)
		require.NoError(t, err)

		app, err := streams.NewApplication(client, buildTopology().Build())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() {
			errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
		}()

		waitForGroupMembers(t, broker, groupID, 1, eventualWait)

		batch1 := map[string]string{
			"batch1-key1": "first",
			"batch1-key2": "second",
			"batch1-key3": "third",
		}
		produceRecords(t, broker, inputTopic, batch1)

		// Wait for records to be processed
		consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier1"), 3, consumeWait)

		// Verify "Mark and AutoCommit" works:
		// Since the client uses AutoCommitMarks, we expect offsets to appear eventually
		// without manual intervention or specific runner configuration.
		eventually(
			t, func() bool {
				offsets := getCommittedOffsets(t, broker, groupID)
				if offsets == nil {
					return false
				}
				topicOffsets, ok := offsets[inputTopic]
				if !ok {
					return false
				}
				// Partition 0 should have committed offset >= 3
				return topicOffsets[0] >= 3
			}, 15*time.Second, "offsets not committed eventually",
		)

		cancel()
		waitForShutdown(t, errCh, shutdownWait)
		client.Close()
	}

	// Phase 2: Restart and verify resume accuracy
	// If the "Mark" logic in the client/runner works, we should not re-process batch1.
	{
		client, err := kafka.NewKgoClient(
			kafka.WithBootstrapServers([]string{broker}),
			kafka.WithGroupID(groupID),
		)
		require.NoError(t, err)
		defer client.Close()

		app, err := streams.NewApplication(client, buildTopology().Build())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
		}()

		waitForGroupMembers(t, broker, groupID, 1, eventualWait)

		batch2 := map[string]string{
			"batch2-key1": "fourth",
			"batch2-key2": "fifth",
		}
		produceRecords(t, broker, inputTopic, batch2)

		allRecords := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier2"), 5, consumeWait)

		require.Len(t, allRecords, 5, "expected exactly 5 unique records in output topic (3 from old, 2 from new)")

		offsets := getCommittedOffsets(t, broker, groupID)
		require.GreaterOrEqual(t, offsets[inputTopic][0], int64(5))

		cancel()
		waitForShutdown(t, errCh, shutdownWait)
	}
}

// TestE2E_OffsetCommit_MultiplePartitions verifies that marking works across multiple partitions
// concurrently, which exercises the thread-safety of the client's MarkRecords.
func TestE2E_OffsetCommit_MultiplePartitions(t *testing.T) {
	broker := ensureContainer(t)
	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 3, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.MapValues(
		source, func(_ context.Context, value string) (string, error) {
			return strings.ToUpper(value), nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	testData := map[string]string{
		"a": "val-a", "b": "val-b", "c": "val-c",
		"d": "val-d", "e": "val-e", "f": "val-f",
	}
	produceRecords(t, broker, inputTopic, testData)
	consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), len(testData), consumeWait)

	// Verify all partitions eventually commit
	eventually(
		t, func() bool {
			offsets := getCommittedOffsets(t, broker, groupID)
			if offsets == nil {
				return false
			}
			topicOffsets, ok := offsets[inputTopic]
			if !ok {
				return false
			}
			totalCommitted := int64(0)
			for _, offset := range topicOffsets {
				totalCommitted += offset
			}
			return totalCommitted >= int64(len(testData))
		}, 10*time.Second, "offsets not committed for all partitions",
	)

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}
