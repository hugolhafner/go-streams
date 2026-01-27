//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/committer"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestE2E_OffsetCommit_ProgressesThroughTopic(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Phase 1: Process first batch
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
			errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner(aggressiveCommitter()))
		}()

		time.Sleep(startupWait)

		batch1 := map[string]string{
			"batch1-key1": "first",
			"batch1-key2": "second",
			"batch1-key3": "third",
		}
		produceRecords(t, broker, inputTopic, batch1)

		time.Sleep(3 * time.Second)

		consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier1"), 3, consumeWait)

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
				return topicOffsets[0] >= 3
			}, 10*time.Second, "offsets not committed",
		)

		cancel()
		waitForShutdown(t, errCh, shutdownWait)
		client.Close()
	}

	// Phase 2: Restart and verify no duplicate processing
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
			errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner(aggressiveCommitter()))
		}()

		time.Sleep(startupWait)

		batch2 := map[string]string{
			"batch2-key1": "fourth",
			"batch2-key2": "fifth",
		}
		produceRecords(t, broker, inputTopic, batch2)

		time.Sleep(3 * time.Second)

		allRecords := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier2"), 5, consumeWait)

		require.Len(t, allRecords, 5, "expected exactly 5 records, no duplicates from restart")

		require.Equal(t, "FIRST", allRecords["batch1-key1"])
		require.Equal(t, "SECOND", allRecords["batch1-key2"])
		require.Equal(t, "THIRD", allRecords["batch1-key3"])
		require.Equal(t, "FOURTH", allRecords["batch2-key1"])
		require.Equal(t, "FIFTH", allRecords["batch2-key2"])

		cancel()
		waitForShutdown(t, errCh, shutdownWait)
	}
}

func TestE2E_OffsetCommit_CommitsAfterMaxCount(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.MapValues(source, strings.ToUpper)
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

	const maxCount = 5

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(
			ctx, runner.NewSingleThreadedRunner(
				runner.WithCommitter(
					func() committer.Committer {
						return committer.NewPeriodicCommitter(
							committer.WithMaxInterval(1*time.Hour), // Disable time-based commits
							committer.WithMaxCount(maxCount),
						)
					},
				),
			),
		)
	}()

	time.Sleep(startupWait)

	records := make([]kgo.Record, 10)
	for i := 0; i < 10; i++ {
		records[i] = kgo.Record{
			Key:   []byte(string(rune('a' + i))),
			Value: []byte(string(rune('a' + i))),
		}
	}
	produceOrderedRecords(t, broker, inputTopic, records)

	time.Sleep(3 * time.Second)

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
			return topicOffsets[0] >= 5
		}, 10*time.Second, "offsets not committed after maxCount",
	)

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_OffsetCommit_CommitsAfterMaxInterval(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.MapValues(source, strings.ToUpper)
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

	const commitInterval = 1 * time.Second

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(
			ctx, runner.NewSingleThreadedRunner(
				runner.WithCommitter(
					func() committer.Committer {
						return committer.NewPeriodicCommitter(
							committer.WithMaxInterval(commitInterval),
							committer.WithMaxCount(1000), // High count, won't trigger
						)
					},
				),
			),
		)
	}()

	time.Sleep(startupWait)

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	produceRecords(t, broker, inputTopic, testData)

	time.Sleep(commitInterval * 3)

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
			return topicOffsets[0] >= 2
		}, 10*time.Second, "offsets not committed after interval",
	)

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_OffsetCommit_MultiplePartitions(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 3, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.MapValues(source, strings.ToUpper)
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
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	time.Sleep(startupWait)

	// Keys chosen to distribute across partitions
	testData := map[string]string{
		"a": "val-a",
		"b": "val-b",
		"c": "val-c",
		"d": "val-d",
		"e": "val-e",
		"f": "val-f",
	}
	produceRecords(t, broker, inputTopic, testData)

	time.Sleep(3 * time.Second)

	consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), len(testData), consumeWait)

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
