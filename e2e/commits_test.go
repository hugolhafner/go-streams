package e2e

import (
	"context"
	"errors"
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

// TestE2E_OffsetCommit_ProgressesThroughTopic verifies that offsets are
// committed correctly and a restart does not reprocess old records.
func TestE2E_OffsetCommit_ProgressesThroughTopic(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	// Helper to create topology
	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Phase 1: Process first batch of records
	{
		client, err := kafka.NewKgoClient(
			kafka.WithBootstrapServers([]string{broker}),
			kafka.WithGroupID(groupID),
		)
		require.NoError(t, err)

		topology := buildTopology().Build()
		app, err := streams.NewApplication(client, topology)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- app.RunWith(
				ctx, runner.NewSingleThreadedRunner(
					// Use aggressive commit settings for testing
					runner.WithCommitter(
						func() committer.Committer {
							return committer.NewPeriodicCommitter(
								committer.WithMaxInterval(500*time.Millisecond),
								committer.WithMaxCount(1),
							)
						},
					),
				),
			)
		}()

		time.Sleep(2 * time.Second)

		// Produce first batch
		batch1 := map[string]string{
			"batch1-key1": "first",
			"batch1-key2": "second",
			"batch1-key3": "third",
		}
		produceRecords(t, broker, inputTopic, batch1)

		// Wait for processing and commit
		time.Sleep(3 * time.Second)

		// Verify first batch was processed
		verifierGroup1 := testGroupID(t, "verifier1")
		records := consumeRecords(t, broker, outputTopic, verifierGroup1, 3, 30*time.Second)
		require.Len(t, records, 3)

		// Verify offsets were committed
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
				// Offset should be at least 3 (we processed 3 records)
				return topicOffsets[0] >= 3
			}, 10*time.Second, "offsets not committed",
		)

		// Shutdown first instance
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				require.NoError(t, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for first instance shutdown")
		}

		client.Close()
	}

	// Phase 2: Restart and verify no duplicate processing
	{
		client, err := kafka.NewKgoClient(
			kafka.WithBootstrapServers([]string{broker}),
			kafka.WithGroupID(groupID), // Same group ID!
		)
		require.NoError(t, err)
		defer client.Close()

		topology := buildTopology().Build()
		app, err := streams.NewApplication(client, topology)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- app.RunWith(
				ctx, runner.NewSingleThreadedRunner(
					runner.WithCommitter(
						func() committer.Committer {
							return committer.NewPeriodicCommitter(
								committer.WithMaxInterval(500*time.Millisecond),
								committer.WithMaxCount(1),
							)
						},
					),
				),
			)
		}()

		time.Sleep(2 * time.Second)

		// Produce second batch
		batch2 := map[string]string{
			"batch2-key1": "fourth",
			"batch2-key2": "fifth",
		}
		produceRecords(t, broker, inputTopic, batch2)

		// Wait for processing
		time.Sleep(3 * time.Second)

		// Count all records in output topic
		// We should have exactly 5 records (3 from batch1 + 2 from batch2)
		// If there were duplicates, we'd have more
		verifierGroup2 := testGroupID(t, "verifier2")
		allRecords := consumeRecords(t, broker, outputTopic, verifierGroup2, 5, 30*time.Second)

		require.Len(t, allRecords, 5, "expected exactly 5 records, no duplicates from restart")

		// Verify we have both batches
		recordMap := make(map[string]string)
		for _, r := range allRecords {
			recordMap[r.Key] = r.Value
		}

		// Batch 1 records
		require.Equal(t, "FIRST", recordMap["batch1-key1"])
		require.Equal(t, "SECOND", recordMap["batch1-key2"])
		require.Equal(t, "THIRD", recordMap["batch1-key3"])

		// Batch 2 records
		require.Equal(t, "FOURTH", recordMap["batch2-key1"])
		require.Equal(t, "FIFTH", recordMap["batch2-key2"])

		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				require.NoError(t, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for second instance shutdown")
		}
	}
}

// TestE2E_OffsetCommit_CommitsAfterMaxCount verifies that offsets are
// committed after processing maxCount records.
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

	topology := builder.Build()

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, topology)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure commit after every 5 records
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

	time.Sleep(2 * time.Second)

	// Produce records in batches
	records := make([]kgo.Record, 10)
	for i := 0; i < 10; i++ {
		records[i] = kgo.Record{
			Key:   []byte(string(rune('a' + i))),
			Value: []byte(string(rune('a' + i))),
		}
	}
	produceOrderedRecords(t, broker, inputTopic, records)

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify offsets were committed (should be committed after 5 and 10 records)
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
			// Should have committed at least after 5 records
			return topicOffsets[0] >= 5
		}, 10*time.Second, "offsets not committed after maxCount",
	)

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for application shutdown")
	}
}

// TestE2E_OffsetCommit_CommitsAfterMaxInterval verifies that offsets are
// committed after the max interval even with few records.
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

	topology := builder.Build()

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, topology)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure commit after 1 second, high record count (won't trigger by count)
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

	time.Sleep(2 * time.Second)

	// Produce just 2 records
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Wait for interval-based commit
	time.Sleep(commitInterval * 3)

	// Verify offsets were committed
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
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for application shutdown")
	}
}

// TestE2E_OffsetCommit_MultiplePartitions verifies that offsets are
// tracked and committed correctly across multiple partitions.
func TestE2E_OffsetCommit_MultiplePartitions(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	// Create topics with 3 partitions
	createTopics(t, broker, 3, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.MapValues(source, strings.ToUpper)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	topology := builder.Build()

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, topology)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(
			ctx, runner.NewSingleThreadedRunner(
				runner.WithCommitter(
					func() committer.Committer {
						return committer.NewPeriodicCommitter(
							committer.WithMaxInterval(500*time.Millisecond),
							committer.WithMaxCount(1),
						)
					},
				),
			),
		)
	}()

	time.Sleep(2 * time.Second)

	// Produce records with different keys to distribute across partitions
	testData := map[string]string{
		"a": "val-a", // These keys should hash to different partitions
		"b": "val-b",
		"c": "val-c",
		"d": "val-d",
		"e": "val-e",
		"f": "val-f",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Wait for processing and commit
	time.Sleep(3 * time.Second)

	// Verify all records were processed
	verifierGroup := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, verifierGroup, len(testData), 30*time.Second)
	require.Len(t, records, len(testData))

	// Verify offsets were committed for all partitions
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
			// Should have offsets committed for multiple partitions
			totalCommitted := int64(0)
			for _, offset := range topicOffsets {
				totalCommitted += offset
			}
			return totalCommitted >= int64(len(testData))
		}, 10*time.Second, "offsets not committed for all partitions",
	)

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for application shutdown")
	}
}
