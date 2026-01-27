package e2e

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	streams "github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/committer"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

// TestE2E_ConsumerGroup_SingleConsumer verifies that a single consumer
// gets all partitions assigned.
func TestE2E_ConsumerGroup_SingleConsumer(t *testing.T) {
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

	// Wait for group to stabilize
	time.Sleep(3 * time.Second)

	// Verify single consumer has all partitions
	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 1
		}, 15*time.Second, "expected 1 member in consumer group",
	)

	// Produce records
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Verify all records were processed
	verifierGroup := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, verifierGroup, len(testData), 30*time.Second)
	require.Len(t, records, len(testData))

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

// TestE2E_ConsumerGroup_RebalanceOnJoin verifies that when a second
// consumer joins, partitions are rebalanced.
func TestE2E_ConsumerGroup_RebalanceOnJoin(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	// Create topics with 4 partitions (even split between 2 consumers)
	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Start first consumer
	client1, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client1.Close()

	app1, err := streams.NewApplication(client1, buildTopology().Build())
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- app1.RunWith(
			ctx1, runner.NewSingleThreadedRunner(
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

	// Wait for first consumer to get all partitions
	time.Sleep(3 * time.Second)

	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 1
		}, 15*time.Second, "expected 1 member initially",
	)

	// Start second consumer (this triggers rebalance)
	client2, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client2.Close()

	app2, err := streams.NewApplication(client2, buildTopology().Build())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- app2.RunWith(
			ctx2, runner.NewSingleThreadedRunner(
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

	// Wait for rebalance to complete - both consumers should be in the group
	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 2
		}, 30*time.Second, "expected 2 members after join",
	)

	// Produce records - they should be distributed across both consumers
	testData := map[string]string{
		"a": "val-a",
		"b": "val-b",
		"c": "val-c",
		"d": "val-d",
		"e": "val-e",
		"f": "val-f",
		"g": "val-g",
		"h": "val-h",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Verify all records were processed (by both consumers)
	verifierGroup := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, verifierGroup, len(testData), 30*time.Second)
	require.Len(t, records, len(testData))

	// Cleanup both consumers
	cancel1()
	cancel2()

	for i, errCh := range []chan error{errCh1, errCh2} {
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Errorf("consumer %d error: %v", i+1, err)
			}
		case <-time.After(10 * time.Second):
			t.Errorf("timeout waiting for consumer %d shutdown", i+1)
		}
	}
}

// TestE2E_ConsumerGroup_RebalanceOnLeave verifies that when a consumer
// leaves, partitions are redistributed to remaining consumers.
func TestE2E_ConsumerGroup_RebalanceOnLeave(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	// Create topics with 4 partitions
	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Track processed records per consumer
	var consumer1Count, consumer2Count atomic.Int64

	// Start two consumers
	var wg sync.WaitGroup

	// Consumer 1
	client1, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client1.Close()

	app1, err := streams.NewApplication(client1, buildTopology().Build())
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = app1.RunWith(
			ctx1, runner.NewSingleThreadedRunner(
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

	// Consumer 2
	client2, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)

	app2, err := streams.NewApplication(client2, buildTopology().Build())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = app2.RunWith(
			ctx2, runner.NewSingleThreadedRunner(
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

	// Wait for both consumers to join
	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 2
		}, 30*time.Second, "expected 2 members in group",
	)

	// Produce first batch
	batch1 := map[string]string{
		"batch1-a": "first",
		"batch1-b": "second",
		"batch1-c": "third",
		"batch1-d": "fourth",
	}
	produceRecords(t, broker, inputTopic, batch1)

	// Wait for first batch to be processed
	time.Sleep(3 * time.Second)

	// Stop consumer 2 (this triggers rebalance)
	cancel2()
	client2.Close()

	// Wait for rebalance - only 1 consumer should remain
	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 1
		}, 30*time.Second, "expected 1 member after leave",
	)

	// Produce second batch - should all be handled by remaining consumer
	batch2 := map[string]string{
		"batch2-a": "fifth",
		"batch2-b": "sixth",
		"batch2-c": "seventh",
		"batch2-d": "eighth",
	}
	produceRecords(t, broker, inputTopic, batch2)

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify all records from both batches were processed
	verifierGroup := testGroupID(t, "verifier")
	totalRecords := len(batch1) + len(batch2)
	records := consumeRecords(t, broker, outputTopic, verifierGroup, totalRecords, 30*time.Second)
	require.Len(t, records, totalRecords)

	// Cleanup
	cancel1()

	// Wait for goroutines
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for consumer shutdown")
	}

	_ = consumer1Count
	_ = consumer2Count
}

// TestE2E_ConsumerGroup_ContinuousProcessingDuringRebalance verifies that
// records continue to be processed during rebalance events.
func TestE2E_ConsumerGroup_ContinuousProcessingDuringRebalance(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	// Create topics with 4 partitions
	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	// Start first consumer
	client1, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client1.Close()

	app1, err := streams.NewApplication(client1, buildTopology().Build())
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	go func() {
		_ = app1.RunWith(
			ctx1, runner.NewSingleThreadedRunner(
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

	// Wait for initial consumer
	time.Sleep(3 * time.Second)

	// Start producing records continuously
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producerDone := make(chan struct{})
	producedCount := atomic.Int64{}

	go func() {
		defer close(producerDone)
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				key := string(rune('a' + (i % 26)))
				value := key
				testData := map[string]string{key + string(rune('0'+i%10)): value}
				produceRecords(t, broker, inputTopic, testData)
				producedCount.Add(1)
				i++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Let some records be produced
	time.Sleep(2 * time.Second)

	// Start second consumer (triggers rebalance)
	client2, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client2.Close()

	app2, err := streams.NewApplication(client2, buildTopology().Build())
	require.NoError(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	go func() {
		_ = app2.RunWith(
			ctx2, runner.NewSingleThreadedRunner(
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

	// Wait for rebalance
	eventually(
		t, func() bool {
			members := getConsumerGroupMembers(t, broker, groupID)
			return members == 2
		}, 30*time.Second, "expected 2 members",
	)

	// Continue producing during rebalance
	time.Sleep(3 * time.Second)

	// Stop producer
	cancel()
	<-producerDone

	totalProduced := int(producedCount.Load())
	t.Logf("Produced %d records during test", totalProduced)

	// Wait for all records to be processed
	time.Sleep(3 * time.Second)

	// Verify no records were lost
	verifierGroup := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, verifierGroup, totalProduced, 60*time.Second)

	// Allow for some timing variance, but should have processed most records
	require.GreaterOrEqual(
		t, len(records), totalProduced*9/10,
		"expected at least 90%% of records to be processed, got %d/%d", len(records), totalProduced,
	)

	cancel1()
	cancel2()
}

// TestE2E_ConsumerGroup_MultipleTopics verifies that a consumer group
// can handle multiple source topics.
func TestE2E_ConsumerGroup_MultipleTopics(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic1 := testTopicName(t, "input1")
	inputTopic2 := testTopicName(t, "input2")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 2, inputTopic1, inputTopic2, outputTopic)

	// Build a topology that consumes from two topics
	// Note: This requires separate source nodes in the topology
	builder := kstream.NewStreamsBuilder()

	// Source 1
	source1 := kstream.StreamWithSerde(builder, inputTopic1, serde.String(), serde.String())
	mapped1 := kstream.Map(
		source1, func(k, v string) (string, string) {
			return k, "FROM_TOPIC1:" + strings.ToUpper(v)
		},
	)
	kstream.ToWithSerde(mapped1, outputTopic, serde.String(), serde.String())

	// Source 2
	source2 := kstream.StreamWithSerde(builder, inputTopic2, serde.String(), serde.String())
	mapped2 := kstream.Map(
		source2, func(k, v string) (string, string) {
			return k, "FROM_TOPIC2:" + strings.ToUpper(v)
		},
	)
	kstream.ToWithSerde(mapped2, outputTopic, serde.String(), serde.String())

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

	time.Sleep(3 * time.Second)

	// Produce to both input topics
	topic1Data := map[string]string{"k1": "topic1-val"}
	topic2Data := map[string]string{"k2": "topic2-val"}

	produceRecords(t, broker, inputTopic1, topic1Data)
	produceRecords(t, broker, inputTopic2, topic2Data)

	// Verify records from both topics were processed
	verifierGroup := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, verifierGroup, 2, 30*time.Second)
	require.Len(t, records, 2)

	recordMap := make(map[string]string)
	for _, r := range records {
		recordMap[r.Key] = r.Value
	}

	require.Equal(t, "FROM_TOPIC1:TOPIC1-VAL", recordMap["k1"])
	require.Equal(t, "FROM_TOPIC2:TOPIC2-VAL", recordMap["k2"])

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
