//go:build e2e

package e2e

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

func TestE2E_ConsumerGroup_SingleConsumer(t *testing.T) {
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

	time.Sleep(3 * time.Second)

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	produceRecords(t, broker, inputTopic, testData)

	records := consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), len(testData), consumeWait)
	require.Len(t, records, len(testData))

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_ConsumerGroup_RebalanceOnJoin(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

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
		errCh1 <- app1.RunWith(ctx1, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	time.Sleep(3 * time.Second)
	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

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
		errCh2 <- app2.RunWith(ctx2, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	waitForGroupMembers(t, broker, groupID, 2, 30*time.Second)

	testData := map[string]string{
		"a": "val-a", "b": "val-b", "c": "val-c", "d": "val-d",
		"e": "val-e", "f": "val-f", "g": "val-g", "h": "val-h",
	}
	produceRecords(t, broker, inputTopic, testData)

	records := consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), len(testData), consumeWait)
	require.Len(t, records, len(testData))

	cancel1()
	cancel2()

	waitForShutdown(t, errCh1, shutdownWait)
	waitForShutdown(t, errCh2, shutdownWait)
}

func TestE2E_ConsumerGroup_RebalanceOnLeave(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

	var wg sync.WaitGroup

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
		_ = app1.RunWith(ctx1, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

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
		_ = app2.RunWith(ctx2, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	waitForGroupMembers(t, broker, groupID, 2, 30*time.Second)

	batch1 := map[string]string{
		"batch1-a": "first",
		"batch1-b": "second",
		"batch1-c": "third",
		"batch1-d": "fourth",
	}
	produceRecords(t, broker, inputTopic, batch1)

	time.Sleep(3 * time.Second)

	cancel2()
	client2.Close()

	waitForGroupMembers(t, broker, groupID, 1, 30*time.Second)

	batch2 := map[string]string{
		"batch2-a": "fifth",
		"batch2-b": "sixth",
		"batch2-c": "seventh",
		"batch2-d": "eighth",
	}
	produceRecords(t, broker, inputTopic, batch2)

	time.Sleep(3 * time.Second)

	totalRecords := len(batch1) + len(batch2)
	records := consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), totalRecords, consumeWait)
	require.Len(t, records, totalRecords)

	cancel1()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for consumer shutdown")
	}
}

func TestE2E_ConsumerGroup_ContinuousProcessingDuringRebalance(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 4, inputTopic, outputTopic)

	buildTopology := func() *kstream.StreamsBuilder {
		builder := kstream.NewStreamsBuilder()
		source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
		mapped := kstream.MapValues(source, strings.ToUpper)
		kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())
		return builder
	}

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
		_ = app1.RunWith(ctx1, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	time.Sleep(3 * time.Second)

	producerCtx, producerCancel := context.WithCancel(context.Background())
	defer producerCancel()

	producerDone := make(chan struct{})
	producedCount := atomic.Int64{}

	go func() {
		defer close(producerDone)
		i := 0
		for {
			select {
			case <-producerCtx.Done():
				return
			default:
				key := string(rune('a' + (i % 26)))
				testData := map[string]string{key + string(rune('0'+i%10)): key}
				produceRecords(t, broker, inputTopic, testData)
				producedCount.Add(1)
				i++
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	time.Sleep(2 * time.Second)

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
		_ = app2.RunWith(ctx2, runner.NewSingleThreadedRunner(aggressiveCommitter()))
	}()

	waitForGroupMembers(t, broker, groupID, 2, 30*time.Second)

	time.Sleep(3 * time.Second)

	producerCancel()
	<-producerDone

	totalProduced := int(producedCount.Load())
	t.Logf("Produced %d records during test", totalProduced)

	time.Sleep(3 * time.Second)

	records := consumeRecords(t, broker, outputTopic, testGroupID(t, "verifier"), totalProduced, 60*time.Second)

	// Allow for some timing variance
	require.GreaterOrEqual(
		t, len(records), totalProduced*9/10,
		"expected at least 90%% of records to be processed, got %d/%d", len(records), totalProduced,
	)

	cancel1()
	cancel2()
}

func TestE2E_ConsumerGroup_MultipleTopics(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic1 := testTopicName(t, "input1")
	inputTopic2 := testTopicName(t, "input2")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 2, inputTopic1, inputTopic2, outputTopic)

	builder := kstream.NewStreamsBuilder()

	source1 := kstream.StreamWithSerde(builder, inputTopic1, serde.String(), serde.String())
	mapped1 := kstream.Map(
		source1, func(k, v string) (string, string) {
			return k, "FROM_TOPIC1:" + strings.ToUpper(v)
		},
	)
	kstream.ToWithSerde(mapped1, outputTopic, serde.String(), serde.String())

	source2 := kstream.StreamWithSerde(builder, inputTopic2, serde.String(), serde.String())
	mapped2 := kstream.Map(
		source2, func(k, v string) (string, string) {
			return k, "FROM_TOPIC2:" + strings.ToUpper(v)
		},
	)
	kstream.ToWithSerde(mapped2, outputTopic, serde.String(), serde.String())

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

	time.Sleep(3 * time.Second)

	produceRecords(t, broker, inputTopic1, map[string]string{"k1": "topic1-val"})
	produceRecords(t, broker, inputTopic2, map[string]string{"k2": "topic2-val"})

	consumed := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier"), 2, consumeWait)

	require.Len(t, consumed, 2)
	require.Equal(t, "FROM_TOPIC1:TOPIC1-VAL", consumed["k1"])
	require.Equal(t, "FROM_TOPIC2:TOPIC2-VAL", consumed["k2"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}
