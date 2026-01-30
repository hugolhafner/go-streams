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

func TestE2E_SimpleTopology_ProcessesRecords(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			return key, strings.ToUpper(value), nil
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

	time.Sleep(startupWait)

	testData := map[string]string{
		"key1": "hello",
		"key2": "world",
		"key3": "kafka",
	}
	produceRecords(t, broker, inputTopic, testData)

	consumed := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier"), len(testData), consumeWait)

	require.Equal(t, "HELLO", consumed["key1"])
	require.Equal(t, "WORLD", consumed["key2"])
	require.Equal(t, "KAFKA", consumed["key3"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_FilterTopology_DropsRecords(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	filtered := kstream.Filter(
		source, func(_ context.Context, key, value string) (bool, error) {
			return len(value) > 5, nil
		},
	)
	kstream.ToWithSerde(filtered, outputTopic, serde.String(), serde.String())

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

	time.Sleep(startupWait)

	testData := map[string]string{
		"key1": "hi",      // 2 chars - filtered
		"key2": "hello",   // 5 chars - filtered (not > 5)
		"key3": "goodbye", // 7 chars - passes
		"key4": "streams", // 7 chars - passes
		"key5": "test",    // 4 chars - filtered
	}
	produceRecords(t, broker, inputTopic, testData)

	consumed := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier"), 2, consumeWait)

	require.Len(t, consumed, 2)
	require.Equal(t, "goodbye", consumed["key3"])
	require.Equal(t, "streams", consumed["key4"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_ChainedProcessors_MapThenFilter(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			if strings.HasPrefix(key, "important") {
				return key, "IMPORTANT: " + value, nil
			}
			return key, "normal: " + value, nil
		},
	)

	filtered := kstream.Filter(
		mapped, func(_ context.Context, key, value string) (bool, error) {
			return strings.HasPrefix(value, "IMPORTANT:"), nil
		},
	)

	kstream.ToWithSerde(filtered, outputTopic, serde.String(), serde.String())

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

	time.Sleep(startupWait)

	testData := map[string]string{
		"important-1": "alert",
		"normal-1":    "info",
		"important-2": "warning",
		"normal-2":    "debug",
	}
	produceRecords(t, broker, inputTopic, testData)

	consumed := consumeAsMap(t, broker, outputTopic, testGroupID(t, "verifier"), 2, consumeWait)

	require.Len(t, consumed, 2)
	require.Equal(t, "IMPORTANT: alert", consumed["important-1"])
	require.Equal(t, "IMPORTANT: warning", consumed["important-2"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_BranchTopology_SplitsStream(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	highPriorityTopic := testTopicName(t, "high")
	lowPriorityTopic := testTopicName(t, "low")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, highPriorityTopic, lowPriorityTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	branches := kstream.Branch(
		source,
		kstream.NewBranch(
			"high", func(_ context.Context, key, value string) (bool, error) {
				return strings.HasPrefix(key, "high"), nil
			},
		),
		kstream.DefaultBranch[string, string]("low"),
	)

	kstream.ToWithSerde(branches.Get("high"), highPriorityTopic, serde.String(), serde.String())
	kstream.ToWithSerde(branches.Get("low"), lowPriorityTopic, serde.String(), serde.String())

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

	time.Sleep(startupWait)

	testData := map[string]string{
		"high-1": "urgent",
		"low-1":  "normal",
		"high-2": "critical",
		"other":  "misc",
	}
	produceRecords(t, broker, inputTopic, testData)

	highConsumed := consumeAsMap(t, broker, highPriorityTopic, testGroupID(t, "high-verifier"), 2, consumeWait)
	require.Equal(t, "urgent", highConsumed["high-1"])
	require.Equal(t, "critical", highConsumed["high-2"])

	lowConsumed := consumeAsMap(t, broker, lowPriorityTopic, testGroupID(t, "low-verifier"), 2, consumeWait)
	require.Equal(t, "normal", lowConsumed["low-1"])
	require.Equal(t, "misc", lowConsumed["other"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait)
}

func TestE2E_EmptyInput_NoOutput(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

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

	time.Sleep(3 * time.Second)

	cancel()
	waitForShutdown(t, errCh, shutdownWait)

	// Verify output topic is empty
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	verifyClient, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(testGroupID(t, "verifier")),
	)
	require.NoError(t, err)
	defer verifyClient.Close()

	err = verifyClient.Subscribe([]string{outputTopic}, noopRebalanceCallback{})
	require.NoError(t, err)

	records, err := verifyClient.Poll(ctx2)
	if err == nil {
		require.Empty(t, records, "expected no records in output topic")
	}
}
