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

// TestE2E_SimpleTopology_ProcessesRecords verifies that a basic
// source -> map -> sink topology correctly processes records from
// an input topic and produces transformed records to an output topic.
func TestE2E_SimpleTopology_ProcessesRecords(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	// Create topics with single partition for deterministic ordering
	createTopics(t, broker, 1, inputTopic, outputTopic)

	// Build topology: source -> uppercase map -> sink
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	// Transform: uppercase the value
	mapped := kstream.Map(
		source, func(key, value string) (string, string) {
			return key, strings.ToUpper(value)
		},
	)

	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	topology := builder.Build()

	// Create Kafka client
	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	// Create and start application
	app, err := streams.NewApplication(client, topology)
	require.NoError(t, err)

	// Run application in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	// Give the consumer time to join the group and get assignments
	time.Sleep(2 * time.Second)

	// Produce test records
	testData := map[string]string{
		"key1": "hello",
		"key2": "world",
		"key3": "kafka",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Consume from output topic and verify
	outputGroupID := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, outputGroupID, len(testData), 30*time.Second)

	require.Len(t, records, len(testData))

	// Build a map of consumed records for easier verification
	consumed := make(map[string]string)
	for _, r := range records {
		consumed[r.Key] = r.Value
	}

	// Verify transformations
	require.Equal(t, "HELLO", consumed["key1"])
	require.Equal(t, "WORLD", consumed["key2"])
	require.Equal(t, "KAFKA", consumed["key3"])

	// Cleanup
	cancel()

	// Wait for clean shutdown
	select {
	case err := <-errCh:
		// Context cancellation is expected
		if err != nil && err != context.Canceled {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for application shutdown")
	}
}

// TestE2E_FilterTopology_DropsRecords verifies that a filter processor
// correctly drops records that don't match the predicate.
func TestE2E_FilterTopology_DropsRecords(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	// Build topology: source -> filter (only values > 5 chars) -> sink
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	filtered := kstream.Filter(
		source, func(key, value string) bool {
			return len(value) > 5
		},
	)

	kstream.ToWithSerde(filtered, outputTopic, serde.String(), serde.String())

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
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	time.Sleep(2 * time.Second)

	// Produce test records - some should pass, some should be filtered
	testData := map[string]string{
		"key1": "hi",      // 2 chars - filtered
		"key2": "hello",   // 5 chars - filtered (not > 5)
		"key3": "goodbye", // 7 chars - passes
		"key4": "streams", // 7 chars - passes
		"key5": "test",    // 4 chars - filtered
	}
	produceRecords(t, broker, inputTopic, testData)

	// Only 2 records should pass the filter
	outputGroupID := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, outputGroupID, 2, 30*time.Second)

	require.Len(t, records, 2)

	consumed := make(map[string]string)
	for _, r := range records {
		consumed[r.Key] = r.Value
	}

	require.Equal(t, "goodbye", consumed["key3"])
	require.Equal(t, "streams", consumed["key4"])

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

// TestE2E_ChainedProcessors_MapThenFilter verifies that multiple
// processors can be chained together.
func TestE2E_ChainedProcessors_MapThenFilter(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	outputTopic := testTopicName(t, "output")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, outputTopic)

	// Build topology: source -> map (add prefix) -> filter (only IMPORTANT) -> sink
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	// First map: add prefix based on key
	mapped := kstream.Map(
		source, func(key, value string) (string, string) {
			if strings.HasPrefix(key, "important") {
				return key, "IMPORTANT: " + value
			}
			return key, "normal: " + value
		},
	)

	// Then filter: only keep IMPORTANT messages
	filtered := kstream.Filter(
		mapped, func(key, value string) bool {
			return strings.HasPrefix(value, "IMPORTANT:")
		},
	)

	kstream.ToWithSerde(filtered, outputTopic, serde.String(), serde.String())

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
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	time.Sleep(2 * time.Second)

	testData := map[string]string{
		"important-1": "alert",
		"normal-1":    "info",
		"important-2": "warning",
		"normal-2":    "debug",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Only important messages should pass
	outputGroupID := testGroupID(t, "verifier")
	records := consumeRecords(t, broker, outputTopic, outputGroupID, 2, 30*time.Second)

	require.Len(t, records, 2)

	consumed := make(map[string]string)
	for _, r := range records {
		consumed[r.Key] = r.Value
	}

	require.Equal(t, "IMPORTANT: alert", consumed["important-1"])
	require.Equal(t, "IMPORTANT: warning", consumed["important-2"])

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

// TestE2E_BranchTopology_SplitsStream verifies that branching
// correctly routes records to different outputs.
func TestE2E_BranchTopology_SplitsStream(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "input")
	highPriorityTopic := testTopicName(t, "high")
	lowPriorityTopic := testTopicName(t, "low")
	groupID := testGroupID(t, "processor")

	createTopics(t, broker, 1, inputTopic, highPriorityTopic, lowPriorityTopic)

	// Build topology with branching
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())

	branches := kstream.Branch(
		source,
		kstream.NewBranch(
			"high", func(key, value string) bool {
				return strings.HasPrefix(key, "high")
			},
		),
		kstream.DefaultBranch[string, string]("low"),
	)

	kstream.ToWithSerde(branches.Get("high"), highPriorityTopic, serde.String(), serde.String())
	kstream.ToWithSerde(branches.Get("low"), lowPriorityTopic, serde.String(), serde.String())

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
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	time.Sleep(2 * time.Second)

	testData := map[string]string{
		"high-1": "urgent",
		"low-1":  "normal",
		"high-2": "critical",
		"other":  "misc",
	}
	produceRecords(t, broker, inputTopic, testData)

	// Verify high priority topic
	highGroupID := testGroupID(t, "high-verifier")
	highRecords := consumeRecords(t, broker, highPriorityTopic, highGroupID, 2, 30*time.Second)
	require.Len(t, highRecords, 2)

	highConsumed := make(map[string]string)
	for _, r := range highRecords {
		highConsumed[r.Key] = r.Value
	}
	require.Equal(t, "urgent", highConsumed["high-1"])
	require.Equal(t, "critical", highConsumed["high-2"])

	// Verify low priority topic (default branch catches the rest)
	lowGroupID := testGroupID(t, "low-verifier")
	lowRecords := consumeRecords(t, broker, lowPriorityTopic, lowGroupID, 2, 30*time.Second)
	require.Len(t, lowRecords, 2)

	lowConsumed := make(map[string]string)
	for _, r := range lowRecords {
		lowConsumed[r.Key] = r.Value
	}
	require.Equal(t, "normal", lowConsumed["low-1"])
	require.Equal(t, "misc", lowConsumed["other"])

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

// TestE2E_EmptyInput_NoOutput verifies that when no records are
// produced, no output is generated.
func TestE2E_EmptyInput_NoOutput(t *testing.T) {
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner())
	}()

	// Let it run for a bit without producing anything
	time.Sleep(3 * time.Second)

	// Stop the application
	cancel()

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			require.NoError(t, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for application shutdown")
	}

	// Verify output topic is empty by trying to consume with a short timeout
	// This should timeout because there are no records
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	outputGroupID := testGroupID(t, "verifier")
	verifyClient, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{broker}),
		kafka.WithGroupID(outputGroupID),
	)
	require.NoError(t, err)
	defer verifyClient.Close()

	err = verifyClient.Subscribe([]string{outputTopic}, noopRebalanceCallback{})
	require.NoError(t, err)

	records, err := verifyClient.Poll(ctx2)
	if err == nil {
		require.Empty(t, records, "expected no records in output topic")
	}
	// Context deadline exceeded is expected since there are no records
}

// noopRebalanceCallback is a no-op implementation of RebalanceCallback for verification.
type noopRebalanceCallback struct{}

func (noopRebalanceCallback) OnAssigned(partitions []kafka.TopicPartition) error { return nil }
func (noopRebalanceCallback) OnRevoked(partitions []kafka.TopicPartition) error  { return nil }
