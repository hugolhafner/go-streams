//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	toxiproxyclient "github.com/Shopify/toxiproxy/v2/client"
	streams "github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

// Test 7: High latency (500ms + 100ms jitter) - no record loss.
func TestE2E_Chaos_HighLatency_NoRecordLoss(t *testing.T) {
	proxyBroker, _, proxy := ensureChaosNetwork(t)

	inputTopic := testTopicName(t, "chaos-latency-in")
	outputTopic := testTopicName(t, "chaos-latency-out")
	groupID := testGroupID(t, "chaos-latency")

	// Create topics via the direct broker (bypassing proxy)
	directBroker := getDirectBroker(t)
	createTopics(t, directBroker, 1, inputTopic, outputTopic)

	// Add latency toxic
	_, err := proxy.AddToxic(
		"latency_down", "latency", "downstream", 1.0, toxiproxyclient.Attributes{
			"latency": 500,
			"jitter":  100,
		},
	)
	require.NoError(t, err)

	// Build topology
	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			return key, strings.ToUpper(value), nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{proxyBroker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner()) }()

	waitForGroupMembers(t, directBroker, groupID, 1, eventualWait)

	// Produce 10 records via the direct broker
	testData := make(map[string]string)
	for i := 0; i < 10; i++ {
		testData[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	produceRecords(t, directBroker, inputTopic, testData)

	// Consume with extended timeout
	consumed := consumeAsMap(t, directBroker, outputTopic, testGroupID(t, "chaos-latency-verify"), 10, 90*time.Second)

	require.Len(t, consumed, 10, "all 10 records should arrive despite high latency")
	for k, v := range testData {
		require.Equal(t, strings.ToUpper(v), consumed[k], "record %s should be uppercased", k)
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}

// Test 8: Network partition - recovery after reconnect.
func TestE2E_Chaos_NetworkPartition_RecoverAfterReconnect(t *testing.T) {
	proxyBroker, _, proxy := ensureChaosNetwork(t)

	inputTopic := testTopicName(t, "chaos-partition-in")
	outputTopic := testTopicName(t, "chaos-partition-out")
	groupID := testGroupID(t, "chaos-partition")

	directBroker := getDirectBroker(t)
	createTopics(t, directBroker, 1, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			return key, "processed:" + value, nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{proxyBroker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner()) }()

	waitForGroupMembers(t, directBroker, groupID, 1, eventualWait)

	// Block all traffic (network partition)
	_, err = proxy.AddToxic(
		"partition", "limit_data", "downstream", 1.0, toxiproxyclient.Attributes{
			"bytes": 0,
		},
	)
	require.NoError(t, err)

	t.Log("Network partitioned for 3 seconds...")
	time.Sleep(3 * time.Second)

	// Remove partition
	err = proxy.RemoveToxic("partition")
	require.NoError(t, err)
	t.Log("Network partition removed")

	// Produce records after reconnection
	testData := map[string]string{
		"after1": "value1",
		"after2": "value2",
		"after3": "value3",
	}
	produceRecords(t, directBroker, inputTopic, testData)

	consumed := consumeAsMap(t, directBroker, outputTopic, testGroupID(t, "chaos-partition-verify"), 3, 60*time.Second)

	require.Len(t, consumed, 3, "all records should arrive after partition heals")
	for k, v := range testData {
		require.Equal(t, "processed:"+v, consumed[k])
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}

// Test 9: Bandwidth throttle (1KB/s) - slow but correct.
func TestE2E_Chaos_BandwidthThrottle_SlowButCorrect(t *testing.T) {
	proxyBroker, _, proxy := ensureChaosNetwork(t)

	inputTopic := testTopicName(t, "chaos-bw-in")
	outputTopic := testTopicName(t, "chaos-bw-out")
	groupID := testGroupID(t, "chaos-bw")

	directBroker := getDirectBroker(t)
	createTopics(t, directBroker, 1, inputTopic, outputTopic)

	// Add bandwidth toxic: 1KB/s downstream
	_, err := proxy.AddToxic(
		"bandwidth_down", "bandwidth", "downstream", 1.0, toxiproxyclient.Attributes{
			"rate": 1024,
		},
	)
	require.NoError(t, err)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			return key, value + "-done", nil
		},
	)
	kstream.ToWithSerde(mapped, outputTopic, serde.String(), serde.String())

	client, err := kafka.NewKgoClient(
		kafka.WithBootstrapServers([]string{proxyBroker}),
		kafka.WithGroupID(groupID),
	)
	require.NoError(t, err)
	defer client.Close()

	app, err := streams.NewApplication(client, builder.Build())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner()) }()

	waitForGroupMembers(t, directBroker, groupID, 1, eventualWait)

	// Produce 5 small records via direct broker
	testData := make(map[string]string)
	for i := 0; i < 5; i++ {
		testData[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	produceRecords(t, directBroker, inputTopic, testData)

	// Extended timeout due to bandwidth throttle
	consumed := consumeAsMap(t, directBroker, outputTopic, testGroupID(t, "chaos-bw-verify"), 5, 120*time.Second)

	require.Len(t, consumed, 5, "all 5 records should arrive despite bandwidth throttle")
	for k, v := range testData {
		require.Equal(t, v+"-done", consumed[k])
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}

// getDirectBroker returns the direct Redpanda broker address for the chaos network's Redpanda.
func getDirectBroker(t *testing.T) string {
	t.Helper()

	// Ensure the chaos network is set up
	ensureChaosNetwork(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr, err := chaosRedpanda.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return addr
}
