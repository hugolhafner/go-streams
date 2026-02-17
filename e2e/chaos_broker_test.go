//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	streams "github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
)

// Test 10: Broker restart with SingleThreaded runner - recovers and processes both batches.
func TestE2E_Chaos_BrokerRestart_SingleThreaded_Recovers(t *testing.T) {
	container, broker := startDedicatedBroker(t)

	inputTopic := testTopicName(t, "broker-restart-st-in")
	outputTopic := testTopicName(t, "broker-restart-st-out")
	groupID := testGroupID(t, "broker-restart-st")

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
	go func() { errCh <- app.RunWith(ctx, runner.NewSingleThreadedRunner()) }()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	// Batch 1
	batch1 := map[string]string{"b1k1": "hello", "b1k2": "world"}
	produceRecords(t, broker, inputTopic, batch1)

	consumed1 := consumeAsMap(t, broker, outputTopic, testGroupID(t, "broker-restart-st-v1"), 2, consumeWait)
	require.Equal(t, "HELLO", consumed1["b1k1"])
	require.Equal(t, "WORLD", consumed1["b1k2"])

	// Stop broker
	t.Log("Stopping broker...")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	err = container.Stop(stopCtx, nil)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// Restart broker
	t.Log("Starting broker...")
	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	err = container.Start(startCtx)
	require.NoError(t, err)

	// Get new broker address after restart
	newBroker, err := container.KafkaSeedBroker(startCtx)
	require.NoError(t, err)

	waitForGroupMembers(t, newBroker, groupID, 1, eventualWait)

	// Batch 2
	batch2 := map[string]string{"b2k1": "after", "b2k2": "restart"}
	produceRecords(t, newBroker, inputTopic, batch2)

	consumed2 := consumeAsMap(t, newBroker, outputTopic, testGroupID(t, "broker-restart-st-v2"), 2, consumeWait*2)
	require.Equal(t, "AFTER", consumed2["b2k1"])
	require.Equal(t, "RESTART", consumed2["b2k2"])

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}

// Test 11: Broker restart with PartitionedRunner - all workers recover.
func TestE2E_Chaos_BrokerRestart_Partitioned_AllWorkersRecover(t *testing.T) {
	container, broker := startDedicatedBroker(t)

	inputTopic := testTopicName(t, "broker-restart-pr-in")
	outputTopic := testTopicName(t, "broker-restart-pr-out")
	groupID := testGroupID(t, "broker-restart-pr")

	createTopics(t, broker, 3, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(
		source, func(_ context.Context, key, value string) (string, string, error) {
			return key, "done:" + value, nil
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
		errCh <- app.RunWith(
			ctx, runner.NewPartitionedRunner(
				runner.WithChannelBufferSize(10),
			),
		)
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	// Batch 1: records across partitions
	batch1 := make(map[string]string)
	for i := 0; i < 6; i++ {
		batch1[fmt.Sprintf("b1k%d", i)] = fmt.Sprintf("v%d", i)
	}
	produceRecords(t, broker, inputTopic, batch1)

	consumed1 := consumeAsMap(t, broker, outputTopic, testGroupID(t, "broker-restart-pr-v1"), 6, consumeWait)
	require.Len(t, consumed1, 6)

	// Stop broker
	t.Log("Stopping broker...")
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	err = container.Stop(stopCtx, nil)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	// Restart broker
	t.Log("Starting broker...")
	startCtx, startCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startCancel()
	err = container.Start(startCtx)
	require.NoError(t, err)

	newBroker, err := container.KafkaSeedBroker(startCtx)
	require.NoError(t, err)

	waitForGroupMembers(t, newBroker, groupID, 1, eventualWait)

	// Batch 2
	batch2 := make(map[string]string)
	for i := 0; i < 6; i++ {
		batch2[fmt.Sprintf("b2k%d", i)] = fmt.Sprintf("v%d", i)
	}
	produceRecords(t, newBroker, inputTopic, batch2)

	consumed2 := consumeAsMap(t, newBroker, outputTopic, testGroupID(t, "broker-restart-pr-v2"), 6, consumeWait*2)
	require.Len(t, consumed2, 6)
	for k, v := range batch2 {
		require.Equal(t, "done:"+v, consumed2[k])
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}
