//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	streams "github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Test 22: Burst 1000 records with PartitionedRunner on 4 partitions.
func TestE2E_Chaos_Burst_1000Records_PartitionedRunner(t *testing.T) {
	broker := ensureContainer(t)

	inputTopic := testTopicName(t, "chaos-burst-in")
	outputTopic := testTopicName(t, "chaos-burst-out")
	groupID := testGroupID(t, "chaos-burst")

	createTopics(t, broker, 4, inputTopic, outputTopic)

	builder := kstream.NewStreamsBuilder()
	source := kstream.StreamWithSerde(builder, inputTopic, serde.String(), serde.String())
	mapped := kstream.Map(source, func(_ context.Context, key, value string) (string, string, error) {
		// 5ms processing delay
		time.Sleep(5 * time.Millisecond)
		return key, "done:" + value, nil
	})
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
		errCh <- app.RunWith(ctx, runner.NewPartitionedRunner(
			runner.WithChannelBufferSize(10),
		))
	}()

	waitForGroupMembers(t, broker, groupID, 1, eventualWait)

	// Produce 1000 records rapidly
	totalRecords := 1000
	records := make([]kgo.Record, totalRecords)
	for i := 0; i < totalRecords; i++ {
		records[i] = kgo.Record{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: []byte(fmt.Sprintf("v%04d", i)),
		}
	}
	produceOrderedRecords(t, broker, inputTopic, records)
	t.Logf("Produced %d records", totalRecords)

	// Consume all records from output with extended timeout
	consumed := consumeRecords(t, broker, outputTopic, testGroupID(t, "chaos-burst-verify"), totalRecords, 120*time.Second)

	require.Len(t, consumed, totalRecords, "all 1000 records should be consumed from output")

	// Verify values are correctly processed
	consumedMap := make(map[string]string, len(consumed))
	for _, r := range consumed {
		consumedMap[r.Key] = r.Value
	}

	for i := 0; i < totalRecords; i++ {
		key := fmt.Sprintf("k%04d", i)
		expected := fmt.Sprintf("done:v%04d", i)
		require.Equal(t, expected, consumedMap[key], "record %s should have correct value", key)
	}

	cancel()
	waitForShutdown(t, errCh, shutdownWait*3)
}
