//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	startupWait  = 2 * time.Second
	shutdownWait = 10 * time.Second
	consumeWait  = 30 * time.Second
	eventualWait = 15 * time.Second
)

var (
	testContainer  *redpanda.Container
	bootstrapAddr  string
	containerOnce  sync.Once
	containerError error
)

func TestMain(m *testing.M) {
	code := m.Run()

	if testContainer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testContainer.Terminate(ctx)
	}

	os.Exit(code)
}

func ensureContainer(t *testing.T) string {
	t.Helper()

	containerOnce.Do(
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			container, err := redpanda.Run(
				ctx,
				"docker.redpanda.com/redpandadata/redpanda:v24.2.1",
				redpanda.WithAutoCreateTopics(),
			)
			if err != nil {
				containerError = fmt.Errorf("failed to start redpanda container: %w", err)
				return
			}

			testContainer = container

			addr, err := container.KafkaSeedBroker(ctx)
			if err != nil {
				containerError = fmt.Errorf("failed to get kafka seed broker: %w", err)
				return
			}

			bootstrapAddr = addr
		},
	)

	require.NoError(t, containerError, "container initialization failed")
	require.NotEmpty(t, bootstrapAddr, "bootstrap address not set")

	return bootstrapAddr
}

func testTopicName(t *testing.T, suffix string) string {
	return fmt.Sprintf("e2e-test-%s-%d", suffix, time.Now().UnixNano())
}

func testGroupID(t *testing.T, suffix string) string {
	return testTopicName(t, suffix+"-group")
}

func createTopics(t *testing.T, broker string, numPartitions int32, topics ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	resp, err := admin.CreateTopics(ctx, numPartitions, 1, nil, topics...)
	require.NoError(t, err)

	for _, topic := range topics {
		topicResp, ok := resp[topic]
		require.True(t, ok, "topic %s not in response", topic)

		if topicResp.Err != nil && topicResp.Err.Error() != "TOPIC_ALREADY_EXISTS" {
			require.NoError(t, topicResp.Err, "failed to create topic %s", topic)
		}
	}

	t.Cleanup(
		func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()

			cleanupClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
			if err != nil {
				return
			}
			defer cleanupClient.Close()

			cleanupAdmin := kadm.NewClient(cleanupClient)
			_, _ = cleanupAdmin.DeleteTopics(cleanupCtx, topics...)
		},
	)
}

func waitForShutdown(t *testing.T, errCh <-chan error, timeout time.Duration) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	case <-time.After(timeout):
		t.Fatal("timeout waiting for application shutdown")
	}
}

func waitForGroupMembers(t *testing.T, broker, groupID string, expectedCount int, timeout time.Duration) {
	t.Helper()

	eventually(
		t, func() bool {
			return getConsumerGroupMembers(t, broker, groupID) == expectedCount
		}, timeout, fmt.Sprintf("expected %d members in consumer group", expectedCount),
	)
}

func produceRecords(t *testing.T, broker, topic string, records map[string]string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()

	for key, value := range records {
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(value),
		}
		results := client.ProduceSync(ctx, record)
		require.NoError(t, results.FirstErr(), "failed to produce record with key %s", key)
	}
}

func produceOrderedRecords(t *testing.T, broker, topic string, records []kgo.Record) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()

	for i := range records {
		records[i].Topic = topic
		results := client.ProduceSync(ctx, &records[i])
		require.NoError(t, results.FirstErr(), "failed to produce record %d", i)
	}
}

type consumedRecord struct {
	Key   string
	Value string
}

func consumeRecords(
	t *testing.T, broker, topic, groupID string, expectedCount int, timeout time.Duration,
) []consumedRecord {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()

	var records []consumedRecord
	for len(records) < expectedCount {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if errors.Is(err.Err, context.DeadlineExceeded) {
					t.Fatalf("timeout waiting for records: got %d, expected %d", len(records), expectedCount)
				}
			}
		}

		fetches.EachRecord(
			func(r *kgo.Record) {
				records = append(
					records, consumedRecord{
						Key:   string(r.Key),
						Value: string(r.Value),
					},
				)
			},
		)
	}

	return records
}

func consumeAsMap(
	t *testing.T, broker, topic, groupID string, expectedCount int, timeout time.Duration,
) map[string]string {
	t.Helper()

	records := consumeRecords(t, broker, topic, groupID, expectedCount, timeout)
	result := make(map[string]string, len(records))
	for _, r := range records {
		result[r.Key] = r.Value
	}
	return result
}

func eventually(t *testing.T, condition func() bool, timeout time.Duration, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				t.Fatalf("condition not met within %v: %s", timeout, msg)
			}
		}
	}
}

func getConsumerGroupMembers(t *testing.T, broker, groupID string) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	groups, err := admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return 0
	}

	group, ok := groups[groupID]
	if !ok {
		return 0
	}

	return len(group.Members)
}

func getCommittedOffsets(t *testing.T, broker, groupID string) map[string]map[int32]int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)

	offsets, err := admin.FetchOffsets(ctx, groupID)
	if err != nil {
		return nil
	}

	result := make(map[string]map[int32]int64)
	offsets.Each(
		func(o kadm.OffsetResponse) {
			if _, ok := result[o.Topic]; !ok {
				result[o.Topic] = make(map[int32]int64)
			}
			result[o.Topic][o.Partition] = o.Offset.At
		},
	)

	return result
}

type noopRebalanceCallback struct{}

func (noopRebalanceCallback) OnAssigned(ctx context.Context, p []kafka.TopicPartition) {}
func (noopRebalanceCallback) OnRevoked(ctx context.Context, p []kafka.TopicPartition)  {}
