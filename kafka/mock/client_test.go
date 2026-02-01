//go:build unit

package mock_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kafka/mock"
	"github.com/stretchr/testify/require"
)

func TestMockClient_ImplementsInterface(t *testing.T) {
	var _ kafka.Client = (*mock.Client)(nil)
}

func TestMockClient_Send(t *testing.T) {
	client := mock.NewClient()

	err := client.Send(context.Background(), "test-topic", []byte("key"), []byte("value"), nil)
	require.NoError(t, err)

	records := client.ProducedRecords()
	require.Len(t, records, 1)
	require.Equal(t, "test-topic", records[0].Topic)
	require.Equal(t, []byte("key"), records[0].Key)
	require.Equal(t, []byte("value"), records[0].Value)
}

func TestMockClient_SendWithHeaders(t *testing.T) {
	client := mock.NewClient()

	headers := map[string][]byte{
		"trace-id":    []byte("abc123"),
		"correlation": []byte("xyz"),
	}

	err := client.Send(context.Background(), "test-topic", []byte("key"), []byte("value"), headers)
	require.NoError(t, err)

	records := client.ProducedRecords()
	require.Len(t, records, 1)
	require.Equal(t, []byte("abc123"), records[0].Headers["trace-id"])
	require.Equal(t, []byte("xyz"), records[0].Headers["correlation"])
}

func TestMockClient_SendMultiple(t *testing.T) {
	client := mock.NewClient()

	for i := 0; i < 5; i++ {
		err := client.Send(context.Background(), "topic", []byte("key"), []byte("value"), nil)
		require.NoError(t, err)
	}

	require.Len(t, client.ProducedRecords(), 5)
}

func TestMockClient_ProducedRecordsForTopic(t *testing.T) {
	client := mock.NewClient()

	_ = client.Send(context.Background(), "topic-a", []byte("k1"), []byte("v1"), nil)
	_ = client.Send(context.Background(), "topic-b", []byte("k2"), []byte("v2"), nil)
	_ = client.Send(context.Background(), "topic-a", []byte("k3"), []byte("v3"), nil)

	topicARecords := client.ProducedRecordsForTopic("topic-a")
	require.Len(t, topicARecords, 2)

	topicBRecords := client.ProducedRecordsForTopic("topic-b")
	require.Len(t, topicBRecords, 1)

	topicCRecords := client.ProducedRecordsForTopic("topic-c")
	require.Len(t, topicCRecords, 0)
}

func TestMockClient_SubscribeAndPoll(t *testing.T) {
	client := mock.NewClient()

	// Add records before subscribing
	client.AddRecords(
		"input", 0,
		mock.SimpleRecord("k1", "v1"),
		mock.SimpleRecord("k2", "v2"),
	)

	// Subscribe with a noop callback
	err := client.Subscribe([]string{"input"}, &noopRebalanceCallback{})
	require.NoError(t, err)

	// Poll should return the records
	records, err := client.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, records, 2)

	require.Equal(t, "input", records[0].Topic)
	require.Equal(t, []byte("k1"), records[0].Key)
	require.Equal(t, []byte("v1"), records[0].Value)
}

func TestMockClient_PollReturnsRecordsInOrder(t *testing.T) {
	client := mock.NewClient(mock.WithMaxPollRecords(1))

	client.AddRecords(
		"topic", 0,
		mock.Record("k1", "v1").WithOffset(0).Build(),
		mock.Record("k2", "v2").WithOffset(1).Build(),
		mock.Record("k3", "v3").WithOffset(2).Build(),
	)

	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})

	// Poll returns one at a time
	r1, _ := client.Poll(context.Background())
	require.Len(t, r1, 1)
	require.Equal(t, []byte("k1"), r1[0].Key)

	r2, _ := client.Poll(context.Background())
	require.Len(t, r2, 1)
	require.Equal(t, []byte("k2"), r2[0].Key)

	r3, _ := client.Poll(context.Background())
	require.Len(t, r3, 1)
	require.Equal(t, []byte("k3"), r3[0].Key)

	// No more records
	r4, _ := client.Poll(context.Background())
	require.Len(t, r4, 0)
}

func TestMockClient_PollWithMaxRecords(t *testing.T) {
	client := mock.NewClient(mock.WithMaxPollRecords(3))

	// Add 5 records
	client.AddRecords("topic", 0, mock.SimpleRecords("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5")...)
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})

	// First poll returns 3
	r1, _ := client.Poll(context.Background())
	require.Len(t, r1, 3)

	// Second poll returns remaining 2
	r2, _ := client.Poll(context.Background())
	require.Len(t, r2, 2)

	// Third poll returns 0
	r3, _ := client.Poll(context.Background())
	require.Len(t, r3, 0)
}

func TestMockClient_Commit(t *testing.T) {
	client := mock.NewClient()

	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	offset := kafka.Offset{Offset: 10, LeaderEpoch: 1}

	err := client.Commit(map[kafka.TopicPartition]kafka.Offset{tp: offset})
	require.NoError(t, err)

	committed, ok := client.CommittedOffset(tp)
	require.True(t, ok)
	require.Equal(t, int64(10), committed.Offset)
	require.Equal(t, int32(1), committed.LeaderEpoch)
}

func TestMockClient_CommitMultiplePartitions(t *testing.T) {
	client := mock.NewClient()

	offsets := map[kafka.TopicPartition]kafka.Offset{
		{Topic: "topic", Partition: 0}: {Offset: 10},
		{Topic: "topic", Partition: 1}: {Offset: 20},
		{Topic: "topic", Partition: 2}: {Offset: 30},
	}

	err := client.Commit(offsets)
	require.NoError(t, err)

	all := client.CommittedOffsets()
	require.Len(t, all, 3)
}

func TestMockClient_SendError(t *testing.T) {
	expectedErr := errors.New("send failed")
	client := mock.NewClient(mock.WithSendError(expectedErr))

	err := client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.ErrorIs(t, err, expectedErr)

	// No records should be produced on error
	require.Len(t, client.ProducedRecords(), 0)
}

func TestMockClient_SendErrorFunc(t *testing.T) {
	client := mock.NewClient()

	// Fail only on specific topic
	client.SetSendErrorFunc(
		func(topic string, key, value []byte) error {
			if topic == "fail-topic" {
				return errors.New("this topic fails")
			}
			return nil
		},
	)

	err1 := client.Send(context.Background(), "ok-topic", []byte("k"), []byte("v"), nil)
	require.NoError(t, err1)

	err2 := client.Send(context.Background(), "fail-topic", []byte("k"), []byte("v"), nil)
	require.Error(t, err2)

	require.Len(t, client.ProducedRecords(), 1)
}

func TestMockClient_PollError(t *testing.T) {
	expectedErr := errors.New("poll failed")
	client := mock.NewClient(mock.WithPollError(expectedErr))

	client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})

	_, err := client.Poll(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestMockClient_CommitError(t *testing.T) {
	expectedErr := errors.New("commit failed")
	client := mock.NewClient(mock.WithCommitError(expectedErr))

	err := client.Commit(
		map[kafka.TopicPartition]kafka.Offset{
			{Topic: "t", Partition: 0}: {Offset: 1},
		},
	)
	require.ErrorIs(t, err, expectedErr)

	// Offsets should not be committed on error
	require.Len(t, client.CommittedOffsets(), 0)
}

func TestMockClient_PingError(t *testing.T) {
	expectedErr := errors.New("ping failed")
	client := mock.NewClient(mock.WithPingError(expectedErr))

	err := client.Ping(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestMockClient_ClearSendError(t *testing.T) {
	client := mock.NewClient(mock.WithSendError(errors.New("initial error")))

	err := client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.Error(t, err)

	// Clear the error
	client.SetSendError(nil)

	err = client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.NoError(t, err)
}

func TestMockClient_SubscribeTriggersOnAssigned(t *testing.T) {
	client := mock.NewClient()

	client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))
	client.AddRecords("topic", 1, mock.SimpleRecord("k2", "v2"))

	cb := &trackingRebalanceCallback{}
	err := client.Subscribe([]string{"topic"}, cb)
	require.NoError(t, err)

	require.Len(t, cb.assigned, 2)
}

func TestMockClient_TriggerAssign(t *testing.T) {
	client := mock.NewClient()

	cb := &trackingRebalanceCallback{}
	_ = client.Subscribe([]string{"topic"}, cb)

	err := client.TriggerAssign(
		[]kafka.TopicPartition{
			{Topic: "topic", Partition: 5},
		},
	)
	require.NoError(t, err)

	require.Contains(t, cb.assigned, kafka.TopicPartition{Topic: "topic", Partition: 5})
}

func TestMockClient_TriggerRevoke(t *testing.T) {
	client := mock.NewClient()

	client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))

	cb := &trackingRebalanceCallback{}
	_ = client.Subscribe([]string{"topic"}, cb)

	// Verify initial assignment
	require.Len(t, client.AssignedPartitions(), 1)

	// Revoke the partition
	err := client.TriggerRevoke(
		[]kafka.TopicPartition{
			{Topic: "topic", Partition: 0},
		},
	)
	require.NoError(t, err)

	require.Len(t, cb.revoked, 1)
	require.Len(t, client.AssignedPartitions(), 0)
}

func TestMockClient_Reset(t *testing.T) {
	client := mock.NewClient()

	client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})
	_, _ = client.Poll(context.Background())
	_ = client.Send(context.Background(), "out", []byte("k"), []byte("v"), nil)
	_ = client.Commit(map[kafka.TopicPartition]kafka.Offset{{Topic: "topic", Partition: 0}: {Offset: 1}})

	// Verify state exists
	require.Len(t, client.ProducedRecords(), 1)
	require.Len(t, client.CommittedOffsets(), 1)

	// Reset
	client.Reset()

	require.Len(t, client.ProducedRecords(), 0)
	require.Len(t, client.CommittedOffsets(), 0)

	// Records should still be available for polling again
	records, _ := client.Poll(context.Background())
	require.Len(t, records, 1)
}

func TestMockClient_Clear(t *testing.T) {
	client := mock.NewClient()

	client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})
	_, _ = client.Poll(context.Background())
	_ = client.Send(context.Background(), "out", []byte("k"), []byte("v"), nil)

	// Clear everything
	client.Clear()

	require.Len(t, client.ProducedRecords(), 0)
	require.Len(t, client.CommittedOffsets(), 0)
	require.Len(t, client.Subscriptions(), 0)

	// No records available
	records, _ := client.Poll(context.Background())
	require.Len(t, records, 0)
}

func TestMockClient_Close(t *testing.T) {
	client := mock.NewClient()

	require.False(t, client.IsClosed())

	client.Close()

	require.True(t, client.IsClosed())
}

func TestMockClient_ConcurrentSend(t *testing.T) {
	client := mock.NewClient()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
		}(i)
	}
	wg.Wait()

	require.Len(t, client.ProducedRecords(), 100)
}

func TestMockClient_ConcurrentPoll(t *testing.T) {
	client := mock.NewClient()

	// Add many records
	for i := 0; i < 100; i++ {
		client.AddRecords("topic", 0, mock.SimpleRecord("k", "v"))
	}
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})

	var wg sync.WaitGroup
	var mu sync.Mutex
	totalRecords := 0

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				records, _ := client.Poll(context.Background())
				if len(records) == 0 {
					return
				}
				mu.Lock()
				totalRecords += len(records)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 100, totalRecords)
}

func TestMockClient_AssertProduced(t *testing.T) {
	client := mock.NewClient()

	_ = client.Send(context.Background(), "topic", []byte("key1"), []byte("value1"), nil)
	_ = client.Send(context.Background(), "topic", []byte("key2"), []byte("value2"), nil)

	// These should pass (using actual testing.T)
	client.AssertProducedCount(t, 2)
	client.AssertProducedCountForTopic(t, "topic", 2)
	client.AssertProduced(t, "topic", []byte("key1"), []byte("value1"))
	client.AssertProducedString(t, "topic", "key2", "value2")
	client.AssertProducedKey(t, "topic", []byte("key1"))
	client.AssertNotProduced(t, "topic", []byte("nonexistent"))
}

func TestMockClient_AssertCommitted(t *testing.T) {
	client := mock.NewClient()

	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	_ = client.Commit(map[kafka.TopicPartition]kafka.Offset{tp: {Offset: 10}})

	client.AssertCommitted(t, tp)
	client.AssertCommittedOffset(t, tp, 10)
	client.AssertCommittedAtLeast(t, tp, 5)
	client.AssertCommittedAtLeast(t, tp, 10)
}

func TestRecord_Builder(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	record := mock.Record("key", "value").
		WithOffset(42).
		WithTimestamp(ts).
		WithHeader("trace", []byte("123")).
		WithLeaderEpoch(5).
		Build()

	require.Equal(t, []byte("key"), record.Key)
	require.Equal(t, []byte("value"), record.Value)
	require.Equal(t, int64(42), record.Offset)
	require.Equal(t, ts, record.Timestamp)
	require.Equal(t, []byte("123"), record.Headers["trace"])
	require.Equal(t, int32(5), record.LeaderEpoch)
}

func TestSimpleRecords(t *testing.T) {
	records := mock.SimpleRecords("k1", "v1", "k2", "v2", "k3", "v3")

	require.Len(t, records, 3)
	require.Equal(t, []byte("k1"), records[0].Key)
	require.Equal(t, []byte("v2"), records[1].Value)
}

func TestSimpleRecords_PanicOnOdd(t *testing.T) {
	require.Panics(
		t, func() {
			//nolint:staticcheck
			mock.SimpleRecords("k1", "v1", "k2")
		},
	)
}

type noopRebalanceCallback struct{}

func (n *noopRebalanceCallback) OnAssigned(partitions []kafka.TopicPartition) error { return nil }
func (n *noopRebalanceCallback) OnRevoked(partitions []kafka.TopicPartition) error  { return nil }

type trackingRebalanceCallback struct {
	assigned []kafka.TopicPartition
	revoked  []kafka.TopicPartition
}

func (t *trackingRebalanceCallback) OnAssigned(partitions []kafka.TopicPartition) error {
	t.assigned = append(t.assigned, partitions...)
	return nil
}

func (t *trackingRebalanceCallback) OnRevoked(partitions []kafka.TopicPartition) error {
	t.revoked = append(t.revoked, partitions...)
	return nil
}
