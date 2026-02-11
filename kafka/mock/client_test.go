//go:build unit

package mockkafka_test

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
	var _ kafka.Client = (*mockkafka.Client)(nil)
}

func TestMockClient_Send(t *testing.T) {
	client := mockkafka.NewClient()

	err := client.Send(context.Background(), "test-topic", []byte("key"), []byte("value"), nil)
	require.NoError(t, err)

	records := client.ProducedRecords()
	require.Len(t, records, 1)
	require.Equal(t, "test-topic", records[0].Topic)
	require.Equal(t, []byte("key"), records[0].Key)
	require.Equal(t, []byte("value"), records[0].Value)
}

func TestMockClient_SendWithHeaders(t *testing.T) {
	client := mockkafka.NewClient()

	headers := []kafka.Header{
		{Key: "trace-id", Value: []byte("abc123")},
		{Key: "correlation", Value: []byte("xyz")},
	}

	err := client.Send(context.Background(), "test-topic", []byte("key"), []byte("value"), headers)
	require.NoError(t, err)

	records := client.ProducedRecords()
	require.Len(t, records, 1)

	traceVal, ok := kafka.HeaderValue(records[0].Headers, "trace-id")
	require.True(t, ok)
	require.Equal(t, []byte("abc123"), traceVal)

	corrVal, ok := kafka.HeaderValue(records[0].Headers, "correlation")
	require.True(t, ok)
	require.Equal(t, []byte("xyz"), corrVal)
}

func TestMockClient_SendMultiple(t *testing.T) {
	client := mockkafka.NewClient()

	for i := 0; i < 5; i++ {
		err := client.Send(context.Background(), "topic", []byte("key"), []byte("value"), nil)
		require.NoError(t, err)
	}

	require.Len(t, client.ProducedRecords(), 5)
}

func TestMockClient_ProducedRecordsForTopic(t *testing.T) {
	client := mockkafka.NewClient()

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
	client := mockkafka.NewClient()

	// Add records before subscribing
	client.AddRecords(
		"input", 0,
		mockkafka.SimpleRecord("k1", "v1"),
		mockkafka.SimpleRecord("k2", "v2"),
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
	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(1))

	client.AddRecords(
		"topic", 0,
		mockkafka.Record("k1", "v1").WithOffset(0).Build(),
		mockkafka.Record("k2", "v2").WithOffset(1).Build(),
		mockkafka.Record("k3", "v3").WithOffset(2).Build(),
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
	client := mockkafka.NewClient(mockkafka.WithMaxPollRecords(3))

	// Add 5 records
	client.AddRecords(
		"topic", 0, mockkafka.SimpleRecords("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5")...,
	)
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

func TestMockClient_MarkRecords(t *testing.T) {
	client := mockkafka.NewClient()

	record1 := kafka.ConsumerRecord{
		Topic:       "topic",
		Partition:   0,
		Offset:      10,
		Key:         []byte("k1"),
		Value:       []byte("v1"),
		LeaderEpoch: 1,
	}
	record2 := kafka.ConsumerRecord{
		Topic:       "topic",
		Partition:   0,
		Offset:      11,
		Key:         []byte("k2"),
		Value:       []byte("v2"),
		LeaderEpoch: 1,
	}

	client.MarkRecords(record1, record2)

	// Verify marked records are tracked
	marked := client.MarkedRecords()
	require.Len(t, marked, 2)

	// Verify marked offsets (should be next offset = record offset + 1)
	offsets := client.MarkedOffsets()
	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	require.Equal(t, int64(12), offsets[tp].Offset) // 11 + 1
}

func TestMockClient_MarkRecords_HighestOffsetWins(t *testing.T) {
	client := mockkafka.NewClient()

	// Mark records out of order
	record1 := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 5}
	record2 := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 10}
	record3 := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 7}

	client.MarkRecords(record1)
	client.MarkRecords(record2)
	client.MarkRecords(record3) // Lower offset, should not override

	offsets := client.MarkedOffsets()
	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	require.Equal(t, int64(11), offsets[tp].Offset) // Still 10 + 1
}

func TestMockClient_Commit(t *testing.T) {
	client := mockkafka.NewClient()

	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	record := kafka.ConsumerRecord{
		Topic:       "topic",
		Partition:   0,
		Offset:      10,
		LeaderEpoch: 1,
	}

	client.MarkRecords(record)
	err := client.Commit(context.Background())
	require.NoError(t, err)

	// Verify committed
	committed, ok := client.CommittedOffset(tp)
	require.True(t, ok)
	require.Equal(t, int64(11), committed.Offset) // next offset
	require.Equal(t, int32(1), committed.LeaderEpoch)

	// Verify marked records were cleared
	require.Len(t, client.MarkedRecords(), 0)
	require.Len(t, client.MarkedOffsets(), 0)
}

func TestMockClient_CommitMultiplePartitions(t *testing.T) {
	client := mockkafka.NewClient()

	records := []kafka.ConsumerRecord{
		{Topic: "topic", Partition: 0, Offset: 10},
		{Topic: "topic", Partition: 1, Offset: 20},
		{Topic: "topic", Partition: 2, Offset: 30},
	}

	client.MarkRecords(records...)
	err := client.Commit(context.Background())
	require.NoError(t, err)

	all := client.CommittedOffsets()
	require.Len(t, all, 3)
	require.Equal(t, int64(11), all[kafka.TopicPartition{Topic: "topic", Partition: 0}].Offset)
	require.Equal(t, int64(21), all[kafka.TopicPartition{Topic: "topic", Partition: 1}].Offset)
	require.Equal(t, int64(31), all[kafka.TopicPartition{Topic: "topic", Partition: 2}].Offset)
}

func TestMockClient_CommitWithoutMark(t *testing.T) {
	client := mockkafka.NewClient()

	// Commit without marking anything should succeed (no-op)
	err := client.Commit(context.Background())
	require.NoError(t, err)

	require.Len(t, client.CommittedOffsets(), 0)
}

func TestMockClient_SendError(t *testing.T) {
	expectedErr := errors.New("send failed")
	client := mockkafka.NewClient(mockkafka.WithSendError(expectedErr))

	err := client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.ErrorIs(t, err, expectedErr)

	// No records should be produced on error
	require.Len(t, client.ProducedRecords(), 0)
}

func TestMockClient_SendErrorFunc(t *testing.T) {
	client := mockkafka.NewClient()

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
	client := mockkafka.NewClient(mockkafka.WithPollError(expectedErr))

	client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})

	_, err := client.Poll(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestMockClient_CommitError(t *testing.T) {
	expectedErr := errors.New("commit failed")
	client := mockkafka.NewClient(mockkafka.WithCommitError(expectedErr))

	record := kafka.ConsumerRecord{Topic: "t", Partition: 0, Offset: 1}
	client.MarkRecords(record)

	err := client.Commit(context.Background())
	require.ErrorIs(t, err, expectedErr)

	// Offsets should not be committed on error
	require.Len(t, client.CommittedOffsets(), 0)

	// Marked records should still be there (not cleared on error)
	require.Len(t, client.MarkedRecords(), 1)
}

func TestMockClient_CommitContextCancellation(t *testing.T) {
	client := mockkafka.NewClient()

	record := kafka.ConsumerRecord{Topic: "t", Partition: 0, Offset: 1}
	client.MarkRecords(record)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before commit

	err := client.Commit(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// Offsets should not be committed
	require.Len(t, client.CommittedOffsets(), 0)
}

func TestMockClient_PingError(t *testing.T) {
	expectedErr := errors.New("ping failed")
	client := mockkafka.NewClient(mockkafka.WithPingError(expectedErr))

	err := client.Ping(context.Background())
	require.ErrorIs(t, err, expectedErr)
}

func TestMockClient_ClearSendError(t *testing.T) {
	client := mockkafka.NewClient(mockkafka.WithSendError(errors.New("initial error")))

	err := client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.Error(t, err)

	// Clear the error
	client.SetSendError(nil)

	err = client.Send(context.Background(), "topic", []byte("k"), []byte("v"), nil)
	require.NoError(t, err)
}

func TestMockClient_SubscribeTriggersOnAssigned(t *testing.T) {
	client := mockkafka.NewClient()

	client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))
	client.AddRecords("topic", 1, mockkafka.SimpleRecord("k2", "v2"))

	cb := &trackingRebalanceCallback{}
	err := client.Subscribe([]string{"topic"}, cb)
	require.NoError(t, err)

	require.Len(t, cb.assigned, 2)
}

func TestMockClient_TriggerAssign(t *testing.T) {
	client := mockkafka.NewClient()

	cb := &trackingRebalanceCallback{}
	_ = client.Subscribe([]string{"topic"}, cb)

	client.TriggerAssign(
		[]kafka.TopicPartition{
			{Topic: "topic", Partition: 5},
		},
	)

	require.Contains(t, cb.assigned, kafka.TopicPartition{Topic: "topic", Partition: 5})
}

func TestMockClient_TriggerRevoke(t *testing.T) {
	client := mockkafka.NewClient()

	client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))

	cb := &trackingRebalanceCallback{}
	_ = client.Subscribe([]string{"topic"}, cb)

	// Verify initial assignment
	require.Len(t, client.AssignedPartitions(), 1)

	// Revoke the partition
	client.TriggerRevoke(
		[]kafka.TopicPartition{
			{Topic: "topic", Partition: 0},
		},
	)

	require.Len(t, cb.revoked, 1)
	require.Len(t, client.AssignedPartitions(), 0)
}

func TestMockClient_Reset(t *testing.T) {
	client := mockkafka.NewClient()

	client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))
	_ = client.Subscribe([]string{"topic"}, &noopRebalanceCallback{})
	records, _ := client.Poll(context.Background())
	client.MarkRecords(records...)
	_ = client.Send(context.Background(), "out", []byte("k"), []byte("v"), nil)
	_ = client.Commit(context.Background())

	// Verify state exists
	require.Len(t, client.ProducedRecords(), 1)
	require.Len(t, client.CommittedOffsets(), 1)

	// Reset
	client.Reset()

	require.Len(t, client.ProducedRecords(), 0)
	require.Len(t, client.CommittedOffsets(), 0)
	require.Len(t, client.MarkedRecords(), 0)
	require.Len(t, client.MarkedOffsets(), 0)

	// Records should still be available for polling again
	polled, _ := client.Poll(context.Background())
	require.Len(t, polled, 1)
}

func TestMockClient_Clear(t *testing.T) {
	client := mockkafka.NewClient()

	client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))
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
	client := mockkafka.NewClient()

	require.False(t, client.IsClosed())

	client.Close()

	require.True(t, client.IsClosed())
}

func TestMockClient_FlushRespectsContext(t *testing.T) {
	client := mockkafka.NewClient()

	// Normal flush should succeed
	err := client.Flush(context.Background())
	require.NoError(t, err)

	// Cancelled context should return error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.Flush(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMockClient_ConcurrentSend(t *testing.T) {
	client := mockkafka.NewClient()

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
	client := mockkafka.NewClient()

	// Add many records
	for i := 0; i < 100; i++ {
		client.AddRecords("topic", 0, mockkafka.SimpleRecord("k", "v"))
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
	client := mockkafka.NewClient()

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
	client := mockkafka.NewClient()

	tp := kafka.TopicPartition{Topic: "topic", Partition: 0}
	record := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 9}
	client.MarkRecords(record)
	_ = client.Commit(context.Background())

	client.AssertCommitted(t, tp)
	client.AssertCommittedOffset(t, tp, 10) // offset 9 + 1
	client.AssertCommittedAtLeast(t, tp, 5)
	client.AssertCommittedAtLeast(t, tp, 10)
}

func TestMockClient_AssertMarked(t *testing.T) {
	client := mockkafka.NewClient()

	record1 := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 5}
	record2 := kafka.ConsumerRecord{Topic: "topic", Partition: 0, Offset: 10}

	client.MarkRecords(record1, record2)

	client.AssertMarkedCount(t, 2)
	client.AssertMarked(t, "topic", 0, 5)
	client.AssertMarked(t, "topic", 0, 10)
	client.AssertMarkedOffset(t, kafka.TopicPartition{Topic: "topic", Partition: 0}, 11) // 10 + 1
}

func TestRecord_Builder(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	record := mockkafka.Record("key", "value").
		WithOffset(42).
		WithTimestamp(ts).
		WithHeader("trace", []byte("123")).
		WithLeaderEpoch(5).
		Build()

	require.Equal(t, []byte("key"), record.Key)
	require.Equal(t, []byte("value"), record.Value)
	require.Equal(t, int64(42), record.Offset)
	require.Equal(t, ts, record.Timestamp)
	traceVal, ok := kafka.HeaderValue(record.Headers, "trace")
	require.True(t, ok)
	require.Equal(t, []byte("123"), traceVal)
	require.Equal(t, int32(5), record.LeaderEpoch)
}

func TestSimpleRecords(t *testing.T) {
	records := mockkafka.SimpleRecords("k1", "v1", "k2", "v2", "k3", "v3")

	require.Len(t, records, 3)
	require.Equal(t, []byte("k1"), records[0].Key)
	require.Equal(t, []byte("v2"), records[1].Value)
}

func TestSimpleRecords_PanicOnOdd(t *testing.T) {
	require.Panics(
		t, func() {
			//nolint:staticcheck
			mockkafka.SimpleRecords("k1", "v1", "k2")
		},
	)
}

type noopRebalanceCallback struct{}

func (n *noopRebalanceCallback) OnAssigned(partitions []kafka.TopicPartition) {}
func (n *noopRebalanceCallback) OnRevoked(partitions []kafka.TopicPartition)  {}

type trackingRebalanceCallback struct {
	assigned []kafka.TopicPartition
	revoked  []kafka.TopicPartition
}

func (t *trackingRebalanceCallback) OnAssigned(partitions []kafka.TopicPartition) {
	t.assigned = append(t.assigned, partitions...)
}

func (t *trackingRebalanceCallback) OnRevoked(partitions []kafka.TopicPartition) {
	t.revoked = append(t.revoked, partitions...)
}
