package mockkafka

import (
	"bytes"
	"testing"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/require"
)

// AssertProducedCount verifies that exactly n records were produced.
func (c *Client) AssertProducedCount(tb testing.TB, expected int) {
	tb.Helper()

	actual := len(c.ProducedRecords())
	require.Equal(tb, expected, actual, "expected %d records, got %d", expected, actual)
}

// AssertProducedCountForTopic verifies that exactly n records were produced to a topic.
func (c *Client) AssertProducedCountForTopic(tb testing.TB, topic string, expected int) {
	tb.Helper()

	actual := len(c.ProducedRecordsForTopic(topic))
	require.Equal(tb, expected, actual, "expected %d records produced to topic %q, got %d", expected, topic, actual)
}

// AssertProduced verifies that a record with the given key and value was produced to the topic.
func (c *Client) AssertProduced(tb testing.TB, topic string, key, value []byte) {
	tb.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) && bytes.Equal(r.Value, value) {
			return
		}
	}

	tb.Errorf(
		"expected record with key=%q value=%q to be produced to topic %q, but it was not found",
		string(key), string(value), topic,
	)
}

// AssertProducedString is a convenience method for string keys and values.
func (c *Client) AssertProducedString(tb testing.TB, topic, key, value string) {
	tb.Helper()
	c.AssertProduced(tb, topic, []byte(key), []byte(value))
}

// AssertProducedKey verifies that a record with the given key was produced to the topic.
func (c *Client) AssertProducedKey(tb testing.TB, topic string, key []byte) {
	tb.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			return
		}
	}

	tb.Errorf(
		"expected record with key=%q to be produced to topic %q, but it was not found",
		string(key), topic,
	)
}

// AssertNotProduced verifies that no record with the given key was produced to the topic.
func (c *Client) AssertNotProduced(tb testing.TB, topic string, key []byte) {
	tb.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			tb.Errorf(
				"expected no record with key=%q to be produced to topic %q, but found value=%q",
				string(key), topic, string(r.Value),
			)
			return
		}
	}
}

// AssertCommitted verifies that an offset was committed for the topic-partition.
func (c *Client) AssertCommitted(tb testing.TB, tp kafka.TopicPartition) {
	tb.Helper()

	_, ok := c.CommittedOffset(tp)
	require.True(tb, ok, "committed offset not found for %s-%d", tp.Topic, tp.Partition)
}

// AssertCommittedOffset verifies that a specific offset was committed.
func (c *Client) AssertCommittedOffset(tb testing.TB, tp kafka.TopicPartition, expectedOffset int64) {
	tb.Helper()

	actual, ok := c.CommittedOffset(tp)
	require.True(
		tb, ok,
		"expected offset %d to be committed for %s-%d, but none found",
		expectedOffset, tp.Topic, tp.Partition,
	)

	require.Equal(
		tb, expectedOffset, actual.Offset, "expected offset %d to be committed for %s-%d, got %d", expectedOffset,
		tp.Topic, tp.Partition, actual.Offset,
	)
}

// AssertCommittedAtLeast verifies that the committed offset is at least the expected value.
func (c *Client) AssertCommittedAtLeast(tb testing.TB, tp kafka.TopicPartition, minOffset int64) {
	tb.Helper()

	actual, ok := c.CommittedOffset(tp)
	require.True(
		tb, ok, "expected offset >= %d to be committed for %s-%d, but none found", minOffset, tp.Topic, tp.Partition,
	)

	require.GreaterOrEqual(
		tb, actual.Offset, minOffset, "expected committed offset >= %d for %s-%d, got %d", minOffset,
		tp.Topic, tp.Partition, actual.Offset,
	)
}

// AssertSubscribed verifies that the client is subscribed to the given topics.
func (c *Client) AssertSubscribed(tb testing.TB, topics ...string) {
	tb.Helper()

	subs := c.Subscriptions()
	subMap := make(map[string]bool)
	for _, s := range subs {
		subMap[s] = true
	}

	for _, topic := range topics {
		if !subMap[topic] {
			tb.Errorf("expected client to be subscribed to topic %q, but it is not", topic)
		}
	}
}

// AssertAssigned verifies that the given partitions are currently assigned.
func (c *Client) AssertAssigned(tb testing.TB, partitions ...kafka.TopicPartition) {
	tb.Helper()

	assigned := c.AssignedPartitions()
	assignedMap := make(map[kafka.TopicPartition]bool)
	for _, p := range assigned {
		assignedMap[p] = true
	}

	for _, p := range partitions {
		if !assignedMap[p] {
			tb.Errorf(
				"expected partition %s-%d to be assigned, but it is not",
				p.Topic, p.Partition,
			)
		}
	}
}

// AssertClosed verifies that Close() was called.
func (c *Client) AssertClosed(tb testing.TB) {
	tb.Helper()

	require.True(tb, c.IsClosed(), "expected client to be closed")
}

// AssertNotClosed verifies that Close() was not called.
func (c *Client) AssertNotClosed(tb testing.TB) {
	tb.Helper()

	require.False(tb, c.IsClosed(), "expected client to not be closed, but it is")
}

// AssertNoProducedRecords verifies that no records were produced.
func (c *Client) AssertNoProducedRecords(tb testing.TB) {
	tb.Helper()

	records := c.ProducedRecords()
	require.Empty(tb, records, "expected no produced records, got %d", len(records))
}

// AssertHeader verifies that a produced record has a specific header.
func (c *Client) AssertHeader(tb testing.TB, topic string, key []byte, headerKey string, headerValue []byte) {
	tb.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			actual, ok := kafka.HeaderValue(r.Headers, headerKey)
			require.True(tb, ok, "record with key=%q missing header %q", string(key), headerKey)
			require.True(
				tb, bytes.Equal(actual, headerValue), "record with key=%q has header %q=%q, expected %q", string(key),
				headerKey, string(headerValue), string(actual),
			)
			return
		}
	}

	tb.Errorf("no record with key=%q found in topic %q", string(key), topic)
}

// AssertMarkedCount verifies that exactly n records have been marked (but not yet committed).
func (c *Client) AssertMarkedCount(tb testing.TB, expected int) {
	tb.Helper()

	actual := len(c.MarkedRecords())
	require.Equal(tb, expected, actual, "expected %d marked records, got %d", expected, actual)
}

// AssertMarked verifies that a record with the given topic, partition, and offset was marked.
func (c *Client) AssertMarked(tb testing.TB, topic string, partition int32, offset int64) {
	tb.Helper()

	records := c.MarkedRecords()
	for _, r := range records {
		if r.Topic == topic && r.Partition == partition && r.Offset == offset {
			return
		}
	}

	tb.Errorf(
		"expected record at %s-%d offset %d to be marked, but it was not found",
		topic, partition, offset,
	)
}

// AssertMarkedOffset verifies that the marked offset for a partition matches the expected value.
// Note: The marked offset is the *next* offset to fetch (record.Offset + 1).
func (c *Client) AssertMarkedOffset(tb testing.TB, tp kafka.TopicPartition, expectedOffset int64) {
	tb.Helper()

	offsets := c.MarkedOffsets()
	actual, ok := offsets[tp]
	require.True(
		tb, ok,
		"expected marked offset for %s-%d, but none found",
		tp.Topic, tp.Partition,
	)

	require.Equal(
		tb, expectedOffset, actual.Offset,
		"expected marked offset %d for %s-%d, got %d",
		expectedOffset, tp.Topic, tp.Partition, actual.Offset,
	)
}

// AssertNoMarkedRecords verifies that no records are currently marked.
func (c *Client) AssertNoMarkedRecords(tb testing.TB) {
	tb.Helper()

	records := c.MarkedRecords()
	require.Empty(tb, records, "expected no marked records, got %d", len(records))
}
