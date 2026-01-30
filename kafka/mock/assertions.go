package mock

import (
	"bytes"
	"testing"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/stretchr/testify/require"
)

// AssertProducedCount verifies that exactly n records were produced.
func (c *Client) AssertProducedCount(t testing.TB, expected int) {
	t.Helper()

	actual := len(c.ProducedRecords())
	require.Equal(t, expected, actual, "expected %d records, got %d", expected, actual)
}

// AssertProducedCountForTopic verifies that exactly n records were produced to a topic.
func (c *Client) AssertProducedCountForTopic(t testing.TB, topic string, expected int) {
	t.Helper()

	actual := len(c.ProducedRecordsForTopic(topic))
	require.Equal(t, expected, actual, "expected %d records produced to topic %q, got %d", expected, topic, actual)
}

// AssertProduced verifies that a record with the given key and value was produced to the topic.
func (c *Client) AssertProduced(t testing.TB, topic string, key, value []byte) {
	t.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) && bytes.Equal(r.Value, value) {
			return
		}
	}

	t.Errorf(
		"expected record with key=%q value=%q to be produced to topic %q, but it was not found",
		string(key), string(value), topic,
	)
}

// AssertProducedString is a convenience method for string keys and values.
func (c *Client) AssertProducedString(t testing.TB, topic, key, value string) {
	t.Helper()
	c.AssertProduced(t, topic, []byte(key), []byte(value))
}

// AssertProducedKey verifies that a record with the given key was produced to the topic.
func (c *Client) AssertProducedKey(t testing.TB, topic string, key []byte) {
	t.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			return
		}
	}

	t.Errorf(
		"expected record with key=%q to be produced to topic %q, but it was not found",
		string(key), topic,
	)
}

// AssertNotProduced verifies that no record with the given key was produced to the topic.
func (c *Client) AssertNotProduced(t testing.TB, topic string, key []byte) {
	t.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			t.Errorf(
				"expected no record with key=%q to be produced to topic %q, but found value=%q",
				string(key), topic, string(r.Value),
			)
			return
		}
	}
}

// AssertCommitted verifies that an offset was committed for the topic-partition.
func (c *Client) AssertCommitted(t testing.TB, tp kafka.TopicPartition) {
	t.Helper()

	_, ok := c.CommittedOffset(tp)
	require.True(t, ok, "committed offset not found for %s-%d", tp.Topic, tp.Partition)
}

// AssertCommittedOffset verifies that a specific offset was committed.
func (c *Client) AssertCommittedOffset(t testing.TB, tp kafka.TopicPartition, expectedOffset int64) {
	t.Helper()

	actual, ok := c.CommittedOffset(tp)
	require.True(
		t, ok,
		"expected offset %d to be committed for %s-%d, but none found",
		expectedOffset, tp.Topic, tp.Partition,
	)

	require.Equal(
		t, expectedOffset, actual.Offset, "expected offset %d to be committed for %s-%d, got %d", expectedOffset,
		tp.Topic, tp.Partition, actual.Offset,
	)
}

// AssertCommittedAtLeast verifies that the committed offset is at least the expected value.
func (c *Client) AssertCommittedAtLeast(t testing.TB, tp kafka.TopicPartition, minOffset int64) {
	t.Helper()

	actual, ok := c.CommittedOffset(tp)
	require.True(
		t, ok, "expected offset >= %d to be committed for %s-%d, but none found", minOffset, tp.Topic, tp.Partition,
	)

	require.GreaterOrEqual(
		t, actual.Offset, minOffset, "expected committed offset >= %d for %s-%d, got %d", minOffset,
		tp.Topic, tp.Partition, actual.Offset,
	)
}

// AssertSubscribed verifies that the client is subscribed to the given topics.
func (c *Client) AssertSubscribed(t testing.TB, topics ...string) {
	t.Helper()

	subs := c.Subscriptions()
	subMap := make(map[string]bool)
	for _, s := range subs {
		subMap[s] = true
	}

	for _, topic := range topics {
		if !subMap[topic] {
			t.Errorf("expected client to be subscribed to topic %q, but it is not", topic)
		}
	}
}

// AssertAssigned verifies that the given partitions are currently assigned.
func (c *Client) AssertAssigned(t testing.TB, partitions ...kafka.TopicPartition) {
	t.Helper()

	assigned := c.AssignedPartitions()
	assignedMap := make(map[kafka.TopicPartition]bool)
	for _, p := range assigned {
		assignedMap[p] = true
	}

	for _, p := range partitions {
		if !assignedMap[p] {
			t.Errorf(
				"expected partition %s-%d to be assigned, but it is not",
				p.Topic, p.Partition,
			)
		}
	}
}

// AssertClosed verifies that Close() was called.
func (c *Client) AssertClosed(t testing.TB) {
	t.Helper()

	require.True(t, c.IsClosed(), "expected client to be closed")
}

// AssertNotClosed verifies that Close() was not called.
func (c *Client) AssertNotClosed(t testing.TB) {
	t.Helper()

	require.False(t, c.IsClosed(), "expected client to not be closed, but it is")
}

// AssertNoProducedRecords verifies that no records were produced.
func (c *Client) AssertNoProducedRecords(t testing.TB) {
	t.Helper()

	records := c.ProducedRecords()
	require.Equal(t, 0, len(records), "expected no produced records, got %d", len(records))
}

// AssertHeader verifies that a produced record has a specific header.
func (c *Client) AssertHeader(t testing.TB, topic string, key []byte, headerKey string, headerValue []byte) {
	t.Helper()

	records := c.ProducedRecordsForTopic(topic)
	for _, r := range records {
		if bytes.Equal(r.Key, key) {
			require.NotNil(t, r.Headers, "record with key=%q has no headers", string(key))

			actual, ok := r.Headers[headerKey]
			require.True(t, ok, "record with key=%q missing header %q", string(key), headerKey)
			require.True(
				t, bytes.Equal(actual, headerValue), "record with key=%q has header %q=%q, expected %q", string(key),
				headerKey, string(headerValue), string(actual),
			)
			return
		}
	}

	t.Errorf("no record with key=%q found in topic %q", string(key), topic)
}
