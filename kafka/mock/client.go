package mock

import (
	"context"
	"sync"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
)

var _ kafka.Client = (*Client)(nil)

// ProducedRecord represents a record that was sent via the mock producer.
type ProducedRecord struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type Client struct {
	mu sync.RWMutex

	recordQueues   map[kafka.TopicPartition][]kafka.ConsumerRecord
	queuePositions map[kafka.TopicPartition]int

	producedRecords  []ProducedRecord
	committedOffsets map[kafka.TopicPartition]kafka.Offset

	subscriptions      []string
	rebalanceCb        kafka.RebalanceCallback
	assignedPartitions []kafka.TopicPartition

	maxPollRecords int
	pollDelay      time.Duration

	sendErr   func(topic string, key, value []byte) error
	pollErr   func() error
	commitErr func(offsets map[kafka.TopicPartition]kafka.Offset) error
	pingErr   error

	closed     bool
	subscribed bool
}

func NewClient(opts ...Option) *Client {
	c := &Client{
		recordQueues:     make(map[kafka.TopicPartition][]kafka.ConsumerRecord),
		queuePositions:   make(map[kafka.TopicPartition]int),
		producedRecords:  make([]ProducedRecord, 0),
		committedOffsets: make(map[kafka.TopicPartition]kafka.Offset),
		maxPollRecords:   10,
		pollDelay:        0,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Subscribe registers the client to consume from the specified topics.
// The rebalance callback will be invoked when partitions are assigned or revoked.
func (c *Client) Subscribe(topics []string, rebalanceCb kafka.RebalanceCallback) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subscribed {
		return nil // Already subscribed, idempotent
	}

	c.subscriptions = topics
	c.rebalanceCb = rebalanceCb
	c.subscribed = true

	// Auto-assign all partitions that have records for subscribed topics
	var partitions []kafka.TopicPartition
	for tp := range c.recordQueues {
		for _, topic := range topics {
			if tp.Topic == topic {
				partitions = append(partitions, tp)
				break
			}
		}
	}

	if len(partitions) > 0 {
		c.assignedPartitions = partitions
		if rebalanceCb != nil {
			// Unlock during callback to prevent deadlock
			c.mu.Unlock()
			_ = rebalanceCb.OnAssigned(partitions)
			c.mu.Lock()
		}
	}

	return nil
}

// Poll retrieves records from the assigned partitions.
// Records are returned in round-robin fashion across partitions.
// Returns up to maxPollRecords (default 10) records per call.
func (c *Client) Poll(ctx context.Context) ([]kafka.ConsumerRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.pollDelay > 0 {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			c.mu.Lock()
			return nil, ctx.Err()
		case <-time.After(c.pollDelay):
		}
		c.mu.Lock()
	}

	if c.pollErr != nil {
		if err := c.pollErr(); err != nil {
			return nil, err
		}
	}

	var records []kafka.ConsumerRecord
	recordCount := 0

	// Round-robin through assigned partitions
	for _, tp := range c.assignedPartitions {
		if recordCount >= c.maxPollRecords {
			break
		}

		queue, exists := c.recordQueues[tp]
		if !exists {
			continue
		}

		pos := c.queuePositions[tp]
		for pos < len(queue) && recordCount < c.maxPollRecords {
			records = append(records, queue[pos])
			pos++
			recordCount++
		}
		c.queuePositions[tp] = pos
	}

	return records, nil
}

// Commit stores the provided offsets as committed.
// These can be verified using CommittedOffsets() or AssertCommitted().
func (c *Client) Commit(offsets map[kafka.TopicPartition]kafka.Offset) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.commitErr != nil {
		if err := c.commitErr(offsets); err != nil {
			return err
		}
	}

	for tp, offset := range offsets {
		c.committedOffsets[tp] = offset
	}

	return nil
}

// Send produces a record to the specified topic.
// The record is stored internally and can be verified using ProducedRecords().
func (c *Client) Send(ctx context.Context, topic string, key, value []byte, headers map[string][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sendErr != nil {
		if err := c.sendErr(topic, key, value); err != nil {
			return err
		}
	}

	// Copy headers to avoid mutation issues
	headersCopy := make(map[string][]byte, len(headers))
	for k, v := range headers {
		headersCopy[k] = v
	}

	c.producedRecords = append(
		c.producedRecords, ProducedRecord{
			Topic:   topic,
			Key:     key,
			Value:   value,
			Headers: headersCopy,
		},
	)

	return nil
}

// Flush is a no-op for the mock client since Send is synchronous.
func (c *Client) Flush(timeout time.Duration) error {
	return nil
}

// Ping checks if the mock client is operational.
// Returns pingErr if configured, otherwise returns nil.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.pingErr
}

// Close marks the client as closed.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
}

// AddRecords adds records to be returned by Poll for a specific topic-partition.
// Records are appended to any existing records for that partition.
func (c *Client) AddRecords(topic string, partition int32, records ...kafka.ConsumerRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tp := kafka.TopicPartition{Topic: topic, Partition: partition}

	// Set topic and partition on each record if not already set
	for i := range records {
		if records[i].Topic == "" {
			records[i].Topic = topic
		}
		if records[i].Partition == 0 && partition != 0 {
			records[i].Partition = partition
		}
		// Auto-assign offsets if not set
		if records[i].Offset == 0 {
			existingLen := len(c.recordQueues[tp])
			records[i].Offset = int64(existingLen + i)
		}
	}

	c.recordQueues[tp] = append(c.recordQueues[tp], records...)
}

// SetSendError configures an error to be returned on all Send calls.
// Pass nil to clear the error.
func (c *Client) SetSendError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		c.sendErr = nil
	} else {
		c.sendErr = func(string, []byte, []byte) error { return err }
	}
}

// SetSendErrorFunc configures a function to determine Send errors.
// The function receives the topic, key, and value and can return an error conditionally.
func (c *Client) SetSendErrorFunc(fn func(topic string, key, value []byte) error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sendErr = fn
}

// SetPollError configures an error to be returned on all Poll calls.
func (c *Client) SetPollError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		c.pollErr = nil
	} else {
		c.pollErr = func() error { return err }
	}
}

// SetPollErrorFunc configures a function to determine Poll errors.
func (c *Client) SetPollErrorFunc(fn func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pollErr = fn
}

// SetCommitError configures an error to be returned on all Commit calls.
func (c *Client) SetCommitError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		c.commitErr = nil
	} else {
		c.commitErr = func(map[kafka.TopicPartition]kafka.Offset) error { return err }
	}
}

// SetCommitErrorFunc configures a function to determine Commit errors.
func (c *Client) SetCommitErrorFunc(fn func(offsets map[kafka.TopicPartition]kafka.Offset) error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.commitErr = fn
}

// SetPingError configures an error to be returned by Ping.
func (c *Client) SetPingError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pingErr = err
}

// TriggerAssign simulates a partition assignment event.
// This calls the OnAssigned callback if one was registered via Subscribe.
func (c *Client) TriggerAssign(partitions []kafka.TopicPartition) error {
	c.mu.Lock()
	cb := c.rebalanceCb
	c.assignedPartitions = append(c.assignedPartitions, partitions...)
	c.mu.Unlock()

	if cb != nil {
		return cb.OnAssigned(partitions)
	}
	return nil
}

// TriggerRevoke simulates a partition revocation event.
// This calls the OnRevoked callback if one was registered via Subscribe.
func (c *Client) TriggerRevoke(partitions []kafka.TopicPartition) error {
	c.mu.Lock()
	cb := c.rebalanceCb

	// Remove revoked partitions from assigned list
	remaining := make([]kafka.TopicPartition, 0, len(c.assignedPartitions))
	for _, assigned := range c.assignedPartitions {
		revoked := false
		for _, p := range partitions {
			if assigned.Topic == p.Topic && assigned.Partition == p.Partition {
				revoked = true
				break
			}
		}
		if !revoked {
			remaining = append(remaining, assigned)
		}
	}
	c.assignedPartitions = remaining
	c.mu.Unlock()

	if cb != nil {
		return cb.OnRevoked(partitions)
	}
	return nil
}

// ProducedRecords returns a copy of all records that have been sent via Send.
func (c *Client) ProducedRecords() []ProducedRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]ProducedRecord, len(c.producedRecords))
	copy(result, c.producedRecords)
	return result
}

// ProducedRecordsForTopic returns all records produced to a specific topic.
func (c *Client) ProducedRecordsForTopic(topic string) []ProducedRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []ProducedRecord
	for _, r := range c.producedRecords {
		if r.Topic == topic {
			result = append(result, r)
		}
	}
	return result
}

// CommittedOffsets returns a copy of all committed offsets.
func (c *Client) CommittedOffsets() map[kafka.TopicPartition]kafka.Offset {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[kafka.TopicPartition]kafka.Offset, len(c.committedOffsets))
	for k, v := range c.committedOffsets {
		result[k] = v
	}
	return result
}

// CommittedOffset returns the committed offset for a specific topic-partition.
// Returns (offset, true) if committed, (Offset{}, false) otherwise.
func (c *Client) CommittedOffset(tp kafka.TopicPartition) (kafka.Offset, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	offset, ok := c.committedOffsets[tp]
	return offset, ok
}

// Subscriptions returns the topics the client is subscribed to.
func (c *Client) Subscriptions() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, len(c.subscriptions))
	copy(result, c.subscriptions)
	return result
}

// AssignedPartitions returns the currently assigned partitions.
func (c *Client) AssignedPartitions() []kafka.TopicPartition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]kafka.TopicPartition, len(c.assignedPartitions))
	copy(result, c.assignedPartitions)
	return result
}

// IsClosed returns whether Close has been called.
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.closed
}

// Reset clears all state, allowing the mock to be reused.
// This clears produced records, committed offsets, and resets queue positions.
// It does not clear the record queues themselves.
func (c *Client) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.producedRecords = make([]ProducedRecord, 0)
	c.committedOffsets = make(map[kafka.TopicPartition]kafka.Offset)
	c.queuePositions = make(map[kafka.TopicPartition]int)
	c.closed = false
}

// Clear removes all state including record queues.
func (c *Client) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.recordQueues = make(map[kafka.TopicPartition][]kafka.ConsumerRecord)
	c.queuePositions = make(map[kafka.TopicPartition]int)
	c.producedRecords = make([]ProducedRecord, 0)
	c.committedOffsets = make(map[kafka.TopicPartition]kafka.Offset)
	c.subscriptions = nil
	c.assignedPartitions = nil
	c.rebalanceCb = nil
	c.subscribed = false
	c.closed = false
}
