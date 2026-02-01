package kafka

import (
	"time"
)

type ConsumerRecord struct {
	Key         []byte
	Value       []byte
	Headers     map[string][]byte
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Timestamp   time.Time
}

func (r ConsumerRecord) TopicPartition() TopicPartition {
	return TopicPartition{
		Topic:     r.Topic,
		Partition: r.Partition,
	}
}

func (r ConsumerRecord) Copy() ConsumerRecord {
	headersCopy := make(map[string][]byte, len(r.Headers))
	for k, v := range r.Headers {
		vCopy := make([]byte, len(v))
		copy(vCopy, v)
		headersCopy[k] = vCopy
	}

	keyCopy := make([]byte, len(r.Key))
	copy(keyCopy, r.Key)

	valueCopy := make([]byte, len(r.Value))
	copy(valueCopy, r.Value)

	return ConsumerRecord{
		Key:         keyCopy,
		Value:       valueCopy,
		Headers:     headersCopy,
		Topic:       r.Topic,
		Partition:   r.Partition,
		Offset:      r.Offset,
		LeaderEpoch: r.LeaderEpoch,
		Timestamp:   r.Timestamp,
	}
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return tp.Topic + "-" + string(tp.Partition)
}

type Offset struct {
	LeaderEpoch int32
	Offset      int64
}
