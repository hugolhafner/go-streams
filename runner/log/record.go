package log

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
