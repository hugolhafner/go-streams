package task

import (
	"github.com/hugolhafner/go-streams/internal/kafka"
)

// Task processes records for a single partition
type Task interface {
	Partition() kafka.TopicPartition
	Process(record kafka.ConsumerRecord) error
	CurrentOffset() kafka.Offset
	Close() error
}
