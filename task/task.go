package task

import (
	"context"

	"github.com/hugolhafner/go-streams/kafka"
)

// Task processes records for a single partition
type Task interface {
	Partition() kafka.TopicPartition
	Process(ctx context.Context, record kafka.ConsumerRecord) error
	CurrentOffset() (kafka.Offset, bool)
	Close() error
}
