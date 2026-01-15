package task

import (
	"github.com/hugolhafner/go-streams/runner/log"
)

// Task processes records for a single partition
type Task interface {
	Partition() log.TopicPartition
	Process(record log.ConsumerRecord) error
	CurrentOffset() log.Offset
	Close() error
}
