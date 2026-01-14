package task

import (
	"github.com/hugolhafner/go-streams/runner/committer"
	"github.com/hugolhafner/go-streams/runner/log"
)

// Task processes records for a single partition
type Task interface {
	Partition() log.TopicPartition
	Process(record log.ConsumerRecord) error
	CurrentOffset() int64
	Committer() committer.Committer
	Close() error
}
