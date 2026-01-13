package task

import (
	"github.com/hugolhafner/go-streams/runner/log"
)

// Manager handles task lifecycle
type Manager interface {
	log.RebalanceCallback

	Tasks() map[log.TopicPartition]Task
	TaskFor(partition log.TopicPartition) (Task, bool)

	GetCommitOffsets() map[log.TopicPartition]int64

	Close() error
}
