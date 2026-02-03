package task

import (
	"github.com/hugolhafner/go-streams/kafka"
)

// Manager handles task lifecycle
type Manager interface {
	CreateTasks(partitions []kafka.TopicPartition) error
	CloseTasks(partitions []kafka.TopicPartition) error
	DeleteTasks(partitions []kafka.TopicPartition) error

	Tasks() map[kafka.TopicPartition]Task
	TaskFor(partition kafka.TopicPartition) (Task, bool)

	Close() error
}
