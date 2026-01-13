package task

import (
	"github.com/hugolhafner/go-streams/runner/log"
)

type Factory interface {
	CreateTask(partition log.TopicPartition, producer log.Producer) (Task, error)
}
