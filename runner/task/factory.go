package task

import (
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/topology"
)

type Factory interface {
	CreateTask(partition log.TopicPartition, producer log.Producer) (Task, error)
}

var _ Factory = (*topologyTaskFactory)(nil)

type topologyTaskFactory struct {
	topology *topology.Topology
}

func NewTopologyTaskFactory(t *topology.Topology) Factory {
	return &topologyTaskFactory{topology: t}
}

func (f *topologyTaskFactory) CreateTask(partition log.TopicPartition, producer log.Producer) (Task, error) {
	panic("not implemented")
}
