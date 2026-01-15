package task

import (
	"fmt"

	"github.com/hugolhafner/go-streams/internal/kafka"
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/topology"
)

type Factory interface {
	CreateTask(partition kafka.TopicPartition, producer kafka.Producer) (Task, error)
}

var _ Factory = (*topologyTaskFactory)(nil)

type topologyTaskFactory struct {
	topology      *topology.Topology
	sourceByTopic map[string]topology.SourceNode
}

type FactoryOption func(*topologyTaskFactory)

func NewTopologyTaskFactory(t *topology.Topology, opts ...FactoryOption) (Factory, error) {
	sourceByTopic := make(map[string]topology.SourceNode)
	for _, sn := range t.SourceNodes() {
		topic := sn.Topic()
		fmt.Println("Registering source topic:", topic)
		if _, exists := sourceByTopic[topic]; exists {
			return nil, fmt.Errorf("duplicate source topic: %s", topic)
		}
		sourceByTopic[topic] = sn
	}

	if len(sourceByTopic) == 0 {
		return nil, fmt.Errorf("topology has no source nodes")
	}

	factory := &topologyTaskFactory{
		topology:      t,
		sourceByTopic: sourceByTopic,
	}

	for _, opt := range opts {
		opt(factory)
	}

	return factory, nil
}

func (f *topologyTaskFactory) CreateTask(partition kafka.TopicPartition, producer kafka.Producer) (Task, error) {
	source, ok := f.sourceByTopic[partition.Topic]
	if !ok {
		return nil, fmt.Errorf("no source node for topic: %s", partition.Topic)
	}

	task := &TopologyTask{
		topology:   f.topology,
		partition:  partition,
		source:     source,
		producer:   producer,
		contexts:   make(map[string]*nodeContext),
		sinks:      make(map[string]*sinkHandler),
		processors: make(map[string]processor.UntypedProcessor),
		offset:     kafka.Offset{Offset: -1, LeaderEpoch: -1},
	}

	return task.init()
}
