package task

import (
	"fmt"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/runner/committer"
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/topology"
)

type Factory interface {
	CreateTask(partition log.TopicPartition, producer log.Producer) (Task, error)
}

var _ Factory = (*topologyTaskFactory)(nil)

type topologyTaskFactory struct {
	topology         *topology.Topology
	sourceByTopic    map[string]topology.SourceNode
	committerFactory func() committer.Committer
}

type FactoryOption func(*topologyTaskFactory)

func WithCommitterFactory(f func() committer.Committer) FactoryOption {
	return func(tf *topologyTaskFactory) {
		tf.committerFactory = f
	}
}

func NewTopologyTaskFactory(t *topology.Topology, opts ...FactoryOption) (Factory, error) {
	sourceByTopic := make(map[string]topology.SourceNode)
	for _, sn := range t.SourceNodes() {
		topic := sn.Topic()
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
		committerFactory: func() committer.Committer {
			return committer.NewPeriodicCommitter()
		},
	}

	for _, opt := range opts {
		opt(factory)
	}

	return factory, nil
}

func (f *topologyTaskFactory) CreateTask(partition log.TopicPartition, producer log.Producer) (Task, error) {
	source, ok := f.sourceByTopic[partition.Topic]
	if !ok {
		return nil, fmt.Errorf("no source node for topic: %s", partition.Topic)
	}

	task := &TopologyTask{
		partition:  partition,
		source:     source,
		producer:   producer,
		contexts:   make(map[string]*nodeContext),
		sinks:      make(map[string]*sinkHandler),
		processors: make(map[string]processor.UntypedProcessor),
		committer:  f.committerFactory(),
		offset:     -1,
	}

	return task.init()
}
