package task

import (
	"fmt"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
	"github.com/hugolhafner/go-streams/runner/committer"
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/topology"
)

var _ Task = (*TopologyTask)(nil)

type TopologyTask struct {
	partition  log.TopicPartition
	source     topology.SourceNode
	processors map[string]processor.UntypedProcessor
	contexts   map[string]*nodeContext
	sinks      map[string]*sinkHandler
	producer   log.Producer
	offset     int64
	committer  committer.Committer
	topology   *topology.Topology
}

func (t *TopologyTask) Partition() log.TopicPartition {
	return t.partition
}

func (t *TopologyTask) CurrentOffset() int64 {
	return t.offset
}

func (t *TopologyTask) Committer() committer.Committer {
	return t.committer
}

func (t *TopologyTask) Process(rec log.ConsumerRecord) error {
	key, err := t.source.KeySerde().Deserialize(rec.Topic, rec.Key)
	if err != nil {
		return fmt.Errorf("deserialize key: %w", err)
	}

	value, err := t.source.ValueSerde().Deserialize(rec.Topic, rec.Value)
	if err != nil {
		return fmt.Errorf("deserialize value: %w", err)
	}

	untypedRec := record.NewUntyped(key, value, record.Metadata{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Headers:   rec.Headers,
	})

	children := t.topology.Children(t.source.Name())
	for _, childName := range children {
		if err := t.processAt(childName, untypedRec); err != nil {
			return err
		}
	}

	// Update offset (commit offset is next offset to read)
	t.offset = rec.Offset + 1

	return nil
}

func (t *TopologyTask) processAt(nodeName string, rec *record.UntypedRecord) error {
	if sink, ok := t.sinks[nodeName]; ok {
		return sink.Process(rec)
	}

	proc, ok := t.processors[nodeName]
	if !ok {
		return fmt.Errorf("unknown node: %s", nodeName)
	}

	return proc.Process(rec)
}

func (t *TopologyTask) Close() error {
	var lastErr error
	for name, proc := range t.processors {
		if err := proc.Close(); err != nil {
			lastErr = fmt.Errorf("close processor %s: %w", name, err)
		}
	}
	return lastErr
}

func (t *TopologyTask) init() (*TopologyTask, error) {
	for name, node := range t.topology.Nodes() {
		if pn, ok := node.(topology.ProcessorNode); ok {
			t.processors[name] = pn.Supplier()()
		}
	}

	contexts := make(map[string]*nodeContext)
	for name, _ := range t.topology.Nodes() {
		children := t.topology.Children(name)
		namedEdges := t.topology.NamedEdges(name)
		contexts[name] = &nodeContext{
			task:       t,
			nodeName:   name,
			children:   children,
			namedEdges: namedEdges,
		}
	}

	sinks := make(map[string]*sinkHandler)
	for name, node := range t.topology.Nodes() {
		if sn, ok := node.(topology.SinkNode); ok {
			sinks[name] = &sinkHandler{
				node:     sn,
				producer: t.producer,
			}
		}
	}

	for name, proc := range t.processors {
		proc.Init(contexts[name])
	}

	return t, nil
}
