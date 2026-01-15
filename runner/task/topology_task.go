package task

import (
	"fmt"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/record"
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
	offset     log.Offset
	topology   *topology.Topology
}

func (t *TopologyTask) Partition() log.TopicPartition {
	return t.partition
}

func (t *TopologyTask) CurrentOffset() log.Offset {
	return t.offset
}

func (t *TopologyTask) Process(rec log.ConsumerRecord) error {
	key, err := t.source.KeySerde().Deserialise(rec.Topic, rec.Key)
	if err != nil {
		return fmt.Errorf("deserialize key: %w", err)
	}

	value, err := t.source.ValueSerde().Deserialise(rec.Topic, rec.Value)
	if err != nil {
		return fmt.Errorf("deserialize value: %w", err)
	}

	fmt.Println("TopologyTask processing record from topic:", rec.Topic, "partition:", rec.Partition, "offset:",
		rec.Offset)
	fmt.Println(string(rec.Value))

	untypedRec := record.NewUntyped(key, value, record.Metadata{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Headers:   rec.Headers,
	})

	children := t.topology.Children(t.source.Name())
	for _, childName := range children {
		fmt.Println("Processing child node:", childName)
		if err := t.processAt(childName, untypedRec); err != nil {
			return err
		}
	}

	t.offset = log.Offset{
		Offset:      rec.Offset + 1,
		LeaderEpoch: rec.LeaderEpoch,
	}

	return nil
}

func (t *TopologyTask) processAt(nodeName string, rec *record.UntypedRecord) error {
	fmt.Println("Processing at node:", nodeName)
	if sink, ok := t.sinks[nodeName]; ok {
		fmt.Println("Processing sink node:", nodeName)
		return sink.Process(rec)
	}

	proc, ok := t.processors[nodeName]
	fmt.Println("Processing processor node:", nodeName)
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

	for name, _ := range t.topology.Nodes() {
		children := t.topology.Children(name)
		namedEdges := t.topology.NamedEdges(name)
		t.contexts[name] = &nodeContext{
			task:       t,
			nodeName:   name,
			children:   children,
			namedEdges: namedEdges,
		}
	}

	for name, node := range t.topology.Nodes() {
		if sn, ok := node.(topology.SinkNode); ok {
			t.sinks[name] = &sinkHandler{
				node:     sn,
				producer: t.producer,
			}
		}
	}

	for name, proc := range t.processors {
		proc.Init(t.contexts[name])
	}

	return t, nil
}
