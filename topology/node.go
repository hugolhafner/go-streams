package topology

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/serde"
)

type NodeType int

const (
	NodeTypeSource NodeType = iota
	NodeTypeProcessor
	NodeTypeSink
)

func (nt NodeType) String() string {
	switch nt {
	case NodeTypeSource:
		return "Source"
	case NodeTypeProcessor:
		return "Processor"
	case NodeTypeSink:
		return "Sink"
	default:
		return "Unknown"
	}
}

// Node represents a processing step in the topology
type Node interface {
	Name() string
	Type() NodeType // Source, Processor, Sink
}

var (
	_ Node          = (*sourceNode)(nil)
	_ SourceNode    = (*sourceNode)(nil)
	_ Node          = (*processorNode)(nil)
	_ ProcessorNode = (*processorNode)(nil)
	_ Node          = (*sinkNode)(nil)
	_ SinkNode      = (*sinkNode)(nil)
)

type sourceNode struct {
	name       string
	topic      string
	keySerde   serde.UntypedDeserialiser
	valueSerde serde.UntypedDeserialiser
}

func (s *sourceNode) Name() string {
	return s.name
}

func (s *sourceNode) Type() NodeType {
	return NodeTypeSource
}

func (s *sourceNode) Topic() string {
	return s.topic
}

func (s *sourceNode) KeySerde() serde.UntypedDeserialiser {
	return s.keySerde
}

func (s *sourceNode) ValueSerde() serde.UntypedDeserialiser {
	return s.valueSerde
}

type processorNode struct {
	name     string
	supplier processor.UntypedSupplier
}

func (p *processorNode) Name() string {
	return p.name
}

func (p *processorNode) Type() NodeType {
	return NodeTypeProcessor
}

func (p *processorNode) Supplier() processor.UntypedSupplier {
	return p.supplier
}

type sinkNode struct {
	name       string
	topic      string
	keySerde   serde.UntypedSerialiser
	valueSerde serde.UntypedSerialiser
}

func (s *sinkNode) Name() string {
	return s.name
}

func (s *sinkNode) Type() NodeType {
	return NodeTypeSink
}

func (s *sinkNode) Topic() string {
	return s.topic
}

func (s *sinkNode) KeySerde() serde.UntypedSerialiser {
	return s.keySerde
}

func (s *sinkNode) ValueSerde() serde.UntypedSerialiser {
	return s.valueSerde
}
