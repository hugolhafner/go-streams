package topology

import (
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
	_ Node = (*SourceNodeDef)(nil)
	_ Node = (*ProcessorNodeDef)(nil)
	_ Node = (*SinkNodeDef)(nil)
)

type SourceNodeDef struct {
	name       string
	topic      string
	keySerde   serde.ErasedDeserializer
	valueSerde serde.ErasedDeserializer
}

func (s *SourceNodeDef) Name() string {
	return s.name
}

func (s *SourceNodeDef) Type() NodeType {
	return NodeTypeSource
}

func (s *SourceNodeDef) Topic() string {
	return s.topic
}

func (s *SourceNodeDef) KeySerde() serde.ErasedDeserializer {
	return s.keySerde
}

func (s *SourceNodeDef) ValueSerde() serde.ErasedDeserializer {
	return s.valueSerde
}

type ProcessorNodeDef struct {
	name     string
	supplier ProcessorSupplier
}

func (p *ProcessorNodeDef) Name() string {
	return p.name
}

func (p *ProcessorNodeDef) Type() NodeType {
	return NodeTypeProcessor
}

type SinkNodeDef struct {
	name       string
	topic      string
	keySerde   serde.ErasedSerializer
	valueSerde serde.ErasedSerializer
}

func (s *SinkNodeDef) Name() string {
	return s.name
}

func (s *SinkNodeDef) Type() NodeType {
	return NodeTypeSink
}

func (s *SinkNodeDef) KeySerde() serde.ErasedSerializer {
	return s.keySerde
}

func (s *SinkNodeDef) ValueSerde() serde.ErasedSerializer {
	return s.valueSerde
}
