package topology

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
	_ Node = (*sourceNodeDef)(nil)
	_ Node = (*processorNodeDef)(nil)
	_ Node = (*sinkNodeDef)(nil)
)

type sourceNodeDef struct {
	name  string
	topic string
}

func (s *sourceNodeDef) Name() string {
	return s.name
}

func (s *sourceNodeDef) Type() NodeType {
	return NodeTypeSource
}

type processorNodeDef struct {
	name     string
	supplier ProcessorSupplier
}

func (p *processorNodeDef) Name() string {
	return p.name
}

func (p *processorNodeDef) Type() NodeType {
	return NodeTypeProcessor
}

type sinkNodeDef struct {
	name  string
	topic string
}

func (s *sinkNodeDef) Name() string {
	return s.name
}

func (s *sinkNodeDef) Type() NodeType {
	return NodeTypeSink
}
