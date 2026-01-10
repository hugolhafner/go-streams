package topology

type Builder struct {
	topology *Topology
}

func NewBuilder() *Builder {
	return &Builder{
		topology: NewTopology(),
	}
}

func (b *Builder) AddSource(name, topic string) *Builder {
	b.topology.nodes[name] = &sourceNodeDef{
		name:  name,
		topic: topic,
	}
	b.topology.sources = append(b.topology.sources, name)
	return b
}

func (b *Builder) AddProcessor(name string, supplier ProcessorSupplier, parents ...string) *Builder {
	b.topology.nodes[name] = &processorNodeDef{
		name:     name,
		supplier: supplier,
	}

	for _, parent := range parents {
		b.topology.edges[parent] = append(b.topology.edges[parent], name)
	}

	return b
}

func (b *Builder) AddProcessorWithChildName(
	name string,
	supplier ProcessorSupplier,
	parent string,
	childName string,
) *Builder {
	b.topology.nodes[name] = &processorNodeDef{
		name:     name,
		supplier: supplier,
	}

	b.topology.edges[parent] = append(b.topology.edges[parent], name)

	if b.topology.namedEdges[parent] == nil {
		b.topology.namedEdges[parent] = make(map[string]string)
	}
	b.topology.namedEdges[parent][childName] = name

	return b
}

func (b *Builder) AddSink(name, topic string, parents ...string) *Builder {
	b.topology.nodes[name] = &sinkNodeDef{
		name:  name,
		topic: topic,
	}
	b.topology.sinks = append(b.topology.sinks, name)

	for _, parent := range parents {
		b.topology.edges[parent] = append(b.topology.edges[parent], name)
	}

	return b
}

func (b *Builder) Build() *Topology {
	return b.topology
}
