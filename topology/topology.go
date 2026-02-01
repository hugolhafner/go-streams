package topology

import (
	"fmt"

	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/serde"
)

type Topology struct {
	nodes      map[string]Node
	edges      map[string][]string
	namedEdges map[string]map[string]string
	sources    []string
	sinks      []string
}

func New() *Topology {
	return &Topology{
		nodes:      make(map[string]Node),
		edges:      make(map[string][]string),
		namedEdges: make(map[string]map[string]string),
		sources:    []string{},
		sinks:      []string{},
	}
}

func (t *Topology) AddSource(
	name, topic string, keySerde serde.UntypedDeserialiser,
	valueSerde serde.UntypedDeserialiser,
) *Topology {
	t.AddNode(
		&sourceNode{
			name:       name,
			topic:      topic,
			keySerde:   keySerde,
			valueSerde: valueSerde,
		},
	)

	return t
}

func (t *Topology) AddProcessor(name string, supplier processor.UntypedSupplier, parents ...string) *Topology {
	t.AddNode(
		&processorNode{
			name:     name,
			supplier: supplier,
		}, parents...,
	)

	return t
}

func (t *Topology) AddSink(
	name, topic string, keySerde serde.UntypedSerialiser, valueSerde serde.UntypedSerialiser,
	parents ...string,
) *Topology {
	t.AddNode(
		&sinkNode{
			name:       name,
			topic:      topic,
			keySerde:   keySerde,
			valueSerde: valueSerde,
		}, parents...,
	)

	return t
}

func (t *Topology) AddProcessorWithChildName(
	name string,
	supplier processor.UntypedSupplier,
	parent string,
	childName string,
) *Topology {
	t.AddNode(
		&processorNode{
			name:     name,
			supplier: supplier,
		}, parent,
	)

	if t.namedEdges[parent] == nil {
		t.namedEdges[parent] = make(map[string]string)
	}
	t.namedEdges[parent][childName] = name

	return t
}

func (t *Topology) AddNode(node Node, parents ...string) {
	t.nodes[node.Name()] = node

	switch node.Type() {
	case NodeTypeSource:
		t.sources = append(t.sources, node.Name())
	case NodeTypeSink:
		t.sinks = append(t.sinks, node.Name())
	default:
	}

	for _, parent := range parents {
		t.edges[parent] = append(t.edges[parent], node.Name())
	}
}

func (t *Topology) GetNode(name string) (Node, bool) {
	node, exists := t.nodes[name]
	return node, exists
}

func (t *Topology) Nodes() map[string]Node {
	return t.nodes
}

func (t *Topology) NamedEdges(parent string) map[string]string {
	if named, ok := t.namedEdges[parent]; ok {
		result := make(map[string]string, len(named))
		for k, v := range named {
			result[k] = v
		}
		return result
	}

	return nil
}

func (t *Topology) Children(parent string) []string {
	return t.edges[parent]
}

func (t *Topology) ChildByName(parent, childName string) string {
	if named, ok := t.namedEdges[parent]; ok {
		return named[childName]
	}
	return ""
}

func (t *Topology) Sources() []string {
	return t.sources
}

func (t *Topology) SourceNodes() []*sourceNode {
	var sourceNodes []*sourceNode
	for _, sourceName := range t.Sources() {
		node, exists := t.GetNode(sourceName)

		if !exists {
			continue
		}

		if sn, ok := node.(*sourceNode); ok {
			sourceNodes = append(sourceNodes, sn)
		}
	}
	return sourceNodes
}

func (t *Topology) Sinks() []string {
	return t.sinks
}

func (t *Topology) SinkNodes() []Node {
	var sinkNodes []Node
	for _, sinkName := range t.Sinks() {
		node, exists := t.GetNode(sinkName)
		if !exists {
			continue
		}

		if sn, ok := node.(*sinkNode); ok {
			sinkNodes = append(sinkNodes, sn)
		}
	}
	return sinkNodes
}

func (t *Topology) PrintTree() {
	visited := make(map[string]bool)
	for _, source := range t.sources {
		t.printNode(source, "", visited)
	}
}

func (t *Topology) printNode(name, prefix string, visited map[string]bool) {
	if visited[name] {
		return
	}
	visited[name] = true

	node, exists := t.nodes[name]
	if !exists {
		return
	}

	msg := fmt.Sprintf("%s- %s (%s", prefix, name, node.Type().String())
	if node.Type() == NodeTypeSource {
		if sn, ok := node.(*sourceNode); ok {
			msg += fmt.Sprintf(", topic=%s", sn.topic)
		}
	} else if node.Type() == NodeTypeSink {
		if sn, ok := node.(*sinkNode); ok {
			msg += fmt.Sprintf(", topic=%s", sn.topic)
		}
	}
	msg += ")"
	fmt.Printf("%s\n", msg)

	children, exists := t.edges[name]
	if !exists {
		return
	}

	for _, child := range children {
		newPrefix := prefix + "  "
		t.printNode(child, newPrefix, visited)
	}
}
