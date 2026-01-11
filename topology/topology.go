package topology

import (
	"fmt"
)

type Topology struct {
	nodes      map[string]Node
	edges      map[string][]string
	namedEdges map[string]map[string]string
	sources    []string
	sinks      []string
}

func NewTopology() *Topology {
	return &Topology{
		nodes:      make(map[string]Node),
		edges:      make(map[string][]string),
		namedEdges: make(map[string]map[string]string),
		sources:    []string{},
		sinks:      []string{},
	}
}

func (t *Topology) Nodes() map[string]Node {
	return t.nodes
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

func (t *Topology) Sinks() []string {
	return t.sinks
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

	fmt.Printf("%s- %s (%s)\n", prefix, name, node.Type().String())

	children, exists := t.edges[name]
	if !exists {
		return
	}

	for _, child := range children {
		newPrefix := prefix + "  "
		t.printNode(child, newPrefix, visited)
	}
}
