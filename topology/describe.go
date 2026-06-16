package topology

import (
	"fmt"
	"sort"
)

// NodeInfo describes a node in the topology graph
type NodeInfo struct {
	ID    string // node name
	Type  NodeType
	Name  string // node name
	Topic string // topic name for source/sink, empty otherwise
}

// EdgeInfo describes a directed edge between two nodes
type EdgeInfo struct {
	ID     string // "source->target"
	Source string // source node name
	Target string // target node name
}

// Description is a snapshot of the topology graph
type Description struct {
	Nodes []NodeInfo
	Edges []EdgeInfo
}

// Describe returns a Description containing all nodes and edges
// in the topology
func (t *Topology) Describe() Description {
	desc := Description{}

	names := make([]string, 0, len(t.nodes))
	for name := range t.nodes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		node := t.nodes[name]
		info := NodeInfo{
			ID:   name,
			Name: name,
			Type: node.Type(),
		}

		switch node.Type() {
		case NodeTypeSource:
			if sn, ok := node.(SourceNode); ok {
				info.Topic = sn.Topic()
			}
		case NodeTypeSink:
			if sn, ok := node.(SinkNode); ok {
				info.Topic = sn.Topic()
			}
		case NodeTypeProcessor:
		default:
		}

		desc.Nodes = append(desc.Nodes, info)

		for _, child := range t.edges[name] {
			desc.Edges = append(
				desc.Edges, EdgeInfo{
					ID:     name + "->" + child,
					Source: name,
					Target: child,
				},
			)
		}
	}

	return desc
}

func (d Description) Print() {
	nodes := make(map[string]NodeInfo)
	edges := make(map[string][]string)

	for _, node := range d.Nodes {
		nodes[node.ID] = node
		edges[node.ID] = make([]string, 0)
	}

	for _, edge := range d.Edges {
		edges[edge.Source] = append(edges[edge.Source], edge.Target)
	}

	visited := make(map[string]bool)
	for _, node := range d.Nodes {
		if node.Type != NodeTypeSource {
			continue
		}

		printNode(node, "", nodes, edges, visited)
	}
}

func printNode(
	node NodeInfo, prefix string, nodes map[string]NodeInfo, edges map[string][]string,
	visited map[string]bool,
) {
	if visited[node.ID] {
		return
	}
	visited[node.ID] = true

	msg := fmt.Sprintf("%s- %s (%s", prefix, node.Name, node.Type.String())
	if node.Type == NodeTypeSource || node.Type == NodeTypeSink {
		msg += ", topic=" + node.Topic
	}

	msg += ")"
	fmt.Println(msg)

	for _, child := range edges[node.ID] {
		newPrefix := prefix + "  "
		if childNode, ok := nodes[child]; ok {
			printNode(childNode, newPrefix, nodes, edges, visited)
		}
	}
}
