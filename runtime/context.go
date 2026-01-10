package runtime

import (
	"github.com/hugolhafner/go-streams/topology"
)

type processorContext struct {
	topology *topology.Topology
	nodeName string
	children map[string]topology.ProcessorNode
}

func newProcessorContext(
	topo *topology.Topology,
	nodeName string,
	children map[string]topology.ProcessorNode,
) *processorContext {
	return &processorContext{
		topology: topo,
		nodeName: nodeName,
		children: children,
	}
}

// Forward sends to all children
func (c *processorContext) Forward(record any) {
	childNames := c.topology.Children(c.nodeName)
	for _, childName := range childNames {
		if child, ok := c.children[childName]; ok {
			child.Process(record)
		}
	}
}

// ForwardTo sends to a specific named child
func (c *processorContext) ForwardTo(childName string, record any) {
	actualNodeName := c.topology.ChildByName(c.nodeName, childName)
	if actualNodeName == "" {
		return
	}

	if child, ok := c.children[actualNodeName]; ok {
		child.Process(record)
	}
}
