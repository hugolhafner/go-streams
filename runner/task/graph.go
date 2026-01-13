package task

import (
	"github.com/hugolhafner/go-streams/topology"
)

type ProcessorGraph struct {
	sourceNode string
	processors map[string]topology.ProcessorNode
	sinkNodes  map[string]topology.SinkNode
	topology   *topology.Topology
}
