package kstream

import (
	"fmt"
	"sync/atomic"

	"github.com/hugolhafner/go-streams/topology"
)

// StreamsBuilder is the entry point for building stream topologies
type StreamsBuilder struct {
	topology *topology.Topology
	counter  atomic.Uint64
}

func NewStreamsBuilder() *StreamsBuilder {
	return &StreamsBuilder{
		topology: topology.New(),
	}
}

func (b *StreamsBuilder) Build() *topology.Topology {
	return b.topology
}

func (b *StreamsBuilder) nextName(prefix string) string {
	id := b.counter.Add(1)
	return fmt.Sprintf("%s-%06d", prefix, id)
}
