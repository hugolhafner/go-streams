package runtime

import (
	"context"

	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*PartitionedRunner)(nil)

type PartitionedRunner struct {
}

func (r PartitionedRunner) Run(ctx context.Context, topology *topology.Topology, config Config) error {
	panic("implement me")
}
