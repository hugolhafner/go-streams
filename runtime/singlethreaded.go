package runtime

import (
	"context"

	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*SingleThreadedRunner)(nil)

type SingleThreadedRunner struct {
}

func (r SingleThreadedRunner) Run(ctx context.Context, topology *topology.Topology, config Config) error {
	panic("implement me")
}
