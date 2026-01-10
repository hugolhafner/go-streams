package runtime

import (
	"context"

	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*WorkerPool)(nil)

type WorkerPool struct {
}

func (r WorkerPool) Run(ctx context.Context, topology *topology.Topology, config Config) error {
	panic("implement me")
}
