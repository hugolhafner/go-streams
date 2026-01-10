package runtime

import (
	"context"

	"github.com/hugolhafner/go-streams/topology"
)

type Runner interface {
	Run(ctx context.Context, topology *topology.Topology, config Config) error
}
