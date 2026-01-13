package runner

import (
	"context"

	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/runner/task"
	"github.com/hugolhafner/go-streams/topology"
)

type Runner interface {
	Run(ctx context.Context) error
}

type Factory = func(t *topology.Topology, f task.Factory, consumer log.Consumer, producer log.Producer) (Runner, error)
