package runner

import (
	"context"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

type Runner interface {
	Run(ctx context.Context) error
}

type Factory = func(
	t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
) (Runner, error)
