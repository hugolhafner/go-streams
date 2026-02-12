package runner

import (
	"context"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

type Runner interface {
	kafka.RebalanceCallback
	Run(ctx context.Context) error
}

type Factory = func(
	t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer, telemetry *otel.Telemetry,
) (Runner, error)
