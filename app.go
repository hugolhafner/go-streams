package streams

import (
	"context"

	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/topology"
)

type Application struct {
	topology *topology.Topology
	config   Config

	client log.Client
}

func NewApplication(topology *topology.Topology, opts ...ConfigOption) *Application {
	config := defaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return NewApplicationWithConfig(topology, config)
}

func NewApplicationWithConfig(topology *topology.Topology, config Config) *Application {
	client := log.NewKgoClient()

	return &Application{
		topology: topology,
		config:   config,
		client:   client,
	}
}

func (a *Application) Run(ctx context.Context, r runner.Factory) error {
	return nil
}
