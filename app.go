package streams

import (
	"context"
	"errors"

	"github.com/hugolhafner/go-streams/topology"
)

type Application struct {
	topology *topology.Topology
	config   Config
}

func NewApplication(topology *topology.Topology, opts ...ConfigOption) *Application {
	config := defaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return NewApplicationWithConfig(topology, config)
}

func NewApplicationWithConfig(topology *topology.Topology, config Config) *Application {
	return &Application{
		topology: topology,
		config:   config,
	}
}

func (a *Application) Run(ctx context.Context) error {
	return errors.New("not implemented")
}
