package streams

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

const Version = "v0.1.0" // x-release-please-version

var (
	ErrAlreadyRunning = errors.New("application is already running")
	ErrClosed         = errors.New("application is closed")
)

type Config struct {
	Logger logger.Logger
}

type ConfigOption func(*Config)

func WithLogger(logger logger.Logger) ConfigOption {
	return func(c *Config) {
		c.Logger = logger
	}
}

func defaultConfig() Config {
	return Config{
		Logger: logger.NewNoopLogger(),
	}
}

type Application struct {
	topology *topology.Topology
	config   Config

	client kafka.Client
	logger logger.Logger

	mu        sync.Mutex
	running   bool
	runner    runner.Runner
	closeOnce sync.Once
	closedCh  chan struct{}
}

func NewApplication(client kafka.Client, topology *topology.Topology, opts ...ConfigOption) (*Application, error) {
	config := defaultConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return NewApplicationWithConfig(client, topology, config)
}

func NewApplicationWithConfig(client kafka.Client, topology *topology.Topology, config Config) (*Application, error) {
	return &Application{
		topology: topology,
		config:   config,
		client:   client,
		logger:   config.Logger,
		closedCh: make(chan struct{}),
	}, nil
}

func (a *Application) Run(ctx context.Context) error {
	return a.RunWith(
		ctx, runner.NewSingleThreadedRunner(
			runner.WithLogger(a.logger),
		),
	)
}

func (a *Application) RunWith(ctx context.Context, factory runner.Factory) error {
	if err := a.startRunning(); err != nil {
		return err
	}
	defer a.Close()

	taskFactory, err := task.NewTopologyTaskFactory(a.topology, a.logger)
	if err != nil {
		return fmt.Errorf("failed to create task factory: %w", err)
	}

	r, err := factory(a.topology, taskFactory, a.client, a.client)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}

	a.mu.Lock()
	a.runner = r
	a.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.closedCh:
			cancel()
		case <-runCtx.Done():
		}
	}()

	return r.Run(runCtx)
}

func (a *Application) Close() {
	a.closeOnce.Do(
		func() {
			a.mu.Lock()
			defer a.mu.Unlock()

			a.running = false
			close(a.closedCh)
		},
	)
}

func (a *Application) startRunning() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return ErrAlreadyRunning
	}

	select {
	case <-a.closedCh:
		return ErrClosed
	default:
	}

	a.running = true
	return nil
}
