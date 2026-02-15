package streams

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const Version = "v0.3.0" // x-release-please-version

var (
	ErrAlreadyRunning = errors.New("application is already running")
	ErrClosed         = errors.New("application is closed")
)

type Config struct {
	Logger         logger.Logger
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

type ConfigOption func(*Config)

func WithLogger(logger logger.Logger) ConfigOption {
	return func(c *Config) {
		c.Logger = logger
	}
}

// WithTracerProvider sets the OpenTelemetry TracerProvider for the application.
func WithTracerProvider(tp trace.TracerProvider) ConfigOption {
	return func(c *Config) {
		c.TracerProvider = tp
	}
}

// WithMeterProvider sets the OpenTelemetry MeterProvider for the application.
func WithMeterProvider(mp metric.MeterProvider) ConfigOption {
	return func(c *Config) {
		c.MeterProvider = mp
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

	client    kafka.Client
	logger    logger.Logger
	telemetry *otel.Telemetry

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
	telem, err := otel.NewTelemetry(config.TracerProvider, config.MeterProvider, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create telemetry: %w", err)
	}

	return &Application{
		topology:  topology,
		config:    config,
		client:    client,
		logger:    config.Logger,
		telemetry: telem,
		closedCh:  make(chan struct{}),
	}, nil
}

func (a *Application) Run(ctx context.Context) error {
	opts := []runner.SingleThreadedOption{
		runner.WithLogger(a.logger),
	}

	return a.RunWith(ctx, runner.NewSingleThreadedRunner(opts...))
}

func (a *Application) RunWith(ctx context.Context, factory runner.Factory) error {
	if err := a.startRunning(); err != nil {
		return err
	}
	defer a.Close()

	taskFactory, err := task.NewTopologyTaskFactory(a.topology, a.logger, task.WithTelemetry(a.telemetry))
	if err != nil {
		return fmt.Errorf("failed to create task factory: %w", err)
	}

	r, err := factory(a.topology, taskFactory, a.client, a.client, a.telemetry)
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
