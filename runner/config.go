package runner

import (
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/logger"
)

// BaseConfig is shared by all runners
type BaseConfig struct {
	Logger           logger.Logger
	PollErrorBackoff backoff.Backoff

	// ErrorHandler is invoked for all errors if more specific handlers are not set
	ErrorHandler errorhandler.Handler

	// SerdeErrorHandler is invoked for serde errors.
	// nil means fall back to ErrorHandler.
	SerdeErrorHandler errorhandler.Handler

	// ProcessingErrorHandler is invoked for processing errors.
	// nil means fall back to ErrorHandler.
	ProcessingErrorHandler errorhandler.Handler

	// ProductionErrorHandler is invoked for production/sink errors.
	// nil means fall back to ErrorHandler.
	ProductionErrorHandler errorhandler.Handler
}

func defaultBaseConfig() BaseConfig {
	l := logger.NewNoopLogger()
	return BaseConfig{
		Logger:           l,
		ErrorHandler:     errorhandler.SilentFail(),
		PollErrorBackoff: backoff.NewFixed(time.Second),
	}
}

type SingleThreadedConfig struct {
	BaseConfig
}

func defaultSingleThreadedConfig() SingleThreadedConfig {
	return SingleThreadedConfig{
		BaseConfig: defaultBaseConfig(),
	}
}

type PartitionedConfig struct {
	BaseConfig
	ChannelBufferSize int
	// WorkerShutdownTimeout is the maximum time a single worker will spend
	// draining its records after context cancellation
	WorkerShutdownTimeout time.Duration
	// DrainTimeout is the maximum time shutdown() will wait for ALL workers
	// to finish draining. Should be >= WorkerShutdownTimeout to avoid
	// returning from Run() while workers are still draining
	DrainTimeout time.Duration
}

func defaultPartitionedConfig() PartitionedConfig {
	return PartitionedConfig{
		BaseConfig:            defaultBaseConfig(),
		ChannelBufferSize:     100,
		WorkerShutdownTimeout: 30 * time.Second,
		DrainTimeout:          60 * time.Second,
	}
}
