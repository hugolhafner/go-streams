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
	ErrorHandler     errorhandler.Handler
	PollErrorBackoff backoff.Backoff
}

func defaultBaseConfig() BaseConfig {
	l := logger.NewNoopLogger()
	return BaseConfig{
		Logger:           l,
		ErrorHandler:     errorhandler.LogAndContinue(l),
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
	ChannelBufferSize     int
	WorkerShutdownTimeout time.Duration
	DrainTimeout          time.Duration
}

func defaultPartitionedConfig() PartitionedConfig {
	return PartitionedConfig{
		BaseConfig:            defaultBaseConfig(),
		ChannelBufferSize:     100,
		WorkerShutdownTimeout: 30 * time.Second,
		DrainTimeout:          60 * time.Second,
	}
}
