package runner

import (
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/logger"
)

type SingleThreadedConfig struct {
	Logger       logger.Logger
	ErrorHandler errorhandler.Handler
}

type SingleThreadedOption func(*SingleThreadedConfig)

func WithErrorHandler(handler errorhandler.Handler) SingleThreadedOption {
	return func(c *SingleThreadedConfig) {
		c.ErrorHandler = handler
	}
}

func WithLogger(logger logger.Logger) SingleThreadedOption {
	return func(c *SingleThreadedConfig) {
		c.Logger = logger
	}
}

func defaultSingleThreadedConfig() SingleThreadedConfig {
	l := logger.NewNoopLogger()

	return SingleThreadedConfig{
		Logger:       l,
		ErrorHandler: errorhandler.LogAndContinue(l),
	}
}
