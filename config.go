package streams

import (
	"github.com/hugolhafner/go-streams/logger"
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
