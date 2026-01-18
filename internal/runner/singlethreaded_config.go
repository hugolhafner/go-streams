package runner

import (
	"github.com/hugolhafner/go-streams/internal/committer"
)

type SingleThreadedConfig struct {
	CommitterFactory func() committer.Committer
}

type SingleThreadedOption func(*SingleThreadedConfig)

func WithCommitter(factory func() committer.Committer) SingleThreadedOption {
	return func(c *SingleThreadedConfig) {
		c.CommitterFactory = factory
	}
}

func defaultSingleThreadedConfig() SingleThreadedConfig {
	return SingleThreadedConfig{
		CommitterFactory: func() committer.Committer {
			return committer.NewPeriodicCommitter()
		},
	}
}
