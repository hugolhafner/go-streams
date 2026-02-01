package committer

import (
	"time"
)

var _ Committer = (*PeriodicCommitter)(nil)

type PeriodicCommitterConfig struct {
	MaxInterval time.Duration
	MaxCount    int
}

type PeriodicCommitterOption func(*PeriodicCommitterConfig)

func WithMaxInterval(d time.Duration) PeriodicCommitterOption {
	return func(cfg *PeriodicCommitterConfig) {
		cfg.MaxInterval = d
	}
}

func WithMaxCount(c int) PeriodicCommitterOption {
	return func(cfg *PeriodicCommitterConfig) {
		cfg.MaxCount = c
	}
}

type PeriodicCommitter struct {
	c          PeriodicCommitterConfig
	count      int
	lastCommit time.Time
	channel    chan struct{}
}

func NewPeriodicCommitter(opts ...PeriodicCommitterOption) *PeriodicCommitter {
	cfg := PeriodicCommitterConfig{
		MaxInterval: 5 * time.Second,
		MaxCount:    100,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &PeriodicCommitter{
		c:          cfg,
		count:      0,
		lastCommit: time.Now(),
		channel:    make(chan struct{}, 1),
	}
}

func (p *PeriodicCommitter) RecordProcessed(count int) {
	p.count += count
	if p.count > 0 && (p.count >= p.c.MaxCount || time.Since(p.lastCommit) >= p.c.MaxInterval) {
		select {
		case p.channel <- struct{}{}:
		default:
		}

		p.count = 0
		p.lastCommit = time.Now()
	}
}

func (p *PeriodicCommitter) C() chan struct{} {
	return p.channel
}

func (p *PeriodicCommitter) Close() {
	close(p.channel)
}
