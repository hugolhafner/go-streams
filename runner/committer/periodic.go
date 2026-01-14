package committer

import (
	"sync"
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

	mu sync.Mutex
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
		mu:         sync.Mutex{},
	}
}

func (p *PeriodicCommitter) RecordProcessed(count int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.count += count
}

func (p *PeriodicCommitter) TryCommit() bool {
	p.mu.Lock()

	should := p.count >= p.c.MaxCount || time.Since(p.lastCommit) >= p.c.MaxInterval
	if !should {
		p.mu.Unlock()
		return false
	}

	return true
}

func (p *PeriodicCommitter) UnlockCommit(ok bool) {
	defer p.mu.Unlock()

	if ok {
		p.count = 0
		p.lastCommit = time.Now()
	}
}
