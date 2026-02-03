package committer

import (
	"context"
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
	channel    chan struct{}
	mu         sync.RWMutex

	tickerCancel context.CancelFunc
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
	p.mu.Lock()
	defer p.mu.Unlock()

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
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.channel
}

func (p *PeriodicCommitter) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.channel)
}

func (p *PeriodicCommitter) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.count = 0
	p.lastCommit = time.Now()
}

func (p *PeriodicCommitter) startIntervalTicker() {
	p.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	p.tickerCancel = cancel
	p.mu.Unlock()

	ticker := time.NewTicker(p.c.MaxInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.RecordProcessed(0)
		}
	}
}

func (p *PeriodicCommitter) Start() {
	go p.startIntervalTicker()
}
