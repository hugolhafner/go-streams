package runner

import (
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/logger"
)

type SingleThreadedOption interface {
	applySingleThreaded(*SingleThreadedConfig)
}

type PartitionedOption interface {
	applyPartitioned(*PartitionedConfig)
}

type loggerOption struct {
	logger logger.Logger
}

func (o loggerOption) applySingleThreaded(c *SingleThreadedConfig) {
	c.Logger = o.logger
}

func (o loggerOption) applyPartitioned(c *PartitionedConfig) {
	c.Logger = o.logger
}

func WithLogger(l logger.Logger) loggerOption {
	return loggerOption{logger: l}
}

type errorHandlerOption struct {
	handler errorhandler.Handler
}

func (o errorHandlerOption) applySingleThreaded(c *SingleThreadedConfig) {
	c.ErrorHandler = o.handler
}

func (o errorHandlerOption) applyPartitioned(c *PartitionedConfig) {
	c.ErrorHandler = o.handler
}

// WithErrorHandler sets the error handler for a runner
func WithErrorHandler(h errorhandler.Handler) errorHandlerOption {
	return errorHandlerOption{handler: h}
}

type channelBufferSizeOption int

func (o channelBufferSizeOption) applyPartitioned(c *PartitionedConfig) {
	if o > 0 {
		c.ChannelBufferSize = int(o)
	}
}

// WithChannelBufferSize sets the buffer size for partition record channels
func WithChannelBufferSize(size int) channelBufferSizeOption {
	return channelBufferSizeOption(size)
}

type workerShutdownTimeoutOption time.Duration

func (o workerShutdownTimeoutOption) applyPartitioned(c *PartitionedConfig) {
	if o > 0 {
		c.WorkerShutdownTimeout = time.Duration(o)
	}
}

// WithWorkerShutdownTimeout sets the timeout for waiting on worker shutdown
func WithWorkerShutdownTimeout(d time.Duration) workerShutdownTimeoutOption {
	return workerShutdownTimeoutOption(d)
}

type drainTimeoutOption time.Duration

func (o drainTimeoutOption) applyPartitioned(c *PartitionedConfig) {
	if o > 0 {
		c.DrainTimeout = time.Duration(o)
	}
}

// WithDrainTimeout sets the timeout for draining partition channels
func WithDrainTimeout(d time.Duration) drainTimeoutOption {
	return drainTimeoutOption(d)
}

type pollErrorBackoffOption struct {
	b backoff.Backoff
}

func (o pollErrorBackoffOption) applySingleThreaded(c *SingleThreadedConfig) {
	if o.b != nil {
		c.PollErrorBackoff = o.b
	}
}

func (o pollErrorBackoffOption) applyPartitioned(c *PartitionedConfig) {
	if o.b != nil {
		c.PollErrorBackoff = o.b
	}
}

func WithPollErrorBackoff(b backoff.Backoff) pollErrorBackoffOption {
	return pollErrorBackoffOption{b: b}
}
