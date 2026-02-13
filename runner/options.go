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

type serdeErrorHandlerOption struct {
	handler errorhandler.Handler
}

func (o serdeErrorHandlerOption) applySingleThreaded(c *SingleThreadedConfig) {
	c.SerdeErrorHandler = o.handler
}

func (o serdeErrorHandlerOption) applyPartitioned(c *PartitionedConfig) {
	c.SerdeErrorHandler = o.handler
}

// WithSerdeErrorHandler sets a handler invoked specifically for serialization/deserialization errors.
// When nil (default), the general ErrorHandler is used as fallback.
func WithSerdeErrorHandler(h errorhandler.Handler) serdeErrorHandlerOption {
	return serdeErrorHandlerOption{handler: h}
}

type processingErrorHandlerOption struct {
	handler errorhandler.Handler
}

func (o processingErrorHandlerOption) applySingleThreaded(c *SingleThreadedConfig) {
	c.ProcessingErrorHandler = o.handler
}

func (o processingErrorHandlerOption) applyPartitioned(c *PartitionedConfig) {
	c.ProcessingErrorHandler = o.handler
}

// WithProcessingErrorHandler sets a handler invoked specifically for processing errors.
// When nil (default), the general ErrorHandler is used as fallback.
func WithProcessingErrorHandler(h errorhandler.Handler) processingErrorHandlerOption {
	return processingErrorHandlerOption{handler: h}
}

type productionErrorHandlerOption struct {
	handler errorhandler.Handler
}

func (o productionErrorHandlerOption) applySingleThreaded(c *SingleThreadedConfig) {
	c.ProductionErrorHandler = o.handler
}

func (o productionErrorHandlerOption) applyPartitioned(c *PartitionedConfig) {
	c.ProductionErrorHandler = o.handler
}

// WithProductionErrorHandler sets a handler invoked specifically for production/sink errors.
// When nil (default), the general ErrorHandler is used as fallback.
func WithProductionErrorHandler(h errorhandler.Handler) productionErrorHandlerOption {
	return productionErrorHandlerOption{handler: h}
}
