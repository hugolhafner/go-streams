package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"go.opentelemetry.io/otel/trace"
)

type recordWithContext struct {
	ctx    context.Context
	record kafka.ConsumerRecord
}

// partitionWorker processes records for a single partition in its own goroutine
type partitionWorker struct {
	partition    kafka.TopicPartition
	task         task.Task
	consumer     kafka.Consumer
	producer     kafka.Producer
	errorHandler errorhandler.Handler
	logger       logger.Logger
	telemetry    *streamsotel.Telemetry

	recordCh     chan recordWithContext
	doneCh       chan struct{}
	stopCh       chan struct{}
	errCh        chan error
	drainTimeout time.Duration

	mu      sync.RWMutex
	stopped bool
}

// newPartitionWorker creates a new worker for the given partition
func newPartitionWorker(
	partition kafka.TopicPartition,
	t task.Task,
	consumer kafka.Consumer,
	producer kafka.Producer,
	errorHandler errorhandler.Handler,
	bufferSize int,
	drainTimeout time.Duration,
	errCh chan error,
	l logger.Logger,
	tel *streamsotel.Telemetry,
) *partitionWorker {
	return &partitionWorker{
		partition:    partition,
		task:         t,
		consumer:     consumer,
		producer:     producer,
		errorHandler: errorHandler,
		telemetry:    tel,
		logger: l.With(
			"component", "partition-worker",
			"topic", partition.Topic,
			"partition", partition.Partition,
		),
		recordCh:     make(chan recordWithContext, bufferSize),
		doneCh:       make(chan struct{}),
		stopCh:       make(chan struct{}),
		errCh:        errCh,
		drainTimeout: drainTimeout,
	}
}

// Start begins processing records in a separate goroutine
func (w *partitionWorker) Start(ctx context.Context) {
	go w.run(ctx)
}

// run is the main processing loop for the worker
func (w *partitionWorker) run(ctx context.Context) {
	defer close(w.doneCh)

	w.logger.Debug("Partition worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Context cancelled, draining remaining records")
			drainCtx, cancel := context.WithTimeout(context.Background(), w.drainTimeout)
			w.drain(drainCtx)
			cancel()
			return

		case <-w.stopCh:
			w.logger.Debug("Stop signal received, returning without drain")
			return

		case r, ok := <-w.recordCh:
			if !ok {
				w.logger.Debug("Record channel closed")
				return
			}

			if err := w.processRecord(r.ctx, r.record); err != nil {
				w.logger.Error("Error processing record", "error", err, "offset", r.record.Offset)
				emitError(w.errCh, w.logger, fmt.Errorf("worker %v: fatal processing error: %w", w.partition, err))
				return
			}
		}
	}
}

// drain processes any remaining records in the channel before stopping
func (w *partitionWorker) drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.logger.Warn("Drain context cancelled, stopping drain")
			return
		case r, ok := <-w.recordCh:
			if !ok {
				return
			}

			recordCtx := trace.ContextWithSpan(ctx, trace.SpanFromContext(r.ctx))
			if err := w.processRecord(recordCtx, r.record); err != nil {
				w.logger.Error(
					"Error processing record during drain, exiting...", "error", err, "offset", r.record.Offset,
				)
				emitError(
					w.errCh, w.logger,
					fmt.Errorf("worker %v: fatal processing error during drain: %w", w.partition, err),
				)
				return
			}
		default:
			return
		}
	}
}

// processRecord handles a single record with error handling and marking
func (w *partitionWorker) processRecord(ctx context.Context, rec kafka.ConsumerRecord) error {
	return processRecordWithRetry(ctx, rec, w.task, w.consumer, w.producer, w.errorHandler, w.telemetry, w.logger)
}

// Submit adds a record to the worker's processing queue
func (w *partitionWorker) Submit(ctx context.Context, record kafka.ConsumerRecord) error {
	w.mu.RLock()
	if w.stopped {
		w.mu.RUnlock()
		return fmt.Errorf("worker for partition %v is stopped", w.partition)
	}
	w.mu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.stopCh:
		return fmt.Errorf("worker for partition %v is stopping", w.partition)
	case w.recordCh <- recordWithContext{ctx: ctx, record: record}:
		return nil
	}
}

// TrySubmit non blocking submit to processing queue, true if submitted, false if full or stopped
func (w *partitionWorker) TrySubmit(ctx context.Context, record kafka.ConsumerRecord) bool {
	w.mu.RLock()
	if w.stopped {
		w.mu.RUnlock()
		return false
	}
	w.mu.RUnlock()

	select {
	case <-w.stopCh:
		return false
	case w.recordCh <- recordWithContext{ctx: ctx, record: record}:
		return true
	default:
		return false
	}
}

// Stop signals the worker to stop and returns immediately
func (w *partitionWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return
	}

	w.stopped = true
	close(w.stopCh)
}

// WaitForStop waits for the worker to fully stop processing
func (w *partitionWorker) WaitForStop(timeout time.Duration) error {
	select {
	case <-w.doneCh:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for partition worker %v to stop", w.partition)
	}
}

// StopAndWait stops the worker and waits for it to finish.
func (w *partitionWorker) StopAndWait(timeout time.Duration) error {
	w.Stop()
	return w.WaitForStop(timeout)
}

// IsStopped returns whether the worker has been stopped
func (w *partitionWorker) IsStopped() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stopped
}

// Partition returns the partition this worker is responsible for
func (w *partitionWorker) Partition() kafka.TopicPartition {
	return w.partition
}

// QueueDepth returns the number of records currently queued for processing
func (w *partitionWorker) QueueDepth() int {
	return len(w.recordCh)
}
