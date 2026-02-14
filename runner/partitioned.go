package runner

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	streamsotel "github.com/hugolhafner/go-streams/otel"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

var _ Runner = (*PartitionedRunner)(nil)
var _ kafka.RebalanceCallback = (*PartitionedRunner)(nil)

// PartitionedRunner processes records in parallel, with one goroutine per partition
type PartitionedRunner struct {
	consumer    kafka.Consumer
	producer    kafka.Producer
	taskManager task.Manager
	topology    *topology.Topology

	errorHandler errorhandler.Handler
	config       PartitionedConfig

	workers map[kafka.TopicPartition]*partitionWorker
	mu      sync.RWMutex

	// pending holds records that couldn't be dispatched because the worker channel was full
	// processed FIFO before new records from Poll
	pending   map[kafka.TopicPartition][]kafka.ConsumerRecord
	paused    map[kafka.TopicPartition]struct{}
	pendingMu sync.Mutex

	errCh chan error

	runCtx context.Context

	logger    logger.Logger
	telemetry *streamsotel.Telemetry
}

// NewPartitionedRunner creates a factory function for PartitionedRunner
func NewPartitionedRunner(opts ...PartitionedOption) Factory {
	config := defaultPartitionedConfig()
	for _, opt := range opts {
		opt.applyPartitioned(&config)
	}

	return func(
		t *topology.Topology,
		f task.Factory,
		consumer kafka.Consumer,
		producer kafka.Producer,
		telemetry *streamsotel.Telemetry,
	) (Runner, error) {
		l := config.Logger.With("component", "runner", "runner", "partitioned")

		return &PartitionedRunner{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, config.Logger),
			topology:    t,
			errorHandler: errorhandler.NewPhaseRouter(
				config.ErrorHandler, config.SerdeErrorHandler,
				config.ProcessingErrorHandler, config.ProductionErrorHandler,
			),
			config:    config,
			workers:   make(map[kafka.TopicPartition]*partitionWorker),
			pending:   make(map[kafka.TopicPartition][]kafka.ConsumerRecord),
			paused:    make(map[kafka.TopicPartition]struct{}),
			errCh:     make(chan error, 1),
			logger:    l,
			telemetry: telemetry,
		}, nil
	}
}

// Run starts the partitioned runner and blocks until the context is cancelled
// or a fatal error occurs.
func (r *PartitionedRunner) Run(ctx context.Context) error {
	defer r.shutdown()

	var cancel context.CancelFunc
	r.runCtx, cancel = context.WithCancel(ctx)
	defer cancel()

	topics := r.topology.SourceTopics()
	if err := r.consumer.Subscribe(topics, r); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	r.logger.Info("Partitioned runner started", "topics", topics)

	var errAttempts uint = 0
	for {
		select {
		case err := <-r.errCh:
			r.logger.Error("Fatal error received in Run()", "error", err)
			return err

		case <-ctx.Done():
			r.logger.Info("Context cancelled, shutting down")
			return nil

		default:
			if err := r.doPoll(ctx); err != nil {
				r.logger.Warn("Poll error", "error", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(r.config.PollErrorBackoff.Next(errAttempts)):
				}
				errAttempts++
			} else {
				errAttempts = 0
			}
		}
	}
}

func (r *PartitionedRunner) doPoll(ctx context.Context) error {
	// process pending before new Poll
	r.dispatchPending(ctx)

	tel := r.telemetry
	pollStart := time.Now()

	var receiveSpan trace.Span
	ctx, receiveSpan = tel.Tracer.Start(
		ctx, "receive",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeReceive,
		),
	)
	records, err := r.consumer.Poll(ctx)

	if err != nil {
		receiveSpan.RecordError(err)
		receiveSpan.End()

		tel.PollDuration.Record(
			ctx, time.Since(pollStart).Seconds(), metric.WithAttributes(
				streamsotel.AttrPollStatus.String(streamsotel.StatusError),
			),
		)
		return fmt.Errorf("failed to poll: %w", err)
	}

	tel.PollDuration.Record(
		ctx, time.Since(pollStart).Seconds(), metric.WithAttributes(
			streamsotel.AttrPollStatus.String(streamsotel.StatusSuccess),
		),
	)

	receiveSpan.SetAttributes(semconv.MessagingBatchMessageCount(len(records)))
	receiveSpan.End()

	if len(records) == 0 {
		r.logger.Debug("No records received from poll")
		return nil
	}

	r.logger.Debug("Polled records", "count", len(records))

	for _, record := range records {
		tel.MessagesConsumed.Add(
			ctx, 1, metric.WithAttributes(
				semconv.MessagingDestinationName(record.Topic),
				semconv.MessagingDestinationPartitionID(strconv.FormatInt(int64(record.Partition), 10)),
			),
		)
		tp := record.TopicPartition()

		worker, ok := r.getWorker(tp)
		if !ok {
			r.logger.Warn(
				"No worker for partition, may have been rebalanced",
				"topic", tp.Topic,
				"partition", tp.Partition,
			)
			continue
		}

		// add to pending before dispatching to make sure ordering stays guaranteed
		r.pendingMu.Lock()
		if _, hasPending := r.paused[tp]; hasPending {
			r.pending[tp] = append(r.pending[tp], record)
			r.pendingMu.Unlock()
			continue
		}
		r.pendingMu.Unlock()

		if worker.TrySubmit(ctx, record) {
			continue
		}

		r.pendingMu.Lock()
		r.pending[tp] = append(r.pending[tp], record)
		r.paused[tp] = struct{}{}
		r.pendingMu.Unlock()

		r.consumer.PausePartitions(tp)
		r.logger.Debug(
			"Paused partition due to backpressure",
			"topic", tp.Topic,
			"partition", tp.Partition,
		)
	}

	return nil
}

// dispatchPending flushes buffered records for paused partitions via TrySubmit
// Partitions that are fully drained are resumed.
func (r *PartitionedRunner) dispatchPending(ctx context.Context) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	var toResume []kafka.TopicPartition

	for tp, records := range r.pending {
		worker, ok := r.getWorker(tp)
		if !ok {
			// partition removed, drop pending records
			delete(r.pending, tp)
			delete(r.paused, tp)
			continue
		}

		dispatched := 0
		for _, rec := range records {
			if !worker.TrySubmit(ctx, rec) {
				break
			}
			dispatched++
		}

		if dispatched == len(records) {
			delete(r.pending, tp)
			delete(r.paused, tp)
			toResume = append(toResume, tp)
		} else {
			r.pending[tp] = records[dispatched:]
		}
	}

	if len(toResume) > 0 {
		r.consumer.ResumePartitions(toResume...)
		for _, tp := range toResume {
			r.logger.Debug(
				"Resumed partition after backpressure drain",
				"topic", tp.Topic,
				"partition", tp.Partition,
			)
		}
	}
}

func (r *PartitionedRunner) getWorker(tp kafka.TopicPartition) (*partitionWorker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	worker, ok := r.workers[tp]
	return worker, ok
}

func (r *PartitionedRunner) OnAssigned(ctx context.Context, partitions []kafka.TopicPartition) {
	r.logger.Info("Partitions assigned", "partitions", partitions)

	if err := r.taskManager.CreateTasks(partitions); err != nil {
		r.logger.Error("Failed to create tasks for assigned partitions", "error", err)
		emitError(r.errCh, r.logger, fmt.Errorf("failed to create tasks: %w", err))
		return
	}

	r.telemetry.TasksActive.Add(
		ctx, int64(len(partitions)), metric.WithAttributes(
			streamsotel.AttrRunnerType.String(streamsotel.RunnerTypePartitioned),
		),
	)

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, tp := range partitions {
		if _, exists := r.workers[tp]; exists {
			r.logger.Warn("Worker already exists for partition", "partition", tp)
			continue
		}

		t, ok := r.taskManager.TaskFor(tp)
		if !ok {
			r.logger.Error("No task found for partition after creation", "partition", tp)
			continue
		}

		worker := newPartitionWorker(
			tp,
			t,
			r.consumer,
			r.producer,
			r.errorHandler,
			r.config.ChannelBufferSize,
			r.config.WorkerShutdownTimeout,
			r.errCh,
			r.logger,
			r.telemetry,
		)

		r.workers[tp] = worker

		// make sure partition is resumed in case it was paused when revoked but then reassigned to this runner
		// should be a no-op if the partition wasn't paused
		r.consumer.ResumePartitions(tp)

		worker.Start(r.runCtx)

		r.logger.Debug("Started worker for partition", "partition", tp)
	}
}

func (r *PartitionedRunner) OnRevoked(ctx context.Context, partitions []kafka.TopicPartition) {
	r.logger.Info("Partitions revoked", "partitions", partitions)

	r.pendingMu.Lock()
	for _, tp := range partitions {
		delete(r.pending, tp)
		delete(r.paused, tp)
	}
	r.pendingMu.Unlock()

	r.mu.Lock()
	workersToStop := make([]*partitionWorker, 0, len(partitions))
	for _, tp := range partitions {
		if worker, exists := r.workers[tp]; exists {
			worker.Stop()
			workersToStop = append(workersToStop, worker)
		}
	}
	r.mu.Unlock()

	var wg sync.WaitGroup
	for _, worker := range workersToStop {
		wg.Add(1)
		go func(w *partitionWorker) {
			defer wg.Done()
			if err := w.WaitForStop(r.config.WorkerShutdownTimeout); err != nil {
				r.logger.Warn(
					"Timeout waiting for worker to stop",
					"partition", w.Partition(),
					"error", err,
				)
			}
		}(worker)
	}
	wg.Wait()

	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.consumer.Commit(commitCtx); err != nil {
		r.logger.Error("Failed to commit offsets on revoke", "error", err)
	}

	if err := r.taskManager.CloseTasks(partitions); err != nil {
		r.logger.Error("Failed to close tasks for revoked partitions", "error", err)
	}

	if err := r.taskManager.DeleteTasks(partitions); err != nil {
		r.logger.Error("Failed to delete tasks for revoked partitions", "error", err)
	}

	r.telemetry.TasksActive.Add(
		ctx, -int64(len(partitions)), metric.WithAttributes(
			streamsotel.AttrRunnerType.String(streamsotel.RunnerTypePartitioned),
		),
	)

	r.mu.Lock()
	for _, tp := range partitions {
		delete(r.workers, tp)
	}
	r.mu.Unlock()

	r.logger.Debug("Completed handling partition revocation")
}

// shutdown gracefully stops all workers and commits final offsets
func (r *PartitionedRunner) shutdown() {
	r.logger.Info("Shutting down partitioned runner")

	r.pendingMu.Lock()
	r.pending = make(map[kafka.TopicPartition][]kafka.ConsumerRecord)
	r.paused = make(map[kafka.TopicPartition]struct{})
	r.pendingMu.Unlock()

	r.mu.RLock()
	allWorkers := make([]*partitionWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		allWorkers = append(allWorkers, worker)
	}
	r.mu.RUnlock()

	// wait for workers to drain and exit
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, worker := range allWorkers {
			wg.Add(1)
			go func(w *partitionWorker) {
				defer wg.Done()
				_ = w.WaitForStop(r.config.WorkerShutdownTimeout)
			}(worker)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		r.logger.Debug("All workers stopped")
	case <-time.After(r.config.DrainTimeout):
		r.logger.Warn("Timeout waiting for workers to stop during shutdown")
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := r.consumer.Commit(commitCtx); err != nil {
		r.logger.Error("Failed to commit offsets during shutdown", "error", err)
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer flushCancel()

	if err := r.producer.Flush(flushCtx); err != nil {
		r.logger.Error("Failed to flush producer during shutdown", "error", err)
	}

	if err := r.taskManager.Close(); err != nil {
		r.logger.Error("Failed to close task manager", "error", err)
	}

	r.logger.Info("Partitioned runner shutdown complete")
}

// WorkerCount returns the number of active partition workers
func (r *PartitionedRunner) WorkerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workers)
}

// WorkerQueueDepths returns the queue depth for each partition worker
func (r *PartitionedRunner) WorkerQueueDepths() map[kafka.TopicPartition]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	depths := make(map[kafka.TopicPartition]int, len(r.workers))
	for tp, worker := range r.workers {
		depths[tp] = worker.QueueDepth()
	}
	return depths
}

// PendingCounts returns the number of pending records per partition
func (r *PartitionedRunner) PendingCounts() map[kafka.TopicPartition]int {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	counts := make(map[kafka.TopicPartition]int, len(r.pending))
	for tp, records := range r.pending {
		counts[tp] = len(records)
	}
	return counts
}

// PausedPartitions returns the set of partitions currently paused due to backpressure
func (r *PartitionedRunner) PausedPartitions() []kafka.TopicPartition {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	result := make([]kafka.TopicPartition, 0, len(r.paused))
	for tp := range r.paused {
		result = append(result, tp)
	}
	return result
}
