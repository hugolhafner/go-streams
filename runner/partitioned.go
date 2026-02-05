package runner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*PartitionedRunner)(nil)
var _ kafka.RebalanceCallback = (*PartitionedRunner)(nil)

// PartitionedRunner processes records in parallel, with one goroutine per partition
type PartitionedRunner struct {
	consumer    kafka.Consumer
	producer    kafka.Producer
	taskManager task.Manager
	topology    *topology.Topology
	config      PartitionedConfig

	workers map[kafka.TopicPartition]*partitionWorker
	mu      sync.RWMutex

	errCh chan error

	runCtx    context.Context
	runCancel context.CancelFunc

	logger logger.Logger
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
	) (Runner, error) {
		l := config.Logger.With("component", "runner", "runner", "partitioned")

		return &PartitionedRunner{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, config.Logger),
			topology:    t,
			config:      config,
			workers:     make(map[kafka.TopicPartition]*partitionWorker),
			errCh:       make(chan error, 1),
			logger:      l,
		}, nil
	}
}

// Run starts the partitioned runner and blocks until the context is cancelled
// or a fatal error occurs.
func (r *PartitionedRunner) Run(ctx context.Context) error {
	r.runCtx, r.runCancel = context.WithCancel(ctx)
	defer r.runCancel()

	topics := r.topology.SourceTopics()
	if err := r.consumer.Subscribe(topics, r); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	defer r.shutdown()

	r.logger.Info("Partitioned runner started", "topics", topics)

	var errAttempts uint = 0
	for {
		select {
		case err := <-r.errCh:
			r.logger.Error("Fatal error received", "error", err)
			return err

		case <-r.runCtx.Done():
			r.logger.Info("Context cancelled, shutting down")
			return nil

		default:
			if err := r.doPoll(); err != nil {
				r.logger.Warn("Poll error", "error", err)
				select {
				case <-r.runCtx.Done():
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

func (r *PartitionedRunner) doPoll() error {
	records, err := r.consumer.Poll(r.runCtx)
	if err != nil {
		return fmt.Errorf("failed to poll: %w", err)
	}

	if len(records) == 0 {
		return nil
	}

	r.logger.Debug("Polled records", "count", len(records))

	for _, record := range records {
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

		if err := worker.Submit(r.runCtx, record); err != nil {
			r.logger.Warn(
				"Failed to submit record to worker",
				"error", err,
				"topic", tp.Topic,
				"partition", tp.Partition,
				"offset", record.Offset,
			)
		}
	}

	return nil
}

func (r *PartitionedRunner) getWorker(tp kafka.TopicPartition) (*partitionWorker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	worker, ok := r.workers[tp]
	return worker, ok
}

func (r *PartitionedRunner) OnAssigned(partitions []kafka.TopicPartition) {
	r.logger.Info("Partitions assigned", "partitions", partitions)

	if err := r.taskManager.CreateTasks(partitions); err != nil {
		r.logger.Error("Failed to create tasks for assigned partitions", "error", err)
		emitError(r.errCh, r.logger, fmt.Errorf("failed to create tasks: %w", err))
		return
	}

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
			r.config.ErrorHandler,
			r.config.ChannelBufferSize,
			r.logger,
		)

		r.workers[tp] = worker
		worker.Start(r.runCtx)

		r.logger.Debug("Started worker for partition", "partition", tp)
	}
}

func (r *PartitionedRunner) OnRevoked(partitions []kafka.TopicPartition) {
	r.logger.Info("Partitions revoked", "partitions", partitions)

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

	r.mu.Lock()
	allWorkers := make([]*partitionWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		worker.Stop()
		allWorkers = append(allWorkers, worker)
	}
	r.mu.Unlock()

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
