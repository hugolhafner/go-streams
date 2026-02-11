package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*SingleThreaded)(nil)
var _ kafka.RebalanceCallback = (*SingleThreaded)(nil)

type SingleThreaded struct {
	consumer    kafka.Consumer
	producer    kafka.Producer
	taskManager task.Manager
	topology    *topology.Topology
	config SingleThreadedConfig

	logger logger.Logger

	// errChan is used to signal fatal errors from goroutines to the main Run loop
	errChan chan error
}

func NewSingleThreadedRunner(
	opts ...SingleThreadedOption,
) Factory {
	config := defaultSingleThreadedConfig()
	for _, opt := range opts {
		opt.applySingleThreaded(&config)
	}

	return func(
		t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
	) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, config.Logger),
			topology:    t,
			config:      config,
			errChan:     make(chan error, 1),
			logger: config.Logger.
				With("component", "runner").
				With("runner", "single-threaded"),
		}, nil
	}
}

func (r *SingleThreaded) shutdown() {
	r.logger.Info("Shutting down runner")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	if err := r.consumer.Commit(ctx); err != nil {
		r.logger.Error("Failed to commit offsets during shutdown", "error", err)
	}

	if err := r.producer.Flush(ctx); err != nil {
		r.logger.Error("Failed to flush producer during shutdown", "error", err)
	}

	r.logger.Info("Shutdown complete")
}

func (r *SingleThreaded) doPoll(ctx context.Context) error {
	r.logger.Debug("Polling for records")

	records, err := r.consumer.Poll(ctx)
	if err != nil {
		return fmt.Errorf("failed to poll: %w", err)
	}

	if len(records) == 0 {
		r.logger.Debug("No records received from poll")
		return nil
	}

	r.logger.Debug("Received records", "records", len(records))

	for _, record := range records {
		r.logger.Debug(
			"Processing record",
			"key", string(record.Key),
			"topic", record.Topic,
			"partition", record.Partition,
			"offset", record.Offset,
		)

		// only errors that should stop the runner are returned here
		if err := r.processRecord(ctx, record); err != nil {
			return fmt.Errorf("fatal error processing record: %w", err)
		}

		r.consumer.MarkRecords(record)
	}

	return nil
}

func (r *SingleThreaded) processRecord(ctx context.Context, record kafka.ConsumerRecord) error {
	t, ok := r.taskManager.TaskFor(record.TopicPartition())
	if !ok {
		r.logger.Warn(
			"No task found for topic partition, may have just been rebalanced, continuing...", "topic_partition",
			record.TopicPartition(),
		)
		return nil
	}

	ec := errorhandler.NewErrorContext(record, nil)

	for {
		err := t.Process(ctx, record)
		if err == nil {
			return nil
		}

		if pErr, ok := task.AsProcessError(err); ok {
			ec = ec.WithNodeName(pErr.Node)
		}
		ec = ec.WithError(err)

		action := r.config.ErrorHandler.Handle(ctx, ec)
		switch action.Type() {
		case errorhandler.ActionTypeFail:
			return err
		case errorhandler.ActionTypeRetry:
			ec = ec.IncrementAttempt()
			continue
		case errorhandler.ActionTypeSendToDLQ:
			a, ok := action.(errorhandler.ActionSendToDLQ)
			if !ok {
				r.logger.Error("Invalid action type, expected ActionSendToDLQ", "action", action.Type().String())
				return errors.New("invalid action type, expected ActionSendToDLQ")
			}

			sendToDLQ(ctx, r.producer, record, ec, a.Topic(), r.logger)
			return nil
		case errorhandler.ActionTypeContinue:
			return nil
		default:
			r.logger.Error(
				"Unknown error handler action, failing record",
				"error", ec.Error,
				"key", ec.Record.Key,
				"topic", ec.Record.Topic,
				"offset", ec.Record.Offset,
				"partition", ec.Record.Partition,
				"attempt", ec.Attempt,
				"node", ec.NodeName,
			)
			return err
		}
	}
}

func (r *SingleThreaded) emitErr(err error) {
	select {
	case r.errChan <- err:
	default:
		r.logger.Error("Error channel full, dropping error", "error", err)
	}
}

func (r *SingleThreaded) OnAssigned(partitions []kafka.TopicPartition) {
	r.logger.Debug("Assigned partitions", "partitions", partitions)
	r.logger.Debug("Creating tasks for partitions")
	if err := r.taskManager.CreateTasks(partitions); err != nil {
		r.logger.Error("Failed to create tasks on assigned partitions", "error", err)
		r.emitErr(err)
	}
}

func (r *SingleThreaded) OnRevoked(partitions []kafka.TopicPartition) {
	// Close tasks first to stop processing new records
	r.logger.Debug("Revoking partitions", "partitions", partitions)
	r.logger.Debug("Closing tasks for revoked partitions")
	if err := r.taskManager.CloseTasks(partitions); err != nil {
		r.logger.Error("Failed to close tasks on revoked partitions", "error", err)
		r.emitErr(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	r.logger.Debug("Forcing a commit prior to revoking partitions")
	if err := r.consumer.Commit(ctx); err != nil {
		r.logger.Error("Failed to commit offsets on revoked partitions", "error", err)
	}

	r.logger.Debug("Deleting tasks for revoked partitions")
	if err := r.taskManager.DeleteTasks(partitions); err != nil {
		r.logger.Error("Failed to delete tasks on revoked partitions", "error", err)
		r.emitErr(err)
	}
}

func (r *SingleThreaded) Run(ctx context.Context) error {
	topics := r.topology.SourceTopics()
	if err := r.consumer.Subscribe(topics, r); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	defer r.shutdown()

	for {
		select {
		case err := <-r.errChan:
			r.logger.Error("Fatal error received in Run()", "error", err)
			return err
		case <-ctx.Done():
			r.logger.Debug("Context closed, shutting down Run()")
			return nil
		default:
			if err := r.doPoll(ctx); err != nil {
				r.logger.Warn("Failed during record poll", "error", err)
				return err
			}
		}
	}
}
