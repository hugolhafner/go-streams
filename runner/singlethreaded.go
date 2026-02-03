package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/committer"
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

	errorHandler errorhandler.Handler
	committer    committer.Committer
	logger       logger.Logger

	// errChan is used to signal fatal errors from goroutines to the main Run loop
	errChan chan error
}

func NewSingleThreadedRunner(
	opts ...SingleThreadedOption,
) Factory {
	config := defaultSingleThreadedConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return func(
		t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
	) (Runner, error) {
		return &SingleThreaded{
			consumer:     consumer,
			producer:     producer,
			taskManager:  task.NewManager(f, producer, config.Logger),
			topology:     t,
			committer:    config.CommitterFactory(),
			errorHandler: config.ErrorHandler,
			errChan:      make(chan error, 1),
			logger: config.Logger.
				With("component", "runner").
				With("runner", "single-threaded"),
		}, nil
	}
}

func (r *SingleThreaded) sourceTopics() []string {
	sourceNodes := r.topology.SourceNodes()
	topics := make([]string, 0, len(sourceNodes))
	for _, node := range sourceNodes {
		topics = append(topics, node.Topic())
	}

	return topics
}

func (r *SingleThreaded) shutdown() {
	r.logger.Info("Shutting down runner")
	r.committer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	if err := r.commitOffsets(ctx); err != nil {
		r.logger.Error("Failed to commit offsets during shutdown", "error", err)
	}
	r.logger.Info("Shutdown complete")
}

func (r *SingleThreaded) commitOffsets(_ context.Context) error {
	offsets := r.taskManager.GetCommitOffsets()
	if len(offsets) == 0 {
		return nil
	}

	return r.consumer.Commit(offsets)
}

func (r *SingleThreaded) doCommit(ctx context.Context) {
	r.logger.Debug("Committing offsets")
	err := r.commitOffsets(ctx)
	if err == nil {
		r.logger.Debug("Successfully committed offsets")
		return
	}

	r.logger.Error("Failed to commit offsets", "error", err)
}

func (r *SingleThreaded) doPoll(ctx context.Context) error {
	r.logger.Debug("Polling for records")

	records, err := r.consumer.Poll(ctx)
	if err != nil {
		return fmt.Errorf("failed to poll: %w", err)
	}

	r.logger.Debug("Received records", "records", len(records))

	for _, record := range records {
		// only errors that should stop the runner are returned here
		if err := r.processRecord(ctx, record); err != nil {
			return fmt.Errorf("fatal error processing record: %w", err)
		}
	}

	r.committer.RecordProcessed(len(records))
	return nil
}

func (r *SingleThreaded) sendToDLQ(
	ctx context.Context, record kafka.ConsumerRecord, ec errorhandler.ErrorContext,
) {
	key := make([]byte, len(record.Key))
	copy(key, record.Key)
	value := make([]byte, len(record.Value))
	copy(value, record.Value)

	headers := make(map[string][]byte)
	for k, v := range record.Headers {
		headers[k] = make([]byte, len(v))
		copy(headers[k], v)
	}

	headers["x-original-topic"] = []byte(record.Topic)
	headers["x-original-partition"] = []byte(fmt.Sprintf("%d", record.Partition))
	headers["x-original-offset"] = []byte(fmt.Sprintf("%d", record.Offset))

	headers["x-error-message"] = []byte(ec.Error.Error())
	headers["x-error-attempt"] = []byte(fmt.Sprintf("%d", ec.Attempt))
	headers["x-error-timestamp"] = []byte(time.Now().Format(time.RFC3339))
	if ec.NodeName != "" {
		headers["x-error-node"] = []byte(ec.NodeName)
	}

	err := r.producer.Send(ctx, "dlq", key, value, headers)

	if err == nil {
		r.logger.Debug(
			"Sent record to DLQ",
			"key", string(key),
			"original_topic", record.Topic,
			"original_partition", record.Partition,
			"original_offset", record.Offset,
		)
	} else {
		r.logger.Error(
			"Failed to send record to DLQ, dropping record",
			"error", err,
			"key", string(key),
			"original_topic", record.Topic,
			"original_partition", record.Partition,
			"original_offset", record.Offset,
		)
	}
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
			t.RecordOffset(record)
			return nil
		}

		if pErr, ok := task.AsProcessError(err); ok {
			ec = ec.WithNodeName(pErr.Node)
		}
		ec = ec.WithError(err)

		action := r.errorHandler.Handle(ctx, ec)
		switch action {
		case errorhandler.ActionFail:
			return err
		case errorhandler.ActionRetry:
			ec = ec.IncrementAttempt()
			continue
		case errorhandler.ActionSendToDLQ:
			r.sendToDLQ(ctx, record, ec)
			t.RecordOffset(record)
			return nil
		case errorhandler.ActionContinue:
			t.RecordOffset(record)
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

func (r *SingleThreaded) OnAssigned(partitions []kafka.TopicPartition) {
	if err := r.taskManager.CreateTasks(partitions); err != nil {
		r.logger.Error("Failed to create tasks on assigned partitions", "error", err)
		r.errChan <- err
	}
}

func (r *SingleThreaded) OnRevoked(partitions []kafka.TopicPartition) {
	// Close tasks first to stop processing new records
	if err := r.taskManager.CloseTasks(partitions); err != nil {
		r.logger.Error("Failed to close tasks on revoked partitions", "error", err)
		r.errChan <- err
		return
	}

	r.doCommit(context.Background())
	if err := r.taskManager.DeleteTasks(partitions); err != nil {
		r.logger.Error("Failed to delete tasks on revoked partitions", "error", err)
		r.errChan <- err
		return
	}
}

func (r *SingleThreaded) Run(ctx context.Context) error {
	topics := r.sourceTopics()
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
		case _, ok := <-r.committer.C():
			if !ok {
				r.logger.Info("Commit loop got shut down signal in Run()")
				return nil
			}

			r.doCommit(ctx)
		default:
			if err := r.doPoll(ctx); err != nil {
				r.logger.Warn("Failed during record poll", "error", err)
				return err
			}
		}
	}
}
