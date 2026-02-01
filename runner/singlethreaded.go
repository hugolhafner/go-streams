package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/committer"
	"github.com/hugolhafner/go-streams/errorhandler"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
	"github.com/hugolhafner/go-streams/task"
	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*SingleThreaded)(nil)

type SingleThreaded struct {
	consumer    kafka.Consumer
	producer    kafka.Producer
	taskManager task.Manager
	topology    *topology.Topology

	errorHandler errorhandler.Handler
	committer    committer.Committer
	logger       logger.Logger
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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
	b := backoff.NewExponential(
		backoff.WithMaxInterval(time.Second*3),
		backoff.WithJitter(0.2),
	)

	var attempt uint = 1
	for {
		r.logger.Debug("Committing offsets", "attempt", attempt)
		err := r.commitOffsets(ctx)
		if err == nil {
			r.logger.Debug("Successfully committed offsets")
			break
		}

		sleep := b.Next(attempt)
		r.logger.Error("Failed to commit offsets", "error", err, "attempt", attempt, "delay", sleep.String())

		select {
		case <-ctx.Done():
			r.logger.Warn("Context closed, stopping commit retries")
			return
		case <-time.After(sleep):
		}

		attempt++
	}
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

func (r *SingleThreaded) processRecord(ctx context.Context, record kafka.ConsumerRecord) error {
	t, ok := r.taskManager.TaskFor(record.TopicPartition())
	if !ok {
		r.logger.Warn(
			"No task found for topic partition", "topic_partition",
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

		ec = ec.WithError(err)

		action := r.errorHandler.Handle(ctx, ec)
		switch action {
		case errorhandler.ActionFail:
			return err
		case errorhandler.ActionRetry:
			ec = ec.IncrementAttempt()
			continue
		case errorhandler.ActionSendToDLQ:
			r.logger.Warn("TODO: Send to DLQ not implemented, continuing", "record", record)
			t.RecordOffset(record)
			return nil
		case errorhandler.ActionContinue:
			t.RecordOffset(record)
			return nil
		}
	}
}

func (r *SingleThreaded) Run(ctx context.Context) error {
	topics := r.sourceTopics()
	if err := r.consumer.Subscribe(topics, r.taskManager); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	defer r.shutdown()

	for {
		select {
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
