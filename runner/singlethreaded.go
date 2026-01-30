package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/go-streams/committer"
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
	config      SingleThreadedConfig

	committer committer.Committer
	logger    logger.Logger
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
		logger logger.Logger,
	) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, logger),
			topology:    t,
			config:      config,
			committer:   config.CommitterFactory(),
			logger: logger.
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
		t, ok := r.taskManager.TaskFor(record.TopicPartition())
		if !ok {
			r.logger.Warn(
				"No task found for topic partition", "topic_partition",
				record.TopicPartition(),
			)
			// TODO: Is this safe?
			continue
		}

		// TODO: Add error custom handling support
		if err := t.Process(ctx, record); err != nil {
			r.logger.Error(
				"Failed to process record", "topic_partition",
				record.TopicPartition(), "offset", record.Offset, "error", err,
			)
		}
	}

	r.committer.RecordProcessed(len(records))
	return nil
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
				r.logger.Warn("Failed to poll for records", "error", err)
				return err
			}
		}
	}
}
