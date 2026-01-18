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

	pollBackoff backoff.Backoff

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

	return func(t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
		logger logger.Logger) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, logger),
			topology:    t,
			pollBackoff: backoff.NewExponential(
				backoff.WithInitialInterval(time.Millisecond*100),
				backoff.WithMaxInterval(time.Second*3),
				backoff.WithMultiplier(2.0),
				backoff.WithJitter(0.2),
			),
			config:    config,
			committer: config.CommitterFactory(),
			logger:    logger,
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
	if err := r.commitOffsets(); err != nil {
		r.logger.Error("Failed to commit offsets during shutdown", "error", err)
	}
}

func (r *SingleThreaded) commitOffsets() error {
	offsets := r.taskManager.GetCommitOffsets()
	if len(offsets) == 0 {
		return nil
	}

	return r.consumer.Commit(offsets)
}

func (r *SingleThreaded) poll(ctx context.Context) ([]kafka.ConsumerRecord, error) {
	r.logger.Debug("Polling for records")

	var attempt uint = 1
	for {
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			wait := r.pollBackoff.Next(attempt)
			attempt++

			r.logger.Error("Failed to poll records", "error", err, "attempt", attempt, "backoff",
				wait.String())

			time.Sleep(wait)
			continue
		}

		r.logger.Debug("Received records", "count", len(records))
		return records, nil
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
			return nil
		default:
		}

		records, err := r.poll(ctx)
		if err != nil {
			return fmt.Errorf("failed to poll: %w", err)
		}

		for _, record := range records {
			t, ok := r.taskManager.TaskFor(record.TopicPartition())
			if !ok {
				r.logger.Warn(
				"No task found for topic partition", "topic_partition",
					record.TopicPartition())
				// TODO: Is this safe?
				continue
			}

			if err := t.Process(record); err != nil {
				// TODO: Error handling strategy
				r.logger.Error("Failed to process record", "error", err)
			}
		}

		r.committer.RecordProcessed(len(records))
		if locked := r.committer.TryCommit(); locked {
			r.logger.Info("Committing offsets")
			if err := r.commitOffsets(); err != nil {
				r.committer.UnlockCommit(false)
				return fmt.Errorf("failed to commit offsets: %w", err)
			}

			r.committer.UnlockCommit(true)
		}
	}
}
