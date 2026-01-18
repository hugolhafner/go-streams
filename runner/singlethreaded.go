package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/dskit/backoff"
	"github.com/hugolhafner/dskit/retry"

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
	r.committer.Close()
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

func (r *SingleThreaded) runCommitLoop() {
	r.logger.Info("Starting commit loop")

	rp := retry.MustNewPolicy(
		"commit",
		retry.WithMaxAttempts(10),
		retry.WithBackoff(
			backoff.NewExponential(
				backoff.WithMaxInterval(time.Second*3),
				backoff.WithJitter(0.2),
			),
		),
	)

	for {
		_, ok := <-r.committer.C()
		if !ok {
			r.logger.Info("Commit loop shutting down")
			return
		}

		r.logger.Debug("Committing offsets")

		if err := retry.Do(
			context.Background(), rp, func(ctx context.Context) error {
				return r.commitOffsets()
			},
		); err != nil {
			r.logger.Error("Failed to commit offsets", "error", err)
		}
	}
}

func (r *SingleThreaded) Run(ctx context.Context) error {
	topics := r.sourceTopics()
	if err := r.consumer.Subscribe(topics, r.taskManager); err != nil {
		return fmt.Errorf("failed to subscribe to topics: %w", err)
	}

	go r.runCommitLoop()
	defer r.shutdown()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

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

			// TODO: Does this need to return an error?
			// nolint:errcheck
			t.Process(record)
		}

		r.committer.RecordProcessed(len(records))
	}
}
