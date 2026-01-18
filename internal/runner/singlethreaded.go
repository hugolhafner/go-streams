package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/internal/committer"
	"github.com/hugolhafner/go-streams/internal/kafka"
	"github.com/hugolhafner/go-streams/internal/task"
	"github.com/hugolhafner/go-streams/logger"
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

	return func(t *topology.Topology, f task.Factory, consumer kafka.Consumer, producer kafka.Producer,
		logger logger.Logger) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer, logger),
			topology:    t,
			committer:   config.CommitterFactory(),
			logger:      logger,
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
		r.logger.Log(logger.ErrorLevel, "Failed to commit offsets during shutdown", "error", err)
	}
}

func (r *SingleThreaded) commitOffsets() error {
	offsets := r.taskManager.GetCommitOffsets()
	if len(offsets) == 0 {
		return nil
	}

	return r.consumer.Commit(offsets)
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

		// TODO: Config timeout
		// TODO: Poll retry?
		records, err := r.consumer.Poll(ctx, time.Millisecond*100)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			return fmt.Errorf("failed to poll: %w", err)
		}

		for _, record := range records {
			t, ok := r.taskManager.TaskFor(record.TopicPartition())
			if !ok {
				r.logger.Log(logger.WarnLevel, "No task found for topic partition", "topic_partition",
					record.TopicPartition())
				// TODO: Is this safe?
				continue
			}

			if err := t.Process(record); err != nil {
				// TODO: Error handling strategy
				return fmt.Errorf("failed to process record: %w", err)
			}
		}

		r.committer.RecordProcessed(len(records))
		if locked := r.committer.TryCommit(); locked {
			r.logger.Log(logger.InfoLevel, "Committing offsets")
			if err := r.commitOffsets(); err != nil {
				r.committer.UnlockCommit(false)
				return fmt.Errorf("failed to commit offsets: %w", err)
			}

			r.committer.UnlockCommit(true)
		}
	}
}
