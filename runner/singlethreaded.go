package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/hugolhafner/go-streams/runner/committer"
	"github.com/hugolhafner/go-streams/runner/log"
	"github.com/hugolhafner/go-streams/runner/task"
	"github.com/hugolhafner/go-streams/topology"
)

var _ Runner = (*SingleThreaded)(nil)

type SingleThreaded struct {
	consumer    log.Consumer
	producer    log.Producer
	taskManager task.Manager
	topology    *topology.Topology
	config      SingleThreadedConfig

	committer committer.Committer
}

func NewSingleThreadedRunner(
	opts ...SingleThreadedOption,
) Factory {
	config := SingleThreadedConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	return func(t *topology.Topology, f task.Factory, consumer log.Consumer, producer log.Producer) (Runner, error) {
		return &SingleThreaded{
			consumer:    consumer,
			producer:    producer,
			taskManager: task.NewManager(f, producer),
			topology:    t,
			config:      config,
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

func (r *SingleThreaded) shutdown() error {
	return nil
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
				return r.shutdown()
			}

			return fmt.Errorf("failed to poll: %w", err)
		}

		for _, record := range records {
			t, ok := r.taskManager.TaskFor(record.TopicPartition())
			if !ok {
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
			if err := r.commitOffsets(); err != nil {
				r.committer.UnlockCommit(false)
				return fmt.Errorf("failed to commit offsets: %w", err)
			}

			r.committer.UnlockCommit(true)
		}
	}
}
