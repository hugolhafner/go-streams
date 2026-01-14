package log

import (
	"context"
	"sync"
	"time"
)

type Consumer interface {
	Subscribe(topics []string, rebalanceCb RebalanceCallback) error
	Poll(ctx context.Context, timeout time.Duration) ([]ConsumerRecord, error)
	Commit(offsets map[TopicPartition]int64) error
	Close() error
}

type RebalanceCallback interface {
	OnAssigned(partitions []TopicPartition) error
	OnRevoked(partitions []TopicPartition) error
}

type CommitStrategy interface {
	RecordProcessed(tp TopicPartition, offset int64)
	TriggerCommit() map[TopicPartition]int64
}

type PeriodicCommitStrategy struct {
	interval   time.Duration
	lastCommit time.Time
	offsets    map[TopicPartition]int64
	mu         sync.Mutex
}
