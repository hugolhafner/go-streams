package kafka

import (
	"context"
	"sync"
	"time"
)

type Client interface {
	Producer
	Consumer

	Ping(ctx context.Context) error
}

type Producer interface {
	Send(ctx context.Context, topic string, key, value []byte, headers map[string][]byte) error
	Flush(timeout time.Duration) error
	Close()
}

type Consumer interface {
	Subscribe(topics []string, rebalanceCb RebalanceCallback) error
	Poll(ctx context.Context, timeout time.Duration) ([]ConsumerRecord, error)
	Commit(offsets map[TopicPartition]Offset) error
	Close()
}

type RebalanceCallback interface {
	// TODO: Should these return errors? If so what should the client do with them?
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
